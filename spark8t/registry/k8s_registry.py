# mypy: ignore-errors

from typing import Any, Dict, List, Optional

from spark8t.backend import AbstractKubeInterface
from spark8t.domain import KubernetesResourceType, ServiceAccount
from spark8t.exceptions import AccountNotFound, K8sResourceNotFound
from spark8t.literals import MANAGED_BY_LABELNAME, PRIMARY_LABELNAME, SPARK8S_LABEL
from spark8t.utils import (
    PercentEncodingSerializer,
    PropertyFile,
    umask_named_temporary_file,
)

from spark8t.registry.interface import AbstractServiceAccountRegistry


class K8sServiceAccountRegistry(AbstractServiceAccountRegistry):
    """Class implementing a ServiceAccountRegistry, based on K8s."""

    _kubernetes_key_serializer = PercentEncodingSerializer("_")

    def __init__(self, kube_interface: AbstractKubeInterface):
        self.kube_interface = kube_interface

    def all(self, namespace: Optional[str] = None) -> List["ServiceAccount"]:
        """Return all existing service accounts."""
        service_accounts = self.kube_interface.get_service_accounts(
            namespace=namespace, labels=[f"{MANAGED_BY_LABELNAME}={SPARK8S_LABEL}"]
        )
        return [
            self._build_service_account_from_raw(raw["metadata"])
            for raw in service_accounts
        ]

    @staticmethod
    def _get_secret_name(name):
        return f"{SPARK8S_LABEL}-sa-conf-{name}"

    def _retrieve_account_configurations(
        self, name: str, namespace: str
    ) -> PropertyFile:
        secret_name = self._get_secret_name(name)

        try:
            secret = self.kube_interface.get_secret(secret_name, namespace=namespace)[
                "data"
            ]
        except Exception:
            return PropertyFile.empty()

        return PropertyFile(
            {
                self._kubernetes_key_serializer.deserialize(key): value
                for key, value in secret.items()
            }
        )

    def _build_service_account_from_raw(self, metadata: Dict[str, Any]):
        name = metadata["name"]
        namespace = metadata["namespace"]
        primary = PRIMARY_LABELNAME in metadata["labels"]

        return ServiceAccount(
            name=name,
            namespace=namespace,
            primary=primary,
            api_server=self.kube_interface.api_server,
            extra_confs=self._retrieve_account_configurations(name, namespace),
        )

    def set_primary(
        self, account_id: str, namespace: Optional[str] = None
    ) -> Optional[str]:
        """Set the primary account to the one related to the provided account id.

        Args:
            account_id: account id to be elected as new primary account
        """

        # Relabeling primary
        primary_account = self.get_primary(namespace)

        if primary_account is not None:
            self.kube_interface.remove_label(
                KubernetesResourceType.SERVICEACCOUNT,
                primary_account.name,
                f"{PRIMARY_LABELNAME}",
                primary_account.namespace,
            )
            self.kube_interface.remove_label(
                KubernetesResourceType.ROLEBINDING,
                f"{primary_account.name}-role-binding",
                f"{PRIMARY_LABELNAME}",
                primary_account.namespace,
            )

        service_account = self.get(account_id)

        if service_account is None:
            raise AccountNotFound(account_id)

        self.kube_interface.set_label(
            KubernetesResourceType.SERVICEACCOUNT,
            service_account.name,
            f"{PRIMARY_LABELNAME}=True",
            service_account.namespace,
        )
        self.kube_interface.set_label(
            KubernetesResourceType.ROLEBINDING,
            f"{service_account.name}-role-binding",
            f"{PRIMARY_LABELNAME}=True",
            service_account.namespace,
        )

        return account_id

    def create(self, service_account: ServiceAccount) -> str:
        """Create a new service account and return ids associated id.

        Args:
            service_account: ServiceAccount to be stored in the registry
        """
        username = service_account.name
        serviceaccount = service_account.id

        rolename = username + "-role"
        rolebindingname = username + "-role-binding"

        self.kube_interface.create(
            KubernetesResourceType.SERVICEACCOUNT,
            username,
            namespace=service_account.namespace,
            **{"username": username},
        )
        self.kube_interface.create(
            KubernetesResourceType.ROLE,
            rolename,
            namespace=service_account.namespace,
            **{
                "resource": ["pods", "configmaps", "services"],
                "verb": ["create", "get", "list", "watch", "delete"],
            },
        )
        self.kube_interface.create(
            KubernetesResourceType.ROLEBINDING,
            rolebindingname,
            namespace=service_account.namespace,
            role=rolename,
            serviceaccount=serviceaccount,
            username=username,
        )

        self.kube_interface.set_label(
            KubernetesResourceType.SERVICEACCOUNT,
            service_account.name,
            f"{MANAGED_BY_LABELNAME}={SPARK8S_LABEL}",
            namespace=service_account.namespace,
        )
        self.kube_interface.set_label(
            KubernetesResourceType.ROLE,
            rolename,
            f"{MANAGED_BY_LABELNAME}={SPARK8S_LABEL}",
            namespace=service_account.namespace,
        )
        self.kube_interface.set_label(
            KubernetesResourceType.ROLEBINDING,
            rolebindingname,
            f"{MANAGED_BY_LABELNAME}={SPARK8S_LABEL}",
            namespace=service_account.namespace,
        )

        if service_account.primary is True:
            self.set_primary(serviceaccount, service_account.namespace)

        if len(service_account.extra_confs) > 0:
            self.set_configurations(serviceaccount, service_account.extra_confs)

        return serviceaccount

    def _create_account_configuration(self, service_account: ServiceAccount):
        secret_name = self._get_secret_name(service_account.name)

        try:
            self.kube_interface.delete(
                KubernetesResourceType.SECRET,
                secret_name,
                namespace=service_account.namespace,
            )
        except Exception:
            pass

        with umask_named_temporary_file(
            mode="w", prefix="spark-dynamic-conf-k8s-", suffix=".conf"
        ) as t:
            self.logger.debug(
                f"Spark dynamic props available for reference at {t.name}\n"
            )

            PropertyFile(
                {
                    self._kubernetes_key_serializer.serialize(key): value
                    for key, value in service_account.extra_confs.props.items()
                }
            ).write(t.file)

            t.flush()

            self.kube_interface.create(
                KubernetesResourceType.SECRET_GENERIC,
                secret_name,
                namespace=service_account.namespace,
                **{"from-env-file": str(t.name)},
            )

    def set_configurations(self, account_id: str, configurations: PropertyFile) -> str:
        """Set a new service account configuration for the provided service account id.

        Args:
            account_id: account id for which configuration ought to be set
            configurations: PropertyFile representing the new configuration to be stored
        """

        namespace, name = account_id.split(":")

        self._create_account_configuration(
            ServiceAccount(
                name=name,
                namespace=namespace,
                api_server=self.kube_interface.api_server,
                extra_confs=configurations,
            )
        )

        return account_id

    def delete(self, account_id: str) -> str:
        """Delete the service account associated with the provided id.

        Args:
            account_id: service account id to be deleted
        """

        namespace, name = account_id.split(":")

        rolename = name + "-role"
        rolebindingname = name + "-role-binding"

        try:
            self.kube_interface.delete(
                KubernetesResourceType.SERVICEACCOUNT, name, namespace=namespace
            )
        except Exception as e:
            self.logger.debug(e)

        try:
            self.kube_interface.delete(
                KubernetesResourceType.ROLE, rolename, namespace=namespace
            )
        except Exception as e:
            self.logger.debug(e)

        try:
            self.kube_interface.delete(
                KubernetesResourceType.ROLEBINDING, rolebindingname, namespace=namespace
            )
        except Exception as e:
            self.logger.debug(e)

        try:
            self.kube_interface.delete(
                KubernetesResourceType.SECRET,
                self._get_secret_name(name),
                namespace=namespace,
            )
        except Exception as e:
            self.logger.debug(e)

        return account_id

    def get(self, account_id: str) -> Optional[ServiceAccount]:
        namespace, username = account_id.split(":")
        try:
            service_account_raw = self.kube_interface.get_service_account(
                username, namespace
            )
        except K8sResourceNotFound:
            return None
        return self._build_service_account_from_raw(service_account_raw["metadata"])
