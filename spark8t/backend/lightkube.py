# mypy: ignore-errors

import base64
import io
from types import MappingProxyType
from typing import Any, Dict, List, Optional, Type, Union

import yaml
from lightkube import Client, KubeConfig, codecs
from lightkube.core.exceptions import ApiError
from lightkube.core.resource import GlobalResource
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Namespace, Secret
from lightkube.resources.core_v1 import ServiceAccount as LightKubeServiceAccount
from lightkube.resources.rbac_authorization_v1 import Role, RoleBinding
from lightkube.types import PatchType

from spark8t.domain import (
    Defaults,
    KubernetesResourceType,
)
from spark8t.exceptions import K8sResourceNotFound
from spark8t.utils import (
    filter_none,
    PropertyFile
)
from .interface import AbstractKubeInterface


class LightKube(AbstractKubeInterface):
    _obj_mapping: dict[KubernetesResourceType, Type[GlobalResource]] = MappingProxyType(
        {
            KubernetesResourceType.ROLE: Role,
            KubernetesResourceType.SERVICEACCOUNT: LightKubeServiceAccount,
            KubernetesResourceType.SECRET: Secret,
            KubernetesResourceType.ROLEBINDING: RoleBinding,
            KubernetesResourceType.SECRET_GENERIC: Secret,
            KubernetesResourceType.NAMESPACE: Namespace,
        }
    )

    def __init__(
        self,
        kube_config_file: Union[str, Dict[str, Any]],
        defaults: Defaults,
        context_name: Optional[str] = None,
    ):
        """Initialise a KubeInterface class from a kube config file.

        Args:
            kube_config_file: kube config path
            context_name: name of the context to be used
        """
        self._kube_config_file = kube_config_file
        self._context_name = context_name
        self.config = KubeConfig.from_file(self.kube_config_file)

        self.defaults = defaults

        if context_name:
            self.client = Client(config=self.config.get(context_name=context_name))
        else:
            self.client = Client(config=self.config.get())

    @property
    def kube_config_file(self) -> Union[str, Dict[str, Any]]:
        """Return the kube config file name"""
        return self._kube_config_file

    def with_context(self, context_name: str):
        """Return a new KubeInterface object using a different context.

        Args:
            context_name: context to be used
        """
        return LightKube(self.kube_config_file, self.defaults, context_name)

    @property
    def context_name(self) -> str:
        """Return current context name."""
        return (
            self.kube_config["current-context"]
            if self._context_name is None
            else self._context_name
        )

    def get_service_account(
        self, account_id: str, namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """Return the  named service account entry.

        Args:
            namespace: namespace where to look for the service account. Default is 'default'
        """

        try:
            service_account = self.client.get(
                res=LightKubeServiceAccount,
                name=account_id,
                namespace=namespace,
            )
        except ApiError as e:
            if e.status.code == 404:
                raise K8sResourceNotFound(
                    account_id, KubernetesResourceType.SERVICEACCOUNT
                )
            raise e
        except Exception as e:
            raise e

        with io.StringIO() as buffer:
            codecs.dump_all_yaml([service_account], buffer)
            buffer.seek(0)
            return yaml.safe_load(buffer)

    def get_service_accounts(
        self, namespace: Optional[str] = None, labels: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Return a list of service accounts, represented as dictionary.

        Args:
            namespace: namespace where to list the service accounts. Default is to None, which will return all service
                       account in all namespaces
            labels: filter to be applied to retrieve service account which match certain labels.
        """
        labels_to_pass = dict()
        if labels:
            for entry in labels:
                if not PropertyFile.is_line_parsable(entry):
                    continue
                k, v = PropertyFile.parse_property_line(entry)
                labels_to_pass[k] = v

        if not namespace:
            namespace = "default"

        with io.StringIO() as buffer:
            codecs.dump_all_yaml(
                self.client.list(
                    res=LightKubeServiceAccount,
                    namespace=namespace,
                    labels=labels_to_pass,
                ),
                buffer,
            )
            buffer.seek(0)
            return list(yaml.safe_load_all(buffer))

    def get_secret(
        self, secret_name: str, namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """Return the data contained in the specified secret.

        Args:
            secret_name: name of the secret
            namespace: namespace where the secret is contained
        """

        with io.StringIO() as buffer:
            codecs.dump_all_yaml(
                [self.client.get(res=Secret, namespace=namespace, name=secret_name)],
                buffer,
            )
            buffer.seek(0)
            secret = yaml.safe_load(buffer)

            result = dict()
            for k, v in secret["data"].items():
                result[k] = base64.b64decode(v).decode("utf-8")

            secret["data"] = result
            return secret

    def set_label(
        self,
        resource_type: KubernetesResourceType,
        resource_name: str,
        label: str,
        namespace: Optional[str] = None,
    ):
        """Set label to a specified resource (type and name).

        Args:
            resource_type: type of the resource to be labeled, e.g. service account, rolebindings, etc.
            resource_name: name of the resource to be labeled
            namespace: namespace where the resource is
        """

        label_fragments = label.split("=")
        patch = {"metadata": {"labels": {label_fragments[0]: label_fragments[1]}}}

        if resource_type == KubernetesResourceType.SERVICEACCOUNT:
            self.client.patch(
                res=LightKubeServiceAccount,
                name=resource_name,
                namespace=namespace,
                obj=patch,
            )
        elif resource_type == KubernetesResourceType.ROLE:
            self.client.patch(
                res=Role, name=resource_name, namespace=namespace, obj=patch
            )
        elif resource_type == KubernetesResourceType.ROLEBINDING:
            self.client.patch(
                res=RoleBinding, name=resource_name, namespace=namespace, obj=patch
            )
        else:
            raise NotImplementedError(
                f"Label setting for resource name {resource_type} not supported yet."
            )

    def remove_label(
        self,
        resource_type: KubernetesResourceType,
        resource_name: str,
        label: str,
        namespace: Optional[str] = None,
    ):
        """Remove label to a specified resource (type and name).

        Args:
            resource_type: type of the resource to be labeled, e.g. service account, rolebindings, etc.
            resource_name: name of the resource to be labeled
            label: label to remove
            namespace: namespace where the resource is
        """
        label_to_remove = f"/metadata/labels/{label.replace('/', '~1')}"
        self.logger.debug(f"Removing label {label_to_remove}")
        patch = [{"op": "remove", "path": label_to_remove}]

        if resource_type == KubernetesResourceType.SERVICEACCOUNT:
            self.client.patch(
                res=LightKubeServiceAccount,
                name=resource_name,
                namespace=namespace,
                obj=patch,
                patch_type=PatchType.JSON,
            )
        elif resource_type == KubernetesResourceType.ROLE:
            self.client.patch(
                res=Role,
                name=resource_name,
                namespace=namespace,
                obj=patch,
                patch_type=PatchType.JSON,
            )
        elif resource_type == KubernetesResourceType.ROLEBINDING:
            self.client.patch(
                res=RoleBinding,
                name=resource_name,
                namespace=namespace,
                obj=patch,
                patch_type=PatchType.JSON,
            )
        else:
            raise NotImplementedError(
                f"Label setting for resource name {resource_type} not supported yet."
            )

    def create_property_file_entries(self, property_file_name) -> Dict[str, str]:
        entries = dict()
        props = PropertyFile.read(property_file_name).props
        for k in props:
            entries[k] = base64.b64encode(str(props[k]).encode("ascii"))
        return props

    def create(
        self,
        resource_type: KubernetesResourceType,
        resource_name: str,
        namespace: Optional[str] = None,
        **extra_args,
    ):
        """Create a K8s resource.

        Args:
            resource_type: type of the resource to be created, e.g. service account, rolebindings, etc.
            resource_name: name of the resource to be created
            namespace: namespace where the resource is
            extra_args: extra parameters that should be provided when creating the resource. Note that each parameter
                        will be prepended with the -- in the cmd, e.g. {"role": "view"} will translate as
                        --role=view in the command. List of parameter values against a parameter key are also accepted.
                        e.g. {"resource" : ["pods", "configmaps"]} which would translate to something like
                        --resource=pods --resource=configmaps
        """

        res = None
        if resource_type == KubernetesResourceType.SERVICEACCOUNT:
            with open(self.defaults.template_serviceaccount) as f:
                res = codecs.load_all_yaml(
                    f,
                    context=filter_none(
                        {
                            "resourcename": resource_name,
                            "namespace": namespace,
                        }
                        | extra_args
                    ),
                ).__getitem__(0)
        elif resource_type == KubernetesResourceType.ROLE:
            with open(self.defaults.template_role) as f:
                res = codecs.load_all_yaml(
                    f,
                    context=filter_none(
                        {
                            "resourcename": resource_name,
                            "namespace": namespace,
                        }
                        | extra_args
                    ),
                ).__getitem__(0)
        elif resource_type == KubernetesResourceType.ROLEBINDING:
            with open(self.defaults.template_rolebinding) as f:
                res = codecs.load_all_yaml(
                    f,
                    context=filter_none(
                        {
                            "resourcename": resource_name,
                            "namespace": namespace,
                        }
                        | extra_args
                    ),
                ).__getitem__(0)
        elif (
            resource_type == KubernetesResourceType.SECRET
            or resource_type == KubernetesResourceType.SECRET_GENERIC
        ):
            res = Secret.from_dict(
                filter_none(
                    {
                        "apiVersion": "v1",
                        "kind": "Secret",
                        "metadata": {"name": resource_name, "namespace": namespace},
                        "stringData": self.create_property_file_entries(
                            extra_args["from-env-file"]
                        ),
                    }
                )
            )
        elif resource_type == KubernetesResourceType.NAMESPACE:
            self.client.create(Namespace(metadata=ObjectMeta(name=resource_name)))
            return
        else:
            raise NotImplementedError(
                f"Label setting for resource name {resource_type} not supported yet."
            )

        self.client.create(obj=res, name=resource_name, namespace=namespace)

    def delete(
        self,
        resource_type: KubernetesResourceType,
        resource_name: str,
        namespace: Optional[str] = None,
    ):
        """Delete a K8s resource.

        Args:
            resource_type: type of the resource to be deleted, e.g. service account, rolebindings, etc.
            resource_name: name of the resource to be deleted
            namespace: namespace where the resource is
        """
        if resource_type == KubernetesResourceType.SERVICEACCOUNT:
            self.client.delete(
                res=LightKubeServiceAccount, name=resource_name, namespace=namespace
            )
        elif resource_type == KubernetesResourceType.ROLE:
            self.client.delete(res=Role, name=resource_name, namespace=namespace)
        elif resource_type == KubernetesResourceType.ROLEBINDING:
            self.client.delete(res=RoleBinding, name=resource_name, namespace=namespace)
        elif (
            resource_type == KubernetesResourceType.SECRET
            or resource_type == KubernetesResourceType.SECRET_GENERIC
        ):
            self.client.delete(res=Secret, name=resource_name, namespace=namespace)
        elif resource_type == KubernetesResourceType.NAMESPACE:
            self.client.delete(res=Namespace, name=resource_name)
        else:
            raise NotImplementedError(
                f"Label setting for resource name {resource_type} not supported yet."
            )

    def exists(
        self,
        resource_type: KubernetesResourceType,
        resource_name: str,
        namespace: Optional[str] = None,
    ) -> bool:
        try:
            if namespace is None:
                obj = self.client.get(self._obj_mapping[resource_type], resource_name)
            else:
                if resource_type == KubernetesResourceType.NAMESPACE:
                    raise ValueError(
                        "Cannot pass namespace with resource_type Namespace"
                    )
                obj = self.client.get(
                    self._obj_mapping[resource_type], resource_name, namespace=namespace
                )
            return obj is not None

        except ApiError as e:
            if "not found" in e.status.message:
                return False
            raise e

