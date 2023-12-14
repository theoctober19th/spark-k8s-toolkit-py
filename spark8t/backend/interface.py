# mypy: ignore-errors

from abc import ABCMeta, abstractmethod
from functools import cached_property
from typing import Any, Dict, List, Optional, Union

import yaml

from spark8t.domain import (
    KubernetesResourceType
)
from spark8t.exceptions import AccountNotFound
from spark8t.utils import (
    WithLogging,
)


class AbstractKubeInterface(WithLogging, metaclass=ABCMeta):
    """Abstract class for implementing Kubernetes Interface."""

    @abstractmethod
    def with_context(self, context_name: str):
        """Return a new KubeInterface object using a different context.

        Args:
            context_name: context to be used
        """
        pass

    @property
    @abstractmethod
    def kube_config_file(self) -> Union[str, Dict[str, Any]]:
        pass

    @cached_property
    def kube_config(self) -> Dict[str, Any]:
        """Return the kube config file parsed as a dictionary"""
        if isinstance(self.kube_config_file, str):
            with open(self.kube_config_file, "r") as fid:
                return yaml.safe_load(fid)
        else:
            return self.kube_config_file

    @cached_property
    def available_contexts(self) -> List[str]:
        """Return the available contexts present in the kube config file."""
        return [context["name"] for context in self.kube_config["contexts"]]

    @property
    @abstractmethod
    def context_name(self) -> str:
        """Return current context name."""
        pass

    @cached_property
    def context(self) -> Dict[str, str]:
        """Return current context."""
        return [
            context["context"]
            for context in self.kube_config["contexts"]
            if context["name"] == self.context_name
        ][0]

    @cached_property
    def cluster(self) -> Dict:
        """Return current cluster."""
        return [
            cluster["cluster"]
            for cluster in self.kube_config["clusters"]
            if cluster["name"] == self.context["cluster"]
        ][0]

    @cached_property
    def api_server(self):
        """Return current K8s api-server endpoint."""
        return self.cluster["server"]

    @cached_property
    def namespace(self):
        """Return current namespace."""
        return self.context.get("namespace", "default")

    @cached_property
    def user(self):
        """Return current admin user."""
        return self.context.get("user", "default")

    @abstractmethod
    def get_service_account(
        self, account_id: str, namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_service_accounts(
        self, namespace: Optional[str] = None, labels: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Return a list of service accounts, represented as dictionary.

        Args:
            namespace: namespace where to list the service accounts. Default is to None, which will return all service
                       account in all namespaces
            labels: filter to be applied to retrieve service account which match certain labels.
        """
        pass

    @abstractmethod
    def get_secret(
        self, secret_name: str, namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """Return the data contained in the specified secret.

        Args:
            secret_name: name of the secret
            namespace: namespace where the secret is contained
        """
        pass

    @abstractmethod
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

        pass

    @abstractmethod
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
            label: label to be removed
            namespace: namespace where the resource is
        """

        pass

    @abstractmethod
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

        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def exists(
        self,
        resource_type: KubernetesResourceType,
        resource_name: str,
        namespace: Optional[str] = None,
    ) -> bool:
        """Check if a K8s resource exists.

        Args:
            resource_type: type of the resource to be deleted, e.g. service account, rolebindings, etc.
            resource_name: name of the resource to be deleted
            namespace: namespace where the resource is
        """
        pass

    def select_by_master(self, master: str):
        api_servers_clusters = {
            cluster["name"]: cluster["cluster"]["server"]
            for cluster in self.kube_config["clusters"]
        }

        self.logger.debug(f"Clusters API: {dict(api_servers_clusters)}")

        contexts_for_api_server = [
            _context["name"]
            for _context in self.kube_config["contexts"]
            if api_servers_clusters[_context["context"]["cluster"]] == master
        ]

        if len(contexts_for_api_server) == 0:
            raise AccountNotFound(master)

        self.logger.info(
            f"Contexts on api server {master}: {', '.join(contexts_for_api_server)}"
        )

        return (
            self
            if self.context_name in contexts_for_api_server
            else self.with_context(contexts_for_api_server[0])
        )
