# mypy: ignore-errors

import base64
import subprocess
from typing import Any, Dict, List, Optional, Union

from spark8t.domain import (
    KubernetesResourceType,
)
from spark8t.exceptions import AccountNotFound, K8sResourceNotFound
from spark8t.literals import MANAGED_BY_LABELNAME, PRIMARY_LABELNAME, SPARK8S_LABEL
from spark8t.utils import (
    execute_command_output,
    listify,
    parse_yaml_shell_output,
)
from .interface import AbstractKubeInterface


class KubeInterface(AbstractKubeInterface):
    """Class for providing an interface for k8s API needed for the spark client."""

    def __init__(
        self,
        kube_config_file: Union[str, Dict[str, Any]],
        context_name: Optional[str] = None,
        kubectl_cmd: str = "kubectl",
    ):
        """Initialise a KubeInterface class from a kube config file.

        Args:
            kube_config_file: kube config path
            context_name: name of the context to be used
            kubectl_cmd: path to the kubectl command to be used to interact with the K8s API
        """
        self._kube_config_file = kube_config_file
        self._context_name = context_name
        self.kubectl_cmd = kubectl_cmd

    @property
    def kube_config_file(self) -> Union[str, Dict[str, Any]]:
        """Return the kube config file name"""
        return self._kube_config_file

    @property
    def context_name(self) -> str:
        """Return current context name."""
        return (
            self.kube_config["current-context"]
            if self._context_name is None
            else self._context_name
        )

    def with_context(self, context_name: str):
        """Return a new KubeInterface object using a different context.

        Args:
            context_name: context to be used
        """
        return KubeInterface(self.kube_config_file, context_name, self.kubectl_cmd)

    def with_kubectl_cmd(self, kubectl_cmd: str):
        """Return a new KubeInterface object using a different kubectl command.

        Args:
            kubectl_cmd: path to the kubectl command to be used
        """
        return KubeInterface(self.kube_config_file, self.context_name, kubectl_cmd)

    def exec(
        self,
        cmd: str,
        namespace: Optional[str] = None,
        context: Optional[str] = None,
        output: Optional[str] = None,
    ) -> Union[str, Dict[str, Any]]:
        """Execute command provided as a string.

        Args:
            cmd: string command to be executed
            namespace: namespace where the command will be executed. If None the exec command will
                executed with no namespace information
            context: context to be used
            output: format for the output of the command. If "yaml" is used, output is returned as a dictionary.

        Raises:
            CalledProcessError: when the bash command fails and exits with code other than 0

        Returns:
            Output of the command, either parsed as yaml or string
        """

        base_cmd = f"{self.kubectl_cmd} --kubeconfig {self.kube_config_file} "

        if namespace and "--namespace" not in cmd or "-n" not in cmd:
            base_cmd += f" --namespace {namespace} "
        if "--context" not in cmd:
            base_cmd += f" --context {context or self.context_name} "

        base_cmd += f"{cmd} -o {output or 'yaml'} "

        self.logger.debug(f"Executing command: {base_cmd}")

        return (
            parse_yaml_shell_output(base_cmd)
            if (output is None) or (output == "yaml")
            else execute_command_output(base_cmd)
        )

    def get_service_account(
        self, account_id: str, namespace: str = "default"
    ) -> Dict[str, Any]:
        """Return the  named service account entry.

        Args:
            namespace: namespace where to look for the service account. Default is 'default'
        """

        cmd = f"get serviceaccount {account_id} -n {namespace}"

        try:
            service_account_raw = self.exec(cmd, namespace=self.namespace)
        except subprocess.CalledProcessError as e:
            if "NotFound" in e.stdout.decode("utf-8"):
                raise K8sResourceNotFound(
                    account_id, KubernetesResourceType.SERVICEACCOUNT
                )
            raise e

        if isinstance(service_account_raw, str):
            raise ValueError(
                f"Error retrieving account id {account_id} in namespace {namespace}"
            )

        self.logger.warning(service_account_raw)

        return service_account_raw

    def get_service_accounts(
        self, namespace: Optional[str] = None, labels: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Return a list of service accounts, represented as dictionary.

        Args:
            namespace: namespace where to list the service accounts. Default is to None, which will return all service
                       account in all namespaces
            labels: filter to be applied to retrieve service account which match certain labels.
        """
        cmd = "get serviceaccount"

        if labels is not None and len(labels) > 0:
            cmd += " ".join([f" -l {label}" for label in labels])

        namespace = " -A" if namespace is None else f" -n {namespace}"

        all_service_accounts_raw = self.exec(cmd + namespace, namespace=None)

        if isinstance(all_service_accounts_raw, str):
            raise ValueError("Malformed output")

        return all_service_accounts_raw["items"]

    def get_secret(
        self, secret_name: str, namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        """Return the data contained in the specified secret.

        Args:
            secret_name: name of the secret
            namespace: namespace where the secret is contained
        """

        try:
            secret = self.exec(
                f"get secret {secret_name} --ignore-not-found",
                namespace=namespace or self.namespace,
            )
        except Exception:
            raise K8sResourceNotFound(secret_name, KubernetesResourceType.SECRET)

        if secret is None or len(secret) == 0 or isinstance(secret, str):
            raise K8sResourceNotFound(secret_name, KubernetesResourceType.SECRET)

        result = dict()
        for k, v in secret["data"].items():
            # k1 = k.replace(".", "\\.")
            # value = self.kube_interface.exec(f"get secret {secret_name}", output=f"jsonpath='{{.data.{k1}}}'")
            result[k] = base64.b64decode(v).decode("utf-8")

        secret["data"] = result
        return secret

    def set_label(
        self,
        resource_type: str,
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
        self.exec(
            f"label {resource_type} {resource_name} {label}",
            namespace=namespace or self.namespace,
        )

    def remove_label(
        self,
        resource_type: str,
        resource_name: str,
        label: str,
        namespace: Optional[str] = None,
    ):
        self.exec(
            f"label {resource_type} {resource_name} {label}-",
            namespace=namespace or self.namespace,
        )

    def create(
        self,
        resource_type: str,
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
        if resource_type == KubernetesResourceType.NAMESPACE:
            self.exec(
                f"create {resource_type} {resource_name}", namespace=None, output="name"
            )
        else:
            # NOTE: removing 'username' to avoid interference with KUBECONFIG
            # ERROR: more than one authentication method found for admin; found [token basicAuth], only one is allowed
            # See for similar:
            # https://stackoverflow.com/questions/53783871/get-error-more-than-one-authentication-method-found-for-tier-two-user-found
            formatted_extra_args = " ".join(
                [
                    f"--{k}={v}"
                    for k, values in extra_args.items()
                    if k != "username"
                    for v in listify(values)
                ]
            )
            self.exec(
                f"create {resource_type} {resource_name} {formatted_extra_args}",
                namespace=namespace or self.namespace,
                output="name",
            )

    def delete(
        self, resource_type: str, resource_name: str, namespace: Optional[str] = None
    ):
        """Delete a K8s resource.

        Args:
            resource_type: type of the resource to be deleted, e.g. service account, rolebindings, etc.
            resource_name: name of the resource to be deleted
            namespace: namespace where the resource is
        """
        self.exec(
            f"delete {resource_type} {resource_name} --ignore-not-found",
            namespace=namespace or self.namespace,
            output="name",
        )

    def exists(
        self,
        resource_type: KubernetesResourceType,
        resource_name: str,
        namespace: Optional[str] = None,
    ) -> bool:
        output = self.exec(
            f"get {resource_type} {resource_name} --ignore-not-found",
            namespace=namespace or self.namespace,
        )
        return output is not None

    @classmethod
    def autodetect(
        cls, context_name: Optional[str] = None, kubectl_cmd: str = "kubectl"
    ) -> "KubeInterface":
        """
        Return a KubeInterface object by auto-parsing the output of the kubectl command.

        Args:
            context_name: context to be used to export the cluster configuration
            kubectl_cmd: path to the kubectl command to be used to interact with the K8s API
        """

        cmd = kubectl_cmd

        if context_name:
            cmd += f" --context {context_name}"

        config = parse_yaml_shell_output(f"{cmd} config view --minify -o yaml")

        return KubeInterface(config, context_name=context_name, kubectl_cmd=kubectl_cmd)

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

