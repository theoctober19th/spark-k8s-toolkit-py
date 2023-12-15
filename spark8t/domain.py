import os
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional

from spark8t.utils import PropertyFile


class Defaults:
    """Class containing all relevant defaults for the application."""

    def __init__(self, environ: Dict = dict(os.environ)):
        """Initialize a Defaults class using the value contained in a dictionary

        Args:
            environ: dictionary representing the environment. Default uses the os.environ key-value pairs.
        """

        self.environ = environ if environ is not None else {}

    @property
    def spark_home(self):
        return self.environ["SPARK_HOME"]

    @property
    def spark_confs(self):
        return self.environ.get("SPARK_CONFS", os.path.join(self.spark_home, "conf"))

    @property
    def spark_user_data(self):
        return self.environ["SPARK_USER_DATA"]

    @property
    def kubectl_cmd(self) -> str:
        """Return default kubectl command."""
        return self.environ.get("SPARK_KUBECTL", "kubectl")

    @property
    def kube_config(self) -> str:
        """Return default kubeconfig to use if not explicitly provided."""
        return self.environ["KUBECONFIG"]

    @property
    def static_conf_file(self) -> str:
        """Return static config properties file packaged with the client artefacts."""
        return f"{self.spark_confs}/spark-defaults.conf"

    @property
    def env_conf_file(self) -> Optional[str]:
        """Return env var provided by user to point to the config properties file with conf overrides."""
        return self.environ.get("SPARK_CLIENT_ENV_CONF")

    @property
    def service_account(self):
        return "spark"

    @property
    def namespace(self):
        return "defaults"

    @property
    def scala_history_file(self):
        return f"{self.spark_user_data}/.scala_history"

    @property
    def spark_submit(self) -> str:
        return f"{self.spark_home}/bin/spark-submit"

    @property
    def spark_shell(self) -> str:
        return f"{self.spark_home}/bin/spark-shell"

    @property
    def pyspark(self) -> str:
        return f"{self.spark_home}/bin/pyspark"

    @property
    def dir_package(self) -> str:
        return os.path.dirname(__file__)

    @property
    def template_dir(self) -> str:
        return f"{self.dir_package}/resources/templates"

    @property
    def template_serviceaccount(self) -> str:
        return f"{self.template_dir}/serviceaccount_yaml.tmpl"

    @property
    def template_role(self) -> str:
        return f"{self.template_dir}/role_yaml.tmpl"

    @property
    def template_rolebinding(self) -> str:
        return f"{self.template_dir}/rolebinding_yaml.tmpl"


@dataclass
class ServiceAccount:
    """Class representing the spark ServiceAccount domain object."""

    name: str
    namespace: str
    api_server: str
    primary: bool = False
    extra_confs: PropertyFile = PropertyFile.empty()

    @property
    def id(self):
        """Return the service account id, as a concatenation of namespace and username."""
        return f"{self.namespace}:{self.name}"

    @property
    def _k8s_configurations(self):
        return PropertyFile(
            {
                "spark.kubernetes.authenticate.driver.serviceAccountName": self.name,
                "spark.kubernetes.namespace": self.namespace,
            }
        )

    @property
    def configurations(self) -> PropertyFile:
        """Return the service account configuration, associated to a given spark service account."""
        return self.extra_confs + self._k8s_configurations


class KubernetesResourceType(str, Enum):
    SERVICEACCOUNT = "serviceaccount"
    ROLE = "role"
    ROLEBINDING = "rolebinding"
    SECRET = "secret"
    SECRET_GENERIC = "secret generic"
    NAMESPACE = "namespace"
