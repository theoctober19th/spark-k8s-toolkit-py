from time import sleep

import pytest

from spark8t.domain import KubernetesResourceType
from spark8t.utils import PropertyFile
from spark8t.utils import umask_named_temporary_file


@pytest.mark.parametrize(
    "kubeinterface_name", [("kubeinterface"), ("lightkubeinterface")]
)
@pytest.mark.parametrize(
    "resource_type, resource_name, namespace",
    [
        (KubernetesResourceType.NAMESPACE, "spark-namespace", None),
        (KubernetesResourceType.SERVICEACCOUNT, "spark-sa", "default"),
    ],
)
def test_create_exists_delete(
    kubeinterface_name, resource_type, resource_name, namespace, request
):
    kubeinterface = request.getfixturevalue(kubeinterface_name)
    kubeinterface.create(resource_type, resource_name, namespace)

    assert kubeinterface.exists(resource_type, resource_name, namespace)

    kubeinterface.delete(resource_type, resource_name, namespace)

    if resource_type == KubernetesResourceType.NAMESPACE:
        sleep(15)

    assert not kubeinterface.exists(resource_type, resource_name, namespace)


@pytest.mark.parametrize(
    "kubeinterface_name", [("kubeinterface"), ("lightkubeinterface")]
)
def test_create_exists_delete_secret(kubeinterface_name, request):
    secret_name = "my-secret"
    namespace = "default"

    property_file = PropertyFile({"key": "value"})

    kubeinterface = request.getfixturevalue(kubeinterface_name)

    with umask_named_temporary_file(
        mode="w", prefix="spark-dynamic-conf-k8s-", suffix=".conf"
    ) as t:
        property_file.write(t.file)

        t.flush()

        kubeinterface.create(
            KubernetesResourceType.SECRET_GENERIC,
            secret_name,
            namespace=namespace,
            **{"from-env-file": str(t.name)},
        )

    assert kubeinterface.exists(
        KubernetesResourceType.SECRET_GENERIC, secret_name, namespace
    )

    kubeinterface.delete(KubernetesResourceType.SECRET_GENERIC, secret_name, namespace)

    assert not kubeinterface.exists(
        KubernetesResourceType.SECRET_GENERIC, secret_name, namespace
    )
