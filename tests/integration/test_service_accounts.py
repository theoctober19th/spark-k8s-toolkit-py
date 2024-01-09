import json
import subprocess
import uuid
from subprocess import CalledProcessError

import pytest

from spark8t.literals import MANAGED_BY_LABELNAME, SPARK8S_LABEL

VALID_BACKENDS = [
    "kubectl",
    # "lightkube",
]

ALLOWED_PERMISSIONS = {
    # "pods": ["create", "get", "list", "watch", "delete"],
    # "configmaps": ["create", "get", "list", "watch", "delete"],
    "services": ["create"],
}


@pytest.fixture
def namespace():
    """A temporary K8S namespace gets cleaned up automatically"""
    namespace_name = str(uuid.uuid4())
    create_command = ["kubectl", "create", "namespace", namespace_name]
    subprocess.run(create_command, check=True)
    yield namespace_name
    destroy_command = ["kubectl", "delete", "namespace", namespace_name]
    subprocess.run(destroy_command, check=True)


@pytest.fixture
def namespaces_and_service_accounts():
    from collections import defaultdict

    result = defaultdict(list)
    for _ in range(3):
        namespace_name = str(uuid.uuid4())
        create_ns_command = ["kubectl", "create", "namespace", namespace_name]
        subprocess.run(create_ns_command, check=True)

        for _ in range(3):
            sa_name = str(uuid.uuid4())
            create_sa_command = [
                "kubectl",
                "create",
                "serviceaccount",
                sa_name,
                "-n",
                namespace_name,
            ]
            subprocess.run(create_sa_command, check=True)
            result[namespace_name].append(sa_name)

    yield result

    for namespace_name in result.keys():
        destroy_command = ["kubectl", "delete", "namespace", namespace_name]
        subprocess.run(destroy_command, check=True)


def run_service_account_registry(*args):
    """Run service_account_registry CLI command with given set of args

    Returns:
        Tuple: A tuple with the content of stdout, stderr and the return code
            obtained when the command is run.
    """
    command = ["python3", "-m", "spark8t.cli.service_account_registry", *args]
    try:
        output = subprocess.run(command, check=True, capture_output=True)
        print(output.stdout.decode(), output.stderr.decode(), output.returncode)
        return output.stdout.decode(), output.stderr.decode(), output.returncode
    except CalledProcessError as e:
        print(e.stdout.decode(), e.stderr.decode(), e.returncode)
        return e.stdout.decode(), e.stderr.decode(), e.returncode


def parameterize(permissions):
    """
    A utility function to parameterize combinations of actions and RBAC permissions.
    """
    parameters = []
    for resource, actions in permissions.items():
        parameters.extend([(action, resource) for action in actions])
    return parameters


@pytest.fixture(params=VALID_BACKENDS)
def service_account(namespace, request):
    """A temporary service account that gets cleaned up automatically."""
    username = str(uuid.uuid4())
    backend = request.param

    run_service_account_registry(
        "create", "--username", username, "--namespace", namespace, "--backend", backend
    )
    return username, namespace


@pytest.mark.parametrize("backend", VALID_BACKENDS)
@pytest.mark.parametrize("action, resource", parameterize(ALLOWED_PERMISSIONS))
def test_create_service_account(namespace, backend, action, resource):
    """Test creation of service account using the CLI.

    Verify that the serviceaccount, role and rolebinding resources are created
    with appropriate tags applied to them. Also verify that the RBAC permissions
    for the created serviceaccount are intact.
    """

    username = "bikalpa"
    role_name = f"{username}-role"
    role_binding_name = f"{username}-role-binding"

    # Create the service account
    run_service_account_registry(
        "create", "--username", username, "--namespace", namespace, "--backend", backend
    )

    # Check if service account was created
    service_account_result = subprocess.run(
        ["kubectl", "get", "serviceaccount", username, "-n", namespace, "-o", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert service_account_result.returncode == 0

    # Check if service account was labelled correctly
    service_account = json.loads(service_account_result.stdout)
    assert service_account is not None
    actual_labels = service_account["metadata"]["labels"]
    expected_labels = {MANAGED_BY_LABELNAME: SPARK8S_LABEL}
    assert actual_labels == expected_labels

    # Check if a role was created
    role_result = subprocess.run(
        ["kubectl", "get", "role", role_name, "-n", namespace, "-o", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert role_result.returncode == 0

    # Check if the role was labelled correctly
    role = json.loads(role_result.stdout)
    assert role is not None
    actual_labels = role["metadata"]["labels"]
    expected_labels = {MANAGED_BY_LABELNAME: SPARK8S_LABEL}
    assert actual_labels == expected_labels

    # Check if a role binding was created
    role_binding_result = subprocess.run(
        [
            "kubectl",
            "get",
            "rolebinding",
            role_binding_name,
            "-n",
            namespace,
            "-o",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert role_binding_result.returncode == 0

    # Check if the role binding was labelled correctly
    role_binding = json.loads(role_binding_result.stdout)
    assert role_binding is not None
    actual_labels = role_binding["metadata"]["labels"]
    expected_labels = {MANAGED_BY_LABELNAME: SPARK8S_LABEL}
    assert actual_labels == expected_labels

    # Check for RBAC permissions
    sa_identifier = f"system:serviceaccount:{namespace}:{username}"
    rbac_check = subprocess.run(
        [
            "kubectl",
            "auth",
            "can-i",
            action,
            resource,
            "--namespace",
            namespace,
            "--as",
            sa_identifier,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert rbac_check.returncode == 0
    assert rbac_check.stdout.strip() == "yes"

