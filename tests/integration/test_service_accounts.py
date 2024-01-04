import json
import logging
import subprocess
import uuid

import pytest

from spark8t.literals import MANAGED_BY_LABELNAME, SPARK8S_LABEL


@pytest.fixture
def namespace():
    namespace_name = str(uuid.uuid4())
    create_command = ["kubectl", "create", "namespace", namespace_name]
    subprocess.run(create_command, check=True)
    yield namespace_name
    destroy_command = ["kubectl", "delete", "namespace", namespace_name]
    subprocess.run(destroy_command, check=True)


def run_service_account_registry(*args):
    command = ["python3", "-m", "spark8t.cli.service_account_registry", *args]
    output = subprocess.run(command, check=True, capture_output=True)
    return output.stdout, output.stderr


ALLOWED_PERMISSIONS = {
    "pods": ["create", "get", "list", "watch", "delete"],
    "configmaps": ["create", "get", "list", "watch", "delete"],
    "services": ["create", "get", "list", "watch", "delete"],
}


def parameterize(permissions):
    parameters = []
    for resource, actions in permissions.items():
        parameters.extend([(action, resource) for action in actions])
    return parameters


# @pytest.mark.parametrize("backend", ["kubectl", "lightkube"])
# @pytest.mark.parametrize("action, resource", parameterize(ALLOWED_PERMISSIONS))
# def test_create_service_account(namespace, backend, action, resource):
#     """Test creation of service account using the CLI.

#     Verify that the serviceaccount, role and rolebinding resources are created
#     with appropriate tags applied to them. Also verify that the RBAC permissions
#     for the created serviceaccount.
#     """

#     username = "bikalpa"
#     role_name = f"{username}-role"
#     role_binding_name = f"{username}-role-binding"

#     # Create the service account
#     run_service_account_registry(
#         "create", "--username", username, "--namespace", namespace, "--backend", backend
#     )

#     # Check if service account was created with appropriate labels
#     service_account_result = subprocess.run(
#         ["kubectl", "get", "serviceaccount", username, "-n", namespace, "-o", "json"],
#         check=True,
#         capture_output=True,
#         text=True,
#     )
#     assert service_account_result.returncode == 0

#     service_account = json.loads(service_account_result.stdout)
#     assert service_account is not None
#     actual_labels = service_account["metadata"]["labels"]
#     expected_labels = {MANAGED_BY_LABELNAME: SPARK8S_LABEL}
#     assert actual_labels == expected_labels

#     # Check if a role was created with appropriate labels
#     role_result = subprocess.run(
#         ["kubectl", "get", "role", role_name, "-n", namespace, "-o", "json"],
#         check=True,
#         capture_output=True,
#         text=True,
#     )
#     assert role_result.returncode == 0

#     role = json.loads(role_result.stdout)
#     assert role is not None
#     actual_labels = role["metadata"]["labels"]
#     expected_labels = {MANAGED_BY_LABELNAME: SPARK8S_LABEL}
#     assert actual_labels == expected_labels

#     # Check if a role binding was created with appropriate labels
#     role_binding_result = subprocess.run(
#         [
#             "kubectl",
#             "get",
#             "rolebinding",
#             role_binding_name,
#             "-n",
#             namespace,
#             "-o",
#             "json",
#         ],
#         check=True,
#         capture_output=True,
#         text=True,
#     )
#     assert role_binding_result.returncode == 0

#     role_binding = json.loads(role_binding_result.stdout)
#     assert role_binding is not None
#     actual_labels = role_binding["metadata"]["labels"]
#     expected_labels = {MANAGED_BY_LABELNAME: SPARK8S_LABEL}
#     assert actual_labels == expected_labels

#     # Check for RBAC permissions
#     sa_identifier = f"system:serviceaccount:{namespace}:{username}"
#     rbac_check = subprocess.run(
#         [
#             "kubectl",
#             "auth",
#             "can-i",
#             action,
#             resource,
#             "--namespace",
#             namespace,
#             "--as",
#             sa_identifier,
#         ],
#         check=True,
#         capture_output=True,
#         text=True,
#     )
#     assert rbac_check.returncode == 0
#     assert rbac_check.stdout.strip() == "yes"


@pytest.fixture(params=["kubectl", "lightkube"])
def service_account(namespace, request):
    username = str(uuid.uuid4())
    backend = request.param

    # Create the service account
    run_service_account_registry(
        "create", "--username", username, "--namespace", namespace, "--backend", backend
    )
    return username, namespace


# @pytest.mark.parametrize("backend", ["kubectl", "lightkube"])
# @pytest.mark.parametrize("action, resource", parameterize(ALLOWED_PERMISSIONS))
# def test_delete_service_account(service_account, backend, action, resource):
#     username, namespace = service_account
#     role_name = f"{username}-role"
#     role_binding_name = f"{username}-role-binding"

#     # Delete the service account
#     run_service_account_registry(
#         "delete", "--username", username, "--namespace", namespace, "--backend", backend
#     )

#     # Check if service account has been deleted
#     service_account_result = subprocess.run(
#         ["kubectl", "get", "serviceaccount", username, "-n", namespace, "-o", "json"],
#         capture_output=True,
#         text=True,
#     )
#     assert service_account_result.returncode != 0

#     # Check if the role corresponding to the service account has also been deleted
#     role_result = subprocess.run(
#         ["kubectl", "get", "role", role_name, "-n", namespace, "-o", "json"],
#         capture_output=True,
#         text=True,
#     )
#     assert role_result.returncode != 0

#     # Check if the associated role binding has been deleted
#     role_binding_result = subprocess.run(
#         [
#             "kubectl",
#             "get",
#             "rolebinding",
#             role_binding_name,
#             "-n",
#             namespace,
#             "-o",
#             "json",
#         ],
#         capture_output=True,
#         text=True,
#     )
#     assert role_binding_result.returncode != 0

#     # Check for RBAC permissions
#     sa_identifier = f"system:serviceaccount:{namespace}:{username}"
#     rbac_check = subprocess.run(
#         [
#             "kubectl",
#             "auth",
#             "can-i",
#             action,
#             resource,
#             "--namespace",
#             namespace,
#             "--as",
#             sa_identifier,
#         ],
#         capture_output=True,
#         text=True,
#     )
#     assert rbac_check.returncode != 0
#     assert rbac_check.stdout.strip() == "no"


@pytest.mark.parametrize("backend", ["kubectl", "lightkube"])
def test_service_accounts_listing(namespace, backend, caplog):
    username1 = str(uuid.uuid4())
    username2 = str(uuid.uuid4())
    username3 = str(uuid.uuid4())

    for username in (username1, username2, username3):
        run_service_account_registry(
            "create", "--username", username, "--namespace", namespace, "--backend", backend
        )
    
    # List the service accountsrun_service_account_registry
    with caplog.at_level(logging.INFO):
        run_service_account_registry("list", "--backend", backend)
    # raise Exception(caplog.text)
    assert len(caplog.records) == 3


# @pytest.mark.parametrize("backend", ["kubectl", "lightkube"])
# def test_service_accounts_get_primary(namespace, backend):
#     username = str(uuid.uuid4())

#     # Create the service account as primary account
#     run_service_account_registry(
#         "create", "--username", username, "--namespace", namespace, "--backend", backend, "--primary"
#     )

#     # Check if the same service account is returned by get-primary
#     run_service_account_registry(
#         "get-primary", "--namespace"
#     )
