import logging
import os
from tempfile import NamedTemporaryFile

from ocs_ci.framework.testlib import skipif_ocp_version, skipif_ocs_version, E2ETest
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.exceptions import CommandFailed


logger = logging.getLogger(__name__)
ERRMSG = "Error in command"


@skipif_ocp_version("<4.13")
@skipif_ocs_version("<4.13")
class TestCRRsourcesValidation(E2ETest):
    """
    Test that check that csi addons resources are not editable after creation
    """

    def test_network_fence_not_editable(self):
        """
        Test case to check that network fence object is not editable once created
        """
        network_fence_yaml = os.path.join(
            constants.TEMPLATE_CSI_ADDONS_DIR, "NetworkFence.yaml"
        )
        res = run_oc_command(cmd=f"create -f {network_fence_yaml}")
        assert (ERRMSG not in res[0]),\
            f"Failed to create resource Network Fence from yaml file {network_fence_yaml}, got result {res}"

        network_fence_name = res[0].split()[0]

        network_fence_original_yaml = run_oc_command(
            f"get {network_fence_name} -o yaml"
        )

        patches = {  # dictionary: patch_name --> patch
            "apiVersion": {"apiVersion": "csiaddons.openshift.io/v1alpha2"},
            "kind": {"kind": "newNetworkFence"},
            "generation": '{"metadata": {"generation": 6456456 }}',
            "creationTime": '{"metadata": {"creationTimestamp": "2022-04-24T19:39:54Z" }}',
            "name": '{"metadata": {"name": "newName" }}',
            "uid": '{"metadata": {"uid": "897b3c9c-c1ce-40e3-95e6-7f3dadeb3e83" }}',
            "secret": '{"spec": {"secret": {"name": "new_secret"}}}',
            "cidrs": '{"spec": {"cidrs": {"10.90.89.66/32", "11.67.12.42/32"}}}',
            "fenceState": '{"spec": {"fenceState": "NotFenced"}}',
            "driver": '{"spec": {"driver": "example.new_driver"}}',
            "new_property": '{"spec": {"new_property": "new_value"}}',
        }

        for patch in patches:
            params = "'" + f"{patches[patch]}" + "'"
            command = f"oc -n openshift-storage patch {network_fence_name} -p {params} --type merge"

            temp_file = NamedTemporaryFile(
                mode="w+", prefix="network_fence_modification", suffix=".sh"
            )
            with open(temp_file.name, "w") as t_file:
                t_file.writelines(command)
            run_cmd(f"chmod 777 {temp_file.name}")
            logger.info(f"Trying to edit property {patch}")

            try:
                run_cmd(f"sh {temp_file.name}")
                res = run_oc_command(f"get {network_fence_name} -o yaml")
                network_fence_modified_yaml = res

                if network_fence_original_yaml != network_fence_modified_yaml:
                    run_oc_command(cmd=f"delete {network_fence_name}")
                    err_msg = (
                        f"Network fence object has been edited but it should not be. \n"
                        f"Property {patch} was changed. \n"
                        f"Original object yaml is {network_fence_original_yaml}\n."
                        f"Edited object yaml is {network_fence_modified_yaml}"
                    )
                    logger.error(err_msg)
                    raise Exception(err_msg)
            except (
                CommandFailed
            ):  # some properties are not editable and CommandFailed exception is thrown
                continue  # just continue to the next property

        res = run_oc_command(cmd=f"delete {network_fence_name}")
        assert (ERRMSG not in res[0]), \
            f"Failed to delete network fence resource with name : {network_fence_name}, got result: {res}"
