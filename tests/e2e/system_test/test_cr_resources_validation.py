import logging
import os
import pytest
from tempfile import NamedTemporaryFile

from ocs_ci.framework.testlib import skipif_ocp_version, skipif_ocs_version, E2ETest
from ocs_ci.framework.pytest_customization.marks import system_test
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.exceptions import CommandFailed

logger = logging.getLogger(__name__)
ERRMSG = "Error in command"


@system_test
@skipif_ocp_version("<4.13")
@skipif_ocs_version("<4.13")
class TestCRRsourcesValidation(E2ETest):
    """
    Test that check that csi addons resources are not editable after creation
    """

    def setup(self):
        self.temp_files_list = []
        self.object_name_to_delete = ""

    def cr_resource_not_editable(
        self, cr_object_kind, yaml_name, patches_non_editable, patches_editable
    ):
        """
        Test that cr object is not editable once created

        Args:
            cr_object_kind (str): cr object kind
            yaml_name (str): name of the yaml file from which the object is to be created
            patches_non_editable (dict, of str: str): patches to be applied by 'oc patch' command. These patches should
                    have no effect. If such a patch is sucessfully applied, the test should fail
            patches_editable (dict, of str: str): patches to be applied by 'oc patch' command. These patches should
                    have an effect. If such a patch is not sucessfully applied, the test should fail

        """
        cr_resource_yaml = os.path.join(constants.TEMPLATE_CSI_ADDONS_DIR, yaml_name)
        res = run_oc_command(cmd=f"create -f {cr_resource_yaml}")
        assert (
            ERRMSG not in res[0]
        ), f"Failed to create resource {cr_object_kind} from yaml file {cr_resource_yaml}, got result {res}"

        cr_resource_name = res[0].split()[0]
        self.object_name_to_delete = cr_resource_name

        cr_resource_original_yaml = run_oc_command(f"get {cr_resource_name} -o yaml")

        # test that all non editable properties are really not editable
        non_editable_properties_errors = {}
        cr_resource_prev_yaml = cr_resource_original_yaml
        for patch in patches_non_editable:
            params = "'" + f"{patches_non_editable[patch]}" + "'"
            command = f"oc -n openshift-storage patch {cr_resource_name} -p {params} --type merge"

            temp_file = NamedTemporaryFile(
                mode="w+", prefix="cr_resource_modification", suffix=".sh"
            )
            with open(temp_file.name, "w") as t_file:
                t_file.writelines(command)
            self.temp_files_list.append(temp_file.name)
            run_cmd(f"chmod 777 {temp_file.name}")
            logger.info(f"Trying to edit property {patch}")

            try:
                run_cmd(f"sh {temp_file.name}")
                cr_resource_modified_yaml = run_oc_command(
                    f"get {cr_resource_name} -o yaml"
                )

                if cr_resource_prev_yaml != cr_resource_modified_yaml:
                    non_editable_properties_errors[patch] = cr_resource_modified_yaml
                    # reset prev yaml to the modified one to track further modifications
                    cr_resource_prev_yaml = cr_resource_modified_yaml
            except (
                CommandFailed
            ):  # some properties are not editable and CommandFailed exception is thrown
                continue  # just continue to the next property

        if non_editable_properties_errors:
            err_msg = (
                f"{cr_object_kind} object has been edited but it should not be. \n"
                f"Changed properties: {list(non_editable_properties_errors.keys())}"
            )
            logger.error(err_msg)

            detailed_err_msg = f"Original object yaml is {cr_resource_original_yaml}\n."
            for prop in non_editable_properties_errors:
                detailed_err_msg += f"Changed property is {prop}. "
                detailed_err_msg += (
                    f"Edited object yaml is {non_editable_properties_errors[prop]}\n"
                )
            logger.error(detailed_err_msg)

            raise Exception(err_msg)

        # test that all editable properties are really editable
        editable_properties_errors = {}
        cr_resource_prev_yaml = cr_resource_original_yaml
        for patch in patches_editable:
            params = "'" + f"{patches_editable[patch]}" + "'"
            command = f"oc -n openshift-storage patch {cr_resource_name} -p {params} --type merge"

            temp_file = NamedTemporaryFile(
                mode="w+", prefix="cr_resource_modification", suffix=".sh"
            )
            with open(temp_file.name, "w") as t_file:
                t_file.writelines(command)
            self.temp_files_list.append(temp_file.name)
            run_cmd(f"chmod 777 {temp_file.name}")
            logger.info(f"Trying to edit property {patch}")

            try:
                run_cmd(f"sh {temp_file.name}")
                cr_resource_modified_yaml = run_oc_command(
                    f"get {cr_resource_name} -o yaml"
                )

                if cr_resource_prev_yaml == cr_resource_modified_yaml:
                    editable_properties_errors[patch] = cr_resource_modified_yaml
                else:
                    # reset prev yaml to the modified one to track further modifications
                    cr_resource_prev_yaml = cr_resource_modified_yaml
            except (
                CommandFailed
            ):  # some properties are not editable and CommandFailed exception is thrown
                editable_properties_errors[patch] = cr_resource_modified_yaml
                continue  # just continue to the next property

        if editable_properties_errors:
            err_msg = (
                f"{cr_object_kind} object has not been edited but it should be. \n"
                f"Unchanged properties: {list(editable_properties_errors.keys())}"
            )
            logger.error(err_msg)
            raise Exception(err_msg)

    def test_network_fence_not_editable(self):
        """
        Test case to check that network fence object is not editable once created
        """

        patches_non_editable = {  # dictionary: patch_name --> patch
            "apiVersion": {"apiVersion": "csiaddons.openshift.io/v1alpha2"},
            "kind": {"kind": "newNetworkFence"},
            "generation": '{"metadata": {"generation": 6456456 }}',
            "creationTime": '{"metadata": {"creationTimestamp": "2022-04-24T19:39:54Z" }}',
            "name": '{"metadata": {"name": "newName" }}',
            "uid": '{"metadata": {"uid": "897b3c9c-c1ce-40e3-95e6-7f3dadeb3e83" }}',
            "secret": '{"spec": {"secret": {"name": "new_secret"}}}',
            "cidrs": '{"spec": {"cidrs": {"10.90.89.66/32", "11.67.12.42/32"}}}',
            "driver": '{"spec": {"driver": "example.new_driver"}}',
            "new_property": '{"spec": {"new_property": "new_value"}}',
        }

        patches_editable = {  # dictionary: patch_name --> patch
            "fenceState": '{"spec": {"fenceState": "Unfenced"}}',
        }

        self.cr_resource_not_editable(
            "Network fence", "NetworkFence.yaml", patches_non_editable, patches_editable
        )

    def test_reclaim_space_cron_job_editable(self):
        """
        Test case to check that reclaim space cron job object is not editable once created
        """

        non_editable_patches = {  # dictionary: patch_name --> patch
            "persistentVolumeClaim": '{"spec": {"jobTemplate": {"spec": {"target" :{"persistentVolumeClaim": "pv"}}}}}',
        }

        self.cr_resource_not_editable(
            "Reclaim space cron job",
            "ReclaimSpaceCronJob.yaml",
            non_editable_patches,
            {},
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Cleanup the test environment

        """

        def finalizer():
            for temp_file in self.temp_files_list:
                if os.path.exists(temp_file):
                    run_cmd(f"rm {temp_file}")

            if self.object_name_to_delete != "":
                res = run_oc_command(cmd=f"delete {self.object_name_to_delete}")
                assert (
                    ERRMSG not in res[0]
                ), f"Failed to delete network fence resource with name: {self.object_name_to_delete}, got result: {res}"

        request.addfinalizer(finalizer)
