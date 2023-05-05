"""
Managed Services related functionalities
"""
import logging
import re

from ocs_ci.helpers.helpers import create_ocs_object_from_kind_and_name
from ocs_ci.ocs.resources import csv
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.managedservice import get_storage_provider_endpoint
from ocs_ci.utility.version import get_semantic_version
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import (
    get_worker_nodes,
    get_node_objs,
    get_node_zone_dict,
    verify_worker_nodes_security_groups,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_ceph_tools_pod, get_osd_pods, get_pod_node
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.utility.utils import convert_device_size
import ocs_ci.ocs.cluster

log = logging.getLogger(__name__)


def verify_provider_topology():
    """
    Verify topology in a Managed Services provider cluster

    1. Verify replica count
    2. Verify total size
    3. Verify OSD size
    4. Verify worker node instance type
    5. Verify worker node instance count
    6. Verify OSD count
    7. Verify OSD CPU and memory

    """
    # importing here to avoid circular import
    from ocs_ci.ocs.resources.storage_cluster import StorageCluster, get_osd_count

    size = f"{config.ENV_DATA.get('size', 4)}Ti"
    replica_count = 3
    osd_size = 4
    instance_type = "m5.2xlarge"
    size_map = {
        "4Ti": {"total_size": 12, "osd_count": 3, "instance_count": 3},
        "8Ti": {"total_size": 24, "osd_count": 6, "instance_count": 6},
        "12Ti": {"total_size": 36, "osd_count": 9, "instance_count": 6},
        "16Ti": {"total_size": 48, "osd_count": 12, "instance_count": 6},
        "20Ti": {"total_size": 60, "osd_count": 15, "instance_count": 6},
    }
    cluster_namespace = config.ENV_DATA["cluster_namespace"]
    storage_cluster = StorageCluster(
        resource_name="ocs-storagecluster",
        namespace=cluster_namespace,
    )

    # Verify replica count
    assert (
        int(storage_cluster.data["spec"]["storageDeviceSets"][0]["replica"])
        == replica_count
    ), (
        f"Replica count is not as expected. Actual:{storage_cluster.data['spec']['storageDeviceSets'][0]['replica']}. "
        f"Expected: {replica_count}"
    )
    log.info(f"Verified that the replica count is {replica_count}")

    # Verify total size
    ct_pod = get_ceph_tools_pod()
    ceph_osd_df = ct_pod.exec_ceph_cmd(ceph_cmd="ceph osd df")
    total_size = int(ceph_osd_df.get("summary").get("total_kb"))
    total_size = convert_device_size(
        unformatted_size=f"{total_size}Ki", units_to_covert_to="TB", convert_size=1024
    )
    assert (
        total_size == size_map[size]["total_size"]
    ), f"Total size {total_size}Ti is not matching the expected total size {size_map[size]['total_size']}Ti"
    log.info(f"Verified that the total size is {size_map[size]['total_size']}Ti")

    # Verify OSD size
    osd_pvc_objs = get_all_pvc_objs(
        namespace=cluster_namespace, selector=constants.OSD_PVC_GENERIC_LABEL
    )
    for pvc_obj in osd_pvc_objs:
        assert (
            pvc_obj.get()["status"]["capacity"]["storage"] == f"{osd_size}Ti"
        ), f"Size of OSD PVC {pvc_obj.name} is not {osd_size}Ti"
    log.info(f"Verified that the size of each OSD is {osd_size}Ti")

    # Verify worker node instance type
    worker_node_names = get_worker_nodes()
    worker_nodes = get_node_objs(worker_node_names)
    for node_obj in worker_nodes:
        assert (
            node_obj.get("metadata")
            .get("metadata")
            .get("labels")
            .get("beta.kubernetes.io/instance-type")
            == instance_type
        ), f"Instance type of the worker node {node_obj.name} is not {instance_type}"
    log.info(f"Verified that the instance type of worker nodes is {instance_type}")

    # Verify worker node instance count
    assert len(worker_node_names) == size_map[size]["instance_count"], (
        f"Worker node instance count is not as expected. Actual instance count is {len(worker_node_names)}. "
        f"Expected {size_map[size]['instance_count']}. List of worker nodes : {worker_node_names}"
    )
    log.info("Verified the number of worker nodes.")

    # Verify OSD count
    osd_count = get_osd_count()
    assert (
        osd_count == size_map[size]["osd_count"]
    ), f"OSD count is not as expected. Actual:{osd_count}. Expected:{size_map[size]['osd_count']}"
    log.info(f"Verified that the OSD count is {size_map[size]['osd_count']}")

    # Verify OSD CPU and memory
    osd_cpu_limit = "1650m"
    osd_cpu_request = "1650m"
    osd_pods = get_osd_pods()
    osd_memory_size = config.ENV_DATA["ms_osd_pod_memory"]
    log.info("Verifying OSD CPU and memory")
    for osd_pod in osd_pods:
        for container in osd_pod.data["spec"]["containers"]:
            if container["name"] == "osd":
                assert container["resources"]["limits"]["cpu"] == osd_cpu_limit, (
                    f"OSD pod {osd_pod.name} container osd doesn't have cpu limit {osd_cpu_limit}. "
                    f"Limit is {container['resources']['limits']['cpu']}"
                )
                assert container["resources"]["requests"]["cpu"] == osd_cpu_request, (
                    f"OSD pod {osd_pod.name} container osd doesn't have cpu request {osd_cpu_request}. "
                    f"Request is {container['resources']['requests']['cpu']}"
                )
                assert (
                    container["resources"]["limits"]["memory"] == osd_memory_size
                ), f"OSD pod {osd_pod.name} container osd doesn't have memory limit {osd_memory_size}"
                assert (
                    container["resources"]["requests"]["memory"] == osd_memory_size
                ), f"OSD pod {osd_pod.name} container osd doesn't have memory request {osd_memory_size}"
    log.info("Verified OSD CPU and memory")

    # Verify OSD distribution
    verify_osd_distribution_on_provider()


def get_used_capacity(msg):
    """
    Verify OSD percent used capacity greate than ceph_full_ratio

    Args:
        msg (str): message to be logged

    Returns:
         float: The percentage of the used capacity in the cluster

    """
    log.info(f"{msg}")
    used_capacity = ocs_ci.ocs.cluster.get_percent_used_capacity()
    log.info(f"Used Capacity is {used_capacity}%")
    return used_capacity


def verify_osd_used_capacity_greater_than_expected(expected_used_capacity):
    """
    Verify OSD percent used capacity greater than ceph_full_ratio

    Args:
        expected_used_capacity (float): expected used capacity

    Returns:
         bool: True if used_capacity greater than expected_used_capacity, False otherwise

    """
    osds_utilization = ocs_ci.ocs.cluster.get_osd_utilization()
    log.info(f"osd utilization: {osds_utilization}")
    for osd_id, osd_utilization in osds_utilization.items():
        if osd_utilization > expected_used_capacity:
            log.info(
                f"OSD ID:{osd_id}:{osd_utilization} greater than {expected_used_capacity}%"
            )
            return True
    return False


def get_ocs_osd_deployer_version():
    """
    Get OCS OSD deployer version from CSV

    Returns:
         Version: OCS OSD deployer version

    """
    csv_kind = OCP(kind="ClusterServiceVersion", namespace="openshift-storage")
    deployer_csv = csv_kind.get(selector=constants.OCS_OSD_DEPLOYER_CSV_LABEL)
    assert (
        "ocs-osd-deployer" in deployer_csv["items"][0]["metadata"]["name"]
    ), "Couldn't find ocs-osd-deployer CSV"
    deployer_version = deployer_csv["items"][0]["spec"]["version"]
    return get_semantic_version(deployer_version)


def verify_osd_distribution_on_provider():
    """
    Verify the OSD distribution on the provider cluster

    """
    size = config.ENV_DATA.get("size", 4)
    nodes_zone = get_node_zone_dict()
    osd_pods = get_osd_pods()
    zone_osd_count = {}

    # Get OSD zone and compare with it's node zone
    for osd_pod in osd_pods:
        osd_zone = osd_pod.get()["metadata"]["labels"]["topology-location-zone"]
        osd_node = get_pod_node(osd_pod).name
        assert osd_zone == nodes_zone[osd_node], (
            f"Zone in OSD label and node's zone are not matching. OSD name:{osd_node.name}, Zone: {osd_zone}. "
            f"Node name: {osd_node}, Zone: {nodes_zone[osd_node]}"
        )
        zone_osd_count[osd_zone] = zone_osd_count.get(osd_zone, 0) + 1

    # Verify the number of OSDs per zone
    for zone, osd_count in zone_osd_count.items():
        # 4Ti is the size of OSD
        assert (
            osd_count == int(size) / 4
        ), f"Zone {zone} does not have {size/4} osd, but {osd_count}"


def verify_storageclient(
    storageclient_name=None, namespace=None, provider_name=None, verify_sc=True
):
    """
    Verify status, values and resources related to a storageclient

    Args:
        storageclient_name (str): Name of the storageclient to be verified. If the name is not given, it will be
            assumed that only one storageclient is present in the cluster.
        namespace (str): Namespace where the storageclient is present.
            Default value will be taken from ENV_DATA["cluster_namespace"]
        provider_name (str): Name of the provider cluster to which the storageclient is connected.
        verify_sc (bool): True to verify the storageclassclaims and storageclasses associated with the storageclient.

    """
    storageclient_obj = OCP(
        kind=constants.STORAGECLIENT,
        namespace=namespace or config.ENV_DATA["cluster_namespace"],
    )
    storageclient = (
        storageclient_obj.get(resource_name=storageclient_name)
        if storageclient_name
        else storageclient_obj.get()["items"][0]
    )
    storageclient_name = storageclient["metadata"]["name"]
    provider_name = provider_name or config.ENV_DATA.get("provider_name", "")
    endpoint_actual = get_storage_provider_endpoint(provider_name)
    assert storageclient["spec"]["storageProviderEndpoint"] == endpoint_actual, (
        f"The value of storageProviderEndpoint is not correct in the storageclient {storageclient['metadata']['name']}."
        f" Value in storageclient is {storageclient['spec']['storageProviderEndpoint']}. "
        f"Value in the provider cluster {provider_name} is {endpoint_actual}"
    )
    log.info(
        f"Verified the storageProviderEndpoint value in the storageclient {storageclient_name}"
    )

    # Verify storageclient status
    assert storageclient["status"]["phase"] == "Connected"
    log.info(f"Storageclient {storageclient_name} is Connected.")

    if verify_sc:
        # Verify storageclassclaims and the presence of storageclasses
        verify_storageclient_storageclass_claims(storageclient_name)
        log.info(
            f"Verified the status of the storageclassclaims associated with the storageclient {storageclient_name}"
        )


def get_storageclassclaims_of_storageclient(storageclient_name):
    """
    Get all storageclassclaims associated with a storageclient

    Args:
        storageclient_name (str): Name of the storageclient

    Returns:
         List: OCS objects of kind Storageclassclaim

    """
    sc_claims = get_all_storageclassclaims()
    return [
        sc_claim
        for sc_claim in sc_claims
        if sc_claim.data["spec"]["storageClient"]["name"] == storageclient_name
    ]


def get_all_storageclassclaims():
    """
    Get all storageclassclaims

    Returns:
         List: OCS objects of kind Storageclassclaim

    """
    sc_claim_obj = OCP(
        kind=constants.STORAGECLASSCLAIM, namespace=config.ENV_DATA["cluster_namespace"]
    )
    sc_claims_data = sc_claim_obj.get()["items"]
    return [OCS(**claim_data) for claim_data in sc_claims_data]


def verify_storageclient_storageclass_claims(storageclient):
    """
    Verify the status of storageclassclaims and the presence of the storageclass associated with the storageclient

    Args:
        storageclient_name (str): Name of the storageclient

    """
    sc_claim_objs = get_storageclassclaims_of_storageclient(storageclient)
    for sc_claim in sc_claim_objs:
        sc_claim.ocp.wait_for_resource(
            condition=constants.STATUS_READY,
            resource_name=sc_claim.name,
            column="PHASE",
            resource_count=1,
        )
        log.info(
            f"Storageclassclaim {sc_claim.name} associated with the storageclient {storageclient} is "
            f"{constants.STATUS_READY}"
        )

        # Create OCS object of kind Storageclass
        sc_obj = create_ocs_object_from_kind_and_name(
            kind=constants.STORAGECLASS,
            resource_name=sc_claim.name,
        )
        # Verify that the Storageclass is present
        sc_obj.get()
        log.info(f"Verified Storageclassclaim and Storageclass {sc_claim.name}")


def verify_pods_in_managed_fusion_namespace():
    """
    Verify the status of pods in the namespace managed-fusion

    """
    log.info(
        f"Verifying the status of the pods in the namespace {constants.MANAGED_FUSION_NAMESPACE}"
    )
    pods_dict = {
        constants.MANAGED_FUSION_ALERTMANAGER_LABEL: 1,
        constants.MANAGED_FUSION_AWS_DATA_GATHER: 1,
        constants.MANAGED_CONTROLLER_LABEL: 1,
        constants.MANAGED_FUSION_PROMETHEUS_LABEL: 1,
        constants.PROMETHEUS_OPERATOR_LABEL: 1,
    }
    pod = OCP(kind=constants.POD, namespace=constants.MANAGED_FUSION_NAMESPACE)
    for label, count in pods_dict.items():
        assert pod.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=label,
            resource_count=count,
            timeout=600,
        )
    log.info(
        f"Verified the status of the pods in the namespace {constants.MANAGED_FUSION_NAMESPACE}"
    )


def verify_faas_resources():
    """
    Verify the presence and status of resources in FaaS clusters

    """
    # Verify pods in managed-fusion namespace
    verify_pods_in_managed_fusion_namespace()

    # Verify secrets
    verify_faas_cluster_secrets()

    # Verify attributes specific to cluster types
    if config.ENV_DATA["cluster_type"].lower() == "provider":
        sc_obj = OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        sc_data = sc_obj.get()["items"][0]
        verify_faas_provider_storagecluster(sc_data)
        verify_faas_provider_resources()
        if get_ocs_osd_deployer_version() >= get_semantic_version("2.0.11-0"):
            verify_provider_topology()
    else:
        verify_storageclient()
        verify_faas_consumer_resources()

    # Verify security
    if config.ENV_DATA["cluster_type"].lower() == "consumer":
        verify_client_operator_security()


def verify_faas_provider_resources():
    """
    Verify resources specific to FaaS provider cluster

    1. Verify CSV phase
    2. Verify ocs-provider-server pod is Running
    3. Verify that Cephcluster is Ready and hostNetworking is True
    4. Verify that the security groups are set up correctly

    """
    # Verify CSV phase
    for csv_prefix in {
        constants.MANAGED_FUSION_AGENT,
        constants.OCS_CSV_PREFIX,
        constants.OSE_PROMETHEUS_OPERATOR,
    }:
        csvs = csv.get_csvs_start_with_prefix(
            csv_prefix, config.ENV_DATA["cluster_namespace"]
        )
        assert (
            len(csvs) == 1
        ), f"Unexpected number of CSVs with name prefix {csv_prefix}: {len(csvs)}"
        csv_name = csvs[0]["metadata"]["name"]
        csv_obj = csv.CSV(
            resource_name=csv_name, namespace=config.ENV_DATA["cluster_namespace"]
        )
        log.info(f"Verify that the CSV {csv_name} is in Succeeded phase.")
        csv_obj.wait_for_phase(phase="Succeeded", timeout=600)

    # Verify ocs-provider-server pod is Running
    pod_obj = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
    pod_obj.wait_for_resource(
        condition="Running", selector=constants.PROVIDER_SERVER_LABEL, resource_count=1
    )

    # Verify that Cephcluster is Ready and hostNetworking is True
    cephcluster = OCP(
        kind=constants.CEPH_CLUSTER, namespace=config.ENV_DATA["cluster_namespace"]
    )
    cephcluster_yaml = cephcluster.get().get("items")[0]
    log.info("Verifying that Cephcluster is Ready and hostNetworking is True")
    assert (
        cephcluster_yaml["status"]["phase"] == "Ready"
    ), f"Status of cephcluster {cephcluster_yaml['metadata']['name']} is {cephcluster_yaml['status']['phase']}"
    assert cephcluster_yaml["spec"]["network"][
        "hostNetwork"
    ], f"hostNetwork is {cephcluster_yaml['spec']['network']['hostNetwork']} in Cephcluster"

    # Verify that the security groups are set up correctly
    assert verify_worker_nodes_security_groups()


def verify_faas_consumer_resources():
    """
    Verify resources specific to FaaS consumer

    1. Verify CSV phase
    2. Verify client endpoint

    """

    # Verify CSV phase
    for csv_prefix in {
        constants.MANAGED_FUSION_AGENT,
        constants.OCS_CLIENT_OPERATOR,
        constants.ODF_CSI_ADDONS_OPERATOR,
        constants.OSE_PROMETHEUS_OPERATOR,
    }:
        csvs = csv.get_csvs_start_with_prefix(
            csv_prefix, config.ENV_DATA["cluster_namespace"]
        )
        assert (
            len(csvs) == 1
        ), f"Unexpected number of CSVs with name prefix {csv_prefix}: {len(csvs)}"
        csv_name = csvs[0]["metadata"]["name"]
        csv_obj = csv.CSV(
            resource_name=csv_name, namespace=config.ENV_DATA["cluster_namespace"]
        )
        log.info(f"Verify that the CSV {csv_name} is in Succeeded phase.")
        csv_obj.wait_for_phase(phase="Succeeded", timeout=600)

    # Verify client endpoint
    client_endpoint = OCP(
        kind=constants.ENDPOINTS,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector="operators.coreos.com/ocs-client-operator.fusion-storage",
    )
    client_ep_yaml = client_endpoint.get().get("items")[0]
    log.info("Verifying that The client endpoint has an IP address")
    ep_ip = client_ep_yaml["subsets"][0]["addresses"][0]["ip"]
    log.info(f"Client endpoint IP is {ep_ip}")
    assert re.match("\\d+(\\.\\d+){3}", ep_ip)


def verify_faas_cluster_secrets():
    """
    Verify the secrets present in FaaS cluster

    """
    secret_cluster_namespace_obj = OCP(
        kind=constants.SECRET, namespace=config.ENV_DATA["cluster_namespace"]
    )
    secret_service_namespace_obj = OCP(
        kind=constants.SECRET, namespace=config.ENV_DATA["service_namespace"]
    )
    managed_fusion_secret_names = [
        "alertmanager-managed-fusion-alertmanager-generated",
        "managed-fusion-agent-config",
        "managed-fusion-alertmanager-secret",
        "prometheus-managed-fusion-prometheus",
    ]
    for secret_name in managed_fusion_secret_names:
        assert secret_service_namespace_obj.is_exist(
            resource_name=secret_name
        ), f"Secret {secret_name} does not exist in {config.ENV_DATA['service_namespace']} namespace"

    if config.ENV_DATA["cluster_type"].lower() == "provider":
        secret_names = [
            constants.MANAGED_ONBOARDING_SECRET,
            constants.MANAGED_PROVIDER_SERVER_SECRET,
            constants.MANAGED_MON_SECRET,
        ]
        for secret_name in secret_names:
            assert secret_cluster_namespace_obj.is_exist(
                resource_name=secret_name
            ), f"Secret {secret_name} does not exist in {config.ENV_DATA['cluster_namespace']} namespace"


def verify_faas_provider_storagecluster(sc_data):
    """
    Verify provider storagecluster

    1. allowRemoteStorageConsumers: true
    2. hostNetwork: true
    3. matchExpressions:
        key: node-role.kubernetes.io/worker
        operator: Exists
        key: node-role.kubernetes.io/infra
        operator: DoesNotExist
    4. storageProviderEndpoint
    5. annotations:
        uninstall.ocs.openshift.io/cleanup-policy: delete
        uninstall.ocs.openshift.io/mode: graceful

    Args:
        sc_data (dict): storagecluster data dictionary

    """
    log.info(
        f"allowRemoteStorageConsumers: {sc_data['spec']['allowRemoteStorageConsumers']}"
    )
    assert sc_data["spec"]["allowRemoteStorageConsumers"]
    log.info(f"hostNetwork: {sc_data['spec']['hostNetwork']}")
    assert sc_data["spec"]["hostNetwork"]
    expressions = sc_data["spec"]["labelSelector"]["matchExpressions"]
    for item in expressions:
        log.info(f"Verifying {item}")
        if item["key"] == "node-role.kubernetes.io/worker":
            assert item["operator"] == "Exists"
        else:
            assert item["operator"] == "DoesNotExist"
    log.info(f"storageProviderEndpoint: {sc_data['status']['storageProviderEndpoint']}")
    assert re.match(
        "(\\w+\\-\\w+\\.\\w+\\-\\w+\\-\\w+\\.elb.amazonaws.com):50051",
        sc_data["status"]["storageProviderEndpoint"],
    )
    annotations = sc_data["metadata"]["annotations"]
    log.info(f"Annotations: {annotations}")
    assert annotations["uninstall.ocs.openshift.io/cleanup-policy"] == "delete"
    assert annotations["uninstall.ocs.openshift.io/mode"] == "graceful"


def verify_client_operator_security():
    """
    Check ocs-client-operator-controller-manager permissions

    1. Verify `runAsUser` is not 0
    2. Verify `SecurityContext.allowPrivilegeEscalation` is set to false
    3. Verify `SecurityContext.capabilities.drop` contains ALL

    """
    pod_obj = OCP(
        kind=constants.POD,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=constants.MANAGED_CONTROLLER_LABEL,
    )
    client_operator_yaml = pod_obj.get().get("items")[0]
    containers = client_operator_yaml["spec"]["containers"]
    for container in containers:
        log.info(f"Checking container {container['name']}")
        userid = container["securityContext"]["runAsUser"]
        log.info(f"runAsUser is {userid}. Verifying it is not 0")
        assert userid > 0
        escalation = container["securityContext"]["allowPrivilegeEscalation"]
        log.info("Verifying allowPrivilegeEscalation is False")
        assert not escalation
        dropped_capabilities = container["securityContext"]["capabilities"]["drop"]
        log.info(f"Dropped capabilities: {dropped_capabilities}")
        assert "ALL" in dropped_capabilities
