import logging
import time
from ocs_ci.ocs.cluster import CephCluster, count_cluster_osd
# from ocs_ci.ocs.resources import pod
import ocs_ci.ocs.resources.pod as pod
from ocs_ci.ocs import constants
from ocs_ci.ocs import defaults
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)

def raw_block_io(pod,size='100G'):
    pod.run_io(storage_type = 'block', size = size)
    return True


# move this function to cluster.py
def get_osd_size():
    ocp = OCP(namespace=defaults.ROOK_CLUSTER_NAMESPACE, kind=constants.STORAGECLUSTER)
    sc = ocp.get()

    # the following series of 'get's return capacity with size qualifier like Gi/Ti attached. So, take out last two
    # characters to get actual capacity in int.
    return sc.get('items')[0].get('spec').get('storageDeviceSets')[0].get('dataPVCTemplate').\
        get('spec').get('resources').get('requests').get('storage')[:-2]


def cluster_fill_get_pod_iter(percent_capacity_to_fill=75):

    # this dictionary is based on the:
    # https://docs.google.com/spreadsheets/d/1XXqDNB6ujiQg--qyoJvB18GPgv_sIi8Xt4-c2tYtD1o/edit#gid=0&range=B15

    dict_pod_iter = {
        "25":
            {
                "512": {"pod": 3, "iteration": 1},
                "2": {"pod": 6, "iteration": 2},
                "4": {"pod": 24, "iteration": 1}
            },
        "50":
            {
                "512": {"pod": 6, "iteration": 1},
                "2": {"pod": 12, "iteration": 2},
                "4": {"pod": 24, "iteration": 2}
            },
        "75":
            {
                "340": {"pod": 6, "iteration": 1},
                "512": {"pod": 9, "iteration": 1},
                "2": {"pod": 18, "iteration": 2},
                "4": {"pod": 36, "iteration": 2}
            },
        "85":
            {
                "340": {"pod": 6, "iteration": 1},
                "512": {"pod": 9, "iteration": 1},
                "2": {"pod": 21, "iteration": 2},
                "4": {"pod": 42, "iteration": 2}
            },
        "37": # Testing purpose on ocs-ci based cluster. Not for production
            {
                "340": {"pod": 3, "iteration": 1}
            }

    }
    osd_size = get_osd_size()
    pods = dict_pod_iter[percent_capacity_to_fill][osd_size]["pod"]
    iterations = dict_pod_iter[percent_capacity_to_fill][osd_size]["iteration"]
    logging.info(f"pods to create = {pods}. iterations to run = {iterations}")
    return pods, iterations


def IOs(percent_to_fillup):
    num_of_osds = count_cluster_osd()
    osd_size = get_osd_size()
    total_capacity = num_of_osds * osd_size
    capacity_to_fillup = total_capacity * percent_to_fillup/100


def cluster_fillup(pod, iterations=1):
    logging.info(f"#### Filling up cluster FROM Pod: {pod.name}")

    cmd1 = """
            mkdir /mnt/temp{1..30} ;
            curl http://download.ceph.com/tarballs/ceph_15.1.0.orig.tar.gz --output ceph.tar.gz;
        """
    pod.exec_sh_cmd_on_pod(cmd1,  sh="bash", timeout=None)
    logging.info("#### executed cmd1")

    cmd2 = """
                for i in {1..10}; do cp ceph.tar.gz /mnt/temp1/ceph_$i.tar.gz & done
            """
    pod.exec_sh_cmd_on_pod(cmd2,  sh="bash", timeout=None)
    logging.info("#### executed cmd2")

    cmd3 = """
                for i in {2..30};
                do 
                        cp -r /mnt/temp1/* /mnt/temp$i/. &
                    done
        """

    for i in range(int(iterations)):
        pod.exec_sh_cmd_on_pod(cmd3,  sh="bash", timeout=None)

    logging.info("#### executed cmd3")
    ceph_obj = CephCluster()
    from ocs_ci.ocs.resources import pod
    ct_pod = pod.get_ceph_tools_pod()
    output = ct_pod.exec_ceph_cmd(ceph_cmd='rados df')
    used_capacity = output.get('total_used')
    logging.info(f"***** Used space = {used_capacity} GiB")


def check_cluster_size(size):
    ceph_obj = CephCluster()
    ct_pod = pod.get_ceph_tools_pod()
    retries = 0
    while True:
        if ceph_obj.get_used_space(ct_pod) >= size:
            logger.info('used space has reached...')
            return True
        else:
            logger.info('rechecking.......')
            retries += 1
            pass
        if retries > 20 and ceph_obj.get_used_space(ct_pod) <= 5:
            logger.error('IOs not happening ')
            return False
        time.sleep(1200)

def cluster_copy_ops(pod, iterations=10):

    for itr in range(iterations):
        # wget ceph.tar.gz
        cmd = """
                curl http://download.ceph.com/tarballs/ceph_15.1.0.orig.tar.gz --output ceph.tar.gz;
            """
        pod.exec_sh_cmd_on_pod(cmd, sh="bash", timeout=None)

        # cp ceph.tar.gz 10 times to each cpdir
        cmd = """
                mkdir /mnt/copy_dir{1..10} ;
                for i in {1..10}; do cp ceph.tar.gz /mnt/copy_dir$i/ceph_$i.tar.gz & done
                """
        pod.exec_sh_cmd_on_pod(cmd, sh="bash", timeout=None)

        # check md5sum
        # since the file to be copied is from a wellknown location, we calculated its md5sum and found it to be:
        # 016c37aa72f12e88127239467ff4962b. We will pass this value to the pod to see if it matches that of the same
        # file copied in different directories of the pod.
        # we could calculate the md5sum by downloading first outside of pod and then comparing it with that of pod.
        # But this will increase the execution time as we have to wait for download to complete once outside pod and
        # once inside pod

        md5sum_val_expected = "016c37aa72f12e88127239467ff4962b"

        for i in range(1, 10):

            # cmd = "md5sum /mnt/copy_dir" + str(i) + "/ceph_" + str(i) + ".tar.gz"

            cmd = "" + "md5sum /mnt/copy_dir" + str(i) + "/ceph_" + str(i) + ".tar.gz" + ""

            output = pod.exec_sh_cmd_on_pod(cmd, sh="bash", timeout=None)
            md5sum_val_got = output.split("  ")[0]
            logger.info(f"#### md5sum obtained for pod: {pod.name} is {md5sum_val_got}")
            logger.info(f"#### Expected was: {md5sum_val_expected}")
            if md5sum_val_got not in md5sum_val_expected:
                logging.error(f"***** md5sum check FAILED. expected: {md5sum_val_expected}, but got {md5sum_val_got}")
                return False
            else:
                logging.info(f"#### md5sum check PASSED")

        # Remove the directories and loop back again
        cmd = """ rm -rf /mnt/copy_dir{1..10} """
        pod.exec_sh_cmd_on_pod(cmd, sh="bash", timeout=None)
        logging.info(f"########### cluster_copy_ops: Completed {itr+1} Iterations")

    return True
