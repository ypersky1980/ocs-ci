import logging
import time
from ocs_ci.ocs import constants
from ocs_ci.ocs import defaults
from ocs_ci.ocs.ocp import OCP
import ocs_ci.ocs.resources.pod as pod_helpers
from ocs_ci.ocs.cluster import CephCluster, count_cluster_osd

from uuid import uuid4

logger = logging.getLogger(__name__)

def raw_block_io(raw_blk_pod,size='100G'):
    raw_blk_pod.run_io(storage_type = 'block', size = size)
    return True


# move this function to cluster.py
def get_osd_size():
    ocp = OCP(namespace=defaults.ROOK_CLUSTER_NAMESPACE, kind=constants.STORAGECLUSTER)
    sc = ocp.get()

    # the following series of 'get's return capacity with size qualifier like Gi/Ti attached. So, take out last two
    # characters to get actual capacity in int.
    return sc.get('items')[0].get('spec').get('storageDeviceSets')[0].get('dataPVCTemplate').\
        get('spec').get('resources').get('requests').get('storage')[:-2]


def cluster_fillup(fill_pod, percent_required_filled):

    logging.info(f"#### Filling up cluster FROM Pod: {fill_pod.name}")

    dir_name = "cluster_fillup_" + uuid4().hex
    cmd1 = "" + "curl http://download.ceph.com/tarballs/ceph_15.1.0.orig.tar.gz --output /tmp/ceph.tar.gz ;" +\
           "mkdir /mnt/" + dir_name + ";" + ""
    logging.info(f"#### Command 1 is: {cmd1}")
    fill_pod.exec_sh_cmd_on_pod(cmd1, sh="bash")
    logging.info("#### executed cmd1")

    ct_pod = pod_helpers.get_ceph_tools_pod()
    src_dir = "/tmp"
    dir_num = 1
    percent_used_capacity = 0
    cluster_filled = False

    while not cluster_filled:
        dest_dir = "/mnt/"+dir_name+"/temp"+str(dir_num)
        cmd = "" + "mkdir " + dest_dir + ""
        fill_pod.exec_sh_cmd_on_pod(cmd, sh="bash")
        for i in range(1, 30):
            if percent_used_capacity >= percent_required_filled:
                logging.info(f"##### Inside if. used = {percent_used_capacity}, required = {percent_required_filled}")
                cluster_filled = True
                break # break the for loop
            cmd = "" + "cp " + src_dir + "/ceph.tar.gz  " + dest_dir + "/. ;" + ""
            logging.info(f"#### Command 3 is: {cmd} ####")
            fill_pod.exec_sh_cmd_on_pod(cmd, sh="bash")
            logging.info("#### executed cmd3 ####")
            # use a function from cluster.py to replace these following 2 lines
            output = ct_pod.exec_ceph_cmd(ceph_cmd='rados df')
            percent_used_capacity = (output.get('total_used') / output.get('total_space')) * 100
            logging.info(f"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ percent_used_capacity = {percent_used_capacity}. "
                         f"percent required to fill = {percent_required_filled} ####")
        dir_num += 1
    """
    while percent_used_capacity < percent_required_filled:

        cmd3 = ""+ "mkdir /mnt/"+dir_name+"/temp"+str(dir_num)+";"\
               "cp -r /mnt/"+ dir_name + "/temp1/."+ " /mnt/"+dir_name+"/temp"+str(dir_num)+"/. ;" + ""
        logging.info(f"#### Command 3 is: {cmd3}")
        fill_pod.exec_sh_cmd_on_pod(cmd3,  sh="bash")
        logging.info("#### executed cmd3")
        output = ct_pod.exec_ceph_cmd(ceph_cmd='rados df')
        percent_used_capacity = (output.get('total_used')/output.get('total_space'))*100
        dir_num += 1
    """
    logging.info(f"Completed cluster fill from pod {fill_pod.name}")
    logging.info(f"Percent filled is {percent_used_capacity}")


def check_cluster_size(size):
    ceph_obj = CephCluster()
    ct_pod = pod_helpers.get_ceph_tools_pod()
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

def cluster_copy_ops(copy_pod, iterations=10):

    dir_name = "cluster_copy_ops_" + uuid4().hex

    for itr in range(iterations):
        # cp ceph.tar.gz 10 times to each cpdir
        cmd = "" + "mkdir /mnt/"+ dir_name + "; mkdir /mnt/"+ dir_name + "/copy_dir{1..10} ; " \
                   "for i in {1..10}; do cp /tmp/ceph.tar.gz /mnt/"+ dir_name + "/copy_dir$i/ceph_$i.tar.gz & done"\
              + ""
        copy_pod.exec_sh_cmd_on_pod(cmd, sh="bash")

        # check md5sum
        # since the file to be copied is from a wellknown location, we calculated its md5sum and found it to be:
        # 016c37aa72f12e88127239467ff4962b. We will pass this value to the pod to see if it matches that of the same
        # file copied in different directories of the pod.
        # (we could calculate the md5sum by downloading first outside of pod and then comparing it with that of pod.
        # But this will increase the execution time as we have to wait for download to complete once outside pod and
        # once inside pod)
        md5sum_val_expected = "016c37aa72f12e88127239467ff4962b"

        # We are not using the pod.verify_data_integrity with the fedora dc pods as of now for the following reason:
        # verify_data_integrity function in pod.py calls check_file_existence which in turn uses 'find' utility to see
        # if the file given exists or not. In fedora pods which this function mainly deals with, the 'find' utility
        # doesn't come by default. It has to be installed. While doing so, 'yum install findutils' hangs.
        # the link "https://forums.fedoraforum.org/showthread.php?320926-failovemethod-option" mentions the solution:
        # to run "sed -i '/^failovermethod=/d' /etc/yum.repos.d/*.repo". This command takes at least 6-7 minutes to
        # complete. If this has to be repeated on 24 pods, then time taken to complete may vary between 20-30 minutes
        # even if they are run in parallel threads.
        # Instead of this if we use shell command: "md5sun" directly we can reduce the time drastically. And hence we
        # are not using verify_data_integrity() here.

        for i in range(1, 10):

            cmd = "" + "md5sum /mnt/"+ dir_name + "/copy_dir" + str(i) +"/ceph_" + str(i) + ".tar.gz " + ""
            output = copy_pod.exec_sh_cmd_on_pod(cmd, sh="bash")
            md5sum_val_got = output.split("  ")[0]
            logger.info(f"#### md5sum obtained for pod: {copy_pod.name} is {md5sum_val_got}")
            logger.info(f"#### Expected was: {md5sum_val_expected}")
            assert md5sum_val_got == md5sum_val_expected, \
                f"***** md5sum check FAILED. expected: {md5sum_val_expected}, but got {md5sum_val_got}"

        logging.info("#### Data Integrity check passed")

        # Remove the directories and loop back again
        cmd = "" + "rm -rf /mnt/" + dir_name + "/copy_dir{1..10}" + ""
        logging.info(f"#### command to remove = {cmd}")
        copy_pod.exec_sh_cmd_on_pod(cmd, sh="bash") #, timeout=None)
        logging.info(f"########### cluster_copy_ops: Completed {itr+1} Iterations")
