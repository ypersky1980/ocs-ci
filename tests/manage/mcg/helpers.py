import boto3

from ocs_ci.ocs import constants
from tests.helpers import logger, craft_s3_command
from concurrent.futures import ThreadPoolExecutor

def retrieve_test_objects_to_pod(podobj, target_dir):
    """
    Downloads all the test objects to a given directory in a given pod.

    Args:
        podobj (OCS): The pod object to download the objects to
        target_dir:  The fully qualified path of the download target folder

    Returns:
        list: A list of the downloaded objects' names

    """
    # Download test objects from the public bucket
    downloaded_objects = []
    # Retrieve a list of all objects on the test-objects bucket and downloads them to the pod
    podobj.exec_cmd_on_pod(command=f'mkdir {target_dir}')
    public_s3 = boto3.resource('s3')
    for obj in public_s3.Bucket(constants.TEST_FILES_BUCKET).objects.all():
        logger.info(f'Downloading {obj.key} from AWS test bucket')
        podobj.exec_cmd_on_pod(
            command=f'sh -c "'
                    f'wget -P {target_dir} '
                    f'https://{constants.TEST_FILES_BUCKET}.s3.amazonaws.com/{obj.key}"'
        )
        downloaded_objects.append(obj.key)
    return downloaded_objects


def sync_object_directory(podobj, src, target, mcg_obj=None):
    """
    Syncs objects between a target and source directories

    Args:
        podobj (OCS): The pod on which to execute the commands and download the objects to
        src (str): Fully qualified object source path
        target (str): Fully qualified object target path
        mcg_obj (MCG, optional): The MCG object to use in case the target or source
                                 are in an MCG

    """
    logger.info(f'Syncing all objects and directories from {src} to {target}')
    retrieve_cmd = f'sync {src} {target}'
    if mcg_obj:
        secrets = [mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
    else:
        secrets = None
    podobj.exec_cmd_on_pod(
        command=craft_s3_command(mcg_obj, retrieve_cmd), out_yaml_format=False,
        secrets=secrets
    ), 'Failed to sync objects'
    # Todo: check that all objects were synced successfully


def rm_object_recursive(podobj, target, mcg_obj, option=''):
    """
    Remove bucket objects with --recursive option

    Args:
        podobj  (OCS): The pod on which to execute the commands and download
                       the objects to
        target (str): Fully qualified bucket target path
        mcg_obj (MCG, optional): The MCG object to use in case the target or
                                 source are in an MCG
        option (str): Extra s3 remove command option

    """
    rm_command = f"rm s3://{target} --recursive {option}"
    podobj.exec_cmd_on_pod(
        command=craft_s3_command(mcg_obj, rm_command),
        out_yaml_format=False,
        secrets=[mcg_obj.access_key_id, mcg_obj.access_key,
                 mcg_obj.s3_endpoint]
    )


def s3_io(mcg_obj, awscli_pod, bucket_factory):
    """
    Running IOs on s3 bucket
    Args:
        mcg_obj (obj): An MCG object containing the MCG S3 connection credentials
        awscli_pod (pod): A pod running the AWSCLI tools
        bucket_factory: Calling this fixture creates a new bucket(s)
    """
    downloaded_files = []
    public_s3 = boto3.resource('s3', region_name=mcg_obj.region)
    with ThreadPoolExecutor() as p:
        for obj in public_s3.Bucket(constants.TEST_FILES_BUCKET).objects.all():
            # Download test object(s)
            logger.info(f'Downloading {obj.key}')
            p.submit(awscli_pod.exec_cmd_on_pod,
                command=f'wget https://{constants.TEST_FILES_BUCKET}.s3.{mcg_obj.region}.amazonaws.com/{obj.key}'
            )
            downloaded_files.append(obj.key)
            downloaded_files.append(obj.key)
    bucketname = bucket_factory(1)[0].name
    logger.info(f'Writing objects to bucket')

    for obj_name in downloaded_files:
        full_object_path = f"s3://{bucketname}/{obj_name}"
        copycommand = f"cp {obj_name} {full_object_path}"
        assert 'Completed' in awscli_pod.exec_cmd_on_pod(
            command=craft_s3_command(mcg_obj, copycommand), out_yaml_format=False,
            secrets=[mcg_obj.access_key_id, mcg_obj.access_key, mcg_obj.s3_endpoint]
        )