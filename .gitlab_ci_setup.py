#!/usr/bin/env python
import base64
import os
import yaml

from os import environ as env
from configparser import ConfigParser


def write_aws_creds():
    aws_profile = env['AWS_PROFILE']
    # Write the credentials file
    creds = ConfigParser()
    creds[aws_profile] = dict(
        aws_access_key_id=env['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=env['AWS_SECRET_ACCESS_KEY'],
    )
    creds_path = env['AWS_SHARED_CREDENTIALS_FILE']
    os.makedirs(
        os.path.dirname(creds_path),
        exist_ok=True,
    )
    with open(creds_path, 'w') as creds_file:
        creds.write(creds_file)

    # Write the config file
    conf = ConfigParser()
    conf[aws_profile] = dict(
        region=env['AWS_REGION'],
        output='text',
    )
    conf_path = env['AWS_CONFIG_FILE']
    os.makedirs(
        os.path.dirname(conf_path),
        exist_ok=True,
    )
    with open(conf_path, 'w') as conf_file:
        conf.write(conf_file)


def write_pull_secret():
    secret_dir = os.path.join(env['CI_PROJECT_DIR'], 'data')
    os.makedirs(secret_dir, exist_ok=True)
    with open(os.path.join(secret_dir, 'pull-secret'), 'w') as secret_file:
        secret_file.write(
            base64.b64decode(env['PULL_SECRET']).decode()
        )


def get_ocsci_conf():
    cluster_user = env['CLUSTER_USER']
    pipeline_id = env['CI_PIPELINE_ID']
    conf_obj = dict(
        RUN=dict(
            log_dir=os.path.join(env['CI_PROJECT_DIR'], 'logs'),
        ),
        ENV_DATA=dict(
            platform='AWS',
            cluster_name=f"{cluster_user}-ocs-ci-{pipeline_id}",
            region=env['AWS_REGION'],
            base_domain=env['AWS_DOMAIN'],
        ),
    )
    return conf_obj


def write_ocsci_conf():
    ocp_conf = get_ocsci_conf()
    ocp_conf['ENV_DATA']['skip_ocs_deployment'] = True
    ocp_conf_path = os.path.join(env['CI_PROJECT_DIR'], 'ocs-ci-ocp.yaml')
    with open(ocp_conf_path, 'w') as ocp_conf_file:
        ocp_conf_file.write(yaml.safe_dump(ocp_conf))

    ocs_conf = get_ocsci_conf()
    ocs_conf['ENV_DATA']['skip_ocp_deployment'] = True
    ocs_conf_path = os.path.join(env['CI_PROJECT_DIR'], 'ocs-ci-ocs.yaml')
    with open(ocs_conf_path, 'w') as ocs_conf_file:
        ocs_conf_file.write(yaml.safe_dump(ocs_conf))


if __name__ == "__main__":
    write_aws_creds()
    write_pull_secret()
    write_ocsci_conf()
