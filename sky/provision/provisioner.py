"""Cloud-neutral VM provision utils."""
import collections
import dataclasses
import json
import os
import shlex
import socket
import subprocess
import time
import traceback
from typing import Dict, List, Optional, Tuple

import colorama

import sky
from sky import clouds
from sky import provision
from sky import sky_logging
from sky import status_lib
from sky.adaptors import aws
from sky.backends import backend_utils
from sky.provision import common as provision_common
from sky.provision import instance_setup
from sky.provision import logging as provision_logging
from sky.provision import metadata_utils
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import resources_utils
from sky.utils import rich_utils
from sky.utils import ux_utils

# Do not use __name__ as we do not want to propagate logs to sky.provision,
# which will be customized in sky.provision.logging.
logger = sky_logging.init_logger('sky.provisioner')

# The maximum number of retries for waiting for instances to be ready and
# teardown instances when provisioning fails.
_MAX_RETRY = 3
_TITLE = '\n\n' + '=' * 20 + ' {} ' + '=' * 20 + '\n'


def _bulk_provision(
        cloud: clouds.Cloud,
        region: clouds.Region,
        zones: Optional[List[clouds.Zone]],
        cluster_name: resources_utils.ClusterName,
        bootstrap_config: provision_common.ProvisionConfig,
) -> provision_common.ProvisionRecord:

    logger.info(f'What is going on???? _bulk_provision')
    logger.info(f'cloud: {cloud}, region: {region}, zones: {zones}, cluster_name: {cluster_name}, bootstrap_config: {bootstrap_config}')
    provider_name = repr(cloud)
    region_name = region.name

    style = colorama.Style

    if not zones:
        # For Azure, zones is always an empty list.
        zone_str = 'all zones'
    else:
        zone_str = ','.join(z.name for z in zones)

    if isinstance(cloud, clouds.Kubernetes):
        logger.info(f'{style.BRIGHT}Launching on {cloud}{style.RESET_ALL} {cluster_name!r}.')
    else:
        logger.info(f'{style.BRIGHT}Launching on {cloud} {region_name}{style.RESET_ALL} ({zone_str})')

    start = time.time()
    with rich_utils.safe_status('[bold cyan]Launching[/]') as status:
        try:
            logger.info(f"Provision record: {provision_common.ProvisionRecord}")
            logger.info(f"Provision record: {provider_name}, {region_name}, {cluster_name.name_on_cloud}, {bootstrap_config}")
            provision_record = provision.run_instances(
                provider_name,
                region_name,
                cluster_name.name_on_cloud,
                config=bootstrap_config
            )
        except Exception as e:
            logger.error(f'{colorama.Fore.YELLOW}Failed to provision {cluster_name!r} on {cloud} {region} ({zone_str}) '
                         'with the following error:'
                         f'{colorama.Style.RESET_ALL}\n'
                         f'{common_utils.format_exception(e)}')
            raise

        logger.info(f"Provision.run_instances has run:")

        backoff = common_utils.Backoff(initial_backoff=1, max_backoff_factor=3)
        logger.debug(f'\nWaiting for instances of {cluster_name!r} to be ready...')
        status.update('[bold cyan]Launching - Checking instance status[/]')

        time.sleep(1)
        for retry_cnt in range(_MAX_RETRY):
            try:
                provision.wait_instances(
                    provider_name,
                    region_name,
                    cluster_name.name_on_cloud,
                    state=status_lib.ClusterStatus.UP
                )
                logger.info(f"Provision.run_instances has run:")
                break
            except (aws.botocore_exceptions().WaiterError, RuntimeError):
                time.sleep(backoff.current_backoff())
        else:
            raise RuntimeError(
                f'Failed to wait for instances of {cluster_name!r} to be ready on the cloud provider after max retries'
                f' {_MAX_RETRY}.')

        logger.debug(f'Instances of {cluster_name!r} are ready after {retry_cnt} retries.')

    logger.debug(f'\nProvisioning {cluster_name!r} took {time.time() - start:.2f} seconds.')

    return provision_record


def bulk_provision(
    cloud: clouds.Cloud,
    region: clouds.Region,
    zones: Optional[List[clouds.Zone]],
    cluster_name: resources_utils.ClusterName,
    num_nodes: int,
    cluster_yaml: str,
    prev_cluster_ever_up: bool,
    log_dir: str,
    image_name: str,
) -> provision_common.ProvisionRecord:
    """Provisions a cluster and wait until fully provisioned.

    Raises:
        StopFailoverError: Raised when during failover cleanup, tearing
            down any potentially live cluster failed despite retries
        Cloud specific exceptions: If the provisioning process failed, cloud-
            specific exceptions will be raised by the cloud APIs.
    """
    original_config = common_utils.read_yaml(cluster_yaml)
    head_node_type = original_config['head_node_type']
    bootstrap_config = provision_common.ProvisionConfig(
        provider_config=original_config['provider'],
        authentication_config=original_config['auth'],
        docker_config=original_config.get('docker', {}),
        # NOTE: (might be a legacy issue) we call it
        # 'ray_head_default' in 'gcp-ray.yaml'
        node_config=original_config['available_node_types'][head_node_type]
        ['node_config'],
        count=num_nodes,
        tags={},
        resume_stopped_nodes=True)

    with provision_logging.setup_provision_logging(log_dir):
        try:
            logger.debug(f'SkyPilot version: {sky.__version__}; '
                         f'commit: {sky.__commit__}')
            logger.debug(_TITLE.format('Provisioning'))
            logger.debug(
                'Provision config:\n'
                f'{json.dumps(dataclasses.asdict(bootstrap_config), indent=2)}')
            return _bulk_provision(cloud, region, zones, cluster_name,
                                   bootstrap_config)
                                   # , image_name)
        except Exception:  # pylint: disable=broad-except
            zone_str = 'all zones'
            if zones:
                zone_str = ','.join(zone.name for zone in zones)
            logger.debug(f'Failed to provision {cluster_name.display_name!r} '
                         f'on {cloud} ({zone_str}).')
            logger.debug(f'bulk_provision for {cluster_name!r} '
                         f'failed. Stacktrace:\n{traceback.format_exc()}')
            # If the cluster was ever up, stop it; otherwise terminate it.
            terminate = not prev_cluster_ever_up
            terminate_str = ('Terminating' if terminate else 'Stopping')
            logger.debug(f'{terminate_str} the failed cluster.')
            retry_cnt = 1
            while True:
                try:
                    teardown_cluster(
                        repr(cloud),
                        cluster_name,
                        terminate=terminate,
                        provider_config=original_config['provider'])
                    break
                except NotImplementedError as e:
                    verb = 'terminate' if terminate else 'stop'
                    # If the underlying cloud does not support stopping
                    # instances, we should stop failover as well.
                    raise provision_common.StopFailoverError(
                        'During provisioner\'s failover, '
                        f'{terminate_str.lower()} {cluster_name!r} failed. '
                        f'We cannot {verb} the resources launched, as it is '
                        f'not supported by {cloud}. Please try launching the '
                        'cluster again, or terminate it with: '
                        f'sky down {cluster_name.display_name}') from e
                except Exception as e:  # pylint: disable=broad-except
                    logger.debug(f'{terminate_str} {cluster_name!r} failed.')
                    logger.debug(f'Stacktrace:\n{traceback.format_exc()}')
                    retry_cnt += 1
                    if retry_cnt <= _MAX_RETRY:
                        logger.debug(f'Retrying {retry_cnt}/{_MAX_RETRY}...')
                        time.sleep(5)
                        continue
                    formatted_exception = common_utils.format_exception(
                        e, use_bracket=True)
                    raise provision_common.StopFailoverError(
                        'During provisioner\'s failover, '
                        f'{terminate_str.lower()} {cluster_name!r} failed. '
                        'This can cause resource leakage. Please check the '
                        'failure and the cluster status on the cloud, and '
                        'manually terminate the cluster. '
                        f'Details: {formatted_exception}') from e
            raise


def teardown_cluster(cloud_name: str, cluster_name: resources_utils.ClusterName,
                     terminate: bool, provider_config: Dict) -> None:
    """Deleting or stopping a cluster.

    Raises:
        Cloud specific exceptions: If the teardown process failed, cloud-
            specific exceptions will be raised by the cloud APIs.
    """
    if terminate:
        provision.terminate_instances(cloud_name, cluster_name.name_on_cloud,
                                      provider_config)
        metadata_utils.remove_cluster_metadata(cluster_name.name_on_cloud)
    else:
        provision.stop_instances(cloud_name, cluster_name.name_on_cloud,
                                 provider_config)


def _ssh_probe_command(ip: str,
                       ssh_port: int,
                       ssh_user: str,
                       ssh_private_key: str,
                       ssh_proxy_command: Optional[str] = None) -> List[str]:
    # NOTE: Ray uses 'uptime' command and 10s timeout, we use the same
    # setting here.
    command = [
        'ssh',
        '-T',
        '-i',
        ssh_private_key,
        f'{ssh_user}@{ip}',
        '-p',
        str(ssh_port),
        '-o',
        'StrictHostKeyChecking=no',
        '-o',
        'PasswordAuthentication=no',
        '-o',
        'ConnectTimeout=10s',
        '-o',
        f'UserKnownHostsFile={os.devnull}',
        '-o',
        'IdentitiesOnly=yes',
        '-o',
        'ExitOnForwardFailure=yes',
        '-o',
        'ServerAliveInterval=5',
        '-o',
        'ServerAliveCountMax=3',
    ]
    if ssh_proxy_command is not None:
        command += ['-o', f'ProxyCommand={ssh_proxy_command}']
    command += ['uptime']
    return command


def _shlex_join(command: List[str]) -> str:
    """Join a command list into a shell command string.

    This is copied from Python 3.8's shlex.join, which is not available in
    Python 3.7.
    """
    return ' '.join(shlex.quote(arg) for arg in command)


def _wait_ssh_connection_direct(ip: str,
                                ssh_port: int,
                                ssh_user: str,
                                ssh_private_key: str,
                                ssh_control_name: Optional[str] = None,
                                ssh_proxy_command: Optional[str] = None,
                                **kwargs) -> Tuple[bool, str]:
    """Wait for SSH connection using raw sockets, and a SSH connection.

    Using raw socket is more efficient than using SSH command to probe the
    connection, before the SSH connection is ready. We use a actual SSH command
    connection to test the connection, after the raw socket connection is ready
    to make sure the SSH connection is actually ready.

    Returns:
        A tuple of (success, stderr).
    """
    del kwargs  # unused
    assert ssh_proxy_command is None, 'SSH proxy command is not supported.'
    try:
        success = False
        stderr = ''
        with socket.create_connection((ip, ssh_port), timeout=1) as s:
            if s.recv(100).startswith(b'SSH'):
                # Wait for SSH being actually ready, otherwise we may get the
                # following error:
                # "System is booting up. Unprivileged users are not permitted to
                # log in yet".
                success = True
        if success:
            return _wait_ssh_connection_indirect(ip, ssh_port, ssh_user,
                                                 ssh_private_key,
                                                 ssh_control_name,
                                                 ssh_proxy_command)
    except socket.timeout:  # this is the most expected exception
        stderr = f'Timeout: SSH connection to {ip} is not ready.'
    except Exception as e:  # pylint: disable=broad-except
        stderr = f'Error: {common_utils.format_exception(e)}'
    command = _ssh_probe_command(ip, ssh_port, ssh_user, ssh_private_key,
                                 ssh_proxy_command)
    logger.debug(f'Waiting for SSH to {ip}. Try: '
                 f'{_shlex_join(command)}. '
                 f'{stderr}')
    return False, stderr


def _wait_ssh_connection_indirect(ip: str,
                                  ssh_port: int,
                                  ssh_user: str,
                                  ssh_private_key: str,
                                  ssh_control_name: Optional[str] = None,
                                  ssh_proxy_command: Optional[str] = None,
                                  **kwargs) -> Tuple[bool, str]:
    """Wait for SSH connection using SSH command.

    Returns:
        A tuple of (success, stderr).
    """
    del ssh_control_name, kwargs  # unused
    command = _ssh_probe_command(ip, ssh_port, ssh_user, ssh_private_key,
                                 ssh_proxy_command)
    message = f'Waiting for SSH using command: {_shlex_join(command)}'
    logger.debug(message)
    try:
        proc = subprocess.run(command,
                              shell=False,
                              check=False,
                              timeout=10,
                              stdout=subprocess.DEVNULL,
                              stderr=subprocess.PIPE)
        if proc.returncode != 0:
            stderr = proc.stderr.decode('utf-8')
            stderr = f'Error: {stderr}'
            logger.debug(f'{message}{stderr}')
            return False, stderr
    except subprocess.TimeoutExpired as e:
        stderr = f'Error: {str(e)}'
        logger.debug(f'{message}Error: {e}')
        return False, stderr
    return True, ''


def wait_for_ssh(cluster_info: provision_common.ClusterInfo,
                 ssh_credentials: Dict[str, str]):
    """Wait until SSH is ready.

    Raises:
        RuntimeError: If the SSH connection is not ready after timeout.
    """
    if (cluster_info.has_external_ips() and
            ssh_credentials.get('ssh_proxy_command') is None):
        # If we can access public IPs, then it is more efficient to test SSH
        # connection with raw sockets.
        waiter = _wait_ssh_connection_direct
    else:
        # See https://github.com/skypilot-org/skypilot/pull/1512
        waiter = _wait_ssh_connection_indirect
    ip_list = cluster_info.get_feasible_ips()
    port_list = cluster_info.get_ssh_ports()

    timeout = 60 * 10  # 10-min maximum timeout
    start = time.time()
    # use a queue for SSH querying
    ips = collections.deque(ip_list)
    ssh_ports = collections.deque(port_list)
    while ips:
        ip = ips.popleft()
        ssh_port = ssh_ports.popleft()
        success, stderr = waiter(ip, ssh_port, **ssh_credentials)
        if not success:
            ips.append(ip)
            ssh_ports.append(ssh_port)
            if time.time() - start > timeout:
                with ux_utils.print_exception_no_traceback():
                    raise RuntimeError(
                        f'Failed to SSH to {ip} after timeout {timeout}s, with '
                        f'{stderr}')
            logger.debug('Retrying in 1 second...')
            time.sleep(1)


def _post_provision_setup(
        cloud_name: str, cluster_name: resources_utils.ClusterName,
        cluster_yaml: str, provision_record: provision_common.ProvisionRecord,
        custom_resource: Optional[str]) -> provision_common.ClusterInfo:

    config_from_yaml = common_utils.read_yaml(cluster_yaml)
    provider_config = config_from_yaml.get('provider')
    cluster_info = provision.get_cluster_info(cloud_name,
                                              provision_record.region,
                                              cluster_name.name_on_cloud,
                                              provider_config=provider_config)

    logger.debug(
        'Provision record:\n'
        f'{json.dumps(dataclasses.asdict(provision_record), indent=2)}\n'
        'Cluster info:\n'
        f'{json.dumps(dataclasses.asdict(cluster_info), indent=2)}')

    head_instance = cluster_info.get_head_instance()
    if head_instance is None:
        e = RuntimeError(f'Provision failed for cluster {cluster_name!r}. '
                         'Could not find any head instance.')
        setattr(e, 'detailed_reason', str(cluster_info))
        raise e

    # Skip the SSH-related steps
    if cluster_info.num_instances > 1:
        logger.debug('Multiple instances detected, skipping SSH setup.')

    # Skip Docker initialization if not needed
    docker_config = config_from_yaml.get('docker', {})
    if docker_config:
        logger.debug(f'Skipping Docker initialization for {cluster_name!r}.')

    # Skip file mounts and runtime setup if SSH is not desired
    logger.debug(f'Skipping file mounts and runtime setup for {cluster_name!r}.')

    logger.info(f'{colorama.Fore.GREEN}Successfully provisioned cluster: '
                f'{cluster_name}{colorama.Style.RESET_ALL}')
    return cluster_info


def post_provision_runtime_setup(
        cloud_name: str, cluster_name: resources_utils.ClusterName,
        cluster_yaml: str, provision_record: provision_common.ProvisionRecord,
        custom_resource: Optional[str],
        log_dir: str) -> provision_common.ClusterInfo:
    """Run internal setup commands after provisioning and before user setup.

    Here are the steps:
    1. Wait for SSH to be ready.
    2. Mount the cloud credentials, skypilot wheel,
       and other necessary files to the VM.
    3. Run setup commands to install dependencies.
    4. Start ray cluster and skylet.

    Raises:
        RuntimeError: If the setup process encounters any error.
    """
    with provision_logging.setup_provision_logging(log_dir):
        try:
            logger.debug(_TITLE.format('System Setup After Provision'))
            logger.info(_TITLE.format('System Setup After Provision'))
            return _post_provision_setup(cloud_name,
                                         cluster_name,
                                         cluster_yaml=cluster_yaml,
                                         provision_record=provision_record,
                                         custom_resource=custom_resource)
        except Exception:  # pylint: disable=broad-except
            logger.error('*** Failed setting up cluster. ***')
            logger.debug(f'Stacktrace:\n{traceback.format_exc()}')
            with ux_utils.print_exception_no_traceback():
                raise
