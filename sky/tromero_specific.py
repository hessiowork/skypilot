import sky
from sky.cli import _make_task_or_dag_from_entrypoint_with_overrides
from sky.clouds import service_catalog
from sky import clouds as sky_clouds
from sky import jobs as managed_jobs


def get_gpu_details(accelerator_str=None, all=True, cloud=None, region=None, all_regions=False):
    # Handle validations as in the original CLI command
    if region is not None and cloud is None:
        raise ValueError('The --region option is only valid when the --cloud option is set.')

    if all_regions and accelerator_str is None:
        raise ValueError('The --all-regions option is only valid when an accelerator is specified.')

    if all_regions and region is not None:
        raise ValueError('--all-regions and --region options cannot be used simultaneously.')

    cloud_obj = sky_clouds.CLOUD_REGISTRY.from_str(cloud) if cloud else None
    service_catalog.validate_region_zone(region, None, clouds=cloud)

    # Prepare to collect GPU details
    gpu_details = []

    clouds_to_list = cloud
    if cloud is None:
        clouds_to_list = [
            c for c in service_catalog.ALL_CLOUDS if c != 'kubernetes'
        ]
    # Retrieve data using methods analogous to the ones in the original CLI command
    results = service_catalog.list_accelerators(
        gpus_only=True,
        name_filter=None,
        quantity_filter=None,
        region_filter=region,
        clouds=clouds_to_list,
        case_sensitive=False,
        all_regions=all_regions
    )

    for gpu, items in results.items():
        for item in items:
            if ('A100' in gpu or 'H100' in gpu) and item.accelerator_count == 4 and (item.cloud == 'RunPod' or
                                                                                     item.cloud == 'Paperspace'):
                detail = {
                    'GPU': gpu,
                    'Quantity': item.accelerator_count,
                    'Cloud': item.cloud,
                    'Hourly Price': round(item.price, 2) if item.price is not None else float('inf'),
                    'Hourly Spot Price': f"${item.spot_price:.2f}" if item.spot_price is not None else "N/A",
                }
                gpu_details.append(detail)
    sorted_gpu_details = sorted(gpu_details, key=lambda x: x['Hourly Price'])
    for detail in sorted_gpu_details:
        print(detail)

    return gpu_details


def launch_job(
    entrypoint,
    name=None,
    workdir=None,
    cloud=None,
    region=None,
    zone=None,
    gpus=None,
    cpus=None,
    memory=None,
    instance_type=None,
    num_nodes=None,
    use_spot=None,
    image_id=None,
    job_recovery=None,
    env_file=None,
    env=None,
    disk_size=None,
    disk_tier=None,
    ports=None,
    detach_run=False,
    retry_until_up=True,
    confirm=True
):
    # Merge environment variables from file and direct specification
    if env_file:
        env = {**env_file, **dict(env)} if env else env_file

    # Create task or DAG from the entrypoint, with all overrides applied
    task_or_dag = _make_task_or_dag_from_entrypoint_with_overrides(
        entrypoint=entrypoint,
        name=name,
        workdir=workdir,
        cloud=cloud,
        region=region,
        zone=zone,
        gpus=gpus,
        cpus=cpus,
        memory=memory,
        instance_type=instance_type,
        num_nodes=num_nodes,
        use_spot=use_spot,
        image_id=image_id,
        env=env,
        disk_size=disk_size,
        disk_tier=disk_tier,
        ports=ports,
        job_recovery=job_recovery,
    )

    # Convert single task to DAG if not already a DAG
    if not isinstance(task_or_dag, sky.Dag):
        dag = sky.Dag()
        dag.add(task_or_dag)
        dag.name = task_or_dag.name if task_or_dag.name else name
    else:
        dag = task_or_dag

    # Optionally print the DAG setup before launching
    # if confirm:
    #     user_confirm = input(f'Launching a managed job {dag.name!r}. Proceed? (y/n): ')
    #     if user_confirm.lower() != 'y':
    #         print("Launch aborted by the user.")
    #         return

    # Launch the job
    managed_jobs.launch(
        dag,
        name,
        detach_run=detach_run,
        retry_until_up=retry_until_up
    )
    print(f'Managed job {dag.name!r} has been launched.')

    # Return the DAG or some identifier if needed
    return dag
