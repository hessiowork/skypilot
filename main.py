import time
import string
import random

import sky
from sky import Task, Resources

from dotenv import load_dotenv
import os

from sky.tromero_specific import get_gpu_details

gpu_list = get_gpu_details()

# Load environment variables from .env file
load_dotenv()

docker_username = os.getenv('docker_username')
docker_password = os.getenv('docker_password')
image_name = os.getenv('image_name')

my_task = Task(
    docker_image=image_name,
    setup=f"docker login -u {docker_username} -p {docker_password}",
    run=f"docker run -e {image_name}",
)

my_task.set_resources(Resources(accelerators={gpu_list[0]["GPU"]: 4}))

# initializing size of string
N = 7

first_char = random.choice(string.ascii_uppercase)
remaining_chars = ''.join(random.choices(string.ascii_uppercase + string.digits, k=N - 1))
random_name = first_char + remaining_chars

try:
    sky_launch = sky.launch(my_task, cluster_name=f"{random_name}-cluster", retry_until_up=True,
                            image_name="test_image_name", detach_run=True, detach_setup=True)
    print(f"Launched task: {sky_launch}", flush=True)
except Exception as e:
    print(f"Failed to launch task: {e}", flush=True)

count = 30
while count > 30:
    print(sky.status())
    if sky.status() == "idle":
        break
    count -= 1
    time.sleep(10)
print(sky.status())

# task = sky.Task(run='echo hello SkyPilot')
# task.set_resources(
#     sky.Resources(cloud=sky.RunPod(), accelerators='A100:4'))
# sky.launch(task, cluster_name='my-cluster')
time.sleep(100)
# sky.down(cluster_name=f"{random_name}-cluster")
# print("Cluster deleted")
# sky.down(cluster_name='li0rakw-cluster')
# sky.down(cluster_name='my-cluster')
