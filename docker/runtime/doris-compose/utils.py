# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import docker
import json
import logging
import os
import pwd
import subprocess
import time
import yaml

DORIS_PREFIX = "doris-"

LOG = None

ENABLE_LOG = True


class Timer(object):

    def __init__(self):
        self.start = time.time()
        self.canceled = False

    def show(self):
        if not self.canceled:
            LOG.info("=== Total run time: {} s".format(
                int(time.time() - self.start)))

    def cancel(self):
        self.canceled = True


def set_enable_log(enabled):
    global ENABLE_LOG
    ENABLE_LOG = enabled
    get_logger().disabled = not enabled


def is_enable_log():
    return ENABLE_LOG


def get_logger(name=None):
    global LOG
    if LOG != None:
        return LOG

    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        formatter = logging.Formatter(
            '%(asctime)s - %(filename)s - %(lineno)dL - %(levelname)s - %(message)s'
        )
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.setLevel(logging.INFO)

    LOG = logger

    return logger


get_logger()


def render_red(s):
    return "\x1B[31m" + str(s) + "\x1B[0m"


def render_green(s):
    return "\x1B[32m" + str(s) + "\x1B[0m"


def render_yellow(s):
    return "\x1B[33m" + str(s) + "\x1B[0m"


def render_blue(s):
    return "\x1B[34m" + str(s) + "\x1B[0m"


def with_doris_prefix(name):
    return DORIS_PREFIX + name


def parse_service_name(service_name):
    import cluster
    if not service_name or not service_name.startswith(DORIS_PREFIX):
        return None, None, None
    pos2 = service_name.rfind("-")
    if pos2 < 0:
        return None, None, None
    id = None
    try:
        id = int(service_name[pos2 + 1:])
    except:
        return None, None, None
    pos1 = service_name.rfind("-", len(DORIS_PREFIX), pos2 - 1)
    if pos1 < 0:
        return None, None, None
    node_type = service_name[pos1 + 1:pos2]
    if node_type not in cluster.Node.TYPE_ALL:
        return None, None, None
    return service_name[len(DORIS_PREFIX):pos1], node_type, id


def get_map_ports(container):
    return {
        int(innner.replace("/tcp", "")): int(outer[0]["HostPort"])
        for innner, outer in container.attrs.get("NetworkSettings", {}).get(
            "Ports", {}).items()
    }


def is_container_running(container):
    return container.status == "running"


# return all doris containers when cluster_names is empty
def get_doris_containers(cluster_names):
    if cluster_names:
        if type(cluster_names) == type(""):
            filter_names = "{}{}-*".format(DORIS_PREFIX, cluster_names)
        else:
            filter_names = "|".join([
                "{}{}-*".format(DORIS_PREFIX, name) for name in cluster_names
            ])
    else:
        filter_names = "{}*".format(DORIS_PREFIX)

    clusters = {}
    client = docker.client.from_env()
    containers = client.containers.list(filters={"name": filter_names})
    for container in containers:
        cluster_name, _, _ = parse_service_name(container.name)
        if not cluster_name:
            continue
        if cluster_names and cluster_name not in cluster_names:
            continue
        if cluster_name not in clusters:
            clusters[cluster_name] = []
        clusters[cluster_name].append(container)
    return clusters


def get_doris_running_containers(cluster_name):
    return {
        container.name: container
        for container in get_doris_containers(cluster_name).get(
            cluster_name, []) if is_container_running(container)
    }


def remove_docker_network(cluster_name):
    client = docker.client.from_env()
    for network in client.networks.list(
            names=[cluster_name + "_" + with_doris_prefix(cluster_name)]):
        network.remove()


def is_dir_empty(dir):
    return False if os.listdir(dir) else True


def exec_shell_command(command, ignore_errors=False, output_real_time=False):
    LOG.info("Exec command: {}".format(command))
    p = subprocess.Popen(command,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    out = ''
    exitcode = None
    if output_real_time:
        while p.poll() is None:
            s = p.stdout.readline().decode('utf-8')
            if ENABLE_LOG and s.rstrip():
                print(s.rstrip())
            out += s
        exitcode = p.wait()
    else:
        out = p.communicate()[0].decode('utf-8')
        exitcode = p.returncode
        if ENABLE_LOG and out:
            print(out)
    if not ignore_errors:
        assert exitcode == 0, out
    return exitcode, out


def exec_docker_compose_command(compose_file,
                                command,
                                options=None,
                                nodes=None,
                                user_command=None,
                                output_real_time=False):
    if nodes != None and not nodes:
        return 0, "Skip"

    compose_cmd = "docker-compose -f {}  {}  {} {} {}".format(
        compose_file, command, " ".join(options) if options else "",
        " ".join([node.service_name() for node in nodes]) if nodes else "",
        user_command if user_command else "")

    return exec_shell_command(compose_cmd, output_real_time=output_real_time)


def get_docker_subnets_prefix16():
    subnet_prefixes = {}
    client = docker.from_env()
    for network in client.networks.list():
        if not network.attrs:
            continue
        ipam = network.attrs.get("IPAM", None)
        if not ipam:
            continue
        configs = ipam.get("Config", None)
        if not configs:
            continue
        for config in configs:
            subnet = config.get("Subnet", None)
            if not subnet:
                continue
            pos1 = subnet.find(".")
            if pos1 <= 0:
                continue
            pos2 = subnet.find(".", pos1 + 1)
            if pos2 <= 0:
                continue
            num1 = subnet[0:pos1]
            num2 = subnet[pos1 + 1:pos2]
            network_part_len = 16
            pos = subnet.find("/")
            if pos != -1:
                network_part_len = int(subnet[pos + 1:])
            if network_part_len < 16:
                for i in range(256):
                    subnet_prefixes["{}.{}".format(num1, i)] = True
            else:
                subnet_prefixes["{}.{}".format(num1, num2)] = True

    LOG.debug("Get docker subnet prefixes: {}".format(subnet_prefixes))

    return subnet_prefixes


def copy_image_directory(image, image_dir, local_dir):
    client = docker.from_env()
    volumes = ["{}:/opt/mount".format(local_dir)]
    if image_dir.endswith("/"):
        image_dir += "."
    elif not image_dir.endswith("."):
        image_dir += "/."
    client.containers.run(
        image,
        remove=True,
        volumes=volumes,
        entrypoint="cp -r  {}  /opt/mount/".format(image_dir))


def enable_dir_with_rw_perm(dir):
    if not os.path.exists(dir):
        return
    client = docker.client.from_env()
    client.containers.run("ubuntu",
                          remove=True,
                          volumes=["{}:/opt/mount".format(dir)],
                          entrypoint="chmod a+rw -R {}".format("/opt/mount"))


def get_path_owner(path):
    try:
        return pwd.getpwuid(os.stat(path).st_uid).pw_name
    except:
        return ""


def get_path_uid(path):
    try:
        return os.stat(path).st_uid
    except:
        return ""


def read_compose_file(file):
    with open(file, "r") as f:
        return yaml.safe_load(f.read())


def write_compose_file(file, compose):
    with open(file, "w") as f:
        f.write(yaml.dump(compose))


def pretty_json(json_data):
    return json.dumps(json_data, indent=4, sort_keys=True)


def is_true(val):
    return str(val) == "true" or str(val) == "1"


def escape_null(val):
    return "" if val == "\\N" else val
