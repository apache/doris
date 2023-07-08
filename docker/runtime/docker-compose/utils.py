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
import logging
import os
import subprocess


def get_logger(name=None):
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

    return logger


LOG = get_logger()


def is_dir_empty(dir):
    return False if os.listdir(dir) else True


def exec_shell_command(command, ignore_errors=False):
    LOG.info("Exec command: {}".format(command))
    p = subprocess.Popen(command,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    out = p.communicate()[0].decode('utf-8')
    if not ignore_errors:
        assert p.returncode == 0, out
    return p.returncode, out


def exec_docker_compose_command(compose_file,
                                command,
                                options=None,
                                services=None):
    compose_cmd = "docker-compose -f {}  {}  {} {}".format(
        compose_file, command, " ".join(options) if options else "",
        " ".join(services) if services else "")
    return exec_shell_command(compose_cmd)


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
