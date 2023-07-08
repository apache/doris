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

import json
import jsonpickle
import os
import os.path
import yaml
import utils

DOCKER_DORIS_PATH = "/opt/apache-doris"
LOCAL_DORIS_PATH = os.getenv("LOCAL_DORIS_PATH")
if not LOCAL_DORIS_PATH:
    LOCAL_DORIS_PATH = "/tmp/doris"

LOCAL_RESOURCE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "resource")
DOCKER_RESOURCE_PATH = os.path.join(DOCKER_DORIS_PATH, "resource")

MASTER_FE_ID = 1
FE_HTTP_PORT = 8030
FE_RPC_PORT = 9020
FE_QUERY_PORT = 9030
FE_EDITLOG_PORT = 9010

BE_PORT = 9060
BE_WEBSVR_PORT = 8040
BE_HEARTBEAT_PORT = 9050
BE_BRPC_PORT = 8060

ID_LIMIT = 10000

LOG = utils.get_logger()

REMOVE_TYPE_DROP = 1
REMOVE_TYPE_DECOMMISSION = 2


def with_doris_prefix(name):
    return "doris-" + name


def get_cluster_path(cluster_name):
    return os.path.join(LOCAL_DORIS_PATH, cluster_name)


def gen_subnet_prefix16():
    used_subnet = utils.get_docker_subnets_prefix16()
    if os.path.exists(LOCAL_DORIS_PATH):
        for cluster_name in os.listdir(LOCAL_DORIS_PATH):
            try:
                cluster = Cluster.load(cluster_name)
                used_subnet[cluster.subnet] = True
            except:
                pass

    for i in range(128, 192):
        for j in range(256):
            subnet = "{}.{}".format(i, j)
            if not used_subnet.get(subnet, False):
                return subnet

    raise Exception("Failed to gen subnet")


class NodeMeta(object):

    def __init__(self, image):
        self.image = image


class Group(object):

    def __init__(self, node_type):
        self.node_type = node_type
        self.nodes = {}  # id : NodeMeta
        self.next_id = 1

    def add(self, id, image):
        assert image
        if not id:
            id = self.next_id
            self.next_id += 1
        if self.get_node(id):
            raise Exception(
                "Failed to add {} with id {}, id has exists".format(
                    self.node_type, id))
        if id > ID_LIMIT:
            raise Exception(
                "Failed to add {} with id {}, id exceeds {}".format(
                    self.node_type, id, ID_LIMIT))
        self.nodes[id] = NodeMeta(image)

        return id

    def get_node_num(self):
        return len(self.nodes)

    def get_all_nodes(self):
        return self.nodes

    def get_node(self, id):
        return self.nodes.get(id, None)

    def on_loaded(self):
        nodes = {}
        for id, node in self.nodes.items():
            nodes[int(id)] = node
        self.nodes = nodes


class Node(object):
    TYPE_FE = "fe"
    TYPE_BE = "be"
    TYPE_ALL = [TYPE_FE, TYPE_BE]

    def __init__(self, cluster_name, id, subnet, meta):
        self.cluster_name = cluster_name
        self.id = id
        self.subnet = subnet
        self.meta = meta

    @staticmethod
    def new(cluster_name, node_type, id, subnet, meta):
        if node_type == Node.TYPE_FE:
            return FE(cluster_name, id, subnet, meta)
        elif node_type == Node.TYPE_BE:
            return BE(cluster_name, id, subnet, meta)
        else:
            raise Exception("Unknown node type {}".format(node_type))

    def init_dir(self):
        path = self.get_path()
        os.makedirs(path, exist_ok=True)

        # copy config to local
        conf_dir = os.path.join(path, "conf")
        if not os.path.exists(conf_dir) or utils.is_dir_empty(conf_dir):
            utils.copy_image_directory(
                self.get_image(), "{}/{}/conf".format(DOCKER_DORIS_PATH,
                                                      self.node_type()),
                conf_dir)
            assert not utils.is_dir_empty(conf_dir), "conf directory {} is empty, " \
                    "check doris path in image is correct".format(conf_dir)

    def node_type(self):
        raise Exception("No implemented")

    def expose_sub_dirs(self):
        return ["conf", "log"]

    def get_name(self):
        return "{}-{}".format(self.node_type(), self.id)

    def get_path(self):
        return os.path.join(get_cluster_path(self.cluster_name),
                            self.get_name())

    def get_image(self):
        return self.meta.image

    def set_image(self, image):
        self.meta.image = image

    def get_ip(self):
        seq = self.id
        part4_size = 200
        seq += part4_size
        if self.node_type() == Node.TYPE_FE:
            seq += 0 * ID_LIMIT
        elif self.node_type() == Node.TYPE_BE:
            seq += 1 * ID_LIMIT
        else:
            seq += 2 * ID_LIMIT
        return "{}.{}.{}".format(self.subnet, int(seq / part4_size),
                                 seq % part4_size)

    def service_name(self):
        return with_doris_prefix("{}-{}".format(self.cluster_name,
                                                self.get_name()))

    def docker_env(self):
        return {
            "MY_IP": self.get_ip(),
            "FE_QUERY_PORT": FE_QUERY_PORT,
            "FE_EDITLOG_PORT": FE_EDITLOG_PORT,
            "BE_HEARTBEAT_PORT": BE_HEARTBEAT_PORT,
            "MASTER_FE_IP": self._get_master_fe().get_ip(),
            "DOCKER_DORIS_PATH": DOCKER_DORIS_PATH,
        }

    def _get_master_fe(self):
        return FE(self.cluster_name, MASTER_FE_ID, self.subnet,
                  NodeMeta(self.get_image()))

    def docker_ports(self):
        raise Exception("No implemented")

    def docker_volumns(self):
        raise Exception("No implemented")

    def docker_depends_on(self):
        if self.node_type() == Node.TYPE_FE and self.id == MASTER_FE_ID:
            return []
        else:
            return [self._get_master_fe().service_name()]

    def remove_command(self, remove_type):
        raise Exception("No implemented")

    def compose(self):
        return {
            "cap_add": ["SYS_PTRACE"],
            "hostname":
            self.get_name(),
            "container_name":
            self.service_name(),
            "command":
            self.docker_command(),
            "environment":
            self.docker_env(),
            "depends_on":
            self.docker_depends_on(),
            "image":
            self.get_image(),
            "networks": {
                with_doris_prefix(self.cluster_name): {
                    "ipv4_address": self.get_ip(),
                }
            },
            "ports":
            self.docker_ports(),
            "ulimits": {
                "core": -1
            },
            "security_opt": ["seccomp:unconfined"],
            "volumes": [
                "{}:{}/{}/{}".format(os.path.join(self.get_path(),
                                                  sub_dir), DOCKER_DORIS_PATH,
                                     self.node_type(), sub_dir)
                for sub_dir in self.expose_sub_dirs()
            ] + [
                "{}:{}:ro".format(LOCAL_RESOURCE_PATH, DOCKER_RESOURCE_PATH),
            ],
        }


class FE(Node):

    def docker_command(self):
        return [
            "bash",
            "{}/be/bin/init_be.sh".format(DOCKER_DORIS_PATH),
            #os.path.join(DOCKER_RESOURCE_PATH, "fe_util.sh"),
        ]

    def docker_ports(self):
        return [FE_HTTP_PORT, FE_EDITLOG_PORT, FE_RPC_PORT, FE_QUERY_PORT]

    def node_type(self):
        return Node.TYPE_FE

    def expose_sub_dirs(self):
        return super().expose_sub_dirs() + ["doris-meta"]

    def remove_command(self, remove_type):
        cmd = "bash {}/fe_util.sh --drop"
        if drop or decommission:
            cmd += " --drop"
        return cmd


class BE(Node):

    def docker_command(self):
        return [
            "bash",
            "{}/be/bin/init_be.sh".format(DOCKER_DORIS_PATH),
            #os.path.join(DOCKER_RESOURCE_PATH, "init_be.sh"),
        ]

    def docker_ports(self):
        return [BE_WEBSVR_PORT, BE_BRPC_PORT, BE_HEARTBEAT_PORT, BE_PORT]

    def node_type(self):
        return Node.TYPE_BE

    def expose_sub_dirs(self):
        return super().expose_sub_dirs() + ["storage"]

    def stop_command(self, drop, decommission):
        cmd = "bash {}/be_util.sh --stop"
        if drop:
            cmd += " --drop"
        elif decommission:
            cmd += " --decommission"
        return cmd


class Cluster(object):

    def __init__(self, name, subnet, image):
        self.name = name
        self.subnet = subnet
        self.image = image
        self.groups = {
            node_type: Group(node_type)
            for node_type in Node.TYPE_ALL
        }

    @staticmethod
    def new(name, image):
        subnet = gen_subnet_prefix16()
        cluster = Cluster(name, subnet, image)
        os.makedirs(cluster.get_path(), exist_ok=True)
        return cluster

    @staticmethod
    def load(name):
        if not name:
            raise Exception("Failed to load cluster, name is empty")
        path = get_cluster_path(name)
        if not os.path.exists(path):
            raise Exception(
                "Failed to load cluster, its directory {} not exists.".format(
                    path))
        meta_path = Cluster._get_meta_file(name)
        if not os.path.exists(meta_path):
            raise Exception(
                "Failed to load cluster, its meta file {} not exists.".format(
                    meta_path))
        with open(meta_path, "r") as f:
            cluster = jsonpickle.loads(f.read())
            for group in cluster.groups.values():
                group.on_loaded()
            return cluster

    @staticmethod
    def _get_meta_file(name):
        return os.path.join(get_cluster_path(name), "meta")

    def get_image(self):
        return self.image

    # cluster's nodes will update image too if cluster update.
    def set_image(self, image):
        self.image = image
        for _, group in self.groups.items():
            for _, node_meta in group.nodes.items():
                node_meta.image = image

    def get_path(self):
        return get_cluster_path(self.name)

    def get_group(self, node_type):
        group = self.groups.get(node_type, None)
        if not group:
            raise Exception("Unknown node_type: {}".format(node_type))
        return group

    def set_node_num(self, node_type, num):
        old_num = self.get_group(node_type).get_node_num()
        for i in range(num - old_num):
            self.add(node_type)
        for i in range(old_num - num):
            self.remove(node_type)

    def get_node(self, node_type, id):
        group = self.get_group(node_type)
        meta = group.get_node(id)
        if not meta:
            raise Exception("No found {} with id {}".format(node_type, id))
        return Node.new(self.name, node_type, id, self.subnet, meta)

    def get_all_nodes(self, node_type):
        group = self.groups.get(node_type, None)
        if not group:
            raise Exception("Unknown node_type: {}".format(node_type))
        return [
            Node.new(self.name, node_type, id, self.subnet, meta)
            for id, meta in group.get_all_nodes().items()
        ]

    def add(self, node_type, id=None, image=None):
        if not image:
            image = self.image

        id = self.get_group(node_type).add(id, image)
        node = self.get_node(node_type, id)
        node.init_dir()
        return node

    def remove(self, node_type, id=None):
        pass

    def save_meta(self):
        with open(Cluster._get_meta_file(self.name), "w") as f:
            f.write(jsonpickle.dumps(self, indent=2))

    def save_compose(self):
        services = {}
        for node_type in self.groups.keys():
            for node in self.get_all_nodes(node_type):
                services[node.service_name()] = node.compose()

        compose = {
            "version": "3",
            "networks": {
                with_doris_prefix(self.name): {
                    "driver": "bridge",
                    "ipam": {
                        "config": [{
                            "subnet": "{}.0.0/16".format(self.subnet),
                        }]
                    }
                }
            },
            "services": services,
        }

        with open(self.get_compose_file(), "w") as f:
            f.write(yaml.dump(compose))

    def get_compose_file(self):
        return os.path.join(self.get_path(), "docker-compose.yml")

    def save(self):
        self.save_meta()
        self.save_compose()
