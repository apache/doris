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

import argparse
import logging
import jsonpickle
import os
import os.path
import subprocess
import tempfile
import yaml

DORIS_LOCAL_ROOT = "/tmp/doris"
DORIS_HOME = "/opt/apache-doris"

MASTER_FE_ID = 1
FE_HTTP_PORT = 8030
FE_RPC_PORT = 9020
FE_QUERY_PORT = 9030
FE_EDITLOG_PORT = 9010

BE_PORT = 9060
BE_WEBSVR_PORT = 8040
BE_HEARTBEAT_PORT = 9050
BE_BRPC_PORT = 8060


def get_logger(level=logging.INFO):
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        '%(asctime)s - %(filename)s - %(lineno)dL - %(levelname)s - %(message)s'
    )
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(level)

    return logger


LOG = get_logger()


def with_doris_prefix(name):
    return "doris-" + name


def get_cluster_path(cluster_name):
    return os.path.join(DORIS_LOCAL_ROOT, cluster_name)


def exec_shell_command(command, check_ok=True):
    LOG.info("run command: " + command)
    p = subprocess.Popen(command,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    out = p.communicate()[0].decode('utf-8')
    if check_ok:
        assert p.returncode == 0, out
    return p.returncode, out


class Meta(object):

    def __init__(self, cluster_name, subnet_prefix, image):
        self.cluster_name = cluster_name
        self.subnet_prefix = subnet_prefix
        self.image = image
        self.idsets = {
            node_type: IdSet(node_type)
            for node_type in Node.TYPE_ALL
        }

    def add_node(self, node_type, id):
        ids = self.idsets.get(node_type)
        if not ids:
            raise Exception("Unknown node type ".format(node_type))
        return ids.add(id)

    @staticmethod
    def load_cluster_meta(cluster_name):
        path = Meta._get_path(cluster_name)
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            return jsonpickle.loads(f.read())

    def save(self):
        with open(Meta._get_path(self.cluster_name), "w") as f:
            f.write(jsonpickle.dumps(self, indent=2))

    @staticmethod
    def _get_path(cluster_name):
        return os.path.join(get_cluster_path(cluster_name), "meta")


class IdSet(object):

    def __init__(self, node_type):
        self.node_type = node_type
        self.ids = []
        self.next_id = 1

    def add(self, id):
        if not id:
            id = self.next_id
            self.next_id += 1
        if id > 255:
            raise Exception("{} id {} exceed 255".format(self.node_type, id))
        if id not in self.ids:
            self.ids.append(id)
            self.ids.sort()
        return id


class Node(object):
    TYPE_FE = "fe"
    TYPE_BE = "be"
    TYPE_ALL = [TYPE_FE, TYPE_BE]

    @staticmethod
    def new(node_type, meta, id):
        if node_type == Node.TYPE_FE:
            return FE(meta, id)
        elif node_type == Node.TYPE_BE:
            return BE(meta, id)
        else:
            raise Exception("Unknown node type {}".format(node_type))

    def __init__(self, meta, id):
        self.meta = meta
        self.id = id

    def init_dir(self):
        path = self.get_path()
        os.makedirs(path, exist_ok=True)

        # copy config to local
        for dir in ("conf", ):
            if not os.path.exists(os.path.join(path, dir)):
                cmd = "docker run -v {}:/opt/mount --rm --entrypoint cp {}  -r {}/{}/{}/ /opt/mount/".format(
                    path, self.meta.image, DORIS_HOME, self.node_type(), dir)
                exec_shell_command(cmd)

        for sub_dir in self.expose_sub_dirs():
            os.makedirs(os.path.join(path, sub_dir), exist_ok=True)

    def node_type(self):
        raise Exception("No implement")

    def expose_sub_dirs(self):
        return ["conf", "log"]

    def get_name(self):
        return "{}-{}".format(self.node_type(), self.id)

    def get_path(self):
        return os.path.join(get_cluster_path(self.meta.cluster_name),
                            self.get_name())

    def get_ip(self):
        num3 = None
        if self.node_type() == Node.TYPE_FE:
            num3 = 1
        elif self.node_type() == Node.TYPE_BE:
            num3 = 2
        else:
            raise Exception("Unknown node type: {}".format(self.node_type()))
        return "{}.0.{}.{}".format(self.meta.subnet_prefix, num3, self.id)

    def service_name(self):
        return with_doris_prefix("{}-{}".format(self.meta.cluster_name,
                                                self.get_name()))

    def docker_env(self):
        return [
            "MY_IP=" + self.get_ip(),
            "FE_QUERY_PORT=" + str(FE_QUERY_PORT),
            "FE_EDITLOG_PORT=" + str(FE_EDITLOG_PORT),
            "BE_HEARTBEAT_PORT=" + str(BE_HEARTBEAT_PORT),
            "MASTER_FE_IP=" + FE(self.meta, MASTER_FE_ID).get_ip(),
        ]

    def docker_ports(self):
        raise Exception("No implement")

    def docker_volumns(self):
        raise Exception("No implement")

    def docker_depends_on(self):
        if self.node_type() == Node.TYPE_FE and self.id == MASTER_FE_ID:
            return []
        else:
            return [FE(self.meta, MASTER_FE_ID).service_name()]

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
            self.meta.image,
            "networks": {
                with_doris_prefix(self.meta.cluster_name): {
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
                "{}:{}/{}/{}".format(os.path.join(self.get_path(), sub_dir),
                                     DORIS_HOME, self.node_type(), sub_dir)
                for sub_dir in self.expose_sub_dirs()
            ],
        }


class FE(Node):

    def docker_command(self):
        return [
            "bash",
            "{}/fe/bin/init_fe.sh".format(DORIS_HOME),
        ]

    def docker_ports(self):
        return [FE_HTTP_PORT, FE_EDITLOG_PORT, FE_RPC_PORT, FE_QUERY_PORT]

    def node_type(self):
        return Node.TYPE_FE

    def expose_sub_dirs(self):
        return super().expose_sub_dirs() + ["doris-meta"]


class BE(Node):

    def docker_command(self):
        return [
            "bash",
            "{}/be/bin/init_be.sh".format(DORIS_HOME),
        ]

    def docker_ports(self):
        return [BE_WEBSVR_PORT, BE_BRPC_PORT, BE_HEARTBEAT_PORT, BE_PORT]

    def node_type(self):
        return Node.TYPE_BE

    def expose_sub_dirs(self):
        return super().expose_sub_dirs() + ["storage"]


class Cluster(object):

    def __init__(self, meta):
        self.meta = meta

    @staticmethod
    def new_cluster(cluster_name, image):
        if cluster_name:
            path = get_cluster_path(cluster_name)
            if os.path.exists(path):
                raise Exception(
                    "Cluster path {} has exists, maybe a duplicate cluster has exists. " \
                    "After shuting it down and deleting its directory, try again.".format(path))

            else:
                os.makedirs(path)
        else:
            cluster_name = os.path.basename(
                tempfile.mkdtemp("", "", DORIS_LOCAL_ROOT))

        subnet_prefix = Cluster._gen_subnet_prefix()
        meta = Meta(cluster_name, subnet_prefix, image)
        return Cluster(meta)

    @staticmethod
    def _gen_subnet_prefix():
        used_subnet_prefix = {}
        for cluster_name in os.listdir(DORIS_LOCAL_ROOT):
            meta = Meta.load_cluster_meta(cluster_name)
            if meta:
                used_subnet_prefix[meta.subnet_prefix] = True
        for i in range(11, 191):
            if not used_subnet_prefix.get(i, False):
                return i
        raise Exception("Failed to init subnet")

    def get_path(self):
        return get_cluster_path(self.meta.cluster_name)

    def get_cluster_name(self):
        return self.meta.cluster_name

    def add_fe(self, id=None):
        self._add_node(Node.TYPE_FE, id)

    def add_be(self, id=None):
        self._add_node(Node.TYPE_BE, id)

    def _add_node(self, node_type, id):
        id = self.meta.add_node(node_type, id)
        Node.new(node_type, self.meta, id).init_dir()

    def save_meta(self):
        self.meta.save()

    def save_compose(self):
        services = {}
        for node_type, idset in self.meta.idsets.items():
            for id in idset.ids:
                node = Node.new(node_type, self.meta, id)
                services[node.service_name()] = node.compose()

        compose = {
            "version": "3",
            "networks": {
                with_doris_prefix(self.meta.cluster_name): {
                    "driver": "bridge",
                    "ipam": {
                        "config": [{
                            "subnet":
                            "{}.0.0.0/8".format(self.meta.subnet_prefix)
                        }]
                    }
                }
            },
            "services": services,
        }

        with open(self.get_compose_path(), "w") as f:
            f.write(yaml.dump(compose))

    def get_compose_path(self):
        return os.path.join(self.get_path(), "docker-compose.yml")

    def save(self):
        self.save_meta()
        self.save_compose()

    def start(self):
        cmd = "docker-compose -f {} up -d".format(self.get_compose_path())
        exec_shell_command(cmd)


def run(args):
    cluster = Cluster.new_cluster(args.name, args.IMAGE)
    LOG.info("Add cluster {}".format(cluster.get_cluster_name()))
    for i in range(args.fe):
        cluster.add_fe()
    for i in range(args.be):
        cluster.add_be()
    cluster.save()
    cluster.start()
    LOG.info("Run cluster {} succ".format(cluster.get_cluster_name()))


def parse_args():
    ap = argparse.ArgumentParser(description="")
    sub_aps = ap.add_subparsers(dest="command")

    ap_run = sub_aps.add_parser("new", help="new a doris cluster")
    ap_run.add_argument("IMAGE", help="specify docker image")
    ap_run.add_argument("--fe", type=int, default=3, help="specify fe count")
    ap_run.add_argument("--be", type=int, default=3, help="specify be count")
    ap_run.add_argument("--name", default="", help="specific cluster name")

    return ap.format_usage(), ap.format_help(), ap.parse_args()


def main():
    usage, _, args = parse_args()
    if args.command == "new":
        return run(args)
    else:
        print(usage)
        return -1


if __name__ == '__main__':
    main()
