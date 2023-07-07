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
import bisect
import logging
import json
import jsonpickle
import os
import os.path
import subprocess
import sys
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

ID_LIMIT = 10000


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

    def __init__(self, cluster_name, subnet, image):
        self.cluster_name = cluster_name
        self.subnet = subnet
        self.image = image
        self.idsets = {
            node_type: IdSet(node_type)
            for node_type in Node.TYPE_ALL
        }

    def add_node(self, node_type, id):
        idset = self.idsets.get(node_type)
        if not idset:
            raise Exception("Unknown node type {}".format(node_type))
        return idset.add(id)

    def contain_node(self, node_type, id):
        idset = self.idsets.get(node_type, None)
        return idset and idset.contains(id)

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
        if id > ID_LIMIT:
            raise Exception("{} id {} exceed {}".format(
                self.node_type, id, ID_LIMIT))
        if not self.contains(id):
            bisect.insort(self.ids, id)
        return id

    def contains(self, id):
        i = bisect.bisect_left(self.ids, id)
        return i < len(self.ids) and self.ids[i] == id


class Node(object):
    TYPE_FE = "fe"
    TYPE_BE = "be"
    TYPE_ALL = [TYPE_FE, TYPE_BE]

    @staticmethod
    def new(meta, node_type, id):
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

        for sub_dir in self.expose_sub_dirs() + [
                "data",
        ]:
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
        seq = self.id
        num4_size = 200
        seq += num4_size
        if self.node_type() == Node.TYPE_FE:
            seq += 0 * ID_LIMIT
        elif self.node_type() == Node.TYPE_BE:
            seq += 1 * ID_LIMIT
        else:
            seq += 2 * ID_LIMIT
        return "{}.{}.{}".format(self.meta.subnet, int(seq / num4_size),
                                 seq % num4_size)

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
            ] + [
                "{}/data:/data".format(self.get_path()),
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
    def new(cluster_name, image):
        subnet = Cluster._gen_subnet()
        meta = Meta(cluster_name, subnet, image)
        os.makedirs(get_cluster_path(cluster_name), exist_ok=True)
        return Cluster(meta)

    @staticmethod
    def load(cluster_name):
        if not cluster_name:
            raise Exception("cluster is empty")
        cluster_path = get_cluster_path(cluster_name)
        if not os.path.exists(cluster_path):
            raise Exception(
                "cluster directory {} not exists.".format(cluster_path))
        meta_path = os.path.join(cluster_path, "meta")
        if not os.path.exists(meta_path):
            raise Exception(
                "cluster meta file {} not exists.".format(meta_path))
        meta = Meta.load_cluster_meta(cluster_name)
        if not meta:
            raise Exception(
                "load cluster meta failed, please check file {}".format(
                    meta_path))
        return Cluster(meta)

    @staticmethod
    def _gen_subnet():
        used_subnet = {}

        def read_docker_subnets():
            code, output = exec_shell_command(
                "docker network ls | awk '{print $1}' | sed 1d",
                check_ok=False)
            if code != 0:
                return
            network_ids = " ".join(net.strip() for net in output.splitlines()
                                   if net.strip())
            if not network_ids:
                return
            code, output = exec_shell_command("docker network inspect " +
                                              network_ids,
                                              check_ok=False)
            if code != 0:
                return
            networks = None
            try:
                networks = json.loads(output)
            except:
                return
            for net in networks:
                ipam = net.get("IPAM", None)
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
                    if subnet.endswith("/8"):
                        for i in range(256):
                            used_subnet["{}.{}".format(num1, i)] = True
                    else:
                        used_subnet["{}.{}".format(num1, num2)] = True

        def read_doris_subnets():
            if not os.path.exists(DORIS_LOCAL_ROOT):
                return
            for cluster_name in os.listdir(DORIS_LOCAL_ROOT):
                meta = Meta.load_cluster_meta(cluster_name)
                if meta:
                    used_subnet[meta.subnet] = True

        read_docker_subnets()
        read_doris_subnets()

        LOG.debug("used_subnet: {}".format(used_subnet))
        for i in range(128, 192):
            for j in range(256):
                subnet = "{}.{}".format(i, j)
                if not used_subnet.get(subnet, None):
                    return subnet

        raise Exception("Failed to init subnet")

    def get_image(self):
        return self.meta.image

    def set_image(self, image):
        self.meta.image = image

    def get_path(self):
        return get_cluster_path(self.meta.cluster_name)

    def get_cluster_name(self):
        return self.meta.cluster_name

    def get_node_num(self, node_type):
        return len(self.meta.idsets.get(node_type, []))

    def add_node(self, node_type, id=None):
        id = self.meta.add_node(node_type, id)
        Node.new(self.meta, node_type, id).init_dir()
        return id

    def save_meta(self):
        self.meta.save()

    def save_compose(self):
        services = {}
        for node_type, idset in self.meta.idsets.items():
            for id in idset.ids:
                node = Node.new(self.meta, node_type, id)
                services[node.service_name()] = node.compose()

        compose = {
            "version": "3",
            "networks": {
                with_doris_prefix(self.meta.cluster_name): {
                    "driver": "bridge",
                    "ipam": {
                        "config": [{
                            "subnet":
                            "{}.0.0/16".format(self.meta.subnet),
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

    def run_docker_compose_with_node(self, cmd, node_type, id):
        node = self._get_node(node_type, id)
        self.run_docker_compose("{} {}".format(cmd, node.service_name()))

    def run_docker_compose(self, cmd):
        return exec_shell_command("docker-compose -f {} {}".format(
            self.get_compose_path(), cmd))

    def _get_node(self, node_type, id):
        if not self.meta.contain_node(node_type, id):
            raise Exception("No found {} node with id {}".format(
                node_type, id))
        return Node.new(self.meta, node_type, id)


def new(args):
    if not args.NAME:
        raise Exception("need specify cluster name")
    if not args.IMAGE:
        raise Exception("need specify image")
    cluster_path = get_cluster_path(args.NAME)
    if os.path.exists(cluster_path):
        raise Exception("cluster path {} exists, shut the previous cluster " \
                "and remove this directory, then try again.".format(cluster_path))
    cluster = Cluster.new(args.NAME, args.IMAGE)
    for i in range(args.fe):
        cluster.add_node(Node.TYPE_FE)
    for i in range(args.be):
        cluster.add_node(Node.TYPE_BE)
    cluster.save()
    LOG.info("Create cluster {} succ, cluster path is {}".format(
        args.NAME, cluster.get_path()))
    if args.no_up:
        LOG.info("Not run cluster cause specific --no-up")
    else:
        cluster.run_docker_compose("up -d --remove-orphans")
        LOG.info("Run cluster {} succ".format(cluster.get_cluster_name()))


def up(args):
    cluster = Cluster.load(args.NAME)
    if args.image:
        cluster.set_image(args.image)
        cluster.save()
    cluster.run_docker_compose("up -d --remove-orphans")
    LOG.info("Run cluster {} succ".format(cluster.get_cluster_name()))


def down(args):
    cluster = Cluster.load(args.NAME)
    cluster.run_docker_compose("down")
    LOG.info("Shutdown cluster {} succ".format(args.NAME))


def add(args):
    cluster = Cluster.load(args.NAME)
    id = cluster.add_node(args.NODE_TYPE)
    cluster.save()
    service_name = Node.new(cluster.meta, args.NODE_TYPE, id).service_name()
    if args.no_up:
        cluster.run_docker_compose("up --no-start {} ".format(service_name))
        LOG.info("Not up new add {} with id {} cause specific --no-up".format(
            args.NODE_TYPE, id))
    else:
        cluster.run_docker_compose("up -d {}".format(service_name))
        LOG.info("Up new add {} with id {} succ".format(args.NODE_TYPE, id))


def start(args):
    cluster = Cluster.load(args.NAME)
    cluster.run_docker_compose_with_node("start", args.NODE_TYPE, args.ID)
    LOG.info("Start {} with id {} succ".format(args.NODE_TYPE, args.ID))


def stop(args):
    cluster = Cluster.load(args.NAME)
    cluster.run_docker_compose_with_node("stop", args.NODE_TYPE, args.ID)
    LOG.info("Stop {} with id {} succ".format(args.NODE_TYPE, args.ID))


def restart(args):
    cluster = Cluster.load(args.NAME)
    cluster.run_docker_compose_with_node("restart", args.NODE_TYPE, args.ID)
    LOG.info("Restart {} with id {} succ".format(args.NODE_TYPE, args.ID))


def get_parser_bool_action(is_store_true):
    if sys.version_info.major == 3 and sys.version_info.minor >= 9:
        return argparse.BooleanOptionalAction
    else:
        return "store_true" if is_store_true else "store_false"


def cmd_for_node_help(action):
    return "{} a {} node".format(action, " ".join(Node.TYPE_ALL))


def parse_args():
    ap = argparse.ArgumentParser(description="")
    sub_aps = ap.add_subparsers(dest="command")

    ap_new = sub_aps.add_parser("new", help="create a new doris cluster")
    ap_new.add_argument("NAME", help="specific cluster name")
    ap_new.add_argument("IMAGE", help="specify docker image")
    ap_new.add_argument("--fe",
                        type=int,
                        default=3,
                        help="specify fe count, default is 3")
    ap_new.add_argument("--be",
                        type=int,
                        default=3,
                        help="specify be count, default is 3")
    ap_new.add_argument("--no-up",
                        default=False,
                        action=get_parser_bool_action(True),
                        help="do not run cluster, only create")

    ap_up = sub_aps.add_parser(
        "up", help="re run a doris cluster, no clean data and log")
    ap_up.add_argument("NAME", default="", help="specific cluster name")
    ap_up.add_argument("--image", default="", help="specify docker image")

    ap_down = sub_aps.add_parser("down", help="shutdown a cluster")
    ap_down.add_argument("NAME", help="specify cluster name")

    ap_add = sub_aps.add_parser("add", help=cmd_for_node_help("add"))
    ap_add.add_argument("NAME", help="specify cluster name")
    ap_add.add_argument("NODE_TYPE",
                        choices=Node.TYPE_ALL,
                        help="specify node type")
    ap_add.add_argument("--no-up",
                        default=False,
                        action=get_parser_bool_action(True),
                        help="do not up this node, only create")

    ap_start = sub_aps.add_parser("start", help=cmd_for_node_help("start"))
    ap_start.add_argument("NAME", help="specify cluster name")
    ap_start.add_argument("NODE_TYPE",
                          choices=Node.TYPE_ALL,
                          help="specify node type")
    ap_start.add_argument("ID", type=int, help="specify node id")

    ap_stop = sub_aps.add_parser("stop", help=cmd_for_node_help("stop"))
    ap_stop.add_argument("NAME", help="specify cluster name")
    ap_stop.add_argument("NODE_TYPE",
                         choices=Node.TYPE_ALL,
                         help="specify node type")
    ap_stop.add_argument("ID", type=int, help="specify node id")

    ap_restart = sub_aps.add_parser("restart",
                                    help=cmd_for_node_help("restart"))
    ap_restart.add_argument("NAME", help="specify cluster name")
    ap_restart.add_argument("NODE_TYPE",
                            choices=Node.TYPE_ALL,
                            help="specify node type")
    ap_restart.add_argument("ID", type=int, help="specify node id")

    return ap.format_usage(), ap.format_help(), ap.parse_args()


def main():
    usage, _, args = parse_args()
    if args.command == "new":
        return new(args)
    elif args.command == "up":
        return up(args)
    elif args.command == "down":
        return down(args)
    elif args.command == "add":
        return add(args)
    elif args.command == "start":
        return start(args)
    elif args.command == "stop":
        return stop(args)
    elif args.command == "restart":
        return restart(args)
    else:
        print(usage)
        return -1


if __name__ == '__main__':
    main()
