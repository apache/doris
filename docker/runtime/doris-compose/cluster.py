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

import configparser
import filelock
import getpass
import hashlib
import jsonpickle
import os
import os.path
import utils
import time

DOCKER_DORIS_PATH = "/opt/apache-doris"
LOCAL_DORIS_PATH = os.getenv("LOCAL_DORIS_PATH", "/tmp/doris")

# an integer between 128 and 191, generally no need to set
DORIS_SUBNET_START = os.getenv("DORIS_SUBNET_START")

LOCAL_RESOURCE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "resource")
DOCKER_RESOURCE_PATH = os.path.join(DOCKER_DORIS_PATH, "resource")
CLOUD_CFG_FILE = os.getenv("DORIS_CLOUD_CFG_FILE",
                           os.path.join(LOCAL_DORIS_PATH, 'cloud.ini'))

FE_HTTP_PORT = 8030
FE_RPC_PORT = 9020
FE_QUERY_PORT = 9030
FE_EDITLOG_PORT = 9010
FE_JAVA_DBG_PORT = 5005
FE_ARROW_FLIGHT_SQL_PORT = 8070

BE_PORT = 9060
BE_WEBSVR_PORT = 8040
BE_HEARTBEAT_PORT = 9050
BE_BRPC_PORT = 8060
BE_ARROW_FLIGHT_SQL_PORT = 8050

FDB_PORT = 4500

MS_PORT = 5000

ID_LIMIT = 10000

IP_PART4_SIZE = 200

CLUSTER_ID = "12345678"

LOG = utils.get_logger()


def get_cluster_path(cluster_name):
    return os.path.join(LOCAL_DORIS_PATH, cluster_name)


def get_node_name(node_type, id):
    return "{}-{}".format(node_type, id)


def get_node_path(cluster_name, node_type, id):
    return os.path.join(get_cluster_path(cluster_name),
                        get_node_name(node_type, id))


def get_compose_file(cluster_name):
    return os.path.join(get_cluster_path(cluster_name), "docker-compose.yml")


def get_status_path(cluster_name):
    return os.path.join(get_cluster_path(cluster_name), "status")


def get_master_fe_addr_path(cluster_name):
    return get_status_path(cluster_name) + "/master_fe_query_addr"


def get_all_cluster_names():
    if not os.path.exists(LOCAL_DORIS_PATH):
        return []
    else:
        return [
            subdir for subdir in os.listdir(LOCAL_DORIS_PATH)
            if os.path.isdir(os.path.join(LOCAL_DORIS_PATH, subdir))
        ]


def gen_subnet_prefix16():
    used_subnet = utils.get_docker_subnets_prefix16()
    for cluster_name in get_all_cluster_names():
        try:
            cluster = Cluster.load(cluster_name)
            used_subnet[cluster.subnet] = True
        except:
            pass

    subnet_begin = 128
    subnet_end = 192

    subnet_part_1 = None
    subnet_part_2 = None
    if DORIS_SUBNET_START:
        subnet_part_1 = int(DORIS_SUBNET_START)
        subnet_part_2 = 0
    else:
        m = hashlib.md5()
        m.update(getpass.getuser().encode("utf-8"))
        hash_val = int(m.hexdigest(), 16)
        # want subnet part ii to be a small num, just less than 100, so don't use 256 here.
        small_width = 100
        slot_num = (subnet_end - subnet_begin) * small_width
        idx = hash_val % slot_num
        if idx < 0:
            idx += slot_num
        subnet_part_1 = subnet_begin + int(idx / small_width)
        subnet_part_2 = idx % small_width

    intervals = [
        [(subnet_part_1, subnet_part_1 + 1), (subnet_part_2, 256)],
        [(subnet_part_1 + 1, subnet_end), (0, 256)],
        [(subnet_begin, subnet_part_1), (0, 256)],
        [(subnet_part_1, subnet_part_1 + 1), (0, subnet_part_2)],
    ]
    for interval in intervals:
        for i in range(interval[0][0], interval[0][1]):
            for j in range(interval[1][0], interval[1][1]):
                subnet = "{}.{}".format(i, j)
                if not used_subnet.get(subnet, False):
                    return subnet

    raise Exception("Failed to gen subnet")


def get_master_fe_endpoint(cluster_name, wait_master_fe_query_addr_file=False):
    if os.path.exists(Cluster._get_meta_file(cluster_name)):
        master_fe_query_addr_file = get_master_fe_addr_path(cluster_name)
        max_retries = 10 if wait_master_fe_query_addr_file else 0
        i = 0
        while True:
            if os.path.exists(master_fe_query_addr_file):
                with open(master_fe_query_addr_file, "r") as f:
                    return f.read().strip()
            i += 1
            if i < max_retries:
                time.sleep(1)
            else:
                break
    try:
        cluster = Cluster.load(cluster_name)
        LOG.info("master file not exist, master ip get from node 1")
        if cluster.is_host_network():
            return cluster.remote_master_fe
        else:
            master_fe = cluster.get_node(Node.TYPE_FE, 1)
            return "{}:{}".format(master_fe.get_ip(),
                                  master_fe.meta["ports"]["query_port"])
    except:
        return ""


def get_node_seq(node_type, id):
    seq = id
    seq += IP_PART4_SIZE
    if node_type == Node.TYPE_FE:
        seq += 0 * ID_LIMIT
    elif node_type == Node.TYPE_BE:
        seq += 1 * ID_LIMIT
    elif node_type == Node.TYPE_MS:
        seq += 2 * ID_LIMIT
    elif node_type == Node.TYPE_RECYCLE:
        seq += 3 * ID_LIMIT
    elif node_type == Node.TYPE_FDB:
        seq += 4 * ID_LIMIT
    else:
        seq += 5 * ID_LIMIT
    return seq


class Group(object):

    def __init__(self, node_type):
        self.node_type = node_type
        self.nodes = {}  # id : NodeMeta
        self.next_id = 1

    def add(self, id, node_meta):
        assert node_meta["image"]
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
        self.nodes[id] = node_meta

        return id

    def remove(self, id):
        self.nodes.pop(id, None)

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


class NodeNetInfo(object):

    def __init__(self, type, id, ip, ports):
        self.type = type
        self.id = id
        self.ip = ip
        self.ports = ports


class Node(object):
    TYPE_FE = "fe"
    TYPE_BE = "be"
    TYPE_MS = "ms"
    TYPE_RECYCLE = "recycle"
    TYPE_FDB = "fdb"
    TYPE_ALL = [TYPE_FE, TYPE_BE, TYPE_MS, TYPE_RECYCLE, TYPE_FDB]

    def __init__(self, cluster, id, meta):
        self.cluster = cluster
        self.id = id
        self.meta = meta

    @staticmethod
    def new(cluster, node_type, id, meta):
        if node_type == Node.TYPE_FE:
            return FE(cluster, id, meta)
        elif node_type == Node.TYPE_BE:
            return BE(cluster, id, meta)
        elif node_type == Node.TYPE_MS:
            return MS(cluster, id, meta)
        elif node_type == Node.TYPE_RECYCLE:
            return RECYCLE(cluster, id, meta)
        elif node_type == Node.TYPE_FDB:
            return FDB(cluster, id, meta)
        else:
            raise Exception("Unknown node type {}".format(node_type))

    # only run once at create the node, later restart or upgrade image will not run
    def init(self):
        self.init_ports()
        self.init_conf()

    def init_ports(self):
        if self.cluster.is_host_network():
            self.meta["ports"] = dict(
                (port_name, utils.get_avail_port())
                for port_name in self.get_default_named_ports().keys())
        else:
            self.meta["ports"] = self.get_default_named_ports()

    def init_conf(self):
        path = self.get_path()
        os.makedirs(path, exist_ok=True)

        # copy config to local
        conf_dir = os.path.join(path, "conf")
        if not os.path.exists(conf_dir) or utils.is_dir_empty(conf_dir):
            self.copy_conf_to_local(conf_dir)
            assert not utils.is_dir_empty(conf_dir), "conf directory {} is empty, " \
                    "check doris path in image is correct".format(conf_dir)
            utils.enable_dir_with_rw_perm(conf_dir)
            config = self.get_add_init_config()
            if config:
                with open(os.path.join(conf_dir, self.conf_file_name()),
                          "a") as f:
                    f.write("\n")
                    f.write("#### start doris-compose add config ####\n\n")
                    for item in config:
                        f.write(item + "\n")
                    f.write("\n#### end doris-compose add config ####\n")
        for sub_dir in self.expose_sub_dirs():
            os.makedirs(os.path.join(path, sub_dir), exist_ok=True)

    def copy_conf_to_local(self, local_conf_dir):
        utils.copy_image_directory(self.get_image(),
                                   "{}/conf".format(self.docker_home_dir()),
                                   local_conf_dir)

    def is_fe(self):
        return self.node_type() == Node.TYPE_FE

    def is_be(self):
        return self.node_type() == Node.TYPE_BE

    def conf_file_name(self):
        return self.node_type() + ".conf"

    def node_type(self):
        raise Exception("No implemented")

    def expose_sub_dirs(self):
        return ["conf", "log"]

    def get_name(self):
        return get_node_name(self.node_type(), self.id)

    def get_path(self):
        return get_node_path(self.cluster.name, self.node_type(), self.id)

    def get_image(self):
        return self.meta["image"]

    def set_image(self, image):
        self.meta["image"] = image

    def get_ip(self):
        if self.cluster.is_host_network():
            # this is a remote node
            if self.meta.get("is_remote", False):
                return self.cluster.remote_master_fe.split(":")[0]
            else:
                return self.cluster.local_network_ip
        else:
            seq = get_node_seq(self.node_type(), self.id)
            return "{}.{}.{}".format(self.cluster.subnet,
                                     int(seq / IP_PART4_SIZE),
                                     seq % IP_PART4_SIZE)

    def get_tde_ak(self):
        return self.cluster.tde_ak

    def get_tde_sk(self):
        return self.cluster.tde_sk

    def get_default_named_ports(self):
        # port_name : default_port
        # the port_name come from fe.conf, be.conf, cloud.conf, etc
        return {}

    @staticmethod
    def get_id_from_ip(ip):
        pos2 = ip.rfind(".")
        pos1 = ip.rfind(".", 0, pos2 - 1)
        num3 = int(ip[pos1 + 1:pos2])
        num4 = int(ip[pos2 + 1:])
        seq = num3 * IP_PART4_SIZE + num4
        while seq > ID_LIMIT:
            seq -= ID_LIMIT
        seq -= IP_PART4_SIZE
        return seq

    def service_name(self):
        return utils.with_doris_prefix("{}-{}".format(self.cluster.name,
                                                      self.get_name()))

    def docker_env(self):
        enable_coverage = self.cluster.coverage_dir

        envs = {
            "MY_IP": self.get_ip(),
            "MY_ID": self.id,
            "MY_TYPE": self.node_type(),
            "DORIS_HOME": os.path.join(self.docker_home_dir()),
            "STOP_GRACE": 1 if enable_coverage else 0,
            "IS_CLOUD": 1 if self.cluster.is_cloud else 0,
            "SQL_MODE_NODE_MGR": 1 if self.cluster.sql_mode_node_mgr else 0,
            "TDE_AK": self.get_tde_ak(),
            "TDE_SK": self.get_tde_sk(),
        }

        if self.cluster.is_cloud:
            envs["META_SERVICE_ENDPOINT"] = self.cluster.get_meta_server_addr()

        # run as host user
        if not self.cluster.is_root_user:
            envs["HOST_USER"] = getpass.getuser()
            envs["HOST_UID"] = os.getuid()
            envs["HOST_GID"] = os.getgid()

        if enable_coverage:
            outfile = "{}/coverage/{}-coverage-{}-{}".format(
                DOCKER_DORIS_PATH, self.node_type(), self.cluster.name,
                self.id)
            if self.node_type() == Node.TYPE_FE:
                envs["JACOCO_COVERAGE_OPT"] = "-javaagent:/jacoco/lib/jacocoagent.jar" \
                    "=excludes=org.apache.doris.thrift:org.apache.doris.proto:org.apache.parquet.format" \
                    ":com.aliyun*:com.amazonaws*:org.apache.hadoop.hive.metastore:org.apache.parquet.format," \
                    "output=file,append=true,destfile=" + outfile
            elif self.node_type() == Node.TYPE_BE:
                envs["LLVM_PROFILE_FILE_PREFIX"] = outfile

        return envs

    def entrypoint(self):
        if self.start_script():
            return [
                "bash",
                os.path.join(DOCKER_RESOURCE_PATH, "entrypoint.sh")
            ] + self.start_script()
        else:
            return None

    def get_add_init_config(self):
        cfg = []
        if self.cluster.is_host_network():
            cfg.append(f"priority_networks = {self.cluster.local_network_ip}")
            cfg += [
                f"{port_name} = {port}"
                for port_name, port in self.meta["ports"].items()
            ]
        return cfg

    def docker_ports(self):
        return list(self.get_default_named_ports().values())

    def docker_home_dir(self):
        raise Exception("No implemented")

    def compose(self):
        volumes = [
            "{}:{}/{}".format(os.path.join(self.get_path(), sub_dir),
                              self.docker_home_dir(), sub_dir)
            for sub_dir in self.expose_sub_dirs()
        ] + [
            "{}:{}:ro".format(LOCAL_RESOURCE_PATH, DOCKER_RESOURCE_PATH),
            "{}:{}/status".format(get_status_path(self.cluster.name),
                                  self.docker_home_dir()),
        ] + [
            "{0}:{0}:ro".format(path)
            for path in ("/etc/localtime", "/etc/timezone",
                         "/usr/share/zoneinfo") if os.path.exists(path)
        ]

        if self.cluster.coverage_dir:
            volumes.append("{}:{}/coverage".format(self.cluster.coverage_dir,
                                                   DOCKER_DORIS_PATH))

        content = {
            "cap_add": ["SYS_PTRACE"],
            "container_name": self.service_name(),
            "environment": self.docker_env(),
            "image": self.get_image(),
            "ulimits": {
                "core": -1
            },
            "security_opt": ["seccomp:unconfined"],
            "volumes": volumes,
        }

        extra_hosts = []
        if self.cluster.is_host_network():
            content["network_mode"] = "host"
        else:
            content["hostname"] = self.get_name()
            content["networks"] = {
                utils.with_doris_prefix(self.cluster.name): {
                    "ipv4_address": self.get_ip(),
                }
            }
            extra_hosts.extend([
                "{}:{}".format(node.get_name(), node.get_ip())
                for node in self.cluster.get_all_nodes()
            ])
            content["ports"] = self.docker_ports()
        user_hosts = getattr(self.cluster, "extra_hosts", [])
        if user_hosts:
            extra_hosts.extend(user_hosts)
        if extra_hosts:
            content["extra_hosts"] = extra_hosts

        if self.entrypoint():
            content["entrypoint"] = self.entrypoint()

        return content


class FE(Node):

    def init(self):
        # for cloud mode, fe is follower or observer, default is observer
        self.meta[
            "is_cloud_follower"] = self.cluster.is_cloud and self.cluster.fe_follower
        super().init()

    def get_add_init_config(self):
        cfg = super().get_add_init_config()
        if self.cluster.fe_config:
            cfg += self.cluster.fe_config
        if self.cluster.is_cloud:
            cfg += [
                "meta_service_endpoint = {}".format(
                    self.cluster.get_meta_server_addr()),
                "deploy_mode = cloud",
            ]

            if self.cluster.sql_mode_node_mgr:
                cfg += [
                    "cluster_id = " + CLUSTER_ID,
                ]
            else:
                cfg += [
                    "cloud_unique_id = " + self.cloud_unique_id(),
                ]

        with open("{}/conf/{}".format(self.get_path(), self.conf_file_name()),
                  "r") as f:
            java_debug_port = self.meta["ports"]["java_debug_port"]
            parser = configparser.ConfigParser()
            parser.read_string('[dummy_section]\n' + f.read())
            section = parser['dummy_section']
            for key in ("JAVA_OPTS", "JAVA_OPTS_FOR_JDK_17"):
                value = section.get(key)
                if value:
                    value = value.strip().strip('"')
                    if key == "JAVA_OPTS":
                        # java 8
                        cfg.append(
                            f"{key} = \"{value} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address={java_debug_port}\""
                        )
                    else:
                        # JAVA_OPTS_FOR_JDK_17
                        # >= java 9
                        cfg.append(
                            f"{key} = \"{value} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:{java_debug_port}\""
                        )

        return cfg

    def docker_env(self):
        envs = super().docker_env()
        if self.cluster.is_cloud:
            envs["CLOUD_UNIQUE_ID"] = self.cloud_unique_id()
            if self.meta["is_cloud_follower"]:
                envs["IS_FE_FOLLOWER"] = 1
        envs["MY_QUERY_PORT"] = self.meta["ports"]["query_port"]
        envs["MY_EDITLOG_PORT"] = self.meta["ports"]["edit_log_port"]
        return envs

    def get_default_named_ports(self):
        return {
            "http_port": FE_HTTP_PORT,
            "rpc_port": FE_RPC_PORT,
            "query_port": FE_QUERY_PORT,
            "edit_log_port": FE_EDITLOG_PORT,
            "arrow_flight_sql_port": FE_ARROW_FLIGHT_SQL_PORT,
            "java_debug_port": FE_JAVA_DBG_PORT,
        }

    def cloud_unique_id(self):
        return "sql_server_{}".format(self.id)

    def start_script(self):
        return ["init_fe.sh"]

    def docker_home_dir(self):
        return os.path.join(DOCKER_DORIS_PATH, "fe")

    def node_type(self):
        return Node.TYPE_FE

    def expose_sub_dirs(self):
        return super().expose_sub_dirs() + ["doris-meta"]


class BE(Node):

    def init(self):
        super().init()
        if self.cluster.is_cloud:
            self.meta["cluster_name"] = self.cluster.be_cluster
        self.init_disk(self.cluster.be_disks)

    def get_add_init_config(self):
        cfg = super().get_add_init_config()
        cfg += [
            'enable_java_support = false',
        ]
        if self.cluster.be_config:
            cfg += self.cluster.be_config
        if self.cluster.is_cloud:
            cfg += [
                'tmp_file_dirs = [ {"path":"./storage/tmp","max_cache_bytes":10240000, "max_upload_bytes":10240000}]',
                'enable_file_cache = true',
                'file_cache_path = [ {{"path": "{}/storage/file_cache", "total_size":53687091200, "query_limit": 10737418240}}]'
                .format(self.docker_home_dir()),
                "deploy_mode = cloud",
            ]

            if self.cluster.be_metaservice_endpoint:
                cfg += [
                    "meta_service_endpoint = {}".format(
                        self.cluster.get_meta_server_addr()),
                ]
            if self.cluster.be_cluster_id:
                cfg += [
                    "cluster_id = " + CLUSTER_ID,
                ]
            if not self.cluster.sql_mode_node_mgr:
                cfg += [
                    "cloud_unique_id = " + self.cloud_unique_id(),
                ]
        return cfg

    def init_disk(self, be_disks):
        path = self.get_path()
        dirs = []
        dir_descs = []
        index = 0
        for disks in be_disks:
            parts = disks.split(",")
            if len(parts) != 1 and len(parts) != 2:
                raise Exception("be disks has error: {}".format(disks))
            type_and_num = parts[0].split("=")
            if len(type_and_num) != 2:
                raise Exception("be disks has error: {}".format(disks))
            tp = type_and_num[0].strip().upper()
            if tp != "HDD" and tp != "SSD":
                raise Exception(
                    "error be disk type: '{}', should be 'HDD' or 'SSD'".
                    format(tp))
            num = int(type_and_num[1].strip())
            capactity = int(parts[1].strip()) if len(parts) >= 2 else -1
            capactity_desc = "_{}gb".format(capactity) if capactity > 0 else ""

            for i in range(num):
                index += 1
                dir_name = "{}{}.{}".format(index, capactity_desc, tp)
                dirs.append("{}/storage/{}".format(path, dir_name))
                dir_descs.append("${{DORIS_HOME}}/storage/{}{}".format(
                    dir_name,
                    ",capacity:" + str(capactity) if capactity > 0 else ""))

        for dir in dirs:
            os.makedirs(dir, exist_ok=True)

        os.makedirs(path + "/storage/file_cache", exist_ok=True)

        with open("{}/conf/{}".format(path, self.conf_file_name()), "a") as f:
            storage_root_path = ";".join(dir_descs) if dir_descs else '""'
            f.write("\nstorage_root_path = {}\n".format(storage_root_path))

    def start_script(self):
        return ["init_be.sh"]

    def docker_env(self):
        envs = super().docker_env()
        envs["MY_HEARTBEAT_PORT"] = self.meta["ports"][
            "heartbeat_service_port"]
        if self.cluster.is_cloud:
            envs["CLOUD_UNIQUE_ID"] = self.cloud_unique_id()
            envs["REG_BE_TO_MS"] = 1 if self.cluster.reg_be else 0
            envs["CLUSTER_NAME"] = self.meta["cluster_name"]
        return envs

    def get_default_named_ports(self):
        return {
            "be_port": BE_PORT,
            "webserver_port": BE_WEBSVR_PORT,
            "heartbeat_service_port": BE_HEARTBEAT_PORT,
            "brpc_port": BE_BRPC_PORT,
            "arrow_flight_sql_port": BE_ARROW_FLIGHT_SQL_PORT,
        }

    def cloud_unique_id(self):
        return "compute_node_{}".format(self.id)

    def docker_home_dir(self):
        return os.path.join(DOCKER_DORIS_PATH, "be")

    def node_type(self):
        return Node.TYPE_BE

    def expose_sub_dirs(self):
        return super().expose_sub_dirs() + ["storage"]


class CLOUD(Node):

    def get_add_init_config(self):
        cfg = super().get_add_init_config()
        cfg.append("fdb_cluster = " + self.cluster.get_fdb_cluster())
        return cfg

    def docker_home_dir(self):
        return os.path.join(DOCKER_DORIS_PATH, "ms")

    def get_default_named_ports(self):
        return {
            "brpc_listen_port": MS_PORT,
        }

    def conf_file_name(self):
        for file in os.listdir(os.path.join(self.get_path(), "conf")):
            if file == "doris_cloud.conf" or file == "selectdb_cloud.conf":
                return file
        return "Not found conf file for ms or recycler"


class MS(CLOUD):

    def get_add_init_config(self):
        cfg = super().get_add_init_config()
        if self.cluster.ms_config:
            cfg += self.cluster.ms_config
        return cfg

    def start_script(self):
        return ["init_cloud.sh", "--meta-service"]

    def node_type(self):
        return Node.TYPE_MS

    def docker_env(self):
        envs = super().docker_env()
        for key, value in self.cluster.cloud_store_config.items():
            envs[key] = value
        return envs


class RECYCLE(CLOUD):

    def get_add_init_config(self):
        cfg = super().get_add_init_config()
        if self.cluster.recycle_config:
            cfg += self.cluster.recycle_config
        return cfg

    def start_script(self):
        return ["init_cloud.sh", "--recycler"]

    def node_type(self):
        return Node.TYPE_RECYCLE


class FDB(Node):

    def get_add_init_config(self):
        return []

    def copy_conf_to_local(self, local_conf_dir):
        os.makedirs(local_conf_dir, exist_ok=True)
        with open(os.path.join(LOCAL_RESOURCE_PATH, "fdb.conf"),
                  "r") as read_file:
            with open(os.path.join(local_conf_dir, self.conf_file_name()),
                      "w") as f:
                publish_addr = "{}:{}".format(self.get_ip(),
                                              self.meta["ports"]["fdb_port"])
                f.write(read_file.read().replace("${PUBLISH-ADDRESS}",
                                                 publish_addr))

        with open(os.path.join(local_conf_dir, "fdb.cluster"), "w") as f:
            f.write(self.cluster.get_fdb_cluster())

    def start_script(self):
        return ["init_fdb.sh"]

    def docker_home_dir(self):
        return os.path.join(DOCKER_DORIS_PATH, "fdb")

    def get_default_named_ports(self):
        return {"fdb_port": FDB_PORT}

    def node_type(self):
        return Node.TYPE_FDB

    def expose_sub_dirs(self):
        return super().expose_sub_dirs() + ["data"]


class Cluster(object):

    def __init__(self, name, subnet, image, is_cloud, is_root_user, fe_config,
                 be_config, ms_config, recycle_config, remote_master_fe,
                 local_network_ip, fe_follower, be_disks, be_cluster, reg_be,
                 extra_hosts, coverage_dir, cloud_store_config,
                 sql_mode_node_mgr, be_metaservice_endpoint, be_cluster_id, tde_ak, tde_sk):
        self.name = name
        self.subnet = subnet
        self.image = image
        self.is_cloud = is_cloud
        self.is_root_user = is_root_user
        self.fe_config = fe_config
        self.be_config = be_config
        self.ms_config = ms_config
        self.recycle_config = recycle_config
        self.remote_master_fe = remote_master_fe
        self.local_network_ip = local_network_ip
        self.fe_follower = fe_follower
        self.be_disks = be_disks
        self.be_cluster = be_cluster
        self.reg_be = reg_be
        self.extra_hosts = extra_hosts
        self.coverage_dir = coverage_dir
        self.cloud_store_config = cloud_store_config
        self.groups = {
            node_type: Group(node_type)
            for node_type in Node.TYPE_ALL
        }
        if self.remote_master_fe:
            # preserve fe id = 1 for the remote master:fe
            self.groups[Node.TYPE_FE].next_id = 2
        self.sql_mode_node_mgr = sql_mode_node_mgr
        self.be_metaservice_endpoint = be_metaservice_endpoint
        self.be_cluster_id = be_cluster_id
        self.tde_ak = tde_ak
        self.tde_sk = tde_sk

    @staticmethod
    def new(name, image, is_cloud, is_root_user, fe_config, be_config,
            ms_config, recycle_config, remote_master_fe, local_network_ip,
            fe_follower, be_disks, be_cluster, reg_be, extra_hosts,
            coverage_dir, cloud_store_config, sql_mode_node_mgr,
            be_metaservice_endpoint, be_cluster_id, tde_ak, tde_sk):
        if not os.path.exists(LOCAL_DORIS_PATH):
            os.makedirs(LOCAL_DORIS_PATH, exist_ok=True)
            os.chmod(LOCAL_DORIS_PATH, 0o777)
        lock_file = os.path.join(LOCAL_DORIS_PATH, "lock")
        with filelock.FileLock(lock_file):
            if os.getuid() == utils.get_path_uid(lock_file):
                os.chmod(lock_file, 0o666)
            subnet = gen_subnet_prefix16() if not remote_master_fe else ""
            cluster = Cluster(name, subnet, image, is_cloud, is_root_user,
                              fe_config, be_config, ms_config, recycle_config,
                              remote_master_fe, local_network_ip, fe_follower,
                              be_disks, be_cluster, reg_be, extra_hosts,
                              coverage_dir, cloud_store_config,
                              sql_mode_node_mgr, be_metaservice_endpoint,
                              be_cluster_id, tde_ak, tde_sk)
            os.makedirs(cluster.get_path(), exist_ok=True)
            os.makedirs(get_status_path(name), exist_ok=True)
            cluster._save_meta()
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

    def is_host_network(self):
        return self.remote_master_fe

    def get_remote_fe_node(self):
        if not self.is_host_network():
            return None
        meta = {
            "image": "",
            "is_remote": True,
            "ports": {
                "query_port": int(self.remote_master_fe.split(":")[1])
            },
        }
        return Node.new(self, Node.TYPE_FE, 1, meta)

    def get_image(self):
        return self.image

    # cluster's nodes will update image too if cluster update.
    def set_image(self, image):
        self.image = image
        for node_type, group in self.groups.items():
            if node_type == Node.TYPE_FDB:
                continue
            for _, node_meta in group.nodes.items():
                node_meta["image"] = image

    def get_path(self):
        return get_cluster_path(self.name)

    def get_group(self, node_type):
        group = self.groups.get(node_type, None)
        if not group:
            raise Exception("Unknown node_type: {}".format(node_type))
        return group

    def get_all_node_net_infos(self):
        return [
            NodeNetInfo(node.node_type(), node.id, node.get_ip(),
                        node.meta["ports"])
            for node in self.get_all_nodes(None, True)
        ]

    def get_node(self, node_type, id, include_remote=False):
        group = self.get_group(node_type)
        meta = group.get_node(id)
        if not meta:
            if include_remote and node_type == Node.TYPE_FE:
                node = self.get_remote_fe_node()
                if node and node.id == id:
                    return node
            raise Exception("No found {} with id {}".format(node_type, id))
        return Node.new(self, node_type, id, meta)

    def get_all_nodes(self, node_type=None, include_remote=False):
        if node_type is None:
            nodes = []
            for nt, group in self.groups.items():
                for id, meta in group.get_all_nodes().items():
                    nodes.append(Node.new(self, nt, id, meta))
            if include_remote:
                node = self.get_remote_fe_node()
                if node:
                    nodes.append(node)
            return nodes

        group = self.groups.get(node_type, None)
        if not group:
            raise Exception("Unknown node_type: {}".format(node_type))
        nodes = [
            Node.new(self, node_type, id, meta)
            for id, meta in group.get_all_nodes().items()
        ]
        if include_remote:
            node = self.get_remote_fe_node()
            if node:
                nodes.append(node)
        return nodes

    def get_all_nodes_num(self):
        num = 0
        for group in self.groups.values():
            num += group.get_node_num()
        return num

    def add(self, node_type, id=None):
        node_meta = {}
        node_meta["image"] = self.image
        id = self.get_group(node_type).add(id, node_meta)
        node = self.get_node(node_type, id)
        if not os.path.exists(node.get_path()):
            node.init()

        return node

    def get_fdb_cluster(self):
        fdb = self.get_node(Node.TYPE_FDB, 1)
        return "123456:123456@{}:{}".format(fdb.get_ip(),
                                            fdb.meta["ports"]["fdb_port"])

    def get_meta_server_addr(self):
        meta_server = self.get_node(Node.TYPE_MS, 1)
        return "{}:{}".format(meta_server.get_ip(),
                              meta_server.meta["ports"]["brpc_listen_port"])

    def get_recycle_addr(self):
        recycler = self.get_node(Node.TYPE_RECYCLE, 1)
        return "{}:{}".format(recycler.get_ip(),
                              recycler.meta["ports"]["brpc_listen_port"])

    def remove(self, node_type, id):
        group = self.get_group(node_type)
        group.remove(id)

    def save(self):
        self._save_meta()
        self._save_compose()

    def _save_meta(self):
        with open(Cluster._get_meta_file(self.name), "w") as f:
            f.write(jsonpickle.dumps(self, indent=2))

    def _save_compose(self):
        services = {}
        for node_type in self.groups.keys():
            for node in self.get_all_nodes(node_type):
                services[node.service_name()] = node.compose()
        compose = {
            "version": "3",
            "services": services,
        }
        if not self.is_host_network():
            compose["networks"] = {
                utils.with_doris_prefix(self.name): {
                    "driver": "bridge",
                    "ipam": {
                        "config": [{
                            "subnet": "{}.0.0/16".format(self.subnet),
                        }]
                    },
                },
            }

        utils.write_compose_file(self.get_compose_file(), compose)

    def get_compose_file(self):
        global get_compose_file
        return get_compose_file(self.name)
