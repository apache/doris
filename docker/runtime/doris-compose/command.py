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
import cluster as CLUSTER
import database
import dateutil.parser
import utils
import os
import os.path
import prettytable
import shutil
import sys
import time

LOG = utils.get_logger()


# return for_all, related_nodes, related_node_num
def get_ids_related_nodes(cluster,
                          fe_ids,
                          be_ids,
                          ms_ids,
                          recycle_ids,
                          fdb_ids,
                          ignore_not_exists=False):
    if fe_ids is None and be_ids is None and ms_ids is None and recycle_ids is None and fdb_ids is None:
        return True, None, cluster.get_all_nodes_num()

    def get_ids_related_nodes_with_type(node_type, ids):
        if ids is None:
            return []
        if not ids:
            return cluster.get_all_nodes(node_type)
        else:
            nodes = []
            for id in ids:
                try:
                    nodes.append(cluster.get_node(node_type, id))
                except Exception as e:
                    if ignore_not_exists:
                        LOG.warning(
                            utils.render_yellow(
                                "Not found {} with id {}".format(
                                    node_type, id)))
                    else:
                        raise e
            return nodes

    type_ids = [
        (CLUSTER.Node.TYPE_FE, fe_ids),
        (CLUSTER.Node.TYPE_BE, be_ids),
        (CLUSTER.Node.TYPE_MS, ms_ids),
        (CLUSTER.Node.TYPE_RECYCLE, recycle_ids),
        (CLUSTER.Node.TYPE_FDB, fdb_ids),
    ]

    nodes = []
    for node_type, ids in type_ids:
        nodes.extend(get_ids_related_nodes_with_type(node_type, ids))

    related_node_num = len(nodes)

    return len(nodes) == cluster.get_all_nodes_num(), nodes, len(nodes)


class Command(object):

    def __init__(self, name):
        self.name = name

    def print_use_time(self):
        return True

    def add_parser(self, args_parsers):
        raise Exception("No implemented")

    def run(self, args):
        raise Exception("No implemented")

    def _add_parser_output_json(self, parser):
        parser.add_argument("--output-json",
                            default=False,
                            action=self._get_parser_bool_action(True),
                            help="output as json, and don't print log.")

    def _add_parser_ids_args(self, parser):
        group = parser.add_argument_group("for existing nodes",
                                          "apply to the existing nodes.")
        group.add_argument("--fe-id", nargs="*", type=int, help="Specify up fe ids, support multiple ids, " \
                "if specific --fe-id but not specific ids, apply to all fe. Example: '--fe-id 2 3' will select fe-2 and fe-3.")
        group.add_argument("--be-id", nargs="*", type=int, help="Specify up be ids, support multiple ids, " \
                "if specific --be-id but not specific ids, apply to all be. Example: '--be-id' will select all backends.")
        group.add_argument(
            "--ms-id",
            nargs="*",
            type=int,
            help=
            "Specify up ms ids, support multiple ids. Only use in cloud cluster."
        )
        group.add_argument(
            "--recycle-id",
            nargs="*",
            type=int,
            help=
            "Specify up recycle ids, support multiple ids. Only use in cloud cluster."
        )
        group.add_argument(
            "--fdb-id",
            nargs="*",
            type=int,
            help=
            "Specify up fdb ids, support multiple ids. Only use in cloud cluster."
        )

    def _get_parser_bool_action(self, is_store_true):
        if self._support_boolean_action():
            return argparse.BooleanOptionalAction
        else:
            return "store_true" if is_store_true else "store_false"

    def _support_boolean_action(self):
        return sys.version_info.major == 3 and sys.version_info.minor >= 9


class SimpleCommand(Command):

    def __init__(self, command, help):
        super().__init__(command)
        self.command = command
        self.help = help

    def add_parser(self, args_parsers):
        help = self.help + " If none of --fe-id, --be-id, --ms-id, --recycle-id, --fdb-id is specific, "\
                "then apply to all containers."
        parser = args_parsers.add_parser(self.command, help=help)
        parser.add_argument("NAME", help="Specify cluster name.")
        self._add_parser_ids_args(parser)
        self._add_parser_output_json(parser)

    def run(self, args):
        cluster = CLUSTER.Cluster.load(args.NAME)
        _, related_nodes, related_node_num = get_ids_related_nodes(
            cluster, args.fe_id, args.be_id, args.ms_id, args.recycle_id,
            args.fdb_id)
        utils.exec_docker_compose_command(cluster.get_compose_file(),
                                          self.command,
                                          nodes=related_nodes)
        show_cmd = self.command[0].upper() + self.command[1:]
        LOG.info(
            utils.render_green("{} succ, total related node num {}".format(
                show_cmd, related_node_num)))


class UpCommand(Command):

    def add_parser(self, args_parsers):
        parser = args_parsers.add_parser("up", help="Create and upgrade doris containers, "\
                "or add new containers. " \
                "If none of --add-fe-num, --add-be-num, --add-ms-num, --add-recycle-num, "\
                "--fe-id, --be-id, --ms-id, --recycle-id, --fdb-id is specific, " \
                "then apply to all containers.")
        parser.add_argument("NAME", default="", help="Specific cluster name.")
        parser.add_argument("IMAGE",
                            default="",
                            nargs="?",
                            help="Specify docker image.")

        parser.add_argument(
            "--cloud",
            default=False,
            action=self._get_parser_bool_action(True),
            help=
            "Create cloud cluster, default is false. Only use when creating new cluster."
        )

        parser.add_argument(
            "--wait-timeout",
            type=int,
            default=0,
            help=
            "Specify wait seconds for fe/be ready for service: 0 not wait (default), "\
            "> 0 max wait seconds, -1 wait unlimited."
        )

        self._add_parser_output_json(parser)

        group1 = parser.add_argument_group("add new nodes",
                                           "add cluster nodes.")
        group1.add_argument(
            "--add-fe-num",
            type=int,
            help=
            "Specify add fe num, default: 3 for a new cluster, 0 for a existing cluster."
        )
        group1.add_argument(
            "--add-be-num",
            type=int,
            help=
            "Specify add be num, default: 3 for a new cluster, 0 for a existing cluster."
        )
        group1.add_argument(
            "--add-ms-num",
            type=int,
            help=
            "Specify add ms num, default: 1 for a new cloud cluster, 0 for a existing cluster. Only use in cloud cluster"
        )
        group1.add_argument(
            "--add-recycle-num",
            type=int,
            help=
            "Specify add recycle num, default: 1 for a new cloud cluster, 0 for a existing cluster. Only use in cloud cluster"
        )
        group1.add_argument("--fe-config",
                            nargs="*",
                            type=str,
                            help="Specify fe configs for fe.conf. "\
                                "Example: --fe-config \"enable_debug_points = true\" \"sys_log_level = ERROR\".")
        group1.add_argument("--be-config",
                            nargs="*",
                            type=str,
                            help="Specify be configs for be.conf. "\
                                    "Example: --be-config \"enable_debug_points = true\" \"enable_auth = true\".")
        group1.add_argument("--ms-config",
                            nargs="*",
                            type=str,
                            help="Specify ms configs for doris_cloud.conf. "\
                                    "Example: --ms-config \"log_level = warn\".")
        group1.add_argument("--recycle-config",
                            nargs="*",
                            type=str,
                            help="Specify recycle configs for doris_cloud.conf. "\
                                    "Example: --recycle-config \"log_level = warn\".")
        group1.add_argument(
            "--fe-follower",
            default=False,
            action=self._get_parser_bool_action(True),
            help=
            "The new added fe is follower but not observer. Only support in cloud mode."
        )
        group1.add_argument("--be-disks",
                            nargs="*",
                            default=["HDD=1"],
                            type=str,
                            help="Specify each be disks, each group is \"disk_type=disk_num[,disk_capactity]\", "\
                                    "disk_type is HDD or SSD, disk_capactity is capactity limit in gb. default: HDD=1. "\
                                  "Example: --be-disks \"HDD=1\", \"SSD=1,10\", \"SSD=2,100\""\
                                  "means each be has 1 HDD without capactity limit, 1 SSD with 10GB capactity limit, "\
                                  "2 SSD with 100GB capactity limit")
        group1.add_argument(
            "--be-cluster",
            type=str,
            help=
            "be cluster name, if not specific, will use compute_cluster. Only use in cloud cluster."
        )

        self._add_parser_ids_args(parser)

        group2 = parser.add_mutually_exclusive_group()
        if self._support_boolean_action():
            group2.add_argument(
                "--start",
                default=True,
                action=self._get_parser_bool_action(False),
                help="Start containers, default is true. If specific --no-start, "\
                "will create or update config image only but not start containers.")
        else:
            group2.add_argument(
                "--no-start",
                dest='start',
                default=True,
                action=self._get_parser_bool_action(False),
                help=
                "Create or update config image only and don't start containers."
            )
        group2.add_argument("--force-recreate",
                           default=False,
                           action=self._get_parser_bool_action(True),
                           help="Recreate containers even if their configuration" \
                                "and image haven't changed. ")

        parser.add_argument("--coverage-dir",
                            default="",
                            help="Set code coverage output directory")

        parser.add_argument("--sql-mode-node-mgr",
                            default=False,
                            action=self._get_parser_bool_action(True),
                            help="Manager fe be via sql instead of http")

        if self._support_boolean_action():
            parser.add_argument(
                "--be-metaservice-endpoint",
                default=True,
                action=self._get_parser_bool_action(False),
                help=
                "Do not set BE meta service endpoint in conf. Default is False."
            )
        else:
            parser.add_argument(
                "--no-be-metaservice-endpoint",
                dest='be_metaservice_endpoint',
                default=True,
                action=self._get_parser_bool_action(False),
                help=
                "Do not set BE meta service endpoint in conf. Default is False."
            )

        if self._support_boolean_action():
            parser.add_argument(
                "--be-cloud-instanceid",
                default=True,
                action=self._get_parser_bool_action(False),
                help=
                "Do not set BE cloud instance ID in conf. Default is False.")
        else:
            parser.add_argument(
                "--no-be-cloud-instanceid",
                dest='be_cloud_instanceid',
                default=True,
                action=self._get_parser_bool_action(False),
                help=
                "Do not set BE cloud instance ID in conf. Default is False.")

        parser.add_argument(
            "--fdb-version",
            type=str,
            default="7.1.26",
            help="fdb image version. Only use in cloud cluster.")

        if self._support_boolean_action():
            parser.add_argument(
                "--detach",
                default=True,
                action=self._get_parser_bool_action(False),
                help="Detached mode: Run containers in the background. If specific --no-detach, "\
                "will run containers in frontend. ")
        else:
            parser.add_argument("--no-detach",
                                dest='detach',
                                default=True,
                                action=self._get_parser_bool_action(False),
                                help="Run containers in frontend. ")

        if self._support_boolean_action():
            parser.add_argument(
                "--reg-be",
                default=True,
                action=self._get_parser_bool_action(False),
                help="Register be to meta server in cloud mode, use for multi clusters test. If specific --no-reg-be, "\
                "will not register be to meta server. ")
        else:
            parser.add_argument(
                "--no-reg-be",
                dest='reg_be',
                default=True,
                action=self._get_parser_bool_action(False),
                help=
                "Don't register be to meta server in cloud mode, use for multi clusters test"
            )

    def run(self, args):
        if not args.NAME:
            raise Exception("Need specific not empty cluster name")
        for_all = True
        add_fdb_num = 0
        try:
            cluster = CLUSTER.Cluster.load(args.NAME)

            if not cluster.is_cloud:
                args.add_ms_num = None
                args.add_recycle_num = None
                args.ms_id = None
                args.recycle_id = None
                args.fdb_id = None

            if args.fe_id != None or args.be_id != None \
                or args.ms_id != None or args.recycle_id != None or args.fdb_id != None \
                or args.add_fe_num or args.add_be_num \
                or args.add_ms_num or args.add_recycle_num:
                for_all = False
        except:
            # a new cluster
            if not args.IMAGE:
                raise Exception("New cluster must specific image") from None
            if args.fe_id != None:
                args.fe_id = None
                LOG.warning(
                    utils.render_yellow("Ignore --fe-id for new cluster"))
            if args.be_id != None:
                args.be_id = None
                LOG.warning(
                    utils.render_yellow("Ignore --be-id for new cluster"))

            args.fdb_id = None
            args.ms_id = None
            args.recycle_id = None

            if args.add_fe_num is None:
                args.add_fe_num = 3
            if args.add_be_num is None:
                args.add_be_num = 3

            cloud_store_config = {}
            if args.cloud:
                add_fdb_num = 1
                if not args.add_ms_num:
                    args.add_ms_num = 1
                if not args.add_recycle_num:
                    args.add_recycle_num = 1
                if not args.be_cluster:
                    args.be_cluster = "compute_cluster"
                cloud_store_config = self._get_cloud_store_config()
            else:
                args.add_ms_num = 0
                args.add_recycle_num = 0

            cluster = CLUSTER.Cluster.new(
                args.NAME, args.IMAGE, args.cloud, args.fe_config,
                args.be_config, args.ms_config, args.recycle_config,
                args.fe_follower, args.be_disks, args.be_cluster, args.reg_be,
                args.coverage_dir, cloud_store_config, args.sql_mode_node_mgr,
                args.be_metaservice_endpoint, args.be_cloud_instanceid)
            LOG.info("Create new cluster {} succ, cluster path is {}".format(
                args.NAME, cluster.get_path()))

        if args.be_cluster and cluster.is_cloud:
            cluster.be_cluster = args.be_cluster

        if cluster.is_cloud:
            cluster.fe_follower = args.fe_follower

        _, related_nodes, _ = get_ids_related_nodes(cluster, args.fe_id,
                                                    args.be_id, args.ms_id,
                                                    args.recycle_id,
                                                    args.fdb_id)
        add_fe_ids = []
        add_be_ids = []
        add_ms_ids = []
        add_recycle_ids = []
        add_fdb_ids = []

        add_type_nums = [
            (CLUSTER.Node.TYPE_FDB, add_fdb_num, add_fdb_ids),
            (CLUSTER.Node.TYPE_MS, args.add_ms_num, add_ms_ids),
            (CLUSTER.Node.TYPE_RECYCLE, args.add_recycle_num, add_recycle_ids),
            (CLUSTER.Node.TYPE_FE, args.add_fe_num, add_fe_ids),
            (CLUSTER.Node.TYPE_BE, args.add_be_num, add_be_ids),
        ]

        if not related_nodes:
            related_nodes = []

        def do_add_node(node_type, add_num, add_ids):
            if not add_num:
                return
            for i in range(add_num):
                node = cluster.add(node_type)
                related_nodes.append(node)
                add_ids.append(node.id)

        for node_type, add_num, add_ids in add_type_nums:
            do_add_node(node_type, add_num, add_ids)

        if args.IMAGE:
            for node in related_nodes:
                node.set_image(args.IMAGE)
            if for_all:
                cluster.set_image(args.IMAGE)

        for node in cluster.get_all_nodes(CLUSTER.Node.TYPE_FDB):
            node.set_image("foundationdb/foundationdb:{}".format(
                args.fdb_version))

        cluster.save()

        options = []
        if not args.start:
            options.append("--no-start")
        else:
            options += ["--remove-orphans"]
            if args.detach:
                options.append("-d")
            if args.force_recreate:
                options.append("--force-recreate")

        related_node_num = len(related_nodes)
        if for_all:
            related_node_num = cluster.get_all_nodes_num()
            related_nodes = None

        output_real_time = args.start and not args.detach
        utils.exec_docker_compose_command(cluster.get_compose_file(),
                                          "up",
                                          options,
                                          related_nodes,
                                          output_real_time=output_real_time)

        ls_cmd = "python docker/runtime/doris-compose/doris-compose.py ls " + cluster.name
        LOG.info("Inspect command: " + utils.render_green(ls_cmd) + "\n")
        LOG.info(
            "Master fe query address: " +
            utils.render_green(CLUSTER.get_master_fe_endpoint(cluster.name)) +
            "\n")

        if not args.start:
            LOG.info(
                utils.render_green(
                    "Not up cluster cause specific --no-start, related node num {}"
                    .format(related_node_num)))
        else:
            LOG.info("Using SQL mode for node management ? {}".format(
                args.sql_mode_node_mgr))

            # Wait for FE master to be elected
            LOG.info("Waiting for FE master to be elected...")
            expire_ts = time.time() + 30
            while expire_ts > time.time():
                db_mgr = database.get_db_mgr(args.NAME, False)
                for id in add_fe_ids:
                    fe_state = db_mgr.get_fe(id)
                    if fe_state is not None and fe_state.alive:
                        break
                    LOG.info("there is no fe ready")
                time.sleep(5)

            if cluster.is_cloud and args.sql_mode_node_mgr:
                db_mgr = database.get_db_mgr(args.NAME, False)
                master_fe_endpoint = CLUSTER.get_master_fe_endpoint(
                    cluster.name)
                # Add FEs except master_fe
                for fe in cluster.get_all_nodes(CLUSTER.Node.TYPE_FE):
                    fe_endpoint = f"{fe.get_ip()}:{CLUSTER.FE_EDITLOG_PORT}"
                    if fe_endpoint != master_fe_endpoint:
                        try:
                            db_mgr.add_fe(fe_endpoint)
                            LOG.info(f"Added FE {fe_endpoint} successfully.")
                        except Exception as e:
                            LOG.error(
                                f"Failed to add FE {fe_endpoint}: {str(e)}")

                # Add BEs
                for be in cluster.get_all_nodes(CLUSTER.Node.TYPE_BE):
                    be_endpoint = f"{be.get_ip()}:{CLUSTER.BE_HEARTBEAT_PORT}"
                    try:
                        db_mgr.add_be(be_endpoint)
                        LOG.info(f"Added BE {be_endpoint} successfully.")
                    except Exception as e:
                        LOG.error(f"Failed to add BE {be_endpoint}: {str(e)}")

                cloud_store_config = self._get_cloud_store_config()

                db_mgr.create_default_storage_vault(cloud_store_config)

            if args.wait_timeout != 0:
                if args.wait_timeout == -1:
                    args.wait_timeout = 1000000000
                expire_ts = time.time() + args.wait_timeout
                while True:
                    db_mgr = database.get_db_mgr(args.NAME, False)
                    dead_frontends = []
                    for id in add_fe_ids:
                        fe_state = db_mgr.get_fe(id)
                        if not fe_state or not fe_state.alive:
                            dead_frontends.append(id)
                    dead_backends = []
                    for id in add_be_ids:
                        be_state = db_mgr.get_be(id)
                        if not be_state or not be_state.alive:
                            dead_backends.append(id)
                    if not dead_frontends and not dead_backends:
                        break
                    if time.time() >= expire_ts:
                        err = ""
                        if dead_frontends:
                            err += "dead fe: " + str(dead_frontends) + ". "
                        if dead_backends:
                            err += "dead be: " + str(dead_backends) + ". "
                        raise Exception(err)
                    time.sleep(1)

            LOG.info(
                utils.render_green(
                    "Up cluster {} succ, related node num {}".format(
                        args.NAME, related_node_num)))

        return {
            "fe": {
                "add_list": add_fe_ids,
            },
            "be": {
                "add_list": add_be_ids,
            },
            "ms": {
                "add_list": add_ms_ids,
            },
            "recycle": {
                "add_list": add_recycle_ids,
            },
            "fdb": {
                "add_list": add_fdb_ids,
            },
        }

    def _get_cloud_store_config(self):
        example_cfg_file = os.path.join(CLUSTER.LOCAL_RESOURCE_PATH,
                                        "cloud.ini.example")
        if not CLUSTER.CLOUD_CFG_FILE:
            raise Exception("Cloud cluster need S3 store, specific its config in a file.\n"     \
                "A example file is " + example_cfg_file + ".\n"   \
                "Then setting the env variable  `export DORIS_CLOUD_CFG_FILE=<cfg-file-path>`.")

        if not os.path.exists(CLUSTER.CLOUD_CFG_FILE):
            raise Exception("Cloud store config file '" +
                            CLUSTER.CLOUD_CFG_FILE + "' not exists.")

        config = {}
        with open(example_cfg_file, "r") as f:
            for line in f.readlines():
                if line.startswith('#'):
                    continue
                pos = line.find('=')
                if pos <= 0:
                    continue
                key = line[0:pos].strip()
                if key:
                    config[key] = ""

        with open(CLUSTER.CLOUD_CFG_FILE, "r") as f:
            for line in f.readlines():
                if line.startswith('#'):
                    continue
                pos = line.find('=')
                if pos <= 0:
                    continue
                key = line[0:pos].strip()
                if key and config.get(key, None) != None:
                    config[key] = line[line.find('=') + 1:].strip()

        for key, value in config.items():
            if not value:
                raise Exception(
                    "Should provide none empty property '{}' in file {}".
                    format(key, CLUSTER.CLOUD_CFG_FILE))
        return config


class DownCommand(Command):

    def add_parser(self, args_parsers):
        parser = args_parsers.add_parser("down",
                                     help="Down doris containers, networks. "\
                                           "It will also remove node from DB. " \
                                           "If none of --fe-id, --be-id, --ms-id, --recycle-id, --fdb-id is specific, "\
                                           "then apply to all containers.")
        parser.add_argument("NAME", help="Specify cluster name")
        self._add_parser_ids_args(parser)
        self._add_parser_output_json(parser)
        parser.add_argument(
            "--clean",
            default=False,
            action=self._get_parser_bool_action(True),
            help=
            "Clean container related files, include expose data, config and logs."
        )
        parser.add_argument(
            "--drop-force",
            default=None,
            action=self._get_parser_bool_action(True),
            help="Drop doris node force. For be, if specific --drop-force, "\
                    "it will send dropp to fe, otherwise send decommission to fe.")

    def run(self, args):
        cluster = None
        try:
            cluster = CLUSTER.Cluster.load(args.NAME)
        except:
            return "Cluster not exists or load failed"
        for_all, related_nodes, related_node_num = get_ids_related_nodes(
            cluster,
            args.fe_id,
            args.be_id,
            args.ms_id,
            args.recycle_id,
            args.fdb_id,
            ignore_not_exists=True)

        LOG.info("down cluster " + args.NAME + " for all " + str(for_all))

        if for_all:
            if os.path.exists(cluster.get_compose_file()):
                try:
                    utils.exec_docker_compose_command(
                        cluster.get_compose_file(), "down",
                        ["-v", "--remove-orphans"])
                except Exception as e:
                    LOG.warn("down cluster has exception: " + str(e))
            try:
                utils.remove_docker_network(cluster.name)
            except Exception as e:
                LOG.warn("remove network has exception: " + str(e))
            if args.clean:
                utils.enable_dir_with_rw_perm(cluster.get_path())
                shutil.rmtree(cluster.get_path())
                LOG.info(
                    utils.render_yellow(
                        "Clean cluster data cause has specific --clean"))
        else:
            db_mgr = database.get_db_mgr(cluster.name)

            for node in related_nodes:
                if node.is_fe():
                    fe_endpoint = "{}:{}".format(node.get_ip(),
                                                 CLUSTER.FE_EDITLOG_PORT)
                    db_mgr.drop_fe(fe_endpoint)
                elif node.is_be():
                    be_endpoint = "{}:{}".format(node.get_ip(),
                                                 CLUSTER.BE_HEARTBEAT_PORT)
                    if args.drop_force:
                        db_mgr.drop_be(be_endpoint)
                    else:
                        db_mgr.decommission_be(be_endpoint)
                else:
                    raise Exception("Unknown node type: {}".format(
                        node.node_type()))

                #utils.exec_docker_compose_command(cluster.get_compose_file(),
                #                                  "stop",
                #                                  nodes=[node])
                utils.exec_docker_compose_command(cluster.get_compose_file(),
                                                  "rm", ["-s", "-v", "-f"],
                                                  nodes=[node])
                if args.clean:
                    utils.enable_dir_with_rw_perm(node.get_path())
                    shutil.rmtree(node.get_path())
                    register_file = "{}/{}-{}-register".format(
                        CLUSTER.get_status_path(cluster.name),
                        node.node_type(), node.get_ip())
                    if os.path.exists(register_file):
                        os.remove(register_file)
                    LOG.info(
                        utils.render_yellow(
                            "Clean {} with id {} data cause has specific --clean"
                            .format(node.node_type(), node.id)))

                cluster.remove(node.node_type(), node.id)
                cluster.save()

        LOG.info(
            utils.render_green(
                "Down cluster {} succ, related node num {}".format(
                    args.NAME, related_node_num)))

        return "down cluster succ"


class ListNode(object):

    def __init__(self):
        self.node_type = ""
        self.id = 0
        self.backend_id = ""
        self.cluster_name = ""
        self.ip = ""
        self.status = ""
        self.container_id = ""
        self.image = ""
        self.created = ""
        self.alive = ""
        self.is_master = ""
        self.query_port = ""
        self.tablet_num = ""
        self.last_heartbeat = ""
        self.err_msg = ""
        self.edit_log_port = 0
        self.heartbeat_port = 0

    def info(self, detail):
        result = [
            self.cluster_name, "{}-{}".format(self.node_type, self.id),
            self.ip, self.status, self.container_id, self.image, self.created,
            self.alive, self.is_master, self.backend_id, self.tablet_num,
            self.last_heartbeat, self.err_msg
        ]
        if detail:
            query_port = ""
            http_port = ""
            heartbeat_port = ""
            edit_log_port = ""
            node_path = CLUSTER.get_node_path(self.cluster_name,
                                              self.node_type, self.id)
            if self.node_type == CLUSTER.Node.TYPE_FE:
                query_port = CLUSTER.FE_QUERY_PORT
                http_port = CLUSTER.FE_HTTP_PORT
                edit_log_port = CLUSTER.FE_EDITLOG_PORT
            elif self.node_type == CLUSTER.Node.TYPE_BE:
                http_port = CLUSTER.BE_WEBSVR_PORT
                heartbeat_port = CLUSTER.BE_HEARTBEAT_PORT
            elif self.node_type == CLUSTER.Node.TYPE_MS or self.node_type == CLUSTER.Node.TYPE_RECYCLE:
                http_port = CLUSTER.MS_PORT
            else:
                pass
            result += [
                query_port, http_port, node_path, edit_log_port, heartbeat_port
            ]
        return result

    def update_db_info(self, db_mgr):
        if self.node_type == CLUSTER.Node.TYPE_FE:
            fe = db_mgr.get_fe(self.id)
            if fe:
                self.alive = str(fe.alive).lower()
                self.is_master = str(fe.is_master).lower()
                self.query_port = fe.query_port
                self.last_heartbeat = fe.last_heartbeat
                self.err_msg = fe.err_msg
                self.edit_log_port = fe.edit_log_port
        elif self.node_type == CLUSTER.Node.TYPE_BE:
            self.backend_id = -1
            be = db_mgr.get_be(self.id)
            if be:
                self.alive = str(be.alive).lower()
                self.backend_id = be.backend_id
                self.tablet_num = be.tablet_num
                self.last_heartbeat = be.last_heartbeat
                self.err_msg = be.err_msg
                self.heartbeat_port = be.heartbeat_port


class GenConfCommand(Command):

    def print_use_time(self):
        return False

    def add_parser(self, args_parsers):
        parser = args_parsers.add_parser(
            "config",
            help="Generate regression-conf-custom.groovy for regression test.")
        parser.add_argument("NAME", default="", help="Specific cluster name.")
        parser.add_argument("DORIS_ROOT_PATH", default="", help="Specify doris or selectdb root path, "\
                "i.e. the parent directory of regression-test.")
        parser.add_argument("--connect-follow-fe",
                            default=False,
                            action=self._get_parser_bool_action(True),
                            help="Connect to follow fe.")
        parser.add_argument("-q",
                            "--quiet",
                            default=False,
                            action=self._get_parser_bool_action(True),
                            help="write config quiet, no need confirm.")

        return parser

    def run(self, args):
        base_conf = '''
jdbcUrl = "jdbc:mysql://{fe_ip}:9030/?useLocalSessionState=true&allowLoadLocalInfile=true"
targetJdbcUrl = "jdbc:mysql://{fe_ip}:9030/?useLocalSessionState=true&allowLoadLocalInfile=true"
feSourceThriftAddress = "{fe_ip}:9020"
feTargetThriftAddress = "{fe_ip}:9020"
syncerAddress = "{fe_ip}:9190"
feHttpAddress = "{fe_ip}:8030"
'''

        cloud_conf = '''
feCloudHttpAddress = "{fe_ip}:18030"
metaServiceHttpAddress = "{ms_endpoint}"
metaServiceToken = "greedisgood9999"
recycleServiceHttpAddress = "{recycle_endpoint}"
instanceId = "default_instance_id"
multiClusterInstance = "default_instance_id"
multiClusterBes = "{multi_cluster_bes}"
cloudUniqueId= "{fe_cloud_unique_id}"
'''
        cluster = CLUSTER.Cluster.load(args.NAME)
        master_fe_ip_ep = CLUSTER.get_master_fe_endpoint(args.NAME)
        if not master_fe_ip_ep:
            print("Not found cluster with name {} in directory {}".format(
                args.NAME, CLUSTER.LOCAL_DORIS_PATH))
            return

        master_fe_ip = master_fe_ip_ep[:master_fe_ip_ep.find(':')]
        fe_ip = ""
        if not args.connect_follow_fe:
            fe_ip = master_fe_ip
        else:
            for fe in cluster.get_all_nodes(CLUSTER.Node.TYPE_FE):
                if fe.get_ip() == master_fe_ip:
                    continue
                else:
                    fe_ip = fe.get_ip()
                    break
            if not fe_ip:
                raise Exception(
                    "Not found follow fe, pls add a follow fe use command `up <your-cluster> --add-fe-num 1`"
                )

        relative_custom_file_path = "regression-test/conf/regression-conf-custom.groovy"
        regression_conf_custom = os.path.join(args.DORIS_ROOT_PATH,
                                              relative_custom_file_path)
        if not args.quiet:
            ans = input(
                "\nwrite file {} ?  y/n: ".format(regression_conf_custom))
            if ans != 'y':
                print("\nNo write regression custom file.")
                return

        annotation_start = "//---------- Start auto generate by doris-compose.py---------"
        annotation_end = "//---------- End auto generate by doris-compose.py---------"

        old_contents = []
        if os.path.exists(regression_conf_custom):
            with open(regression_conf_custom, "r") as f:
                old_contents = f.readlines()
        with open(regression_conf_custom, "w") as f:
            # write auto gen config
            f.write(annotation_start)
            f.write(base_conf.format(fe_ip=fe_ip))
            if cluster.is_cloud:
                multi_cluster_bes = ",".join([
                    "{}:{}:{}:{}:{}".format(be.get_ip(),
                                            CLUSTER.BE_HEARTBEAT_PORT,
                                            CLUSTER.BE_WEBSVR_PORT,
                                            be.cloud_unique_id(),
                                            CLUSTER.BE_BRPC_PORT)
                    for be in cluster.get_all_nodes(CLUSTER.Node.TYPE_BE)
                ])
                f.write(
                    cloud_conf.format(
                        fe_ip=fe_ip,
                        ms_endpoint=cluster.get_meta_server_addr(),
                        recycle_endpoint=cluster.get_recycle_addr(),
                        multi_cluster_bes=multi_cluster_bes,
                        fe_cloud_unique_id=cluster.get_node(
                            CLUSTER.Node.TYPE_FE, 1).cloud_unique_id()))
            f.write(annotation_end + "\n\n")
            annotation_end_line_count = -1

            # write not-auto gen config
            in_annotation = False
            annotation_end_line_idx = -100
            for line_idx, line in enumerate(old_contents):
                line = line.rstrip()
                if line == annotation_start:
                    in_annotation = True
                elif line == annotation_end:
                    in_annotation = False
                    annotation_end_line_idx = line_idx
                elif not in_annotation:
                    if line or line_idx != annotation_end_line_idx + 1:
                        f.write(line + "\n")

        print("\nWrite succ: " + regression_conf_custom)


class ListCommand(Command):

    def add_parser(self, args_parsers):
        parser = args_parsers.add_parser(
            "ls", help="List running doris compose clusters.")
        parser.add_argument(
            "NAME",
            nargs="*",
            help=
            "Specify multiple clusters, if specific, show all their containers."
        )
        self._add_parser_output_json(parser)
        parser.add_argument("--detail",
                            default=False,
                            action=self._get_parser_bool_action(True),
                            help="Print more detail fields.")

    def _handle_data(self, header, datas):
        if utils.is_enable_log():
            table = prettytable.PrettyTable(
                [utils.render_green(field) for field in header])
            for row in datas:
                table.add_row(row)
            print(table)
            return ""
        else:
            datas.insert(0, header)
            return datas

    def run(self, args):
        COMPOSE_MISSING = "(missing)"
        COMPOSE_BAD = "(bad)"
        COMPOSE_GOOD = ""

        SERVICE_DEAD = "dead"

        class ComposeService(object):

            def __init__(self, name, ip, image):
                self.name = name
                self.ip = ip
                self.image = image

        def parse_cluster_compose_file(cluster_name):
            compose_file = CLUSTER.get_compose_file(cluster_name)
            if not os.path.exists(compose_file):
                return COMPOSE_MISSING, {}
            try:
                compose = utils.read_compose_file(compose_file)
                if not compose:
                    return COMPOSE_BAD, {}
                services = compose.get("services", {})
                if services is None:
                    return COMPOSE_BAD, {}
                return COMPOSE_GOOD, {
                    service: ComposeService(
                        service,
                        list(service_conf["networks"].values())[0]
                        ["ipv4_address"], service_conf["image"])
                    for service, service_conf in services.items()
                }
            except:
                return COMPOSE_BAD, {}

        clusters = {}
        search_names = []
        if args.NAME:
            search_names = args.NAME
        else:
            search_names = CLUSTER.get_all_cluster_names()

        for cluster_name in search_names:
            status, services = parse_cluster_compose_file(cluster_name)
            clusters[cluster_name] = {"status": status, "services": services}

        docker_clusters = utils.get_doris_containers(args.NAME)
        for cluster_name, containers in docker_clusters.items():
            cluster_info = clusters.get(cluster_name, None)
            if not cluster_info:
                cluster_info = {"status": COMPOSE_MISSING, "services": {}}
                clusters[cluster_name] = cluster_info
            for container in containers:
                #if container.status == "running" and cluster_info[
                #        "status"] == COMPOSE_GOOD and (
                #            container.name not in cluster_info["services"]):
                #    container.status = "orphans"
                cluster_info["services"][container.name] = container

        TYPE_COMPOSESERVICE = type(ComposeService("", "", ""))
        if not args.NAME:
            header = ("CLUSTER", "OWNER", "STATUS", "MASTER FE", "CLOUD",
                      "CONFIG FILES")
            rows = []
            for name in sorted(clusters.keys()):
                cluster_info = clusters[name]
                service_statuses = {}
                for _, container in cluster_info["services"].items():
                    status = SERVICE_DEAD if type(
                        container) == TYPE_COMPOSESERVICE else container.status
                    service_statuses[status] = service_statuses.get(status,
                                                                    0) + 1
                show_status = ",".join([
                    "{}({})".format(status, count)
                    for status, count in service_statuses.items()
                ])
                owner = utils.get_path_owner(CLUSTER.get_cluster_path(name))
                compose_file = CLUSTER.get_compose_file(name)

                is_cloud = ""
                try:
                    cluster = CLUSTER.Cluster.load(name)
                    is_cloud = "true" if cluster.is_cloud else "false"
                except:
                    pass

                rows.append((name, owner, show_status,
                             CLUSTER.get_master_fe_endpoint(name), is_cloud,
                             "{}{}".format(compose_file,
                                           cluster_info["status"])))
            return self._handle_data(header, rows)

        header = [
            "CLUSTER", "NAME", "IP", "STATUS", "CONTAINER ID", "IMAGE",
            "CREATED", "alive", "is_master", "backend_id", "tablet_num",
            "last_heartbeat", "err_msg"
        ]
        if args.detail:
            header += [
                "query_port",
                "http_port",
                "path",
                "edit_log_port",
                "heartbeat_port",
            ]

        rows = []
        for cluster_name in sorted(clusters.keys()):
            fe_ids = {}
            be_ids = {}
            services = clusters[cluster_name]["services"]
            db_mgr = database.get_db_mgr(cluster_name, False)

            nodes = []
            for service_name, container in services.items():
                _, node_type, id = utils.parse_service_name(container.name)
                node = ListNode()
                node.cluster_name = cluster_name
                node.node_type = node_type
                node.id = id
                node.update_db_info(db_mgr)
                nodes.append(node)

                if node_type == CLUSTER.Node.TYPE_FE:
                    fe_ids[id] = True
                elif node_type == CLUSTER.Node.TYPE_BE:
                    be_ids[id] = True

                if type(container) == TYPE_COMPOSESERVICE:
                    node.ip = container.ip
                    node.image = container.image
                    node.status = SERVICE_DEAD
                else:
                    node.created = dateutil.parser.parse(
                        container.attrs.get("Created")).astimezone().strftime(
                            "%Y-%m-%d %H:%M:%S")
                    node.ip = list(
                        container.attrs["NetworkSettings"]
                        ["Networks"].values())[0]["IPAMConfig"]["IPv4Address"]
                    node.image = ",".join(container.image.tags)
                    if not node.image:
                        node.image = container.attrs["Config"]["Image"]
                    node.container_id = container.short_id
                    node.status = container.status
                    if node.container_id and \
                        node_type in (CLUSTER.Node.TYPE_FDB,
                                     CLUSTER.Node.TYPE_MS,
                                     CLUSTER.Node.TYPE_RECYCLE):
                        node.alive = "true"

            for id, fe in db_mgr.fe_states.items():
                if fe_ids.get(id, False):
                    continue
                node = ListNode()
                node.cluster_name = cluster_name
                node.node_type = CLUSTER.Node.TYPE_FE
                node.id = id
                node.status = SERVICE_DEAD
                node.update_db_info(db_mgr)
                nodes.append(node)
            for id, be in db_mgr.be_states.items():
                if be_ids.get(id, False):
                    continue
                node = ListNode()
                node.cluster_name = cluster_name
                node.node_type = CLUSTER.Node.TYPE_BE
                node.id = id
                node.status = SERVICE_DEAD
                node.update_db_info(db_mgr)
                nodes.append(node)

            def get_node_seq(node):
                return CLUSTER.get_node_seq(node.node_type, node.id)

            for node in sorted(nodes, key=get_node_seq):
                rows.append(node.info(args.detail))

        return self._handle_data(header, rows)


class GetCloudIniCommand(Command):

    def add_parser(self, args_parsers):
        parser = args_parsers.add_parser("get-cloud-ini",
                                         help="Get cloud.init")
        parser.add_argument(
            "NAME",
            nargs="*",
            help=
            "Specify multiple clusters, if specific, show all their containers."
        )
        self._add_parser_output_json(parser)

    def _handle_data(self, header, datas):
        if utils.is_enable_log():
            table = prettytable.PrettyTable(
                [utils.render_green(field) for field in header])
            for row in datas:
                table.add_row(row)
            print(table)
            return ""
        else:
            datas.insert(0, header)
            return datas

    def run(self, args):

        header = ["key", "value"]

        rows = []

        with open(CLUSTER.CLOUD_CFG_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    rows.append([key.strip(), value.strip()])

        return self._handle_data(header, rows)


ALL_COMMANDS = [
    UpCommand("up"),
    DownCommand("down"),
    SimpleCommand("start", "Start the doris containers. "),
    SimpleCommand("stop", "Stop the doris containers. "),
    SimpleCommand("restart", "Restart the doris containers. "),
    SimpleCommand("pause", "Pause the doris containers. "),
    SimpleCommand("unpause", "Unpause the doris containers. "),
    GetCloudIniCommand("get-cloud-ini"),
    GenConfCommand("config"),
    ListCommand("ls"),
]
