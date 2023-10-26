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
def get_ids_related_nodes(cluster, fe_ids, be_ids, ignore_not_exists=False):
    if fe_ids is None and be_ids is None:
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

    nodes = get_ids_related_nodes_with_type(
        CLUSTER.Node.TYPE_FE, fe_ids) + get_ids_related_nodes_with_type(
            CLUSTER.Node.TYPE_BE, be_ids)

    related_node_num = len(nodes)

    return len(nodes) == cluster.get_all_nodes_num(), nodes, len(nodes)


class Command(object):

    def __init__(self, name):
        self.name = name

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

    def _get_parser_bool_action(self, is_store_true):
        if sys.version_info.major == 3 and sys.version_info.minor >= 9:
            return argparse.BooleanOptionalAction
        else:
            return "store_true" if is_store_true else "store_false"


class SimpleCommand(Command):

    def __init__(self, command, help):
        super().__init__(command)
        self.command = command
        self.help = help

    def add_parser(self, args_parsers):
        help = self.help + " If none of --fe-id, --be-id is specific, then apply to all containers."
        parser = args_parsers.add_parser(self.command, help=help)
        parser.add_argument("NAME", help="Specify cluster name.")
        self._add_parser_ids_args(parser)
        self._add_parser_output_json(parser)

    def run(self, args):
        cluster = CLUSTER.Cluster.load(args.NAME)
        _, related_nodes, related_node_num = get_ids_related_nodes(
            cluster, args.fe_id, args.be_id)
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
                "If none of --add-fe-num, --add-be-num, --fe-id, --be-id is specific, " \
                "then apply to all containers.")
        parser.add_argument("NAME", default="", help="Specific cluster name.")
        parser.add_argument("IMAGE",
                            default="",
                            nargs="?",
                            help="Specify docker image.")

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
        group1.add_argument("--be-disk-num",
                            default=None,
                            type=int,
                            help="Specify be disk num, default is 1.")

        self._add_parser_ids_args(parser)

        group2 = parser.add_mutually_exclusive_group()
        group2.add_argument(
            "--start",
            default=True,
            action=self._get_parser_bool_action(False),
            help="Start containers, default is true. If specific --no-start, "\
            "will create or update config image only but not start containers.")
        group2.add_argument("--force-recreate",
                           default=False,
                           action=self._get_parser_bool_action(True),
                           help="Recreate containers even if their configuration" \
                                "and image haven't changed. ")

    def run(self, args):
        if not args.NAME:
            raise Exception("Need specific not empty cluster name")
        for_all = True
        try:
            cluster = CLUSTER.Cluster.load(args.NAME)
            if args.fe_id != None or args.be_id != None or args.add_fe_num or args.add_be_num:
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
            cluster = CLUSTER.Cluster.new(args.NAME, args.IMAGE,
                                          args.fe_config, args.be_config,
                                          args.be_disk_num)
            LOG.info("Create new cluster {} succ, cluster path is {}".format(
                args.NAME, cluster.get_path()))
            if not args.add_fe_num:
                args.add_fe_num = 3
            if not args.add_be_num:
                args.add_be_num = 3

        _, related_nodes, _ = get_ids_related_nodes(cluster, args.fe_id,
                                                    args.be_id)
        add_be_ids = []
        add_fe_ids = []
        if not related_nodes:
            related_nodes = []
        if args.add_fe_num:
            for i in range(args.add_fe_num):
                fe = cluster.add(CLUSTER.Node.TYPE_FE)
                related_nodes.append(fe)
                add_fe_ids.append(fe.id)
        if args.add_be_num:
            for i in range(args.add_be_num):
                be = cluster.add(CLUSTER.Node.TYPE_BE)
                related_nodes.append(be)
                add_be_ids.append(be.id)
        if args.IMAGE:
            for node in related_nodes:
                node.set_image(args.IMAGE)
        if for_all and args.IMAGE:
            cluster.set_image(args.IMAGE)
        cluster.save()

        options = []
        if not args.start:
            options.append("--no-start")
        else:
            options = ["-d", "--remove-orphans"]
            if args.force_recreate:
                options.append("--force-recreate")

        related_node_num = len(related_nodes)
        if for_all:
            related_node_num = cluster.get_all_nodes_num()
            related_nodes = None

        utils.exec_docker_compose_command(cluster.get_compose_file(), "up",
                                          options, related_nodes)
        if not args.start:
            LOG.info(
                utils.render_green(
                    "Not up cluster cause specific --no-start, related node num {}"
                    .format(related_node_num)))
        else:
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

        db_mgr = database.get_db_mgr(args.NAME, False)
        return {
            "fe": {
                "add_list": add_fe_ids,
            },
            "be": {
                "add_list": add_be_ids,
            },
        }


class DownCommand(Command):

    def add_parser(self, args_parsers):
        parser = args_parsers.add_parser("down",
                                     help="Down doris containers, networks. "\
                                           "It will also remove node from DB. " \
                                           "If none of --fe-id, --be-id is specific, "\
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
            cluster, args.fe_id, args.be_id, ignore_not_exists=True)

        if for_all:
            utils.exec_docker_compose_command(cluster.get_compose_file(),
                                              "down",
                                              ["-v", "--remove-orphans"])
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
                    register_file = "{}/{}-register".format(
                        CLUSTER.get_status_path(cluster.name), node.get_ip())
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

    def info(self, detail):
        result = [
            self.cluster_name, "{}-{}".format(self.node_type, self.id),
            self.ip, self.status, self.container_id, self.image, self.created,
            self.alive, self.is_master, self.query_port, self.backend_id,
            self.tablet_num, self.last_heartbeat, self.err_msg
        ]
        if detail:
            query_port = ""
            http_port = ""
            if self.node_type == CLUSTER.Node.TYPE_FE:
                query_port = CLUSTER.FE_QUERY_PORT
                http_port = CLUSTER.FE_HTTP_PORT
            elif self.node_type == CLUSTER.Node.TYPE_BE:
                http_port = CLUSTER.BE_WEBSVR_PORT
            else:
                pass
            result += [
                query_port,
                http_port,
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
        elif self.node_type == CLUSTER.Node.TYPE_BE:
            self.backend_id = -1
            be = db_mgr.get_be(self.id)
            if be:
                self.alive = str(be.alive).lower()
                self.backend_id = be.backend_id
                self.tablet_num = be.tablet_num
                self.last_heartbeat = be.last_heartbeat
                self.err_msg = be.err_msg


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
        parser.add_argument(
            "-a",
            "--all",
            default=False,
            action=self._get_parser_bool_action(True),
            help=
            "Show all clusters, include stopped and bad doris compose projects."
        )
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
                    service:
                    ComposeService(
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
        elif os.path.exists(CLUSTER.LOCAL_DORIS_PATH):
            search_names = os.listdir(CLUSTER.LOCAL_DORIS_PATH)

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
            header = ("CLUSTER", "OWNER", "STATUS", "CONFIG FILES")
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
                if not args.all and service_statuses.get("running", 0) == 0:
                    continue
                owner = utils.get_path_owner(CLUSTER.get_cluster_path(name))
                compose_file = CLUSTER.get_compose_file(name)
                rows.append((name, owner, show_status,
                             "{}{}".format(compose_file,
                                           cluster_info["status"])))
            return self._handle_data(header, rows)

        header = [
            "CLUSTER", "NAME", "IP", "STATUS", "CONTAINER ID", "IMAGE",
            "CREATED", "alive", "is_master", "query_port", "backend_id",
            "tablet_num", "last_heartbeat", "err_msg"
        ]
        if args.detail:
            header += [
                "query_port",
                "http_port",
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
                    node.container_id = container.short_id
                    node.status = container.status

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

            def get_key(node):
                key = node.id
                if node.node_type == CLUSTER.Node.TYPE_FE:
                    key += 0 * CLUSTER.ID_LIMIT
                elif node.node_type == CLUSTER.Node.TYPE_BE:
                    key += 1 * CLUSTER.ID_LIMIT
                else:
                    key += 2 * CLUSTER.ID_LIMIT
                return key

            for node in sorted(nodes, key=get_key):
                rows.append(node.info(args.detail))

        return self._handle_data(header, rows)


ALL_COMMANDS = [
    UpCommand("up"),
    DownCommand("down"),
    SimpleCommand("start", "Start the doris containers. "),
    SimpleCommand("stop", "Stop the doris containers. "),
    SimpleCommand("restart", "Restart the doris containers. "),
    SimpleCommand("pause", "Pause the doris containers. "),
    SimpleCommand("unpause", "Unpause the doris containers. "),
    ListCommand("ls"),
]
