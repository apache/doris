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
import utils
from multiprocessing.pool import ThreadPool
import os
import os.path
import prettytable
import shutil
import sys

LOG = utils.get_logger()


def up(args):
    if not args.NAME:
        raise Exception("Need specific not empty cluster name")
    try:
        cluster = CLUSTER.Cluster.load(args.NAME)
        if args.IMAGE:
            cluster.set_image(args.IMAGE)
    except:
        if not args.IMAGE:
            raise Exception("New cluster must specific image")
        cluster = CLUSTER.Cluster.new(args.NAME, args.IMAGE)
        LOG.info("Create new cluster {} succ, cluster path is {}".format(
            args.NAME, cluster.get_path()))
        if not args.fe:
            args.fe = 3
        if not args.be:
            args.be = 3

    if args.fe:
        cluster.set_node_num(CLUSTER.Node.TYPE_FE, args.fe)
    if args.be:
        cluster.set_node_num(CLUSTER.Node.TYPE_BE, args.be)

    cluster.save()

    if args.no_up:
        LOG.info(utils.render_green("Not up cluster cause specific --no-up"))
    else:
        options = ["-d", "--remove-orphans"]
        if args.force_recreate:
            options.append("--force-recreate")
        utils.exec_docker_compose_command(cluster.get_compose_file(), "up",
                                          options)
        LOG.info(utils.render_green("Up cluster {} succ".format(args.NAME)))


def down(args):
    cluster = CLUSTER.Cluster.load(args.NAME)
    utils.exec_docker_compose_command(cluster.get_compose_file(), "down",
                                      ["--remove-orphans"])
    if args.clean:
        shutil.rmtree(cluster.get_path())
    LOG.info(utils.render_green("Down cluster {} succ".format(args.NAME)))


def add(args):
    cluster = CLUSTER.Cluster.load(args.NAME)
    new_nodes = []
    if args.fe:
        for i in range(args.fe):
            node = cluster.add(CLUSTER.Node.TYPE_FE, image=args.IMAGE)
            new_nodes.append(node)
    if args.be:
        for i in range(args.be):
            node = cluster.add(CLUSTER.Node.TYPE_BE, image=args.IMAGE)
            new_nodes.append(node)

    LOG.info("Add new fe ids: {}, new be ids {}".format([
        node.id
        for node in new_nodes if node.node_type() == CLUSTER.Node.TYPE_FE
    ], [
        node.id
        for node in new_nodes if node.node_type() == CLUSTER.Node.TYPE_BE
    ]))

    if not new_nodes:
        LOG.info(utils.render_green("No add new nodes"))
        return

    cluster.save()
    if args.no_start:
        utils.exec_docker_compose_command(
            cluster.get_compose_file(), "up", ["--no-start"],
            [node.service_name() for node in new_nodes])
        LOG.info(
            utils.render_green(
                "Not start new add nodes cause specific --no-start"))
    else:
        utils.exec_docker_compose_command(
            cluster.get_compose_file(), "up", ["-d"],
            [node.service_name() for node in new_nodes])
        LOG.info(
            utils.render_green("Start new add succ, relate {} nodes".format(
                len(new_nodes))))


def get_relate_cluster_and_nodes(args, ignore_not_exists=False):
    cluster = CLUSTER.Cluster.load(args.NAME)

    def get_nodes_with_ids(node_type, ids):
        if ids is None:
            return []
        elif not ids:
            return cluster.get_all_node(node_type)
        elif not ignore_not_exists:
            return [cluster.get_node(node_type, id) for id in ids]
        else:
            nodes = []
            for id in ids:
                try:
                    nodes.append(cluster.get_node(node_type, id))
                except:
                    LOG.warning("Not contains {} with id {}".format(
                        node_type, id))
            return nodes

    nodes = get_nodes_with_ids(CLUSTER.Node.TYPE_FE,
                               args.fe) + get_nodes_with_ids(
                                   CLUSTER.Node.TYPE_BE, args.be)

    return cluster, nodes


def start(args):
    cluster, nodes = get_relate_cluster_and_nodes(args)
    if args.IMAGE:
        for node in nodes:
            node.set_image(args.IMAGE)
    utils.exec_docker_compose_command(
        cluster.get_compose_file(),
        "start",
        services=[node.service_name() for node in nodes])
    LOG.info(
        utils.render_green("Start run succ, total relate {} nodes".format(
            len(nodes))))


def stop(args):
    cluster, nodes = get_relate_cluster_and_nodes(args, ignore_not_exists=True)
    if not nodes:
        LOG.info(
            utils.render_green("Stop run succ, total relate {} nodes".format(
                len(nodes))))
        return

    if not args.drop and not args.decommission:
        utils.exec_docker_compose_command(
            cluster.get_compose_file(),
            "stop",
            services=[node.service_name() for node in nodes])
        LOG.info(
            utils.render_green("Stop run succ, total relate {} nodes".format(
                len(nodes))))
        return

    if args.drop:
        LOG.info("Start drop nodes cause has specify --drop")
    else:
        LOG.info("Start decommission nodes cause has specify --decommission")

    db_mgr = database.get_db_mgr(cluster.name)
    for node in nodes:
        if node.is_fe():
            fe_endpoint = "{}:{}".format(node.get_ip(),
                                         CLUSTER.FE_EDITLOG_PORT)
            db_mgr.drop_fe(fe_endpoint)
        elif node.is_be():
            be_endpoint = "{}:{}".format(node.get_ip(),
                                         CLUSTER.BE_HEARTBEAT_PORT)
            if args.drop:
                db_mgr.drop_be(be_endpoint)
            else:
                db_mgr.decommission_be(be_endpoint)
        else:
            raise Exception("Unknown node type: {}".format(node.node_type()))

        utils.exec_docker_compose_command(cluster.get_compose_file(),
                                          "stop",
                                          services=[node.service_name()])

        LOG.info("Stop {} with id {} with ip {} succ".format(
            node.node_type(), node.id, node.get_ip()))

        cluster.remove(node.node_type(), node.id)
        cluster.save()

        if args.clean:
            for node in nodes:
                shutil.rmtree(node.get_path())
            LOG.info("Clean nodes's files cause has specify --clean")

    LOG.info(
        utils.render_green("Stop final succ, total relate {} nodes".format(
            len(nodes))))


def restart(args):
    cluster, nodes = get_relate_cluster_and_nodes(args)
    if args.IMAGE:
        for node in nodes:
            node.set_image(args.IMAGE)
    utils.exec_docker_compose_command(
        cluster.get_compose_file(),
        "restart",
        services=[node.service_name() for node in nodes])
    LOG.info(
        utils.render_green("Restart run succ, total relate {} nodes".format(
            len(nodes))))


def ls(args):
    COMPOSE_MISSING = "(missing)"
    COMPOSE_BAD = "(bad)"
    COMPOSE_GOOD = ""

    SERVICE_DEAD = "dead"

    class ComposeService(object):

        def __init__(self, ip, image):
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
                    list(service_conf["networks"].values())[0]["ipv4_address"],
                    service_conf["image"])
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
            if container.status == "running" and cluster_info[
                    "status"] == COMPOSE_GOOD and (
                        container.name not in cluster_info["services"]):
                container.status = "orphans"
            cluster_info["services"][container.name] = container

    TYPE_COMPOSESERVICE = type(ComposeService("", ""))
    if not args.NAME:
        headers = (utils.render_green(field)
                   for field in ("CLUSTER", "STATUS", "CONFIG FILES"))
        table = prettytable.PrettyTable(headers)
        for name in sorted(clusters.keys()):
            cluster_info = clusters[name]
            service_statuses = {}
            for _, container in cluster_info["services"].items():
                status = SERVICE_DEAD if type(
                    container) == TYPE_COMPOSESERVICE else container.status
                service_statuses[status] = service_statuses.get(status, 0) + 1
            show_status = ",".join([
                "{}({})".format(status, count)
                for status, count in service_statuses.items()
            ])
            if not args.all and service_statuses.get("running", 0) == 0:
                continue
            compose_file = CLUSTER.get_compose_file(name)
            table.add_row(
                (name, show_status, "{}{}".format(compose_file,
                                                  cluster_info["status"])))
        print(table)
        return

    headers = (utils.render_green(field)
               for field in ("CLUSTER", "NAME", "IP", "STATUS", "CONTAINER ID",
                             "IMAGE", "CREATED", "alive", "is_master",
                             "query_port", "tablet_num", "last_heartbeat",
                             "err_msg"))
    table = prettytable.PrettyTable(headers)
    for name in sorted(clusters.keys()):
        services = clusters[name]["services"]
        db_mgr = database.get_db_mgr(name, False)
        for service_name, container in services.items():
            if type(container) == TYPE_COMPOSESERVICE:
                table.add_row(
                    (name, service_name, container.ip, SERVICE_DEAD, "",
                     container.image, "", "", "", "", "", "", ""))
            else:
                create = container.attrs.get("Created",
                                             "")[:19].replace("T", " ")
                #show_ports = ", ".join([
                #    "{}=>{}".format(inner, outer) for inner, outer in
                #    utils.get_map_ports(container).items()
                #])
                ip = list(
                    container.attrs["NetworkSettings"]
                    ["Networks"].values())[0]["IPAMConfig"]["IPv4Address"]
                show_image = ",".join(container.image.tags)
                alive = ""
                is_master = ""
                query_port = ""
                last_heartbeat = ""
                err_msg = ""
                tablet_num = ""
                _, node_type, id = utils.parse_service_name(container.name)
                if node_type == CLUSTER.Node.TYPE_FE:
                    fe = db_mgr.get_fe(id)
                    if fe:
                        alive = str(fe.alive).lower()
                        is_master = str(fe.is_master).lower()
                        query_port = fe.query_port
                        last_heartbeat = fe.last_heartbeat
                        err_msg = fe.err_msg
                elif node_type == CLUSTER.Node.TYPE_BE:
                    be = db_mgr.get_be(id)
                    if be:
                        alive = str(be.alive).lower()
                        tablet_num = be.tablet_num
                        last_heartbeat = be.last_heartbeat
                        err_msg = be.err_msg

                table.add_row(
                    (name, service_name, ip, container.status,
                     container.short_id, show_image, create, alive, is_master,
                     query_port, tablet_num, last_heartbeat, err_msg))
    print(table)


def get_parser_bool_action(is_store_true):
    if sys.version_info.major == 3 and sys.version_info.minor >= 9:
        return argparse.BooleanOptionalAction
    else:
        return "store_true" if is_store_true else "store_false"


def add_cluster_and_node_list_arg(ap):
    ap.add_argument("NAME", help="Specify cluster name")
    ap.add_argument("--fe", nargs="*", type=int, help="Specify fe ids, support multiple ids, " \
            "if specific --fe but not specific ids, apply to all fe")
    ap.add_argument("--be", nargs="*", type=int, help="Specify be ids, support multiple ids, " \
            "if specific --be but not specific ids, apply to all be")


def parse_args():
    ap = argparse.ArgumentParser(description="")
    sub_aps = ap.add_subparsers(dest="command")

    ap_up = sub_aps.add_parser("up", help="Create and start doris containers")
    ap_up.add_argument("NAME", default="", help="Specific cluster name")
    ap_up.add_argument("IMAGE",
                       default="",
                       nargs="?",
                       help="Specify docker image")
    ap_up.add_argument("--fe",
                       type=int,
                       help="Specify fe count, default 3 for new cluster")
    ap_up.add_argument("--be",
                       type=int,
                       help="Specify be count, default 3 for new cluster")
    up_group = ap_up.add_mutually_exclusive_group()
    up_group.add_argument("--no-up",
                          default=False,
                          action=get_parser_bool_action(True),
                          help="Not run cluster, only create")
    up_group.add_argument("--force-recreate",
                       default=False,
                       action=get_parser_bool_action(True),
                       help="Recreate containers even if their configuration" \
                            "and image haven't changed. ")
    #up_group.add_argument("-d",
    #                      "--daemon",
    #                      default=False,
    #                      action=get_parser_bool_action(True),
    #                      help="Run up container in background")

    ap_down = sub_aps.add_parser(
        "down", help="Stop and remove doris containers, networks")
    ap_down.add_argument("NAME", help="Specify cluster name")
    ap_down.add_argument("--clean",
                         default=False,
                         action=get_parser_bool_action(True),
                         help="Clean data and logs")

    ap_add = sub_aps.add_parser("add", help="Add doris containers")
    ap_add.add_argument("NAME", help="Specify cluster name")
    ap_add.add_argument("IMAGE",
                        default="",
                        nargs="?",
                        help="New add containers' image")
    ap_add.add_argument("--fe", type=int, help="Specify new add fe num")
    ap_add.add_argument("--be", type=int, help="Specify new add be num")
    ap_add.add_argument("--no-start",
                        default=False,
                        action=get_parser_bool_action(True),
                        help="Not start new node, create only")

    ap_start = sub_aps.add_parser("start", help="Start doris containers")
    add_cluster_and_node_list_arg(ap_start)
    ap_start.add_argument("IMAGE",
                          default="",
                          nargs="?",
                          help="Update containers' image")

    ap_stop = sub_aps.add_parser("stop", help="Stop doris containers")
    add_cluster_and_node_list_arg(ap_stop)
    ap_stop.add_argument("--clean",
                         default=False,
                         action=get_parser_bool_action(True),
                         help="Clean related containers data and logs")
    stop_group = ap_stop.add_mutually_exclusive_group()
    stop_group.add_argument(
        "--decommission",
        default=None,
        action=get_parser_bool_action(True),
        help=
        "Decommission doris node. for fe send drop sql, for be send decommission sql"
    )
    stop_group.add_argument(
        "--drop",
        default=None,
        action=get_parser_bool_action(True),
        help="Drop doris node. for fe and be, both send drop sql")

    ap_restart = sub_aps.add_parser("restart", help="Restart doris containers")
    ap_restart.add_argument("IMAGE",
                            default="",
                            nargs="?",
                            help="Update containers' image")
    add_cluster_and_node_list_arg(ap_restart)

    ap_ls = sub_aps.add_parser("ls", help="List running compose projects")
    ap_ls.add_argument(
        "NAME",
        nargs="*",
        help="Specify multiple clusters, if specific, show all their containers"
    )
    ap_ls.add_argument("-a",
                       "--all",
                       default=False,
                       action=get_parser_bool_action(True),
                       help="Show all stopped and bad doris compose projects")

    return ap.format_usage(), ap.format_help(), ap.parse_args()


def main():
    usage, _, args = parse_args()
    timer = utils.Timer()
    if args.command == "up":
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
    elif args.command == "ls":
        return ls(args)
    else:
        timer.cancel()
        print(usage)
        return -1


if __name__ == '__main__':
    main()
