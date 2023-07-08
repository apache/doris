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

from cluster import Cluster, Node, REMOVE_TYPE_DECOMMISSION, REMOVE_TYPE_DECOMMISSION
import argparse
import utils
from multiprocessing.pool import ThreadPool
import shutil
import sys

LOG = utils.get_logger()


def up(args):
    if not args.NAME:
        raise Exception("Need specific not empty cluster name")
    try:
        cluster = Cluster.load(args.NAME)
        if args.IMAGE:
            cluster.set_image(args.IMAGE)
    except:
        if not args.IMAGE:
            raise Exception("New cluster must specific image")
        cluster = Cluster.new(args.NAME, args.IMAGE)
        LOG.info("Create new cluster {} succ, cluster path is {}".format(
            args.NAME, cluster.get_path()))
        if not args.fe:
            args.fe = 3
        if not args.be:
            args.be = 3

    if args.fe:
        cluster.set_node_num(Node.TYPE_FE, args.fe)
    if args.be:
        cluster.set_node_num(Node.TYPE_BE, args.be)

    cluster.save()

    if args.no_up:
        LOG.info("Not up cluster cause specific --no-up")
    else:
        utils.exec_docker_compose_command(cluster.get_compose_file(), "up",
                                          ["-d", "--remove-orphans"])
        LOG.info("Up cluster {} succ".format(args.NAME))


def down(args):
    cluster = Cluster.load(args.NAME)
    utils.exec_docker_compose_command(cluster.get_compose_file(), "down",
                                      ["--remove-orphans"])
    if args.clean:
        shutil.rmtree(cluster.get_path())
    LOG.info("Down cluster {} succ".format(args.NAME))


def add(args):
    cluster = Cluster.load(args.NAME)
    new_nodes = []
    if args.fe:
        for i in range(args.fe):
            node = cluster.add(Node.TYPE_FE, image=args.IMAGE)
            new_nodes.append(node)
    if args.be:
        for i in range(args.be):
            node = cluster.add(Node.TYPE_BE, image=args.IMAGE)
            new_nodes.append(node)

    LOG.info("Add new fe ids: {}, new be ids {}".format(
        [node.id for node in new_nodes if node.node_type() == Node.TYPE_FE],
        [node.id for node in new_nodes if node.node_type() == Node.TYPE_BE]))

    if not new_nodes:
        return

    cluster.save()
    if args.no_start:
        utils.exec_docker_compose_command(
            cluster.get_compose_file, ["--no-start"],
            [node.service_name() for node in new_nodes])
        LOG.info("Not start new add nodes cause specific --no-start")
    else:
        utils.exec_docker_compose_command(
            cluster.get_compose_file, ["-d"],
            [node.service_name() for node in new_nodes])
        LOG.info("Start new add succ, relate {} nodes".format(len(new_nodes)))


def get_relate_cluster_and_nodes(args):
    cluster = Cluster.load(args.NAME)

    def get_nodes_with_ids(node_type, ids):
        if ids is None:
            return []
        elif not ids:
            return cluster.get_all_node(node_type)
        else:
            return [cluster.get_node(node_type, id) for id in ids]

    nodes = get_nodes_with_ids(Node.TYPE_FE, args.fe) + get_nodes_with_ids(
        Node.TYPE_BE, args.be)

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
    LOG.info("Start run succ, total relate {} nodes".format(len(nodes)))


def remove(node, drop, decommission):
    cluster.run_docker_compose_with_nodes("stop", nodes)


def stop(args):
    cluster, nodes = get_relate_cluster_and_nodes(args)
    if not nodes:
        LOG.info("Stop run succ, total relate {} nodes".format(len(nodes)))
        return

    if not args.drop and not args.decommission:
        utils.exec_docker_compose_command(
            cluster.get_compose_file(), "stop",
            [node.service_name() for node in nodes])
        LOG.info("Stop run succ, total relate {} nodes".format(len(nodes)))
        return

    try:
        pool = ThreadPool(process=10)
        tasks = [
            pool.apply_async(remove, node, drop, decommission)
            for node in nodes
        ]
        for _ in tasks.get():
            pass
    finally:
        pool.close()
        pool.join

    if args.drop:
        LOG.info("Drop nodes cause has specify --drop")
    else:
        LOG.info("Decommission nodes cause has specify --decommission")

    if args.clean:
        utils.exec_docker_compose_command(
            cluster.get_compose_file(), "down", ["--remove-orphans"],
            [node.service_name() for node in nodes])
        for node in nodes:
            shutil.rmtree(node.get_path())
        LOG.info("Clean nodes's files cause has specify --clean")

    for node in nodes:
        cluster.remove(node.node_type(), node.id)

    cluster.save()
    LOG.info("Stop run succ, total relate {} nodes".format(len(nodes)))


def restart(args):
    cluster, nodes = get_relate_cluster_and_nodes(args)
    if args.IMAGE:
        for node in nodes:
            node.set_image(args.IMAGE)
    utils.exec_docker_compose_command(
        cluster.get_compose_file(),
        "restart",
        services=[node.service_name() for node in nodes])
    LOG.info("Restart run succ, total relate {} nodes".format(len(nodes)))


def get_parser_bool_action(is_store_true):
    if sys.version_info.major == 3 and sys.version_info.minor >= 9:
        return argparse.BooleanOptionalAction
    else:
        return "store_true" if is_store_true else "store_false"


def add_cluster_and_node_list_arg(ap):
    ap.add_argument("NAME", help="specify cluster name")
    ap.add_argument("--fe", nargs="*", type=int, help="fe ids, support multiple ids, " \
            "if specific --fe but not specific ids, apply to all fe")
    ap.add_argument("--be", nargs="*", type=int, help="be ids, support multiple ids, " \
            "if specific --be but not specific ids, apply to all be")


def parse_args():
    ap = argparse.ArgumentParser(description="")
    sub_aps = ap.add_subparsers(dest="command")

    ap_up = sub_aps.add_parser(
        "up", help="re run a doris cluster, no clean data and log")
    ap_up.add_argument("NAME", default="", help="specific cluster name")
    ap_up.add_argument("IMAGE",
                       default="",
                       nargs="?",
                       help="specify docker image")
    ap_up.add_argument("--fe",
                       type=int,
                       help="specify fe count, default 3 for new cluster")
    ap_up.add_argument("--be",
                       type=int,
                       help="specify be count, default 3 for new cluster")
    ap_up.add_argument("--no-up",
                       default=False,
                       action=get_parser_bool_action(True),
                       help="do not run cluster, only create")

    ap_down = sub_aps.add_parser("down", help="shutdown a cluster")
    ap_down.add_argument("NAME", help="specify cluster name")
    ap_down.add_argument("--clean",
                         default=False,
                         action=get_parser_bool_action(True),
                         help="clean all files")

    ap_add = sub_aps.add_parser("add", help="add multiple nodes")
    ap_add.add_argument("NAME", help="specify cluster name")
    ap_add.add_argument("--fe", type=int, help="specify new add fe num")
    ap_add.add_argument("--be", type=int, help="specify new add be num")
    ap_add.add_argument("--no-start",
                        default=False,
                        action=get_parser_bool_action(True),
                        help="do not start this new node, create only")

    ap_start = sub_aps.add_parser("start", help="start multiple nodes")
    add_cluster_and_node_list_arg(ap_start)
    ap_start.add_argument("IMAGE",
                          default="",
                          nargs="?",
                          help="update node docker image")

    ap_stop = sub_aps.add_parser("stop", help="stop multiple nodes")
    add_cluster_and_node_list_arg(ap_stop)
    ap_stop.add_argument("--clean",
                         default=False,
                         action=get_parser_bool_action(True),
                         help="clean nodes' files")
    group = ap_stop.add_mutually_exclusive_group()
    group.add_argument(
        "--decommission",
        default=None,
        action=get_parser_bool_action(True),
        help="drop node. for fe send drop sql, for be send decommission sql")
    group.add_argument("--drop",
                       default=None,
                       action=get_parser_bool_action(True),
                       help="drop node. for fe and be, both send drop sql")

    ap_restart = sub_aps.add_parser("restart", help="restart multiple nodes")
    ap_restart.add_argument("IMAGE",
                            default="",
                            nargs="?",
                            help="update node docker image")
    add_cluster_and_node_list_arg(ap_restart)

    return ap.format_usage(), ap.format_help(), ap.parse_args()


def main():
    usage, _, args = parse_args()
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
    else:
        print(usage)
        return -1


if __name__ == '__main__':
    main()
