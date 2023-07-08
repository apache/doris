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

import cluster as CLUSTER
import os.path
import pymysql
import utils

LOG = utils.get_logger()


class DBFE(object):

    def __init__(self, id, query_port, is_master, alive, last_heartbeat,
                 err_msg):
        self.id = id
        self.query_port = query_port
        self.is_master = is_master
        self.alive = alive
        self.last_heartbeat = last_heartbeat
        self.err_msg = err_msg


class DBBE(object):

    def __init__(self, id, decommissioned, alive, last_heartbeat, err_msg):
        self.id = id
        self.decommissioned = decommissioned
        self.alive = alive
        self.last_heartbeat = last_heartbeat
        self.err_msg = err_msg


def get_db_states(cluster_name):
    assert cluster_name
    containers = utils.get_doris_containers(cluster_name).get(
        cluster_name, None)
    if not containers:
        return None, None
    running_fe = {}
    for container in containers:
        if utils.is_container_running(container):
            _, node_type, id = utils.parse_service_name(container.name)
            if node_type == CLUSTER.Node.TYPE_FE:
                query_port = utils.get_map_ports(container).get(
                    CLUSTER.FE_QUERY_PORT, None)
                if query_port:
                    running_fe[id] = query_port
    if not running_fe:
        return None, None

    master_fe_ip_file = os.path.join(CLUSTER.get_status_path(cluster_name),
                                     "master_fe_ip")
    query_port = None
    if os.path.exists(master_fe_ip_file):
        with open(master_fe_ip_file, "r") as f:
            master_fe_ip = f.read()
            if master_fe_ip:
                master_id = CLUSTER.Node.get_id_from_ip(master_fe_ip)
                query_port = running_fe.get(master_id, None)
    if not query_port:
        query_port = list(running_fe.values())[0]

    fe_states = {}
    be_states = {}
    with pymysql.connect(user="root", host="127.0.0.1",
                         port=query_port) as conn:
        with conn.cursor() as cursor:
            cursor.execute("show frontends;")
            for record in cursor.fetchall():
                ip = record[1]
                is_master = record[7] == "true"
                alive = record[10] == "true"
                last_heartbeat = record[12]
                err_msg = record[14]
                id = CLUSTER.Node.get_id_from_ip(ip)
                query_port = running_fe.get(id, None)
                fe_states[id] = DBFE(id, query_port, is_master, alive,
                                     last_heartbeat, err_msg)

            cursor.execute("show backends;")
            for record in cursor.fetchall():
                ip = record[1]
                last_heartbeat = record[7]
                alive = record[8] == "true"
                decommissioned = record[9] == "true"
                err_msg = record[18]
                id = CLUSTER.Node.get_id_from_ip(ip)
                be_states[id] = DBBE(id, decommissioned, alive, last_heartbeat,
                                     err_msg)

    return fe_states, be_states
