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


class FEState(object):

    def __init__(self, id, query_port, is_master, alive, last_heartbeat,
                 err_msg):
        self.id = id
        self.query_port = query_port
        self.is_master = is_master
        self.alive = alive
        self.last_heartbeat = last_heartbeat
        self.err_msg = err_msg


class BEState(object):

    def __init__(self, id, decommissioned, alive, last_heartbeat, err_msg):
        self.id = id
        self.decommissioned = decommissioned
        self.alive = alive
        self.last_heartbeat = last_heartbeat
        self.err_msg = err_msg


class DB(object):

    def __init__(self):
        self.fe_states = {}
        self.be_states = {}
        self.query_port = -1
        self.conn = None
        self.updated = False

    def set_query_port(self, query_port):
        self.query_port = query_port

    def get_fe(self, id):
        return self.fe_states.get(id, None)

    def get_be(self, id):
        return self.be_states.get(id, None)

    def update_states(self, query_ports):
        self._update_fe_states(query_ports)
        self._update_be_states()
        self.updated = True

    def _update_fe_states(self, query_ports):
        fe_states = {}
        alive_master_fe_port = None
        for record in self._exec_sql("show frontends"):
            ip = record[1]
            is_master = record[7] == "true"
            alive = record[10] == "true"
            last_heartbeat = record[12]
            err_msg = record[14]
            id = CLUSTER.Node.get_id_from_ip(ip)
            query_port = query_ports.get(id, None)
            fe = FEState(id, query_port, is_master, alive, last_heartbeat,
                         err_msg)
            fe_states[id] = fe
            if is_master and alive and query_port:
                alive_master_fe_port = query_port
        self.fe_states = fe_states
        if alive_master_fe_port and alive_master_fe_port != self.query_port:
            self.conn = pymysql.connect(user="root",
                                        host="127.0.0.1",
                                        port=alive_master_fe_port)
            self.query_port = alive_master_fe_port

    def _update_be_states(self):
        be_states = {}
        for record in self._exec_sql("show backends"):
            ip = record[1]
            last_heartbeat = record[7]
            alive = record[8] == "true"
            decommissioned = record[9] == "true"
            err_msg = record[18]
            id = CLUSTER.Node.get_id_from_ip(ip)
            be = BEState(id, decommissioned, alive, last_heartbeat, err_msg)
            be_states[id] = be
        self.be_states = be_states

    def _exec_sql(self, sql):
        self._prepare_conn()
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def _prepare_conn(self):
        if self.conn:
            return
        if self.query_port <= 0:
            raise Exception("Not set query_port")
        self.conn = pymysql.connect(user="root",
                                    host="127.0.0.1",
                                    port=self.query_port)


def get_current_db(cluster_name):
    assert cluster_name
    db = DB()
    containers = utils.get_doris_containers(cluster_name).get(
        cluster_name, None)
    if not containers:
        return db
    alive_fe_ports = {}
    for container in containers:
        if utils.is_container_running(container):
            _, node_type, id = utils.parse_service_name(container.name)
            if node_type == CLUSTER.Node.TYPE_FE:
                query_port = utils.get_map_ports(container).get(
                    CLUSTER.FE_QUERY_PORT, None)
                if query_port:
                    alive_fe_ports[id] = query_port
    if not alive_fe_ports:
        return db

    master_fe_ip_file = os.path.join(CLUSTER.get_status_path(cluster_name),
                                     "master_fe_ip")
    query_port = None
    if os.path.exists(master_fe_ip_file):
        with open(master_fe_ip_file, "r") as f:
            master_fe_ip = f.read()
            if master_fe_ip:
                master_id = CLUSTER.Node.get_id_from_ip(master_fe_ip)
                query_port = alive_fe_ports.get(master_id, None)
    if not query_port:
        # A new cluster's master is fe-1
        if 1 in alive_fe_ports:
            query_port = alive_fe_ports[1]
        query_port = list(alive_fe_ports.values())[0]

    db.set_query_port(query_port)
    try:
        db.update_states(alive_fe_ports)
    except Exception as e:
        LOG.exception(e)

    return db
