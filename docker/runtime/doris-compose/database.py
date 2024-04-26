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
import time
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

    def __init__(self, id, backend_id, decommissioned, alive, tablet_num,
                 last_heartbeat, err_msg):
        self.id = id
        self.backend_id = backend_id
        self.decommissioned = decommissioned
        self.alive = alive
        self.tablet_num = tablet_num
        self.last_heartbeat = last_heartbeat
        self.err_msg = err_msg


class DBManager(object):

    def __init__(self):
        self.fe_states = {}
        self.be_states = {}
        self.query_port = -1
        self.conn = None

    def set_query_port(self, query_port):
        self.query_port = query_port

    def get_fe(self, id):
        return self.fe_states.get(id, None)

    def get_be(self, id):
        return self.be_states.get(id, None)

    def load_states(self, query_ports):
        self._load_fe_states(query_ports)
        self._load_be_states()

    def drop_fe(self, fe_endpoint):
        id = CLUSTER.Node.get_id_from_ip(fe_endpoint[:fe_endpoint.find(":")])
        try:
            self._exec_query(
                "ALTER SYSTEM DROP FOLLOWER '{}'".format(fe_endpoint))
            LOG.info("Drop fe {} with id {} from db succ.".format(
                fe_endpoint, id))
        except Exception as e:
            if str(e).find("frontend does not exist") >= 0:
                LOG.info(
                    "Drop fe {} with id {} from db succ cause it does not exist in db."
                    .format(fe_endpoint, id))
                return
            raise e

    def drop_be(self, be_endpoint):
        id = CLUSTER.Node.get_id_from_ip(be_endpoint[:be_endpoint.find(":")])
        try:
            self._exec_query(
                "ALTER SYSTEM DROPP BACKEND '{}'".format(be_endpoint))
            LOG.info("Drop be {} with id {} from db succ.".format(
                be_endpoint, id))
        except Exception as e:
            if str(e).find("backend does not exists") >= 0:
                LOG.info(
                    "Drop be {} with id {} from db succ cause it does not exist in db."
                    .format(be_endpoint, id))
                return
            raise e

    def decommission_be(self, be_endpoint):
        old_tablet_num = 0
        id = CLUSTER.Node.get_id_from_ip(be_endpoint[:be_endpoint.find(":")])
        start_ts = time.time()
        if id not in self.be_states:
            self._load_be_states()
        if id in self.be_states:
            be = self.be_states[id]
            old_tablet_num = be.tablet_num
            if not be.alive:
                raise Exception("Decommission be {} with id {} fail " \
                        "cause it's not alive, maybe you should specific --drop-force " \
                        " to dropp it from db".format(be_endpoint, id))
        try:
            self._exec_query(
                "ALTER SYSTEM DECOMMISSION BACKEND '{}'".format(be_endpoint))
            LOG.info("Mark be {} with id {} as decommissioned, start migrate its tablets, " \
                    "wait migrating job finish.".format(be_endpoint, id))
        except Exception as e:
            if str(e).find("Backend does not exist") >= 0:
                LOG.info("Decommission be {} with id {} from db succ " \
                        "cause it does not exist in db.".format(be_endpoint, id))
                return
            raise e

        while True:
            self._load_be_states()
            be = self.be_states.get(id, None)
            if not be:
                LOG.info("Decommission be {} succ, total migrate {} tablets, " \
                        "has drop it from db.".format(be_endpoint, old_tablet_num))
                return
            LOG.info(
                    "Decommission be {} status: alive {}, decommissioned {}. " \
                    "It is migrating its tablets, left {}/{} tablets. Time elapse {} s."
                .format(be_endpoint, be.alive, be.decommissioned, be.tablet_num, old_tablet_num,
                        int(time.time() - start_ts)))

            time.sleep(5)

    def _load_fe_states(self, query_ports):
        fe_states = {}
        alive_master_fe_port = None
        for record in self._exec_query('''
            select Host, IsMaster, Alive, LastHeartbeat, ErrMsg
            from frontends()'''):
            ip, is_master, alive, last_heartbeat, err_msg = record
            is_master = utils.is_true(is_master)
            alive = utils.is_true(alive)
            id = CLUSTER.Node.get_id_from_ip(ip)
            query_port = query_ports.get(id, "")
            last_heartbeat = utils.escape_null(last_heartbeat)
            fe = FEState(id, query_port, is_master, alive, last_heartbeat,
                         err_msg)
            fe_states[id] = fe
            if is_master and alive and query_port:
                alive_master_fe_port = query_port
        self.fe_states = fe_states
        if alive_master_fe_port and alive_master_fe_port != self.query_port:
            self.query_port = alive_master_fe_port
            self._reset_conn()

    def _load_be_states(self):
        be_states = {}
        for record in self._exec_query('''
            select BackendId, Host, LastHeartbeat, Alive, SystemDecommissioned, TabletNum, ErrMsg
            from backends()'''):
            backend_id, ip, last_heartbeat, alive, decommissioned, tablet_num, err_msg = record
            backend_id = int(backend_id)
            alive = utils.is_true(alive)
            decommissioned = utils.is_true(decommissioned)
            tablet_num = int(tablet_num)
            id = CLUSTER.Node.get_id_from_ip(ip)
            last_heartbeat = utils.escape_null(last_heartbeat)
            be = BEState(id, backend_id, decommissioned, alive, tablet_num,
                         last_heartbeat, err_msg)
            be_states[id] = be
        self.be_states = be_states

    def _exec_query(self, sql):
        self._prepare_conn()
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def _prepare_conn(self):
        if self.conn:
            return
        if self.query_port <= 0:
            raise Exception("Not set query_port")
        self._reset_conn()

    def _reset_conn(self):
        self.conn = pymysql.connect(user="root",
                                    host="127.0.0.1",
                                    read_timeout=10,
                                    port=self.query_port)


def get_db_mgr(cluster_name, required_load_succ=True):
    assert cluster_name
    db_mgr = DBManager()
    containers = utils.get_doris_containers(cluster_name).get(
        cluster_name, None)
    if not containers:
        return db_mgr
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
        return db_mgr

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
        else:
            query_port = list(alive_fe_ports.values())[0]

    db_mgr.set_query_port(query_port)
    try:
        db_mgr.load_states(alive_fe_ports)
    except Exception as e:
        if required_load_succ:
            raise e
        #LOG.exception(e)

    return db_mgr
