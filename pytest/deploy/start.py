#!/bin/env python
# -*- coding: utf-8 -*-
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

"""
This module start Palo.
Date:    2015/10/07 17:23:06
"""
import os
import time
import threading
import socket

import env_config
import execute
import load_cluster


def start_one_fe(host_name):
    """start one fe
    """
    cmd = 'export JAVA_HOME=%s;cd %s/fe;sh bin/start_fe.sh --daemon' % \
            (env_config.JAVA_HOME, env_config.fe_path)
    status, output = execute.exe_cmd(cmd, host_name)
    time.sleep(10)


def start_one_fe_with_helper(host_name, master_host_port=None):
    """start one fe with helper
    """
    edit_log_port = env_config.fe_query_port - 20
    if master_host_port is None:
        master_host_port = '%s:%d' % (env_config.master, edit_log_port)

    cmd = 'export JAVA_HOME=%s;cd %s/fe;sh bin/start_fe.sh --helper %s --daemon' % \
            (env_config.JAVA_HOME, env_config.fe_path, master_host_port)
    status, output = execute.exe_cmd(cmd, host_name)
    time.sleep(10)


def start_one_be(host_name):
    """start one be
    """
    cmd = 'export PATH=$PATH:/sbin; export JAVA_HOME=%s; cd %s/be; ' \
          'sh bin/start_be.sh --daemon' % (env_config.JAVA_HOME, env_config.be_path)
    status, output = execute.exe_cmd(cmd, host_name)
    time.sleep(10)


def start_master():
    """start master
    """
    start_one_fe(env_config.master)


def start_other_fe():
    """start fe
    """
    start_fe_threads = []
    for host_name in env_config.follower_list + \
            env_config.observer_list + env_config.dynamic_add_fe_list:
        t = threading.Thread(target=start_one_fe_with_helper, args=(host_name,))
        t.start()
        start_fe_threads.append(t)

    for t in start_fe_threads:
        t.join()


def start_be():
    """start be
    """
    start_be_threads = []
    for host_name in env_config.be_list + env_config.dynamic_add_be_list:
        t = threading.Thread(target=start_one_be, args=(host_name,))
        t.start()
        start_be_threads.append(t)

    for t in start_be_threads:
        t.join()


def add_be():
    """add be
    """
    for host in env_config.be_list:
        sql = 'ALTER SYSTEM ADD BACKEND "%s:%d"' % (host, env_config.heartbeat_service_port)
        cmd = "mysql -h %s -P%s -u root -e '%s'" % (env_config.master, env_config.fe_query_port, sql)
        os.system(cmd)


def add_follower():
    """add follower
    """
    for follower in env_config.follower_list:
        sql = 'ALTER SYSTEM ADD FOLLOWER "%s:%d"' % (socket.gethostbyname(follower), env_config.edit_log_port)
        cmd = "mysql -h %s -P%s -u root -e '%s'" % (env_config.master, env_config.fe_query_port, sql)
        os.system(cmd)


def add_observer():
    """add observer
    """
    edit_log_port = env_config.fe_query_port - 20
    for observer in env_config.observer_list:
        sql = 'ALTER SYSTEM ADD OBSERVER "%s:%d"' % (socket.gethostbyname(observer), env_config.edit_log_port)
        cmd = "mysql -h %s -P%s -u root -e '%s'" % (env_config.master, env_config.fe_query_port, sql)
        os.system(cmd)


def add_load_cluster():
    """add load cluster
    """
    sql_1, sql_2 = load_cluster.gen_add_load_cluster_sql('TestUser')
    cmd_1 = 'mysql -h %s -P%s -u TestUser@test_cluster -e "%s"' % (env_config.master, env_config.fe_query_port, sql_1)
    os.system(cmd_1)
    cmd_2 = 'mysql -h %s -P%s -u TestUser@test_cluster -e "%s"' % (env_config.master, env_config.fe_query_port, sql_2)
    os.system(cmd_2)


def create_test_cluster():
    """create test cluster
    """
    sql_1 = 'CREATE CLUSTER test_cluster PROPERTIES("instance_num"="4") IDENTIFIED BY ""'
    sql_2 = 'enter test_cluster;CREATE USER "TestUser" SUPERUSER'
    cmd_1 = "mysql -h %s -P%s -u root -e '%s'" % (env_config.master, env_config.fe_query_port, sql_1)
    os.system(cmd_1)
    cmd_2 = "mysql -h %s -P%s -u root -e '%s'" % (env_config.master, env_config.fe_query_port, sql_2)
    os.system(cmd_2)


def add_default_load_cluster():
    """default load cluster for user root"""
    sql_1, sql_2 = load_cluster.gen_add_load_cluster_sql('root')
    cmd_1 = 'mysql -h %s -P%s -u root@default_cluster -p%s -e "%s"' % (env_config.master,
                                                                       env_config.fe_query_port,
                                                                       env_config.fe_password, sql_1)
    os.system(cmd_1)
    cmd_2 = 'mysql -h %s -P%s -u root@default_cluster -p%s -e "%s"' % (env_config.master,
                                                                       env_config.fe_query_port,
                                                                       env_config.fe_password, sql_2)
    os.system(cmd_2)


def add_password():
    """add root user password"""
    sql = "set password for 'root'@'%%' = PASSWORD('%s')" % env_config.fe_password
    cmd = 'mysql -h %s -P%s -uroot -e "%s"' % (env_config.master, env_config.fe_query_port, sql)
    os.system(cmd)


def add_brokers():
    """add broker"""
    for broker_name, broker_node in env_config.broker_list.items():
        sql = "ALTER SYSTEM ADD BROKER %s '%s'" % (broker_name, broker_node)
        cmd = 'mysql -h %s -P%s -uroot -e "%s"' % (env_config.master, env_config.fe_query_port, sql)
        os.system(cmd)


def add_auditload_plugin():
    """add audit load plugin"""
    sql = """
          create table doris_audit_db__.doris_audit_tbl__ \
          ( \
              query_id varchar(48) comment 'Unique query id', \
              \`time\` datetime not null comment 'Query start time', \
              client_ip varchar(32) comment 'Client IP', \
              user varchar(64) comment 'User name', \
              catalog varchar(128) comment 'Catalog of this query', \
              db varchar(96) comment 'Database of this query', \
              state varchar(8) comment 'Query result state. EOF, ERR, OK', \
              query_time bigint comment 'Query execution time in millisecond', \
              scan_bytes bigint comment 'Total scan bytes of this query', \
              scan_rows bigint comment 'Total scan rows of this query', \
              return_rows bigint comment 'Returned rows of this query', \
              stmt_id int comment 'An incremental id of statement', \
              is_query tinyint comment 'Is this statemt a query. 1 or 0', \
              frontend_ip varchar(32) comment 'Frontend ip of executing this statement', \
              cpu_time_ms bigint comment 'Total scan cpu time in millisecond of this query', \
              sql_hash varchar(48) comment 'Hash value for this query', \
              sql_digest varchar(48) comment 'Sql digest for this query', \
              peak_memory_bytes bigint comment 'Peak memory bytes used on all backends of this query', \
              stmt string comment 'The original statement, trimed if longer than 2G, ' \
          ) engine=OLAP \
          duplicate key(query_id, \`time\`, client_ip) \
          partition by range(\`time\`) () \
          distributed by hash(query_id) buckets 1 \
          properties( \
              'dynamic_partition.time_unit' = 'HOUR', \
              'dynamic_partition.start' = '-48', \
              'dynamic_partition.end' = '3', \
              'dynamic_partition.prefix' = 'p', \
              'dynamic_partition.buckets' = '1', \
              'dynamic_partition.enable' = 'true', \
              'replication_num' = '3' \
          );
          """
    cmd = 'mysql -h %s -P%s -uroot -p%s -e "%s"' % (env_config.master, env_config.fe_query_port,
                                                    env_config.fe_password, "create database doris_audit_db__")
    os.system(cmd)
    cmd = 'mysql -h %s -P%s -uroot -p%s -e "%s"' % (env_config.master, env_config.fe_query_port,
                                                    env_config.fe_password, sql)
    os.system(cmd)
    sql = "INSTALL PLUGIN FROM '%s/fe/plugin_auditloader'" % env_config.fe_path
    cmd = 'mysql -h %s -P%s -uroot -p%s -e "%s"' % (env_config.master, env_config.fe_query_port,
                                                    env_config.fe_password, sql)
    os.system(cmd)


def start_palo(init_state=False, deploy_audit=False):
    """start palo
    """
    start_master()
    time.sleep(30)
    if init_state:
        add_follower()
        add_observer()
        add_be()
        add_brokers()
        add_password()
    start_other_fe()
    start_be()
    time.sleep(5)
    if deploy_audit:
        add_auditload_plugin()


if __name__ == '__main__':
    start_palo()
