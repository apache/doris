#!/usr/bin/env python
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

import MySQLdb

# NOTE: The default organization of meta info is cascading, we flatten its structure
# NOTE: We get schema-hash from proc '/dbs/db_id/tbl_id/'index_schema'
class FeMetaResolver:
    def __init__(self, fe_host, query_port, user, query_pwd):
        self.fe_host = fe_host
        self.query_port = query_port
        self.user = user
        self.query_pwd = query_pwd

        self.db = None
        self.cur = None

        self.be_list = []
        self.db_list = []
        # Only base tables, excluding rollups
        self.table_list = []
        # All rollups, including base tables
        self.rollup_map = {}
        self.partition_list = []
        self.index_list = []
        self.tablet_map = {}

    def init(self):
        self.connect_mysql()
        self.fetch_be_list();
        self.fetch_db_list();
        self.fetch_table_list();
        self.fetch_rollup_map();
        self.fetch_partition_list();
        self.fetch_idx_list();
        self._merge_schema_hash_to_idx_list()
        self.fetch_tablet_list();
        self.close()

    def connect_mysql(self):
        try:
            self.db = MySQLdb.connect(host=self.fe_host, port=self.query_port, 
                                      user=self.user,
                                      passwd=self.query_pwd)
            self.cur = self.db.cursor()
        except MySQLdb.Error as e:
            print ("Failed to connect fe server. error %s:%s" % (str(e.args[0]), e.args[1]))
            exit(-1);

    def exec_sql(self, sql):
        try:
            self.cur.execute(sql)
        except MySQLdb.Error as e:
            print ("exec sql error %s:%s" % (str(e.args[0]), e.args[1]))
            exit(-1);

    def close(self):
        if self.db.open:
            self.cur.close()
            self.db.close()

    def fetch_be_list(self):
        show_be_sql = "show backends"
        self.exec_sql(show_be_sql);
        be_list = self.cur.fetchall()
        for be_tuple in be_list :
            be = {}
            be['be_id'] = long(be_tuple[0])
            be['ip'] = be_tuple[2]
            be['http_port'] = be_tuple[5]
            self.be_list.append(be)

        return

    def fetch_db_list(self):
        show_database_sql = "show proc \"/dbs\" "
        self.exec_sql(show_database_sql);
        db_list = self.cur.fetchall()
        for db_tuple in db_list :
            db = {}
            if long(db_tuple[0]) <= 0:
                continue
            db['db_id'] = long(db_tuple[0])
            db['db_name'] = db_tuple[1]
            self.db_list.append(db)

    def fetch_table_list(self):
        for db in self.db_list:
            self._fetch_tables_by_db(db)

    def _fetch_tables_by_db(self, db):
        sql = "show proc \"/dbs/%s\" " % db['db_id']
        self.exec_sql(sql);
        table_list = self.cur.fetchall()
        for table_tuple in table_list :
            if table_tuple[6] == "OLAP":
                table = {}
                table['db_id'] = db['db_id'] 
                table['db_name'] = db['db_name'] 
                table['tbl_id'] = long(table_tuple[0])
                table['tbl_name'] = table_tuple[1]
                self.table_list.append(table)
        return

    def fetch_rollup_map(self):
        for table in self.table_list:
            self._fetch_rollups_by_table(table);

    def _fetch_rollups_by_table(self, table):
        sql = "show proc \"/dbs/%s/%s/index_schema\" " % (table['db_id'], table['tbl_id'])
        self.exec_sql(sql);
        index_list = self.cur.fetchall()
        for index_tuple in index_list :
            index = {}
            index['tbl_id'] = table['tbl_id'] 
            index['tbl_name'] = table['tbl_name'] 
            index['idx_id'] = long(index_tuple[0])
            index['schema_hash'] = long(index_tuple[3])
            self.rollup_map[index['idx_id']] = index
        return

    def fetch_partition_list(self):
        for table in self.table_list:
            self._fetch_partitions_by_table(table);

    def _fetch_partitions_by_table(self, table):
        sql = "show proc \"/dbs/%s/%s/partitions\" " % (table['db_id'], table['tbl_id'])
        self.exec_sql(sql);
        partition_list = self.cur.fetchall()
        for partition_tuple in partition_list :
            partition = {}
            partition['db_id'] = table['db_id'] 
            partition['db_name'] = table['db_name'] 
            partition['tbl_id'] = table['tbl_id'] 
            partition['tbl_name'] = table['tbl_name'] 
            partition['partition_id'] = long(partition_tuple[0])
            partition['partition_name'] = partition_tuple[1]
            self.partition_list.append(partition)
        return

    def fetch_idx_list(self):
        for partition in self.partition_list:
            self._fetch_idxes_by_partition(partition);

    def _fetch_idxes_by_partition(self, partition):
        sql = "show proc \"/dbs/%s/%s/partitions/%s\" " % \
                (partition['db_id'], partition['tbl_id'], partition['partition_id'])
        self.exec_sql(sql);
        index_list = self.cur.fetchall()
        for idx_tuple in index_list :
            idx = {}
            idx['db_id'] = partition['db_id'] 
            idx['db_name'] = partition['db_name'] 
            idx['tbl_id'] = partition['tbl_id'] 
            idx['tbl_name'] = partition['tbl_name'] 
            idx['partition_id'] = partition['partition_id']
            idx['partition_name'] = partition['partition_name']
            idx['idx_id'] = long(idx_tuple[0])
            idx['idx_name'] = idx_tuple[1]
            idx['idx_state'] = idx_tuple[2]
            self.index_list.append(idx)
        return

    def _merge_schema_hash_to_idx_list(self):
        for index in self.index_list:
            idx_id = index['idx_id']
            rollup = self.rollup_map.get(idx_id)
            index['schema_hash'] = rollup['schema_hash']

    def fetch_tablet_list(self):
        for index in self.index_list:
            self._fetch_tablets_by_index(index);

    def _fetch_tablets_by_index(self, index):
        sql = "show proc \"/dbs/%s/%s/partitions/%s/%s\" " % \
                (index['db_id'], index['tbl_id'], index['partition_id'], index['idx_id'])
        self.exec_sql(sql);
        tablet_list = self.cur.fetchall()
        for tablet_tuple in tablet_list :
            tablet = {}
            tablet['db_id'] = index['db_id'] 
            tablet['db_name'] = index['db_name'] 
            tablet['tbl_id'] = index['tbl_id'] 
            tablet['tbl_name'] = index['tbl_name'] 
            tablet['partition_id'] = index['partition_id']
            tablet['partition_name'] = index['partition_name']
            tablet['idx_id'] = index['idx_id']
            tablet['idx_name'] = index['idx_name'] 
            tablet['idx_state'] = index['idx_state']
            tablet['tablet_id'] = long(tablet_tuple[0])
            tablet['replica_id'] = long(tablet_tuple[1])
            tablet['be_id'] = long(tablet_tuple[2])
            tablet['schema_hash'] = index["schema_hash"]
            self.tablet_map[tablet['tablet_id']] = tablet
        return

    def debug_output(self):
        print "be_list:"
        self._print_list(self.be_list)
        print
        print "database_list:"
        self._print_list(self.db_list)
        print
        print "table_list:"
        self._print_list(self.table_list)
        print
        print "rollup_list:"
        self._print_list(self.rollup_map.values())
        print
        print "partition_list:"
        self._print_list(self.partition_list)
        print
        print "index_list:"
        self._print_list(self.index_list)
        print
        print "tablet_map:(%s), print up to ten here:" % len(self.tablet_map)
        self._print_list(self.tablet_map.values()[0:10])
        print

    def _print_list(self, one_list):
        for item in one_list:
            print item

    def get_tablet_by_id(self, tablet_id):
        return self.tablet_map.get(tablet_id)

    def get_all_tablets(self):
        return self.tablet_map.values()

