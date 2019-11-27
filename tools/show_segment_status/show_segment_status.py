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

import ConfigParser
import re
import sys
import os
import json
from urllib import urlopen

from fe_meta_resolver import FeMetaResolver 
from be_tablet_reslover import BeTabletResolver 

class Calc:
    def __init__(self, fe_meta, be_resolver):
        self.fe_meta = fe_meta
        self.be_resolver = be_resolver

    def calc_cluster_summary(self):
        self.calc_table_and_be_summary("", "", 0)
        return

    def calc_table_summary(self, db_name, table_name):
        self.calc_table_and_be_summary(db_name, table_name, 0)
        return

    def calc_be_summary(self, be_id):
        self.calc_table_and_be_summary("", "", be_id)
        return

    def calc_table_and_be_summary(self, db_name, table_name, be_id):
        total_rs_count = 0
        beta_rs_count = 0
        total_rs_size = 0
        beta_rs_size = 0
        total_rs_row_count = 0
        beta_rs_row_count = 0

        for tablet in self.fe_meta.get_all_tablets():
            # The db_name from meta contain cluster name, so use 'in' here
            if len(db_name) != 0 and (not (db_name in tablet['db_name'])):
                continue
            if len(table_name) != 0 and (tablet['tbl_name'] != table_name):
                continue;
            if be_id != 0 and tablet['be_id'] != be_id:
                continue
            rowsets = self.be_resolver.get_rowsets_by_tablet(tablet['tablet_id'])
            # If tablet has gone away, ignore it 
            if rowsets is None:
                continue
            for tablet_info in rowsets:
                total_rs_count += 1
                total_rs_row_count += tablet_info['num_rows']
                total_rs_size +=  tablet_info['data_disk_size']
                if tablet_info['is_beta']:
                    beta_rs_count += 1
                    beta_rs_size += tablet_info['data_disk_size']
                    beta_rs_row_count += tablet_info['num_rows']

        content_str = ""
        if len(db_name) != 0:
            content_str += ("db=%s " % db_name)
        if len(table_name) != 0:
            content_str += ("table=%s " % table_name)
        if be_id != 0:
            content_str += ("be=%s " % be_id)
        print "==========SUMMARY(%s)===========" % (content_str)
        print "rowset_count: %s / %s" % (beta_rs_count, total_rs_count)
        print "rowset_disk_size: %s / %s" % (beta_rs_size, total_rs_size)
        print "rowset_row_count: %s / %s" % (beta_rs_row_count, total_rs_row_count)
        print "==========================================================="
        return;

def main():
    cf = ConfigParser.ConfigParser()
    cf.read("./conf")
    fe_host = cf.get('cluster', 'fe_host')
    query_port = int(cf.get('cluster', 'query_port'))
    user = cf.get('cluster', 'user')
    query_pwd = cf.get('cluster', 'query_pwd')

    db_name = cf.get('cluster', 'db_name')
    table_name = cf.get('cluster', 'table_name')
    be_id = cf.getint('cluster', 'be_id')

    print "============= CONF ============="
    print "fe_host =", fe_host
    print "fe_query_port =", query_port
    print "user =", user 
    print "db_name =", db_name 
    print "table_name =", table_name 
    print "be_id =", be_id 
    print "===================================="

    fe_meta = FeMetaResolver(fe_host, query_port, user, query_pwd)
    fe_meta.init()
    fe_meta.debug_output()

    be_resolver = BeTabletResolver(fe_meta.be_list, fe_meta.tablet_map)
    be_resolver.init()
    be_resolver.debug_output()

    calc = Calc(fe_meta, be_resolver)
    calc.calc_cluster_summary()
    calc.calc_table_summary(db_name, table_name);
    calc.calc_be_summary(be_id);

if __name__ == '__main__':
    main()

