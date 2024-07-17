import pymysql as MySQLdb

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

class FeUserResolver:
    def __init__(self, fe_host, query_port, user, query_pwd):
        self.fe_host = fe_host
        self.query_port = query_port
        self.user = user
        self.query_pwd = query_pwd
        self.db = None
        self.cur = None
        self.user_list = []

    def init(self):
        self.connect_mysql();
        self._fetch_property_by_user();
        self.close()

    def connect_mysql(self):
        try:
            self.db = MySQLdb.connect(host=self.fe_host, port=self.query_port,
                                      user=self.user,
                                      passwd=self.query_pwd)
            self.cur = self.db.cursor()
        except MySQLdb.Error as e:
            print("Failed to connect fe server. error %s:%s" % (str(e.args[0]), e.args[1]))
            exit(-1);

    def exec_sql(self, sql):
        try:
            self.cur.execute(sql)
        except MySQLdb.Error as e:
            print("exec sql error %s:%s" % (str(e.args[0]), e.args[1]))
            exit(-1);

    def close(self):
        if self.db.open:
            self.cur.close()
            self.db.close()

    def _fetch_property_by_user(self):
        sql_all_grants = "show all grants;"
        self.exec_sql(sql_all_grants)
        user_grant_list = self.cur.fetchall()
        for user_grant in user_grant_list:
            tmp_dict = {}
            tmp_dict['user_identity'] = user_identity = user_grant[0].split('@')[0].replace("'", '').replace('"', '')
            if user_identity.lower() != 'admin':
                tmp_dict['user_grant_host'] = user_grant[0].split('@')[1].replace("'", '').replace('"', '')
                tmp_dict['global_privs'] = user_grant[2]
                tmp_dict['catalog_privs'] = user_grant[3]
                tmp_dict['database_privs'] = user_grant[4]
                tmp_dict['table_privs'] = user_grant[5]
                tmp_dict['resource_privs'] = user_grant[6]
                sql = "SHOW PROPERTY FOR %s" % (user_identity)
                self.exec_sql(sql);
                property_list = self.cur.fetchall()
                self.user_list.append({**tmp_dict, **dict(property_list)})
            else:
                continue
        return

    def print_list(self, is_g):
        one_list = self.user_list
        if not is_g:
            print('+----------------------------------+-----------------------------------')
            print('| Item                             | Info                             |')
            print('+----------------------------------+-----------------------------------')
            for index, items in enumerate(one_list):
                for k, v in items.items():
                    print('|  ' + k.ljust(32) + '|  ' + ('' if v is None else v).ljust(32) + '|')
                print('+----------------------------------+----------------------------------+')
        else:
            keys = one_list[0].keys()
            print('|'.join(keys))
            for one in one_list:
                row_data = []
                for key in keys:
                    value = str(one.get(key)).ljust(10)
                    row_data.append(value)
                print('|'.join(row_data))
