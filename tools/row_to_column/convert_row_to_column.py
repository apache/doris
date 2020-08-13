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
import json
import os 
import re
import sys
import time
from urllib import urlopen

import MySQLdb

class convert_row_to_column(object):
    def connect(self, host, port, http_port, username, password):
        """
        Use MySQLdb to connect to PALO 
        """
        self.host = host
        self.port = port
        self.http_port = http_port
        self.username = username
        self.passwd = password
        try:
            self.db = MySQLdb.connect(host=self.host, port=self.port, 
                                      user=self.username,
                                      passwd=self.passwd)
            self.cur = self.db.cursor()
        except MySQLdb.Error as e:
            print ("error %s:%s" % (str(e.args[0]), e.args[1]))
    
    def close(self):
        if self.db.open:
            self.cur.close()
            self.db.close()

    def run(self):
        url_list = "http://%s:%s@%s:%s/api/_get_ddl?db=default_cluster" % (
                        self.username, self.passwd, self.host, self.http_port)

        url = None
        show_databases_sql = "show databases"
        self.cur.execute(show_databases_sql)
        databases = self.cur.fetchall()
        for database_tuple in databases :
        #for database in ["habo_db", "tieba_recommend"]:
            database = database_tuple[0]
            show_tables_sql = "show tables from `" + database + "`"
            self.cur.execute(show_tables_sql)
            for table_tuple in self.cur:
                table = table_tuple[0]
                url = "%s:%s&tbl=%s" % (url_list, database, table)
                try:
                    doc = urlopen(url).read();
                    doc = json.loads(doc)
                except Exception as err:
                    print "url: %s, error: %s" % (url, err)
                    continue
                create_table_stmt = doc["TABLE"]
                ddl = create_table_stmt[0].encode("utf-8")
                if ddl.find("\"storage_type\" = \"ROW\"") != -1 :
                    table = re.search('CREATE TABLE `(.*)`', ddl).group(1)
                    print "alter table " + database + "." + table + " set(\"storage_type\"=\"column\");"

def main():
    cf = ConfigParser.ConfigParser()
    cf.read("./conf")
    host = cf.get('cluster', 'fe_host')
    port = int(cf.get('cluster', 'port'))
    http_port = int(cf.get('cluster', 'http_port'))
    user = cf.get('cluster', 'username')
    passwd = cf.get('cluster', 'password')

    converter = convert_row_to_column()
    converter.connect(host, port, http_port, user, passwd)
    converter.run();
    converter.close()

if __name__ == '__main__':
    main()

