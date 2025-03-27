// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


/*
 * After running all regression-tests, check all db and tables
 * have same data.
 */
Suite.metaClass.check_multi_cluster_result = { clusterNames ->
    def getColumnCount = { tableName ->
        def r = sql """ show columns from ${tableName} """
        return r[0].size()
    }

    def getSelectSql = { tableName ->
        def nColumns = getColumnCount(tableName)
        def s
        if (nColumns > 1) {
            s = "select * from ${tableName} order by 1"
            for (int i = 1; i < nColumns; i++) {
                s += ",${i+1}"
            }
        } else {
            s = "select * from ${tableName}"
        }

        s += " limit 100"
        return s
    }

    // main logic
    def clusters = clusterNames.tokenize(",")
    if (clusters.size() <= 1) {
        return
    }

    def ignore_dbs = ["mysql", "information_schema"]
    def dbs = sql """ show databases """
    dbs.forEach { db ->
        db = db[0]
        if (ignore_dbs.contains(db)) {
            return
        }
        sql """ use ${db} """
        def tables = sql """ show tables """
        tables.forEach { t -> 
            t = t[0]
            logger.info("process db: ${db}, table: ${t}")
            def q = getSelectSql(t)
            def result = []
            clusters.forEach { cluster ->
                sql """ use @${cluster} """
                result.add(sql """ ${q} """ )
            }

            if (result.toSet().size() != 1) {
                throw new IllegalArgumentException(""" different result, db: ${db}, table: ${t}. \n${results}  """)
            } 
        }
    }
}
