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

suite("test_upgrade_downgrade_olap_mtmv_2","p0,mtmv,restart_fe") {
    String suiteName = "mtmv_up_down_olap"
    String dbName = context.config.getDbNameByFile(context.file)
    String mvName = "${suiteName}_mtmv"
    String tableName = "${suiteName}_table"

    String dropTableName1 = """${suiteName}_DropTableName1"""
    String dropTableName2 = """${suiteName}_DropTableName2"""
    String dropTableName4 = """${suiteName}_DropTableName4"""
    String dropMtmvName1 = """${suiteName}_dropMtmvName1"""
    String dropMtmvName2 = """${suiteName}_dropMtmvName2"""
    String dropMtmvName3 = """${suiteName}_dropMtmvName3"""



    def get_follower_ip = {
        def result = sql """show frontends;"""
        logger.info("result:" + result)
        for (int i = 0; i < result.size(); i++) {
            if (result[i][7] == "FOLLOWER" && result[i][8] == "false" && result[i][11] == "true") {
                return result[i][1]
            }
        }
        return "null"
    }
    def get_master_ip = {
        def result = sql """show frontends;"""
        logger.info("result:" + result)
        for (int i = 0; i < result.size(); i++) {
            if (result[i][7] == "FOLLOWER" && result[i][8] == "true" && result[i][11] == "true") {
                return result[i][1]
            }
        }
        return "null"
    }

    def sql2 = "SELECT a.* FROM ${dropTableName2} a inner join ${dropTableName4} b on a.user_id=b.user_id;"

    def follower_ip = get_follower_ip()
    def master_ip = get_master_ip()

    def tokens = context.config.jdbcUrl.split('/')
    def url_tmp1 = tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"
    def follower_jdbc_url = url_tmp1.replaceAll(/\/\/[0-9.]+:/, "//${follower_ip}:")
    logger.info("follower_jdbc_url: " + follower_jdbc_url)

    def master_jdbc_url = url_tmp1.replaceAll(/\/\/[0-9.]+:/, "//${master_ip}:")
    logger.info("master_jdbc_url: " + master_jdbc_url)

    connect('root', context.config.jdbcPassword, follower_jdbc_url) {
        mv_rewrite_success(sql2, dropMtmvName2)
    }

    connect('root', context.config.jdbcPassword, master_ip) {
        mv_rewrite_success(sql2, dropMtmvName2)
    }

}
