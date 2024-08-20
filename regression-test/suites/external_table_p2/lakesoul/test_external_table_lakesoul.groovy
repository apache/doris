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

suite("test_external_table_lakesoul", "p2,external,lakesoul,external_remote,external_remote_lakesoul") {
    String enabled = context.config.otherConfigs.get("enablelakesoulTest")
      // query data test
        def q1 = """ select count(*) from region; """
        def q11 = """ select count(*) from nation; """
        // data test
         def q2 = """ select * from nation  order by n_name; """
         def q3 = """ select * from nation order by n_name limit 2; """
         def q9 = """ select * from lineitem limit 2; """ // mutil types
        // test partition table filter
         def q4 =  """ select * from supplier  where s_nationkey = 1 limit 2; """
         def q5 = """ select * from supplier  where s_nationkey < 2 limit 2; """
         def q6 = """ select * from nation  where n_name = 'CHINA' or n_name like 'C%'; """
            
	def q7 =  """ select * from nation,region where n_nationkey = r_regionkey; """
        def q8 =  """ select count(*) from region group by r_regionkey; """


    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "lakesoul"
        String db_name = "default"
        String pg_user = context.config.otherConfigs.get("lakesoulPGUser")
        String pg_pwd = context.config.otherConfigs.get("lakesoulPGPwd")
        String pg_url = context.config.otherConfigs.get("lakesoulPGUrl")
        String minio_ak = context.config.otherConfigs.get("lakesoulMinioAK")
        String minio_sk = context.config.otherConfigs.get("lakesoulMinioSK")
        String minio_endpoint = context.config.otherConfigs.get("lakesoulMinioEndpoint")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog lakesoul  properties (
            'type'='lakesoul',
            'lakesoul.pg.username'='${pg_user}',
            'lakesoul.pg.password'='${pg_pwd}',
            'lakesoul.pg.url'='${pg_url}',
            'minio.endpoint'='${minio_endpoint}',
            'minio.access_key'='${minio_ak}',
            'minio.secret_key'='${minio_sk}'
            );"""

            // analyze
            sql """use `${catalog_name}`.`${db_name}`"""

        sql q1
        sql q2
        sql q3
        sql q4
        sql q5
        sql q6
        sql q7
        sql q8
        sql q9
        sql q11

    }
}
