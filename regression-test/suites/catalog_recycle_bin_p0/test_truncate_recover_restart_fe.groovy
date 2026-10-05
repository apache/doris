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
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType

suite("test_truncate_recover_restart_fe", 'docker') {
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.enableDebugPoints()
    options.feConfigs.add('publish_wait_time_second=-1')
    options.feConfigs.add('commit_timeout_second=10')
    options.feConfigs.add('sys_log_verbose_modules=org.apache.doris')
    options.beConfigs.add('enable_java_support=false')
    docker(options) {
        def table = "test_truncate_recover_restart_fe"
        // create table and insert data
        sql """ drop table if exists ${table}"""
        sql """
        create table ${table} (
            `id` int(11),
            `name` varchar(128),
            `da` date
        )
        engine=olap
        duplicate key(id)
        partition by range(da)(
            PARTITION p3 VALUES LESS THAN ('2023-01-01'),
            PARTITION p4 VALUES LESS THAN ('2024-01-01'),
            PARTITION p5 VALUES LESS THAN ('2025-01-01')
        )
        distributed by hash(id) buckets 2
        properties(
            "replication_num"="1",
            "light_schema_change"="true"
        );
        """

        sql """ insert into ${table} values(1, 'a', '2022-01-02'); """
        sql """ insert into ${table} values(2, 'a', '2023-01-02'); """
        sql """ insert into ${table} values(3, 'a', '2024-01-02'); """
        sql """ SYNC;"""

        qt_select_check_1 """ select * from  ${table} order by id,name,da; """

        sql """ truncate  table ${table}; """
        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()
        qt_select_check_2 """ select * from  ${table} order by id,name,da; """
        
        // after truncate , there would be new partition created,
        // drop forcefully so that this partitions not kept in recycle bin.
        sql """ ALTER TABLE ${table} DROP PARTITION p3 force; """
        sql """ ALTER TABLE ${table} DROP PARTITION p4 force; """
        sql """ ALTER TABLE ${table} DROP PARTITION p5 force; """

        sql """ recover partition p3  from ${table}; """
        sql """ recover partition p4  from ${table}; """
        sql """ recover partition p5  from ${table}; """    

        qt_select_check_3 """ select * from  ${table} order by id,name,da; """
        
        sql """ truncate  table ${table} PARTITION(p3); """

        qt_select_check_4 """ select * from  ${table} order by id,name,da; """ 
        // drop forcefully the partition which is created in the truncate partition process.
        sql """ ALTER TABLE ${table} DROP PARTITION p3 force; """
        sql """ recover partition p3  from ${table}; """
        qt_select_check_5 """ select * from  ${table} order by id,name,da; """    

        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()        
        qt_select_check_6 """ select * from  ${table} order by id,name,da; """    
    }
}
