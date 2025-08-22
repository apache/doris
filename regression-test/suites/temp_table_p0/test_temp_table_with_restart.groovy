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

suite('test_temp_table_with_restart', 'p0,docker') {
    def options = new ClusterOptions()
    options.setFeNum(2)
    options.setBeNum(1)
    options.connectToFollower = true
    docker(options) {
        sql """
        CREATE TEMPORARY TABLE IF NOT EXISTS `t_test_temp_table5` (
            `id` int,
            `add_date` date,
            `name` varchar(32)
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ('replication_num' = '1');
        """

        sql """
        insert into t_test_temp_table5 values (11,"2017-01-15","Alice"),(12,"2017-02-12","Bob"),(13,"2017-03-20","Carl");
        """

        def select_result10 = sql "select * from t_test_temp_table5"
        assertEquals(select_result10.size(), 3)

        def masterFeIndex = cluster.getMasterFe().index
        cluster.restartFrontends(masterFeIndex)
        sleep(30 * 1000)

        // after master fe switch, temp table created in non-master fe should still exist
        select_result10 = sql "select * from t_test_temp_table5"
        assertEquals(select_result10.size(), 3)

        def nonMasterFeIndex = cluster.getOneFollowerFe().index
        cluster.restartFrontends(nonMasterFeIndex)
        sleep(30 * 1000)

        // when the connected fe is shutdown, temp table created in it should be deleted
        context.reconnectFe()
        try {
            sql "select * from t_test_temp_table5;"
            throw new IllegalStateException("Should throw error")
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("does not exist in database"), ex.getMessage())
        }

        select_result10 = sql "show data"
        def containTempTable = false
        for(int i = 0; i < select_result10.size(); i++) {
            if (select_result10[i][0].contains("t_test_temp_table5")) {
                containTempTable = true;
            }
        }
        assertFalse(containTempTable)
    }

}