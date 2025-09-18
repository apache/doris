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
import com.mysql.cj.jdbc.StatementImpl
import org.apache.doris.regression.suite.ClusterOptions

suite("test_fe_cached_partition_version", 'docker') {
    if (!isCloudMode()) {
        return
    }
    def options = new ClusterOptions()
    // one master, one observer
    options.setFeNum(2)
    options.setBeNum(1)
    options.cloudMode = true

    def insert_sql = { sql, expected_row_count ->
        def stmt = prepareStatement """ ${sql}  """
        def result = stmt.executeUpdate()
        logger.info("insert result: " + result)
        def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
        logger.info("result server info: " + serverInfo)
        assertEquals(result, expected_row_count)
        assertTrue(serverInfo.contains("'status':'VISIBLE'"))
    }
    // connect to follower/observer for testing, master will update
    options.connectToFollower = true
    for (def j = 0; j < 2; j++) {
        docker(options) {
            def tbl = 'test_cloud_partition_version_cache_ttl_ms'
            sql """ DROP TABLE IF EXISTS ${tbl} """
            try {
                sql """set global cloud_partition_version_cache_ttl_ms=0"""
                sql """
                    CREATE TABLE IF NOT EXISTS ${tbl} (
                        region VARCHAR(50) NOT NULL COMMENT "地区",
                        sales INT NOT NULL COMMENT "销售额"
                    )
                    ENGINE = olap
                    DUPLICATE KEY(region)
                    PARTITION BY LIST (region) 
                    (
                        PARTITION p_east VALUES IN ("Shanghai", "Nanjing"),
                        PARTITION p_south VALUES IN ("Guangzhou", "Shenzhen"),
                        PARTITION p_north VALUES IN ("Beijing", "Tianjin")
                    )
                    DISTRIBUTED BY HASH(region) BUCKETS 10
                    PROPERTIES (
                        "replication_num" = "1"
                    );
                    """
                def result = sql_return_maparray """ select * from ${tbl} """
                assertEquals(0, result.size())

                insert_sql """INSERT INTO ${tbl} VALUES ('Shanghai', 1})""", 1
                insert_sql """INSERT INTO ${tbl} VALUES ('Guangzhou', 1})""", 1
                result = sql_return_maparray """ select * from ${tbl} """
                assertEquals(2, result.size())

                // very large expiration time, test it is old
                sql """set global cloud_partition_version_cache_ttl_ms=10000000"""
                // trigger cache version of shanghai
                result = sql_return_maparray """ select * from ${tbl} where region = 'Shanghai' """
                assertEquals(1, result.size())

                insert_sql """INSERT INTO ${tbl} VALUES ('Shanghai', 1})""", 1
                result = sql_return_maparray """ select * from ${tbl} where region = 'Shanghai' """
                // observer/follower cannot see update since large expiration
                if (options.connectToFollower == true) {
                    assertEquals(1, result.size())
                } else { // master will always refresh cached verison after a load txn commit
                    assertEquals(2, result.size())
                }

                result = sql_return_maparray """ select * from ${tbl} """
                // observer/follower cannot see update since large expiration
                if (options.connectToFollower == true) {
                    assertEquals(2, result.size())
                } else { // master will always refresh cached verison after a load txn commit
                    assertEquals(3, result.size())
                }

                insert_sql """INSERT INTO ${tbl} VALUES ('Beijing', 1})""", 1
                result = sql_return_maparray """ select * from ${tbl} where region = 'Beijing' """
                // observer/follower cannot see update since large expiration
                if (options.connectToFollower == true) {
                    assertEquals(0, result.size())
                } else { // master will always refresh cached verison after a load txn commit
                    assertEquals(1, result.size())
                }

                // test small expiration
                sql """set global cloud_partition_version_cache_ttl_ms=1000"""
                Thread.sleep(1100)
                // all 4 inserts should be seen after expiration, select and refresh expiration
                result = sql_return_maparray """ select * from ${tbl} """
                assertEquals(4, result.size())

                insert_sql """INSERT INTO ${tbl} VALUES ('Beijing', 1})""", 1
                // refresh expiration
                result = sql_return_maparray """ select * from ${tbl} where region = 'Beijing' """
                // observer/follower cannot see update since cache expiration no reached
                if (options.connectToFollower == true) {
                    assertEquals(1, result.size())
                } else { // master will always refresh cached verison after a load txn commit
                    assertEquals(2, result.size())
                }

                // use the cached version
                result = sql_return_maparray """ select * from ${tbl} where region = 'Beijing' """
                // observer/follower cannot see update since cache expiration no reached
                if (options.connectToFollower == true) {
                    assertEquals(1, result.size())
                } else { // master will always refresh cached verison after a load txn commit
                    assertEquals(2, result.size())
                }

                Thread.sleep(1100) // wait version cache for exiration again
                insert_sql """INSERT INTO ${tbl} VALUES ('Beijing', 1})""", 1
                // refresh expiration, the insert will be seen since the version has expired
                result = sql_return_maparray """ select * from ${tbl} where region = 'Beijing' """
                assertEquals(3, result.size())

                // all the insert will be seen since the version has expired, refresh expiration
                result = sql_return_maparray """ select * from ${tbl} """
                assertEquals(6, result.size())

                // test no expiration, disable cache partition version 
                insert_sql """INSERT INTO ${tbl} VALUES ('Guangzhou', 1})""", 1
                sql """set global cloud_partition_version_cache_ttl_ms=0"""
                result = sql_return_maparray """ select * from ${tbl} """
                assertEquals(7, result.size())

                insert_sql """INSERT INTO ${tbl} VALUES ('Shanghai', 1})""", 1
                insert_sql """INSERT INTO ${tbl} VALUES ('Guangzhou', 1})""", 1
                insert_sql """INSERT INTO ${tbl} VALUES ('Beijing', 1})""", 1
                // data present immediately without any cached versions
                result = sql_return_maparray """ select * from ${tbl} """
                assertEquals(10, result.size())
            } finally {
            }
        }
        // 2. connect to master
        options.connectToFollower = false
    }
}
