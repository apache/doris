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

suite("test_packed_delete_bitmap", "docker") {
    if (!isCloudMode()) {
        return
    }

    // Test 1: BE restart with packed delete bitmap
    def options1 = new ClusterOptions()
    options1.beConfigs += [
        'delete_bitmap_store_write_version=2',
        'delete_bitmap_store_read_version=2',
        'delete_bitmap_store_v2_max_bytes_in_fdb=0',
        'enable_sync_tablet_delete_bitmap_by_cache=false',
        'enable_delete_bitmap_store_v2_check_correctness=true',
        'enable_java_support=false',
        'enable_packed_file=true'
    ]
    options1.setFeNum(1)
    options1.setBeNum(1)
    options1.cloudMode = true

    docker(options1) {
        def tableName = "test_be_restart"
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `k` int(11) NOT NULL,
                `v` int(11) NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "disable_auto_compaction"="true",
                "replication_num" = "1"
            );
        """

        // Insert data to create delete bitmap in packed file
        sql """ INSERT INTO ${tableName} VALUES(1, 1), (2, 2); """
        sql """ INSERT INTO ${tableName} VALUES(3, 3), (4, 4); """
        sql """ INSERT INTO ${tableName} VALUES(1, 10), (3, 30); """

        order_qt_before_restart "SELECT * FROM ${tableName};"

        // Restart BE
        logger.info("Restarting backends...")
        cluster.restartBackends()

        // Query after restart - should read delete bitmap from packed file
        order_qt_after_restart "SELECT * FROM ${tableName};"

        // Insert more data after restart
        sql """ INSERT INTO ${tableName} VALUES(2, 20), (4, 40); """
        order_qt_after_insert "SELECT * FROM ${tableName};"
    }

    // Test 2: Multiple rowsets write delete bitmap to same packed file
    def options2 = new ClusterOptions()
    options2.beConfigs += [
        'delete_bitmap_store_write_version=2',
        'delete_bitmap_store_read_version=2',
        'delete_bitmap_store_v2_max_bytes_in_fdb=0',
        'enable_sync_tablet_delete_bitmap_by_cache=false',
        'enable_delete_bitmap_store_v2_check_correctness=true',
        'enable_java_support=false',
        'enable_packed_file=true',
        'packed_file_size_threshold_bytes=10485760'  // 10MB - large enough to hold multiple delete bitmaps
    ]
    options2.setFeNum(1)
    options2.setBeNum(1)
    options2.cloudMode = true

    docker(options2) {
        def tableName = "test_multi_rowset"
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `k` int(11) NOT NULL,
                `v` varchar(100) NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "disable_auto_compaction"="true",
                "replication_num" = "1"
            );
        """

        // Insert multiple rowsets - their delete bitmaps should go to same packed file
        for (int i = 0; i < 10; i++) {
            sql """ INSERT INTO ${tableName} VALUES(${i}, 'value_${i}_v1'); """
        }

        // Update some rows to create more delete bitmaps
        for (int i = 0; i < 10; i++) {
            sql """ INSERT INTO ${tableName} VALUES(${i}, 'value_${i}_v2'); """
        }

        order_qt_multi_rowset "SELECT * FROM ${tableName} ORDER BY k;"

        // Restart and verify
        cluster.restartBackends()
        order_qt_multi_rowset_after_restart "SELECT * FROM ${tableName} ORDER BY k;"
    }

    // Test 3: Large delete bitmap exceeds small file threshold - fallback to direct write
    def options3 = new ClusterOptions()
    options3.beConfigs += [
        'delete_bitmap_store_write_version=2',
        'delete_bitmap_store_read_version=2',
        'delete_bitmap_store_v2_max_bytes_in_fdb=0',
        'enable_sync_tablet_delete_bitmap_by_cache=false',
        'enable_delete_bitmap_store_v2_check_correctness=true',
        'enable_java_support=false',
        'enable_packed_file=true',
        'small_file_threshold_bytes=100'  // Very small threshold to trigger direct write
    ]
    options3.setFeNum(1)
    options3.setBeNum(1)
    options3.cloudMode = true

    docker(options3) {
        def tableName = "test_large_bitmap"
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `k` int(11) NOT NULL,
                `v` varchar(1000) NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "disable_auto_compaction"="true",
                "replication_num" = "1"
            );
        """

        // Insert enough data to create a large delete bitmap
        def values = []
        for (int i = 0; i < 100; i++) {
            values.add("(${i}, 'value_${i}_initial')")
        }
        sql """ INSERT INTO ${tableName} VALUES ${values.join(',')}; """

        // Update all rows to create delete bitmap entries
        values = []
        for (int i = 0; i < 100; i++) {
            values.add("(${i}, 'value_${i}_updated')")
        }
        sql """ INSERT INTO ${tableName} VALUES ${values.join(',')}; """

        order_qt_large_bitmap "SELECT COUNT(*) FROM ${tableName};"
        order_qt_large_bitmap_sample "SELECT * FROM ${tableName} WHERE k < 5 ORDER BY k;"

        // Restart and verify
        cluster.restartBackends()
        order_qt_large_bitmap_after_restart "SELECT COUNT(*) FROM ${tableName};"
        order_qt_large_bitmap_sample_after_restart "SELECT * FROM ${tableName} WHERE k < 5 ORDER BY k;"
    }
}
