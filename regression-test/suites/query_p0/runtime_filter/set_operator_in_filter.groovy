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

// Regression test for race condition in Set operator (INTERSECT/EXCEPT) with
// IN_FILTER runtime filter type.
//
// Before the fix, when runtime_filter_type=1 (IN) and runtime_filter_max_in_num
// was small, a race between the build sink's close() and the probe's hash table
// refresh could cause RuntimeFilterWrapper::insert() to fail with InternalError.
// The fix snapshots hash_table_size in sink(eos) before set_ready() and uses the
// saved value in close() for RF processing. Also overrides finishdependency() so
// the pipeline task properly waits for sync_filter_size completion.

suite("set_operator_in_filter") {
    sql "DROP TABLE IF EXISTS set_rf_t1"
    sql "DROP TABLE IF EXISTS set_rf_t2"
    sql "DROP TABLE IF EXISTS set_rf_t3"

    sql """
        CREATE TABLE set_rf_t1 (
            k1 INT NOT NULL,
            v1 VARCHAR(64) NOT NULL
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """

    sql """
        CREATE TABLE set_rf_t2 (
            k1 INT NOT NULL,
            v1 VARCHAR(64) NOT NULL
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """

    sql """
        CREATE TABLE set_rf_t3 (
            k1 INT NOT NULL,
            v1 VARCHAR(64) NOT NULL
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """

    // Insert >10 unique values into t1 so that with max_in_num=10 the IN filter
    // would overflow. This is the key condition that triggers the original bug.
    sql """
        INSERT INTO set_rf_t1 VALUES
        (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),
        (6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j'),
        (11,'k'),(12,'l'),(13,'m'),(14,'n'),(15,'o'),
        (16,'p'),(17,'q'),(18,'r'),(19,'s'),(20,'t');
    """

    // t2 shares some values with t1 (for INTERSECT) and has extras (for EXCEPT)
    sql """
        INSERT INTO set_rf_t2 VALUES
        (1,'a'),(3,'c'),(5,'e'),(7,'g'),(9,'i'),
        (11,'k'),(13,'m'),(15,'o'),(17,'q'),(19,'s'),
        (21,'u'),(22,'v');
    """

    // t3 is the probe-side table for the join that triggers RF generation
    sql """
        INSERT INTO set_rf_t3 VALUES
        (1,'x'),(3,'x'),(5,'x'),(7,'x'),(9,'x'),
        (11,'x'),(13,'x'),(15,'x'),(100,'x'),(200,'x');
    """

    sql "sync"

    // Use pure IN_FILTER type with a small max_in_num to reproduce the conditions
    // that triggered the race condition bug.
    sql "set runtime_filter_type = 1"
    sql "set runtime_filter_max_in_num = 10"
    sql "set disable_join_reorder = true"

    // INTERSECT with join: the INTERSECT probe shrinks the hash table, which
    // previously could race with RF processing in the build sink's close().
    order_qt_intersect_in_rf """
        SELECT T.k1 FROM (
            SELECT k1 FROM set_rf_t1
            INTERSECT
            SELECT k1 FROM set_rf_t2
        ) T JOIN set_rf_t3 ON T.k1 = set_rf_t3.k1;
    """

    // EXCEPT with join: similar scenario but with EXCEPT semantics.
    order_qt_except_in_rf """
        SELECT T.k1 FROM (
            SELECT k1 FROM set_rf_t1
            EXCEPT
            SELECT k1 FROM set_rf_t2
        ) T JOIN set_rf_t3 ON T.k1 = set_rf_t3.k1;
    """

    // Run INTERSECT query multiple times to increase coverage of the timing
    // window. Before the fix, this would intermittently fail.
    for (int i = 0; i < 5; i++) {
        sql """
            SELECT T.k1 FROM (
                SELECT k1 FROM set_rf_t1
                INTERSECT
                SELECT k1 FROM set_rf_t2
            ) T JOIN set_rf_t3 ON T.k1 = set_rf_t3.k1
            ORDER BY T.k1;
        """
    }

    // Also test with max_in_num=0 (always overflow immediately)
    sql "set runtime_filter_max_in_num = 0"

    order_qt_intersect_in_rf_zero """
        SELECT T.k1 FROM (
            SELECT k1 FROM set_rf_t1
            INTERSECT
            SELECT k1 FROM set_rf_t2
        ) T JOIN set_rf_t3 ON T.k1 = set_rf_t3.k1;
    """
}
