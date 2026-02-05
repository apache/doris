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

suite("test_timestamptz_mv") {
    sql "set time_zone = '+08:00'; "
    sql """
        DROP TABLE IF EXISTS `timestamptz_mv`;
    """
    sql """
        CREATE TABLE `timestamptz_mv` (
          `id` int,
          `name` varchar(20),
          `ts_tz` TIMESTAMPTZ
        ) DUPLICATE KEY(`id`)
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """
    INSERT INTO timestamptz_mv VALUES
    (1, 'a', '2023-01-01 12:00:00 +08:00'),
    (1, 'a', '2023-01-02 12:00:00 +08:00'),
    (1, 'a', '2023-01-03 12:00:00 +08:00'),
    (2, 'b', '2023-02-02 12:00:00 +08:00'),
    (2, 'b', '2023-02-03 12:00:00 +08:00'),
    (2, 'b', '2023-02-04 12:00:00 +08:00'),
    (3, 'c', '2023-03-03 12:00:00 +08:00');
    """

    qt_all """
        SELECT * FROM timestamptz_mv ORDER BY 1, 2, 3;
    """

    sql """
    drop materialized view if exists mv_timestamptz on timestamptz_mv;
    """
    create_sync_mv(context.dbName, "timestamptz_mv", "mv_timestamptz", """
        SELECT name as mv_name, ts_tz as mv_ts_tz
        FROM timestamptz_mv
        WHERE ts_tz < '2023-03-03 12:00:00 +08:00';
    """
    )
    /*
    // TODO: MaterializedViewRewriteFail
    MaterializedViewRewriteFail:
 CBO.internal.regression_test_datatype_p0_timestamptz.timestamptz_mv.mv_timestamptz fail
  FailInfo: Predicate compensate fail:query predicates = Predicates ( pulledUpPredicates=[], predicatesUnderBreaker=[] ),
 query equivalenceClass = EquivalenceClass{equivalenceSlotMap={}}, 
view predicates = Predicates ( pulledUpPredicates=[(cast(ts_tz#2 as DATETIMEV2(6)) < 2023-03-03 12:00:00.000000)], predicatesUnderBreaker=[] ),
    mv_rewrite_success("""
        SELECT * FROM timestamptz_mv WHERE ts_tz < '2023-03-03 12:00:00 +08:00' ORDER BY 1, 2;
    """,
    "mv_timestamptz"
    )
    */
    qt_mv_1 """
        SELECT * FROM timestamptz_mv WHERE ts_tz < '2023-03-03 12:00:00 +08:00' ORDER BY 1, 2;
    """

    sql """
    drop materialized view if exists mv_timestamptz on timestamptz_mv;
    """
    create_sync_mv(context.dbName, "timestamptz_mv", "mv_timestamptz", """
        SELECT id as mv_id, MAX(ts_tz) as mv_max, MIN(ts_tz) as mv_min
        FROM timestamptz_mv
        GROUP BY id;
    """
    )

    mv_rewrite_success("""
        SELECT id, max(ts_tz), min(ts_tz) FROM timestamptz_mv group by id ORDER BY 1, 2;
    """,
    "mv_timestamptz"
    )

    qt_mv_2 """
        SELECT id, max(ts_tz), min(ts_tz) FROM timestamptz_mv group by id ORDER BY 1, 2;
    """
}