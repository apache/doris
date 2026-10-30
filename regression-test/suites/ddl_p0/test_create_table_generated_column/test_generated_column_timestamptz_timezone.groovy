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

import org.junit.Assert;

/**
 * A generated column that interprets or renders a TIMESTAMPTZ value in a time-zone dependent way is
 * rejected at table creation: BE evaluates the stored expression in the write/load session time zone, so
 * data loaded from a different zone would silently store a different value, and the FE-side session
 * guard cannot survive execution (MergeGuardExpr and expression translation drop it).
 *
 * The classification looks at the operation plus source and result types, including the implicit cast to
 * the declared column type: `rendered STRING GENERATED ALWAYS AS (ts)` is a plain slot reference in the
 * user expression, and only the implicit TIMESTAMPTZ -> STRING cast (added later by BindSink's coercion
 * project) renders the instant in the load session zone. The reverse implicit cast of an offset-free
 * string slot into a TIMESTAMPTZ generated column interprets the string in the load session zone and is
 * rejected for the same reason.
 *
 * Zone-invariant operations (comparing two TIMESTAMPTZ instants, copying an instant verbatim) are allowed
 * and must materialize identically across load zones.
 */
suite("test_generated_column_timestamptz_timezone","ddl") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"

    // STRING GENERATED ALWAYS AS (ts) renders the instant with the write-session offset: the same instant
    // is stored as different strings by different write sessions, so it is rejected like any other
    // rendering of an instant into a non-TIMESTAMPTZ representation.
    test {
        sql """
        create table gencol_ts_to_string(
            id int,
            ts TIMESTAMPTZ(6),
            rendered STRING generated always as (ts) not null
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        exception "time-zone sensitive"
    }

    // Rendering a TIMESTAMPTZ into a zone-free non-string target (DATETIME) loses the offset and depends
    // on the write/load session zone. Must be rejected.
    test {
        sql """
        create table gencol_ts_to_dt(
            id int,
            ts TIMESTAMPTZ(6),
            dt DATETIME(6) generated always as (ts) not null
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        exception "time-zone sensitive"
    }

    // The reverse implicit cast of an offset-free string slot into a TIMESTAMPTZ generated column
    // interprets the string in the write session zone. Must be rejected.
    test {
        sql """
        create table gencol_string_to_ts(
            id int,
            s STRING,
            ts TIMESTAMPTZ(6) generated always as (s) not null
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        exception "time-zone sensitive"
    }

    // An explicit cast into TIMESTAMPTZ has the same zone dependence even though the only operand is a
    // VARCHAR column.
    test {
        sql """
        create table gencol_cast_to_ts(
            id int,
            s STRING,
            ts TIMESTAMPTZ(6) generated always as (cast(s as TIMESTAMPTZ(6))) not null
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        exception "time-zone sensitive"
    }

    // IS (NOT) NULL only reads the null-ness of its operand, so a nullable CASE built from a zone-sensitive
    // expression is zone dependent too: the same instants make the condition true in one write session and
    // false in another. Must be rejected.
    test {
        sql """
        create table gencol_case_is_null(
            id int,
            ts TIMESTAMPTZ(6),
            ts2 TIMESTAMPTZ(6),
            broken BOOLEAN generated always as (
                (case when date_trunc(ts, 'day') = ts2 then null else 1 end) is null) not null
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        exception "time-zone sensitive"
    }

    // A struct cast is compared leaf by leaf: keeping one TIMESTAMPTZ field while rendering another into a
    // zone-free representation still depends on the write session zone. Must be rejected.
    test {
        sql """
        create table gencol_struct_mixed(
            id int,
            s struct<a: TIMESTAMPTZ(6), b: TIMESTAMPTZ(6)>,
            g struct<a: TIMESTAMPTZ(6), b: STRING> generated always as (s) not null
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        exception "time-zone sensitive"
    }

    // A struct cast is positional and may rename the fields: relabeling an unchanged TIMESTAMPTZ field never
    // renders or interprets the instant in the session zone, so it must be accepted.
    sql "DROP TABLE IF EXISTS gencol_struct_rename"
    sql """
    create table gencol_struct_rename(
        id int,
        s struct<a: TIMESTAMPTZ(6)>,
        g struct<b: TIMESTAMPTZ(6)> generated always as (s) not null
    )
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    """

    // A TIMESTAMPTZ generated column that stores the instant directly (no rendering) is zone-invariant.
    sql "DROP TABLE IF EXISTS gencol_copy_ts"
    sql """
    create table gencol_copy_ts(
        id int,
        ts TIMESTAMPTZ(6),
        ts_copy TIMESTAMPTZ(6) generated always as (ts) not null
    )
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    """

    // Comparing two TIMESTAMPTZ instants is zone-invariant and must be accepted.
    sql "DROP TABLE IF EXISTS gencol_cmp_ts"
    sql """
    create table gencol_cmp_ts(
        id int,
        ts1 TIMESTAMPTZ(6),
        ts2 TIMESTAMPTZ(6),
        same BOOLEAN generated always as (ts1 = ts2) not null
    )
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    """

    // Cross-zone: the same instant loaded from two different zone sessions must materialize identical
    // generated-column values (the comparison result and the copied instant are zone-invariant).
    sql "SET time_zone = '+00:00'"
    sql "INSERT INTO gencol_cmp_ts(id, ts1, ts2) VALUES (1, '2024-01-01 00:30:00+00:00', '2024-01-01 00:30:00+00:00')"
    sql "INSERT INTO gencol_copy_ts(id, ts) VALUES (1, '2024-01-01 00:30:00+00:00')"
    sql "INSERT INTO gencol_struct_rename(id, s) VALUES (1, named_struct('a', '2024-01-01 00:30:00+00:00'))"
    sql "sync"
    sql "SET time_zone = '+08:00'"
    sql "INSERT INTO gencol_cmp_ts(id, ts1, ts2) VALUES (2, '2024-01-01 08:30:00+08:00', '2024-01-01 08:30:00+08:00')"
    sql "INSERT INTO gencol_copy_ts(id, ts) VALUES (2, '2024-01-01 08:30:00+08:00')"
    sql "INSERT INTO gencol_struct_rename(id, s) VALUES (2, named_struct('a', '2024-01-01 08:30:00+08:00'))"
    sql "sync"

    // Read in a fixed zone so the asserted strings are load-zone independent.
    sql "SET time_zone = '+00:00'"
    def cmpRes = sql "SELECT id, same FROM gencol_cmp_ts ORDER BY id"
    Assert.assertEquals(2, cmpRes.size())
    // BOOLEAN renders as "true"/"1" depending on the driver; normalize and require truthy for both rows
    Assert.assertTrue("both rows loaded from different zones must compare equal, got " + cmpRes,
            cmpRes[0][1].toString().equalsIgnoreCase("true")
                    || cmpRes[0][1].toString().equals("1"))
    Assert.assertTrue("both rows loaded from different zones must compare equal, got " + cmpRes,
            cmpRes[1][1].toString().equalsIgnoreCase("true")
                    || cmpRes[1][1].toString().equals("1"))

    def copyRes = sql "SELECT id, CAST(ts_copy AS STRING) FROM gencol_copy_ts ORDER BY id"
    Assert.assertEquals(2, copyRes.size())
    Assert.assertTrue("expected 2024-01-01 00:30:00.000000+00:00, got " + copyRes[0][1],
            copyRes[0][1].toString().contains("2024-01-01 00:30:00.000000+00:00"))
    Assert.assertTrue("expected 2024-01-01 00:30:00.000000+00:00, got " + copyRes[1][1],
            copyRes[1][1].toString().contains("2024-01-01 00:30:00.000000+00:00"))

    // The renamed struct field keeps the instant verbatim, so both load sessions must store the same value.
    def renameRes = sql "SELECT id, CAST(g.b AS STRING) FROM gencol_struct_rename ORDER BY id"
    Assert.assertEquals(2, renameRes.size())
    Assert.assertTrue("expected 2024-01-01 00:30:00.000000+00:00, got " + renameRes[0][1],
            renameRes[0][1].toString().contains("2024-01-01 00:30:00.000000+00:00"))
    Assert.assertTrue("expected 2024-01-01 00:30:00.000000+00:00, got " + renameRes[1][1],
            renameRes[1][1].toString().contains("2024-01-01 00:30:00.000000+00:00"))
}
