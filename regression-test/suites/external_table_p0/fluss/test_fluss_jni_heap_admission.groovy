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

// enable_jni_heap_admission has the connectors declare, on every range whose JNI reader holds much of
// BE's JVM heap, how much it will hold, and BE opens those readers only while what it admitted fits
// its budget. It is off by default. Turned on it may make a reader wait for room, and nothing else: the
// rows must come back exactly as they do with it off.
//
// The reads that declare are here: fluss primary-key buckets read whole (PK_FULL), and a union read of a
// primary-key table, whose tail is a PK_TAIL range and whose lake half the paimon connector plans. A log
// read declares nothing and is here as the case that must not change either. Each query is recorded
// with the variable on, and compared with the same query with it off; the comparison stays in the code
// because what it asserts is the agreement.
//
// Fixtures come from docker/thirdparties/docker-compose/fluss/sql/init.sql and init-lake-tail.sql, and
// are static: this suite never writes.
suite("test_fluss_jni_heap_admission", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String minioPort = context.config.otherConfigs.get("fluss_minio_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String catalogName = "test_fluss_jni_heap_admission"

    // Off unless a statement asks for it.
    qt_default_off """show variables like 'enable_jni_heap_admission'"""

    sql """drop catalog if exists ${catalogName}"""
    // required: a union read that quietly fell back to fluss alone would never plan a lake split, and
    // the paimon half of the declaration would go untested.
    sql """
        create catalog ${catalogName} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "fluss.union_read.mode" = "required"
        );
    """
    sql """switch ${catalogName}"""
    sql """use fluss_test"""
    // The C++ glue exists only for the v2 file scanner, and the session variable that picks between
    // them is randomised by the fuzzy mode this pipeline runs.
    sql """set enable_file_scanner_v2 = true"""

    def rowsOf = { String query -> sql(query).collect { row -> row.collect { it.toString() } } }
    def sameWithAdmissionOff = { String query ->
        sql """set enable_jni_heap_admission = false"""
        def off = rowsOf(query)
        sql """set enable_jni_heap_admission = true"""
        def on = rowsOf(query)
        assertEquals(off, on, "enable_jni_heap_admission changed the rows of: ${query}\noff=${off}\non=${on}")
    }

    sql """set enable_jni_heap_admission = true"""

    // --- primary-key buckets read whole -------------------------------------
    order_qt_pk_rows """select id, name, score from pk_basic"""
    order_qt_pk_count """select count(*) from pk_basic"""
    order_qt_pk_part """select id, name, dt from pk_part"""
    sameWithAdmissionOff("select id, name, score from pk_basic order by id")
    sameWithAdmissionOff("select id, name, dt from pk_part order by id, dt")

    // --- a union read: the lake half from paimon, the tail a PK_TAIL --------
    order_qt_union_rows """select id, name from lake_pk_multi"""
    order_qt_union_count """select count(*) from lake_pk_multi"""
    sameWithAdmissionOff("select id, name from lake_pk_multi order by id")

    // --- a log read declares nothing ----------------------------------------
    order_qt_log_rows """select id, name, price from log_basic"""
    sameWithAdmissionOff("select id, name, price from log_basic order by id")

    sql """set enable_jni_heap_admission = false"""
    sql """switch internal"""
}
