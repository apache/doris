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

import groovy.json.JsonSlurper

// enable_jni_heap_admission has the connectors declare, on every range whose JNI reader holds much of
// BE's JVM heap, how much it will hold, and BE opens those readers only while what it admitted fits
// its budget. It is off by default. Turned on it may make a reader wait for room, and nothing else: the
// rows must come back exactly as they do with it off.
//
// So two things are checked. Every query here returns with the variable on what it returns with it
// off; the comparison stays in the code because what it asserts is the agreement. And the reads that
// hold much do declare it, as the profile's JvmHeapDeclaredBytes shows: a union read of a primary-key
// table, whose log tail is a PK_TAIL range, and the same read with force_jni_scanner, whose lake half is
// then a paimon JNI split as well; a log read declares nothing.
//
// The primary-key buckets read whole (PK_FULL) declare nothing here. A PK_FULL range declares the change
// log after its bucket's kv snapshot, and this environment snapshots every ten seconds while nothing
// writes after init, so by the time this suite runs every snapshot covers its whole log. Those reads are
// here for the agreement; FlussJniHeapEstimateTest covers what a PK_FULL range declares.
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
    // required: a union read that quietly fell back to fluss alone would plan neither a PK_TAIL range
    // nor a lake split, and neither declaration would be tested.
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

    def feGet = { String path ->
        def conn = new URL("http://${context.config.feHttpAddress}${path}").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }
    long MB = 1024L * 1024
    def bytesPerUnit = ["": 1L, "B": 1L, "KB": 1024L, "MB": MB, "GB": 1024L * MB, "TB": 1024L * 1024 * MB]
    // The JvmHeapDeclaredBytes of `query`, a select read with profiling on, in bytes: one per scan
    // instance that read through JNI.
    def declaredHeapOf = { String query ->
        assertTrue(query.startsWith("select "), query)
        String token = UUID.randomUUID().toString()
        sql """set enable_profile = true"""
        sql(query.replaceFirst("select ", "select /* ${token} */ "))
        sql """set enable_profile = false"""
        String profileId = ""
        for (int attempt = 0; attempt < 20 && profileId == ""; attempt++) {
            def row = new JsonSlurper().parseText(feGet("/rest/v1/query_profile")).data.rows.find {
                it["Sql Statement"].toString().contains(token)
            }
            if (row == null) {
                Thread.sleep(300)
            } else {
                profileId = row["Profile ID"].toString()
            }
        }
        assertTrue(profileId != "", "no profile for: ${query}")
        // JNI counters are only in the per-instance DetailProfile, which BE reports a little after the
        // statement returns.
        List<Long> declared = []
        for (int attempt = 0; attempt < 20 && declared.isEmpty(); attempt++) {
            declared = feGet("/api/profile/text/?query_id=${profileId}").readLines()
                    .findAll { it.contains("- JvmHeapDeclaredBytes: ") }
                    .collect { line ->
                        def m = line =~ /JvmHeapDeclaredBytes: ([0-9]+[.,][0-9]+) ?([KMGT]?B)?/
                        assertTrue(m.find(), line)
                        (long) (Double.parseDouble(m.group(1).replace(',', '.')) * bytesPerUnit[m.group(2) ?: ""])
                    }
            if (declared.isEmpty()) {
                Thread.sleep(500)
            }
        }
        assertFalse(declared.isEmpty(), "no JvmHeapDeclaredBytes in the profile of: ${query}")
        return declared
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
    // The tail is a PK_TAIL range, which declares the rows it will keep.
    def unionDeclared = declaredHeapOf("select id, name from lake_pk_multi order by id")
    assertTrue(unionDeclared.any { it > 0 }, "the union read declared no heap: ${unionDeclared}")

    // --- the same through JNI: the lake half a paimon JNI split -------------
    // A paimon JNI split declares at least a dictionary page - a megabyte - for every column of every
    // file it reads, far more than this fixture's tail.
    sql """set force_jni_scanner = true"""
    order_qt_union_rows_jni """select id, name from lake_pk_multi"""
    sameWithAdmissionOff("select id, name from lake_pk_multi order by id")
    def jniDeclared = declaredHeapOf("select id, name from lake_pk_multi order by id")
    assertTrue(jniDeclared.any { it >= MB }, "the paimon JNI split of the union read declared no heap: ${jniDeclared}")
    sql """set force_jni_scanner = false"""

    // --- a log read declares nothing ----------------------------------------
    order_qt_log_rows """select id, name, price from log_basic"""
    sameWithAdmissionOff("select id, name, price from log_basic order by id")
    def logDeclared = declaredHeapOf("select id, name, price from log_basic order by id")
    assertTrue(logDeclared.every { it == 0 }, "the log read declared heap: ${logDeclared}")

    sql """set enable_jni_heap_admission = false"""
    sql """switch internal"""
}
