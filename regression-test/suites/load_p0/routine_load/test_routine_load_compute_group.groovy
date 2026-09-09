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

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

// Declaring `compute_group` on a routine load job.
//
// The declaration is stored in the job's property map under the key `compute_group`, which is the
// same key and value space the final (owner, compute_group, workload_group) design uses, so the
// metadata is read back as an explicit pin by later versions without conversion.
//
// The property-level checks run before the Kafka data source is touched, so the negative cases do
// not need a broker; only the "job really runs there" cases do.
suite("test_routine_load_compute_group", "p0") {
    String tableName = "test_routine_load_compute_group_tbl"
    String jobName = "test_routine_load_compute_group_job"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` INT NULL,
            `k2` STRING NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // Returns the statement instead of running it: inside a `test { }` block the `sql` method must
    // be the action's own, so the SQL has to be handed to it there rather than executed here.
    def createJobSql = { String computeGroup ->
        return """
            CREATE ROUTINE LOAD ${jobName} ON ${tableName}
            COLUMNS TERMINATED BY ","
            PROPERTIES ("compute_group" = "${computeGroup}")
            FROM KAFKA ("kafka_broker_list" = "127.0.0.1:19092", "kafka_topic" = "unused_topic");
        """.toString()
    }

    if (!isCloudMode()) {
        // Non-cloud is out of scope for this transitional change, so the key can never appear in
        // non-cloud metadata and upgrading such a cluster has nothing to convert. The cloud-mode
        // check runs first, so even the reserved DEFAULT is refused with this message.
        for (String value : ["any_group", "DEFAULT", "default"]) {
            test {
                sql createJobSql(value)
                exception "only supported in cloud mode"
            }
        }
        sql """DROP TABLE IF EXISTS ${tableName}"""
        return
    }

    // ---------------- cloud mode ----------------

    // DEFAULT is reserved by the final design ("follow the owner's default group"). A job pinned to
    // a group literally named DEFAULT would be silently reinterpreted after an upgrade.
    for (String reserved : ["DEFAULT", "default", "Default"]) {
        test {
            sql createJobSql(reserved)
            exception "reserved value"
        }
    }

    test {
        sql createJobSql("cg_that_does_not_exist")
        exception "not found"
    }

    def currentComputeGroup = sql_return_maparray("show clusters")
            .stream().filter(cg -> cg.is_current == "TRUE").findFirst().orElse(null)
    assertNotNull(currentComputeGroup)
    def cgName = currentComputeGroup.cluster
    logger.info("current compute group: ${cgName}")

    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("kafka test not enabled, skipping the running-job part")
        sql """DROP TABLE IF EXISTS ${tableName}"""
        return
    }

    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def kafka_broker = "${externalEnvIp}:${kafka_port}"
    def topic = "test_routine_load_compute_group"

    def props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")
    def producer = new KafkaProducer<>(props)
    for (int i = 0; i < 5; i++) {
        producer.send(new ProducerRecord<>(topic, "${i},row${i}".toString()))
    }
    producer.close()

    try {
        sql """
            CREATE ROUTINE LOAD ${jobName} ON ${tableName}
            COLUMNS TERMINATED BY ","
            PROPERTIES (
                "max_batch_interval" = "5",
                "max_batch_rows" = "300000",
                "max_batch_size" = "209715200",
                "compute_group" = "${cgName}"
            )
            FROM KAFKA (
                "kafka_broker_list" = "${kafka_broker}",
                "kafka_topic" = "${topic}",
                "property.kafka_default_offsets" = "OFFSET_BEGINNING"
            );
        """

        // SHOW must report the compute group the job actually runs in, and the declaration must be
        // visible in the job properties so operators can inventory pinned jobs before an upgrade.
        def show = sql_return_maparray("SHOW ROUTINE LOAD FOR ${jobName}").get(0)
        logger.info("show routine load: ${show}")
        assertEquals(cgName, show.ComputeGroup)
        assertTrue(show.JobProperties.contains("compute_group"))
        assertTrue(show.JobProperties.contains(cgName))

        // The job runs, i.e. pinning did not break the load path.
        def count = 0
        while (count < 60) {
            def state = sql_return_maparray("SHOW ROUTINE LOAD FOR ${jobName}").get(0).State
            def rows = sql "SELECT COUNT(*) FROM ${tableName}"
            logger.info("state=${state}, rows=${rows}")
            assertNotEquals("PAUSED", state)
            if (rows.get(0).get(0) >= 5) {
                break
            }
            sleep(2000)
            count++
        }
        assertEquals(5, sql("SELECT COUNT(*) FROM ${tableName}").get(0).get(0))

        // ALTER validates the new value immediately instead of letting a bad name pause the job on
        // the next task. ALTER ROUTINE LOAD only accepts PAUSED jobs.
        sql "PAUSE ROUTINE LOAD FOR ${jobName}"
        test {
            sql """ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES("compute_group" = "DEFAULT");"""
            exception "reserved value"
        }
        test {
            sql """ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES("compute_group" = "cg_nope");"""
            exception "not found"
        }

        sql """ALTER ROUTINE LOAD FOR ${jobName} PROPERTIES("compute_group" = "${cgName}");"""
        def showAfterAlter = sql_return_maparray("SHOW ROUTINE LOAD FOR ${jobName}").get(0)
        assertEquals(cgName, showAfterAlter.ComputeGroup)

        sql "RESUME ROUTINE LOAD FOR ${jobName}"
    } finally {
        try {
            sql "STOP ROUTINE LOAD FOR ${jobName}"
        } catch (Exception e) {
            logger.info("stop routine load failed: ${e.getMessage()}")
        }
        sql """DROP TABLE IF EXISTS ${tableName}"""
    }

    // The binding is re-checked before every task, not only at CREATE time: revoking the job
    // owner's USAGE on the declared compute group must pause the job with that reason.
    //
    // The check is deliberately placed before the task takes any resource - before backend
    // allocation and before the load transaction is begun - so that a group which has been dropped
    // outright cannot first surface as "no available BE found". That ordering is what
    // RoutineLoadComputeGroupTest#testDroppedComputeGroupFailsTaskBeforeBackendAllocation pins
    // down; here we cover the privilege half end to end, which needs a real running job.
    String revokeUser = "test_rl_cg_revoke_user"
    String revokePwd = "Test_12345"
    String revokeJob = "test_rl_cg_revoke_job"
    String revokeTable = "test_rl_cg_revoke_tbl"

    sql """DROP TABLE IF EXISTS ${revokeTable}"""
    sql """
        CREATE TABLE ${revokeTable} (
            `k1` INT NULL,
            `k2` STRING NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """DROP USER IF EXISTS ${revokeUser}"""
    sql """CREATE USER '${revokeUser}' IDENTIFIED BY '${revokePwd}'"""
    sql """GRANT LOAD_PRIV, SELECT_PRIV ON *.*.* TO ${revokeUser}"""
    sql """GRANT USAGE_PRIV ON COMPUTE GROUP `${cgName}` TO ${revokeUser}"""

    try {
        // Created by the user, so RoutineLoadJob#userIdentity - the identity the per task check
        // runs against - is that user rather than the admin running this suite.
        connect(revokeUser, "${revokePwd}", context.config.jdbcUrl) {
            sql """use ${context.dbName}"""
            sql """
                CREATE ROUTINE LOAD ${revokeJob} ON ${revokeTable}
                COLUMNS TERMINATED BY ","
                PROPERTIES (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200",
                    "compute_group" = "${cgName}"
                )
                FROM KAFKA (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${topic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
        }

        def runningBeforeRevoke = false
        for (int i = 0; i < 60; i++) {
            def state = sql_return_maparray("SHOW ROUTINE LOAD FOR ${revokeJob}").get(0).State
            if (state == "RUNNING") {
                runningBeforeRevoke = true
                break
            }
            sleep(1000)
        }
        assertTrue(runningBeforeRevoke, "the job must be running before its privilege is revoked")

        sql """REVOKE USAGE_PRIV ON COMPUTE GROUP `${cgName}` FROM ${revokeUser}"""

        // The check runs ahead of the "is there more data to consume" probe, so the job is refused
        // on the next scheduling round whether or not the topic has new records.
        def pausedForComputeGroup = false
        def sawBeAvailabilityReason = false
        def lastSeen = ""
        for (int i = 0; i < 90; i++) {
            def row = sql_return_maparray("SHOW ROUTINE LOAD FOR ${revokeJob}").get(0)
            def reason = row.ReasonOfStateChanged == null ? "" : row.ReasonOfStateChanged
            lastSeen = "state=${row.State}, reason=${reason}"
            if (reason.contains("no available BE found")) {
                sawBeAvailabilityReason = true
            }
            if (reason.contains("USAGE denied") && reason.contains(cgName)) {
                pausedForComputeGroup = true
                break
            }
            sleep(1000)
        }
        assertTrue(pausedForComputeGroup,
                "revoking USAGE must fail the next task with the real reason, last seen: ${lastSeen}".toString())
        assertFalse(sawBeAvailabilityReason,
                "a compute group problem must never be reported as a BE availability problem")

        // Nothing undoes a REVOKE by itself, so the pause carries CANNOT_RESUME_ERR and the job
        // must stay down instead of being auto resumed into the same failure every few minutes.
        for (int i = 0; i < 15; i++) {
            def state = sql_return_maparray("SHOW ROUTINE LOAD FOR ${revokeJob}").get(0).State
            assertEquals("PAUSED", state,
                    "a job paused by a revoked privilege must not be auto resumed")
            sleep(1000)
        }
    } finally {
        try {
            sql "STOP ROUTINE LOAD FOR ${revokeJob}"
        } catch (Exception e) {
            logger.info("stop routine load failed: ${e.getMessage()}")
        }
        sql """DROP TABLE IF EXISTS ${revokeTable}"""
        sql """DROP USER IF EXISTS ${revokeUser}"""
    }
}
