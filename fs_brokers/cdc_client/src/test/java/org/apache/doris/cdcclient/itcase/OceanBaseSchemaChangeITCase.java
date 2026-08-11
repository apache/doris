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

package org.apache.doris.cdcclient.itcase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.doris.cdcclient.common.Env;
import org.apache.doris.job.cdc.split.SnapshotSplit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

class OceanBaseSchemaChangeITCase extends OceanBaseTestBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(1_300_000);

    private String jobId;
    private String database;

    @BeforeEach
    void setUp() throws Exception {
        jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
        database = "oceanbase_schema_" + jobId;
        createDatabase(
                database,
                "CREATE TABLE t_user (id INT NOT NULL, name VARCHAR(50), PRIMARY KEY (id))",
                "INSERT INTO t_user VALUES (1,'alice'), (2,'bob')");
    }

    @AfterEach
    void tearDown() throws Exception {
        Env.getCurrentEnv().close(jobId);
        dropDatabase(database);
    }

    @Test
    void addAndDropColumnSurviveOffsetCommitFailureAndReaderRebuild() throws Exception {
        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        oceanBaseHarness(jobId, database, "t_user", "initial", mock)) {
            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_user");
            harness.writeSnapshot(splits);
            harness.enterBinlog(splits);
            String offsetBeforeAlter = harness.committedOffset();
            String schemasBeforeAlter = harness.committedTableSchemas();
            assertThat(schemasBeforeAlter).isNotNull().doesNotContain("city");

            execute(
                    "ALTER TABLE t_user ADD COLUMN city VARCHAR(50)",
                    "INSERT INTO t_user (id, name, city) VALUES (3, 'carol', 'beijing')");

            mock.failCommitOffset();
            assertThatThrownBy(() -> harness.continueBinlog(1, Duration.ofSeconds(90)))
                    .isInstanceOf(Exception.class);
            assertThat(mock.failedCommitOffsetCount()).isPositive();
            assertThat(harness.committedOffset()).isEqualTo(offsetBeforeAlter);
            assertThat(harness.committedTableSchemas()).isEqualTo(schemasBeforeAlter);

            mock.allowCommitOffset();
            harness.rebuildReaderOnNextWrite();
            List<String> retried = harness.continueBinlog(1, Duration.ofSeconds(90));

            assertThat(harness.executedDdls()).hasSize(2);
            assertThat(harness.executedDdls()).allMatch(ddl -> ddl.contains("ADD COLUMN"));
            assertThat(mock.ddlResponses().get(1))
                    .contains("Can not add column which already exists");
            assertThat(retried).hasSize(1);
            JsonNode added = MAPPER.readTree(retried.get(0));
            assertThat(added.get("id").asInt()).isEqualTo(3);
            assertThat(added.get("city").asText()).isEqualTo("beijing");
            assertThat(harness.committedTableSchemas()).contains("city");

            execute(
                    "ALTER TABLE t_user DROP COLUMN city",
                    "INSERT INTO t_user (id, name) VALUES (4, 'dave')");
            List<String> afterDrop = harness.continueBinlog(1, Duration.ofSeconds(90));

            assertThat(harness.executedDdls().get(2)).contains("DROP COLUMN").contains("city");
            assertThat(afterDrop).hasSize(1);
            JsonNode dropped = MAPPER.readTree(afterDrop.get(0));
            assertThat(dropped.get("id").asInt()).isEqualTo(4);
            assertThat(dropped.has("city")).isFalse();
            assertThat(harness.committedTableSchemas()).doesNotContain("city");
        }
    }

    private void execute(String... statements) throws Exception {
        try (Connection connection = connection(database);
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }
}
