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

import org.apache.doris.cdcclient.common.Constants;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

class OceanBaseWriteDmlITCase extends OceanBaseTestBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(1_100_000);

    private String jobId;
    private String database;

    @BeforeEach
    void setUp() throws Exception {
        jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
        database = "oceanbase_dml_" + jobId;
        createDatabase(
                database,
                "CREATE TABLE t_user (id INT NOT NULL, name VARCHAR(50), PRIMARY KEY (id))",
                "INSERT INTO t_user VALUES (1,'alice'), (2,'bob'), (3,'carol')");
    }

    @AfterEach
    void tearDown() throws Exception {
        Env.getCurrentEnv().close(jobId);
        dropDatabase(database);
    }

    @Test
    void snapshotAndBinlogDmlUseOceanBaseReader() throws Exception {
        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        oceanBaseHarness(jobId, database, "t_user", "initial", mock)) {
            List<SnapshotSplit> splits = harness.fetchAllSnapshotSplits("t_user");
            harness.writeSnapshot(splits);

            Map<Integer, JsonNode> snapshotById = indexById(harness.loadedRecords());
            assertThat(snapshotById).containsOnlyKeys(1, 2, 3);
            harness.enterBinlog(splits);

            try (Connection connection = connection(database);
                    Statement statement = connection.createStatement()) {
                statement.execute("INSERT INTO t_user VALUES (4, 'dave')");
                statement.execute("UPDATE t_user SET name = 'alice2' WHERE id = 1");
                statement.execute("DELETE FROM t_user WHERE id = 2");
            }

            List<String> binlog = harness.continueBinlog(3, Duration.ofSeconds(90));
            assertThat(binlog).hasSize(3);
            Map<Integer, JsonNode> binlogById = indexById(binlog);
            assertThat(binlogById).containsOnlyKeys(1, 2, 4);
            assertThat(binlogById.get(4).get("name").asText()).isEqualTo("dave");
            assertThat(binlogById.get(4).get(Constants.DORIS_DELETE_SIGN).asInt()).isZero();
            assertThat(binlogById.get(1).get("name").asText()).isEqualTo("alice2");
            assertThat(binlogById.get(1).get(Constants.DORIS_DELETE_SIGN).asInt()).isZero();
            assertThat(binlogById.get(2).get(Constants.DORIS_DELETE_SIGN).asInt()).isEqualTo(1);

            harness.rebuildReaderOnNextWrite();
            try (Connection connection = connection(database);
                    Statement statement = connection.createStatement()) {
                statement.execute("INSERT INTO t_user VALUES (5, 'eve')");
            }

            List<String> resumed = harness.continueBinlog(1, Duration.ofSeconds(90));
            assertThat(indexById(resumed)).containsOnlyKeys(5);
        }
    }

    private Map<Integer, JsonNode> indexById(List<String> records) throws Exception {
        Map<Integer, JsonNode> result = new HashMap<>();
        for (String record : records) {
            JsonNode node = MAPPER.readTree(record);
            result.put(node.get("id").asInt(), node);
        }
        return result;
    }
}
