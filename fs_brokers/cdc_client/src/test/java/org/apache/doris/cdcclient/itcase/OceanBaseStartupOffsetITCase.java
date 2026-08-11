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

import org.apache.doris.cdcclient.common.Env;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

class OceanBaseStartupOffsetITCase extends OceanBaseTestBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong JOB_ID_SEQ = new AtomicLong(1_200_000);

    private String jobId;
    private String database;

    @BeforeEach
    void setUp() throws Exception {
        jobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());
        database = "oceanbase_offset_" + jobId;
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
    void latestReadsOnlyChangesAfterReaderStarts() throws Exception {
        awaitInitialRowsInBinlog();

        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        oceanBaseHarness(jobId, database, "t_user", "latest", mock)) {
            harness.enterBinlogFromStartupMode();
            insert("INSERT INTO t_user VALUES (3,'carol')");

            assertThat(ids(harness.continueBinlog(1, Duration.ofSeconds(90))))
                    .containsExactly(3);
            assertThat(ids(harness.loadedRecords())).doesNotContain(1, 2);
        }
    }

    @Test
    void earliestReplaysExistingBinlogChanges() throws Exception {
        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        oceanBaseHarness(jobId, database, "t_user", "earliest", mock)) {
            assertThat(ids(harness.readBinlogFromStartupMode(2, Duration.ofSeconds(90))))
                    .containsExactlyInAnyOrder(1, 2);
        }
    }

    @Test
    void specificOffsetReplaysChangesAfterRecordedPosition() throws Exception {
        awaitInitialRowsInBinlog();

        String[] position = currentBinlogPosition();
        String offset =
                String.format(
                        "{\"file\":\"%s\",\"pos\":\"%s\"}", position[0], position[1]);
        insert("INSERT INTO t_user VALUES (3,'carol')");

        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        oceanBaseHarness(jobId, database, "t_user", offset, mock)) {
            assertThat(ids(harness.readBinlogFromStartupMode(1, Duration.ofSeconds(90))))
                    .containsExactly(3);
            assertThat(ids(harness.loadedRecords())).doesNotContain(1, 2);
        }
    }

    private void awaitInitialRowsInBinlog() throws Exception {
        String probeJobId = String.valueOf(JOB_ID_SEQ.incrementAndGet());

        try (MockDorisServer mock = new MockDorisServer();
                CdcClientWriteHarness harness =
                        oceanBaseHarness(
                                probeJobId, database, "t_user", "earliest", mock)) {
            assertThat(
                            ids(
                                    harness.readBinlogFromStartupMode(
                                            2, Duration.ofSeconds(90))))
                    .containsExactlyInAnyOrder(1, 2);
        }
    }

    private String[] currentBinlogPosition() throws Exception {
        try (Connection connection = connection(database);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW MASTER STATUS")) {
            if (!resultSet.next()) {
                throw new IllegalStateException("SHOW MASTER STATUS returned no row");
            }
            return new String[] {resultSet.getString("File"), resultSet.getString("Position")};
        }
    }

    private void insert(String sql) throws Exception {
        try (Connection connection = connection(database);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private List<Integer> ids(List<String> records) throws Exception {
        List<Integer> result = new ArrayList<>();
        for (String record : records) {
            JsonNode node = MAPPER.readTree(record);
            result.add(node.get("id").asInt());
        }
        return result;
    }
}
