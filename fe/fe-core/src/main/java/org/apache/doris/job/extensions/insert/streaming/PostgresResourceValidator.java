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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.util.StreamingJobUtils;

import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fail-fast validation of PostgreSQL replication slot and publication at CREATE JOB time,
 * before the CDC client connects. Catches mistakes (user-provided slot/publication missing,
 * conflicting jobId) with actionable errors. Validation runs only on initial create; restarts
 * skip this path by design, so an active slot held by the previous BE does not self-conflict.
 */
public class PostgresResourceValidator {

    public static void validate(Map<String, String> sourceProperties, String jobId, List<String> tableNames)
            throws JobException {
        String slotName = resolveSlotName(sourceProperties, jobId);
        String publicationName = resolvePublicationName(sourceProperties, jobId);
        // Pattern-match ownership: name equals the default = Doris-owned (auto); otherwise user.
        String defaultSlot = DataSourceConfigKeys.defaultSlotName(jobId);
        String defaultPub = DataSourceConfigKeys.defaultPublicationName(jobId);
        boolean slotUserProvided = !defaultSlot.equals(slotName);
        boolean pubUserProvided = !defaultPub.equals(publicationName);
        String pgSchema = sourceProperties.get(DataSourceConfigKeys.SCHEMA);
        List<String> qualifiedTables = new ArrayList<>();
        for (String name : tableNames) {
            qualifiedTables.add(pgSchema + "." + name);
        }

        JdbcClient jdbcClient = StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, sourceProperties);
        try (Connection conn = jdbcClient.getConnection()) {
            boolean pubExists = publicationExists(conn, publicationName);
            if (!pubExists && pubUserProvided) {
                throw new JobException(
                        "publication does not exist: " + publicationName
                                + ". Create it before starting the job or omit "
                                + DataSourceConfigKeys.PUBLICATION_NAME
                                + " to let Doris create one.");
            }
            if (pubExists) {
                List<String> missing = findMissingTables(conn, publicationName, qualifiedTables);
                if (!missing.isEmpty()) {
                    if (pubUserProvided) {
                        throw new JobException(
                                "publication " + publicationName
                                        + " is missing required tables: " + missing
                                        + ". Add them via ALTER PUBLICATION ... ADD TABLE before starting.");
                    } else {
                        throw new JobException(
                                "publication " + publicationName
                                        + " already exists but does not cover the configured"
                                        + " include_tables (missing: " + missing
                                        + "). Another Doris cluster may be using the same jobId."
                                        + " Please set " + DataSourceConfigKeys.PUBLICATION_NAME
                                        + " explicitly to avoid the conflict.");
                    }
                }
            }
            Boolean slotActive = queryReplicationSlotActive(conn, slotName);
            if (slotUserProvided && slotActive == null) {
                throw new JobException(
                        "replication slot does not exist: " + slotName
                                + ". Create it before starting the job or omit "
                                + DataSourceConfigKeys.SLOT_NAME
                                + " to let Doris create one.");
            }
            if (!slotUserProvided && Boolean.TRUE.equals(slotActive)) {
                throw new JobException(
                        "replication slot " + slotName
                                + " is active, held by another consumer. Another Doris"
                                + " cluster may be using the same jobId. Please set "
                                + DataSourceConfigKeys.SLOT_NAME
                                + " explicitly to avoid the conflict.");
            }
        } catch (JobException e) {
            throw e;
        } catch (Exception e) {
            throw new JobException(
                    "Failed to validate PG resources for publication " + publicationName
                            + ": " + e.getMessage(), e);
        } finally {
            jdbcClient.closeClient();
        }
    }

    private static String resolveSlotName(Map<String, String> config, String jobId) {
        String name = config.get(DataSourceConfigKeys.SLOT_NAME);
        return StringUtils.isNotBlank(name) ? name : DataSourceConfigKeys.defaultSlotName(jobId);
    }

    private static String resolvePublicationName(Map<String, String> config, String jobId) {
        String name = config.get(DataSourceConfigKeys.PUBLICATION_NAME);
        return StringUtils.isNotBlank(name) ? name : DataSourceConfigKeys.defaultPublicationName(jobId);
    }

    private static boolean publicationExists(Connection conn, String publicationName) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM pg_publication WHERE pubname = ?")) {
            ps.setString(1, publicationName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static List<String> findMissingTables(Connection conn, String publicationName, List<String> tables)
            throws Exception {
        Set<String> covered = new HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ?")) {
            ps.setString(1, publicationName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    covered.add(rs.getString(1) + "." + rs.getString(2));
                }
            }
        }
        List<String> missing = new ArrayList<>();
        for (String table : tables) {
            if (!covered.contains(table)) {
                missing.add(table);
            }
        }
        return missing;
    }

    /** Returns the slot's active flag, or null when the slot does not exist. */
    private static Boolean queryReplicationSlotActive(Connection conn, String slotName) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT active FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return rs.getBoolean(1);
            }
        }
    }
}
