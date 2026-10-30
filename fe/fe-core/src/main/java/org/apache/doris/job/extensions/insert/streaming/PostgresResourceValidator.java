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

import org.apache.doris.common.Config;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.util.StreamingJobUtils;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Fail-fast validation of PostgreSQL replication slot and publication at CREATE JOB time,
 * before the CDC client connects. Catches mistakes (user-provided slot/publication missing,
 * conflicting jobId) with actionable errors. ALTER only checks or extends the publication;
 * replication slot validation runs only on initial create.
 */
public class PostgresResourceValidator {

    public static void validate(Map<String, String> sourceProperties, String jobId, List<String> tableNames)
            throws JobException {
        // PG truncates an over-long db name, so the slot lookup never matches it; reject up front.
        checkDatabaseNameLength(sourceProperties.get(DataSourceConfigKeys.DATABASE));
        String slotName = resolveSlotName(sourceProperties, jobId);
        String publicationName = resolvePublicationName(sourceProperties, jobId);
        // Pattern-match ownership: name equals the default = Doris-owned (auto); otherwise user.
        String defaultSlot = DataSourceConfigKeys.defaultSlotName(jobId);
        String defaultPub = DataSourceConfigKeys.defaultPublicationName(jobId);
        boolean slotUserProvided = !defaultSlot.equals(slotName);
        boolean pubUserProvided = !defaultPub.equals(publicationName);
        List<String> qualifiedTables = qualifyTables(sourceProperties, tableNames);

        JdbcClient jdbcClient = StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, sourceProperties);
        try (Connection conn = jdbcClient.getConnection()) {
            if (pubUserProvided) {
                validateUserPublication(conn, publicationName, qualifiedTables);
            } else if (publicationExists(conn, publicationName)) {
                List<String> missing = findMissingTables(conn, publicationName, qualifiedTables);
                if (!missing.isEmpty()) {
                    throw new JobException(
                            "publication " + publicationName
                                    + " already exists but does not cover the configured"
                                    + " include_tables (missing: " + missing
                                    + "). Another Doris cluster may be using the same jobId."
                                    + " Please set " + DataSourceConfigKeys.PUBLICATION_NAME
                                    + " explicitly to avoid the conflict.");
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

    public static void ensurePublicationCoversTables(Map<String, String> sourceProperties, String jobId,
            List<String> tableNames) throws JobException {
        String configuredName = sourceProperties.get(DataSourceConfigKeys.PUBLICATION_NAME);
        // Jobs created before per-job publication names use the CDC client's legacy default.
        String publicationName = StringUtils.isNotBlank(configuredName)
                ? configuredName : DataSourceConfigKeys.LEGACY_PUBLICATION_NAME;
        List<String> qualifiedTables = qualifyTables(sourceProperties, tableNames);
        JdbcClient jdbcClient = StreamingJobUtils.getJdbcClient(DataSourceType.POSTGRES, sourceProperties);
        try (Connection conn = jdbcClient.getConnection()) {
            if (!DataSourceConfigKeys.defaultPublicationName(jobId).equals(publicationName)) {
                validateUserPublication(conn, publicationName, qualifiedTables);
            } else {
                List<String> missing = findMissingTables(conn, publicationName, qualifiedTables);
                if (!missing.isEmpty()) {
                    String schema = sourceProperties.get(DataSourceConfigKeys.SCHEMA);
                    List<String> quotedTables = new ArrayList<>();
                    for (String table : missing) {
                        quotedTables.add(quoteIdentifier(schema) + "."
                                + quoteIdentifier(table.substring(schema.length() + 1)));
                    }
                    // Preserve old members if ALTER fails before the new job state is persisted.
                    try (Statement statement = conn.createStatement()) {
                        statement.execute("ALTER PUBLICATION " + quoteIdentifier(publicationName)
                                + " ADD TABLE " + String.join(", ", quotedTables));
                    }
                }
            }
        } catch (JobException e) {
            throw e;
        } catch (Exception e) {
            throw new JobException("Failed to prepare PG publication " + publicationName
                    + ": " + e.getMessage(), e);
        } finally {
            jdbcClient.closeClient();
        }
    }

    private static List<String> qualifyTables(Map<String, String> sourceProperties, List<String> tableNames) {
        String schema = sourceProperties.get(DataSourceConfigKeys.SCHEMA);
        List<String> qualifiedTables = new ArrayList<>();
        for (String table : tableNames) {
            qualifiedTables.add(schema + "." + table);
        }
        return qualifiedTables;
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static void validateUserPublication(Connection conn, String publicationName, List<String> tables)
            throws Exception {
        if (!publicationExists(conn, publicationName)) {
            throw new JobException("publication does not exist: " + publicationName
                    + ". Create it before starting the job or omit " + DataSourceConfigKeys.PUBLICATION_NAME
                    + " to let Doris create one.");
        }
        List<String> missing = findMissingTables(conn, publicationName, tables);
        if (!missing.isEmpty()) {
            throw new JobException("publication " + publicationName + " is missing required tables: " + missing
                    + ". Add them via ALTER PUBLICATION ... ADD TABLE before starting.");
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

    private static void checkDatabaseNameLength(String database) throws JobException {
        if (StringUtils.isBlank(database)) {
            return;
        }
        // PG measures the identifier limit in bytes (NAMEDATALEN-1), so compare encoded bytes.
        int bytes = database.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > Config.streaming_pg_max_identifier_length) {
            throw new JobException("database name '" + database + "' is " + bytes + " bytes, exceeding "
                    + Config.streaming_pg_max_identifier_length + "; PostgreSQL truncates it and the"
                    + " replication-slot lookup would fail.");
        }
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
        // This view also expands FOR ALL TABLES publications used by legacy jobs.
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
