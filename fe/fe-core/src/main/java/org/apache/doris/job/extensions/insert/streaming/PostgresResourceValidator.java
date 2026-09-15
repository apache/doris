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
import org.apache.doris.connector.spi.ConnectorQueryResult;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.job.util.StreamingSourceClient;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
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
        // PG truncates an over-long db name, so the slot lookup never matches it; reject up front.
        checkDatabaseNameLength(sourceProperties.get(DataSourceConfigKeys.DATABASE));
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

        try (StreamingSourceClient conn = StreamingJobUtils.openSourceClient(DataSourceType.POSTGRES,
                sourceProperties)) {
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

    private static boolean publicationExists(StreamingSourceClient conn, String publicationName) throws Exception {
        ConnectorQueryResult rs = conn.executeQuery("SELECT 1 FROM pg_publication WHERE pubname = ?",
                Collections.singletonList(publicationName));
        return !rs.isEmpty();
    }

    private static List<String> findMissingTables(StreamingSourceClient conn, String publicationName,
            List<String> tables) throws Exception {
        Set<String> covered = new HashSet<>();
        ConnectorQueryResult rs = conn.executeQuery(
                "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ?",
                Collections.singletonList(publicationName));
        for (List<Object> row : rs.getRows()) {
            covered.add(row.get(0) + "." + row.get(1));
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
    private static Boolean queryReplicationSlotActive(StreamingSourceClient conn, String slotName) throws Exception {
        ConnectorQueryResult rs = conn.executeQuery("SELECT active FROM pg_replication_slots WHERE slot_name = ?",
                Collections.singletonList(slotName));
        if (rs.isEmpty()) {
            return null;
        }
        // The driver hands a PostgreSQL boolean back as a Boolean; anything else reads as "not active".
        return Boolean.TRUE.equals(rs.getRows().get(0).get(0));
    }
}
