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

package org.apache.doris.cdcclient.source.reader.postgres;

import org.apache.doris.cdcclient.common.Constants;
import org.apache.doris.cdcclient.exception.CdcClientException;
import org.apache.doris.cdcclient.source.factory.DataSource;
import org.apache.doris.cdcclient.source.reader.JdbcIncrementalSourceReader;
import org.apache.doris.cdcclient.utils.ConfigUtil;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.CompareOffsetRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresStreamFetchTask;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffset;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.source.utils.CustomPostgresSchema;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresTypeUtils;
import org.apache.flink.cdc.connectors.postgres.source.utils.TableDiscoveryUtils;
import org.apache.flink.table.types.DataType;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.time.Conversions;
import lombok.Data;
import org.postgresql.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class PostgresSourceReader extends JdbcIncrementalSourceReader {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSourceReader.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public PostgresSourceReader() {
        super();
    }

    @Override
    public void initialize(long jobId, DataSource dataSource, Map<String, String> config) {
        PostgresSourceConfig sourceConfig = generatePostgresConfig(config, jobId, 0);
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        LOG.info("Creating slot for job {}, user {}", jobId, sourceConfig.getUsername());
        createSlotForGlobalStreamSplit(dialect);
        super.initialize(jobId, dataSource, config);
    }

    /**
     * copy from org.apache.flink.cdc.connectors.postgres.source
     * .enumerator.PostgresSourceEnumerator.createSlotForGlobalStreamSplit
     *
     * <p>Create slot for the unique global stream split.
     *
     * <p>Currently all startup modes need read the stream split. We need open the slot before
     * reading the globalStreamSplit to catch all data changes.
     */
    private void createSlotForGlobalStreamSplit(PostgresDialect postgresDialect) {
        try (PostgresConnection connection = postgresDialect.openJdbcConnection()) {
            SlotState slotInfo =
                    connection.getReplicationSlotState(
                            postgresDialect.getSlotName(), postgresDialect.getPluginName());
            // skip creating the replication slot when the slot exists.
            if (slotInfo != null) {
                LOG.info(
                        "The replication slot {} already exists, skip creating it.",
                        postgresDialect.getSlotName());
                return;
            }
            PostgresReplicationConnection replicationConnection =
                    postgresDialect.openPostgresReplicationConnection(connection);
            replicationConnection.createReplicationSlot();
            replicationConnection.close(false);

        } catch (Throwable t) {
            throw new CdcClientException(
                    String.format(
                            "Fail to get or create slot, the slot name is %s. Due to: %s ",
                            postgresDialect.getSlotName(), ExceptionUtils.getRootCauseMessage(t)),
                    t);
        }
    }

    @Override
    protected PostgresSourceConfig getSourceConfig(JobBaseConfig config) {
        return generatePostgresConfig(config);
    }

    @Override
    protected PostgresSourceConfig getSourceConfig(JobBaseConfig config, int subtaskId) {
        return generatePostgresConfig(config.getConfig(), config.getJobId(), subtaskId);
    }

    /** Generate PostgreSQL source config from JobBaseConfig */
    private PostgresSourceConfig generatePostgresConfig(JobBaseConfig config) {
        return generatePostgresConfig(config.getConfig(), config.getJobId(), 0);
    }

    /** Generate PostgreSQL source config from Map config */
    private PostgresSourceConfig generatePostgresConfig(
            Map<String, String> cdcConfig, Long jobId, int subtaskId) {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();

        // Parse JDBC URL to extract connection info
        String jdbcUrl = cdcConfig.get(DataSourceConfigKeys.JDBC_URL);
        Preconditions.checkNotNull(jdbcUrl, "jdbc_url is required");

        // PostgreSQL JDBC URL format: jdbc:postgresql://host:port/database
        Properties props = Driver.parseURL(jdbcUrl, null);
        Preconditions.checkNotNull(props, "Invalid JDBC URL: " + jdbcUrl);

        String hostname = props.getProperty("PGHOST");
        String port = props.getProperty("PGPORT");
        String databaseFromUrl = props.getProperty("PGDBNAME");
        Preconditions.checkNotNull(hostname, "host is required");
        Preconditions.checkNotNull(port, "port is required");

        configFactory.hostname(hostname);
        configFactory.port(Integer.parseInt(port));
        configFactory.username(cdcConfig.get(DataSourceConfigKeys.USER));
        configFactory.password(cdcConfig.get(DataSourceConfigKeys.PASSWORD));

        String database = cdcConfig.get(DataSourceConfigKeys.DATABASE);
        String finalDatabase = StringUtils.isNotEmpty(database) ? database : databaseFromUrl;
        Preconditions.checkNotNull(finalDatabase, "database is required");
        configFactory.database(finalDatabase);

        String schema = cdcConfig.get(DataSourceConfigKeys.SCHEMA);
        Preconditions.checkNotNull(schema, "schema is required");
        configFactory.schemaList(new String[] {schema});
        configFactory.includeSchemaChanges(false);

        // Set table list
        String includingTables = cdcConfig.get(DataSourceConfigKeys.INCLUDE_TABLES);
        if (StringUtils.isNotEmpty(includingTables)) {
            String[] includingTbls =
                    Arrays.stream(includingTables.split(","))
                            .map(t -> schema + "." + t.trim())
                            .toArray(String[]::new);
            configFactory.tableList(includingTbls);
        }

        // Set startup options
        String startupMode = cdcConfig.get(DataSourceConfigKeys.OFFSET);
        if (DataSourceConfigKeys.OFFSET_INITIAL.equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.initial());
        } else if (DataSourceConfigKeys.OFFSET_EARLIEST.equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.earliest());
        } else if (DataSourceConfigKeys.OFFSET_LATEST.equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.latest());
        } else if (ConfigUtil.isJson(startupMode)) {
            throw new RuntimeException("Unsupported json offset " + startupMode);
        } else if (ConfigUtil.is13Timestamp(startupMode)) {
            // start from timestamp
            Long ts = Long.parseLong(startupMode);
            configFactory.startupOptions(StartupOptions.timestamp(ts));
        } else {
            throw new RuntimeException("Unknown offset " + startupMode);
        }

        // Set split size if provided
        if (cdcConfig.containsKey(DataSourceConfigKeys.SNAPSHOT_SPLIT_SIZE)) {
            configFactory.splitSize(
                    Integer.parseInt(cdcConfig.get(DataSourceConfigKeys.SNAPSHOT_SPLIT_SIZE)));
        }

        Properties dbzProps = ConfigUtil.getDefaultDebeziumProps();
        dbzProps.put("interval.handling.mode", "string");
        configFactory.debeziumProperties(dbzProps);

        configFactory.serverTimeZone(
                ConfigUtil.getPostgresServerTimeZoneFromProps(props).toString());
        configFactory.slotName(getSlotName(jobId));
        configFactory.decodingPluginName("pgoutput");
        configFactory.heartbeatInterval(
                Duration.ofMillis(Constants.DEBEZIUM_HEARTBEAT_INTERVAL_MS));

        // support scan partition table
        configFactory.setIncludePartitionedTables(true);

        // subtaskId use pg create slot in snapshot phase, slotname is slot_name_subtaskId
        return configFactory.create(subtaskId);
    }

    private String getSlotName(Long jobId) {
        return "doris_cdc_" + jobId;
    }

    @Override
    protected IncrementalSourceScanFetcher getSnapshotSplitReader(
            JobBaseConfig config, int subtaskId) {
        PostgresSourceConfig sourceConfig = getSourceConfig(config, subtaskId);
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        PostgresSourceFetchTaskContext taskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, dialect);
        IncrementalSourceScanFetcher snapshotReader =
                new IncrementalSourceScanFetcher(taskContext, subtaskId);
        return snapshotReader;
    }

    @Override
    protected IncrementalSourceStreamFetcher getBinlogSplitReader(JobBaseConfig config) {
        PostgresSourceConfig sourceConfig = getSourceConfig(config);
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        PostgresSourceFetchTaskContext taskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, dialect);
        // subTaskId maybe add jobId?
        IncrementalSourceStreamFetcher binlogReader =
                new IncrementalSourceStreamFetcher(taskContext, 0);
        return binlogReader;
    }

    @Override
    protected OffsetFactory getOffsetFactory() {
        return new PostgresOffsetFactory();
    }

    @Override
    protected Offset createOffset(Map<String, ?> offset) {
        return PostgresOffset.of(offset);
    }

    @Override
    protected Offset createInitialOffset() {
        return PostgresOffset.INITIAL_OFFSET;
    }

    @Override
    protected Offset createNoStoppingOffset() {
        return PostgresOffset.NO_STOPPING_OFFSET;
    }

    @Override
    protected JdbcDataSourceDialect getDialect(JdbcSourceConfig sourceConfig) {
        return new PostgresDialect((PostgresSourceConfig) sourceConfig);
    }

    @Override
    protected DataType fromDbzColumn(Column splitColumn) {
        return PostgresTypeUtils.fromDbzColumn(splitColumn);
    }

    /**
     * Why not call dialect.displayCurrentOffset(sourceConfig) ? The underlying system calls
     * `txid_current()` to advance the WAL log. Here, it's just a query; retrieving the LSN is
     * sufficient because `PostgresOffset.compare` only compares the LSN.
     */
    @Override
    public Map<String, String> getEndOffset(JobBaseConfig jobConfig) {
        PostgresSourceConfig sourceConfig = getSourceConfig(jobConfig);
        try {
            PostgresDialect dialect = new PostgresDialect(sourceConfig);
            try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
                PostgresConnection pgConnection = (PostgresConnection) jdbcConnection;
                Long lsn = pgConnection.currentXLogLocation();
                Map<String, String> offsetMap = new HashMap<>();
                offsetMap.put(SourceInfo.LSN_KEY, lsn.toString());
                offsetMap.put(
                        SourceInfo.TIMESTAMP_USEC_KEY,
                        String.valueOf(Conversions.toEpochMicros(Instant.MIN)));
                return offsetMap;
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public int compareOffset(CompareOffsetRequest compareOffsetRequest) {
        Map<String, String> offsetFirst = compareOffsetRequest.getOffsetFirst();
        Map<String, String> offsetSecond = compareOffsetRequest.getOffsetSecond();

        PostgresOffset postgresOffset1 = PostgresOffset.of(offsetFirst);
        PostgresOffset postgresOffset2 = PostgresOffset.of(offsetSecond);
        return postgresOffset1.compareTo(postgresOffset2);
    }

    @Override
    protected Map<TableId, TableChanges.TableChange> discoverTableSchemas(JobBaseConfig config) {
        PostgresSourceConfig sourceConfig = getSourceConfig(config);
        try {
            PostgresDialect dialect = new PostgresDialect(sourceConfig);
            try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
                List<TableId> tableIds =
                        TableDiscoveryUtils.listTables(
                                sourceConfig.getDatabaseList().get(0),
                                jdbcConnection,
                                sourceConfig.getTableFilters(),
                                sourceConfig.includePartitionedTables());
                CustomPostgresSchema customPostgresSchema =
                        new CustomPostgresSchema((PostgresConnection) jdbcConnection, sourceConfig);
                return customPostgresSchema.getTableSchema(tableIds);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected FetchTask<SourceSplitBase> createFetchTaskFromSplit(
            JobBaseConfig jobConfig, SourceSplitBase split) {
        PostgresSourceConfig sourceConfig = getSourceConfig(jobConfig);
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        FetchTask<SourceSplitBase> fetchTask = dialect.createFetchTask(split);
        return fetchTask;
    }

    /**
     * This method commits values up to the startOffset of the current split; even if
     * `CommitFeOffset` fails, Data after the startOffset will not be cleared.
     */
    @Override
    public void commitSourceOffset(Long jobId, SourceSplit sourceSplit) {
        try {
            if (sourceSplit instanceof StreamSplit) {
                Offset offsetToCommit = ((StreamSplit) sourceSplit).getStartingOffset();
                if (getCurrentFetchTask() != null
                        && getCurrentFetchTask() instanceof PostgresStreamFetchTask) {
                    ((PostgresStreamFetchTask) getCurrentFetchTask())
                            .commitCurrentOffset(offsetToCommit);
                    LOG.info(
                            "Committing job {} postgres offset {} for {}",
                            jobId,
                            offsetToCommit,
                            getCurrentFetchTask().getSplit());
                }
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to commit {} postgres offset for split {}: {}",
                    jobId,
                    sourceSplit,
                    e.getMessage(),
                    e);
        }
    }

    @Override
    public void close(JobBaseConfig jobConfig) {
        super.close(jobConfig);
        // drop pg slot
        try {
            PostgresSourceConfig sourceConfig = getSourceConfig(jobConfig);
            PostgresDialect dialect = new PostgresDialect(sourceConfig);
            String slotName = getSlotName(jobConfig.getJobId());
            LOG.info(
                    "Dropping postgres replication slot {} for job {}",
                    slotName,
                    jobConfig.getJobId());
            dialect.removeSlot(slotName);
        } catch (Exception ex) {
            LOG.warn(
                    "Failed to drop postgres replication slot for job {}: {}",
                    jobConfig.getJobId(),
                    ex.getMessage());
        }
    }
}
