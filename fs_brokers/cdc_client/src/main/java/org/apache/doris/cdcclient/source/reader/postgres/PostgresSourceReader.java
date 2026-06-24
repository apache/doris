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
import org.apache.doris.cdcclient.source.deserialize.PostgresDebeziumJsonDeserializer;
import org.apache.doris.cdcclient.source.factory.DataSource;
import org.apache.doris.cdcclient.source.reader.JdbcIncrementalSourceReader;
import org.apache.doris.cdcclient.utils.ConfigUtil;
import org.apache.doris.cdcclient.utils.SmallFileMgr;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.CompareOffsetRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;
import org.apache.doris.job.cdc.request.JobBaseRecordRequest;

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
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresQueryUtils;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresTypeUtils;
import org.apache.flink.cdc.connectors.postgres.source.utils.TableDiscoveryUtils;
import org.apache.flink.table.types.DataType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresConnectorConfig.AutoCreateMode;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
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
import org.postgresql.core.BaseConnection;
import org.postgresql.core.ServerVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class PostgresSourceReader extends JdbcIncrementalSourceReader {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSourceReader.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Object SLOT_CREATION_LOCK = new Object();

    public PostgresSourceReader() {
        super();
        this.setSerializer(new PostgresDebeziumJsonDeserializer());
    }

    @Override
    public void initialize(String jobId, DataSource dataSource, Map<String, String> config) {
        PostgresSourceConfig sourceConfig = generatePostgresConfig(config, jobId, 0);
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        // Doris-owned publication: pre-create it covering all include_tables (autocreate is
        // DISABLED).
        if (isPublicationDorisOwned(config, jobId)) {
            createPublicationForDorisOwned(dialect, config, jobId);
        }
        // Only create the slot when Doris owns it (name == default); user-provided slots must
        // pre-exist, validated at CREATE JOB.
        if (isSlotDorisOwned(config, jobId)) {
            synchronized (SLOT_CREATION_LOCK) {
                LOG.info("Creating slot for job {}, user {}", jobId, sourceConfig.getUsername());
                createSlotForGlobalStreamSplit(dialect);
            }
        }
        super.initialize(jobId, dataSource, config);
        // Inject PG schema refresher so the deserializer can fetch accurate column types on DDL
        if (serializer instanceof PostgresDebeziumJsonDeserializer) {
            ((PostgresDebeziumJsonDeserializer) serializer)
                    .setPgSchemaRefresher(
                            tableId -> refreshSingleTableSchema(tableId, config, jobId));
        }
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

    /**
     * Create/ensure the Doris-owned publication for all include_tables (idempotent, multi-BE safe).
     */
    private void createPublicationForDorisOwned(
            PostgresDialect dialect, Map<String, String> config, String jobId) {
        String pubName = resolvePublicationName(config, jobId);
        String schema = config.get(DataSourceConfigKeys.SCHEMA);
        String[] qualified = ConfigUtil.getTableList(schema, config);
        if (qualified.length == 0) {
            throw new CdcClientException("No tables to create publication " + pubName);
        }
        String tableList =
                Arrays.stream(qualified)
                        .map(
                                q ->
                                        new TableId(null, schema, q.substring(q.indexOf('.') + 1))
                                                .toDoubleQuotedString())
                        .collect(Collectors.joining(", "));
        // Mirrors debezium PostgresReplicationConnection#initPublication: check existence, then
        // CREATE ... FOR TABLE / ALTER ... SET TABLE (here always the full include_tables set).
        try (PostgresConnection conn = dialect.openJdbcConnection();
                Statement stmt = conn.connection().createStatement()) {
            long count;
            try (ResultSet rs =
                    stmt.executeQuery(
                            "SELECT COUNT(1) FROM pg_publication WHERE pubname = '"
                                    + pubName
                                    + "'")) {
                rs.next();
                count = rs.getLong(1);
            }
            if (count == 0) {
                // Preserve debezium FILTERED behavior: on PG 13+ publish partitioned-root changes
                // as the root table, matching configFactory.setIncludePartitionedTables(true).
                String pubViaRootSuffix =
                        ((BaseConnection) conn.connection())
                                        .haveMinimumServerVersion(ServerVersion.v13)
                                ? " WITH (publish_via_partition_root = true)"
                                : "";
                stmt.execute(
                        "CREATE PUBLICATION "
                                + pubName
                                + " FOR TABLE "
                                + tableList
                                + pubViaRootSuffix);
            } else {
                stmt.execute("ALTER PUBLICATION " + pubName + " SET TABLE " + tableList);
            }
            LOG.info("Ensured publication {} for tables {}", pubName, tableList);
        } catch (SQLException e) {
            throw new CdcClientException(
                    "Failed to create publication " + pubName + ": " + e.getMessage(), e);
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
            Map<String, String> cdcConfig, String jobId, int subtaskId) {
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
        String[] tableList = ConfigUtil.getTableList(schema, cdcConfig);
        Preconditions.checkArgument(tableList.length >= 1, "include_tables or table is required");
        configFactory.tableList(tableList);

        // Set startup options
        String startupMode = cdcConfig.get(DataSourceConfigKeys.OFFSET);
        if (DataSourceConfigKeys.OFFSET_INITIAL.equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.initial());
        } else if (DataSourceConfigKeys.OFFSET_SNAPSHOT.equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.snapshot());
        } else if (DataSourceConfigKeys.OFFSET_EARLIEST.equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.earliest());
        } else if (DataSourceConfigKeys.OFFSET_LATEST.equalsIgnoreCase(startupMode)) {
            configFactory.startupOptions(StartupOptions.latest());
        } else if (ConfigUtil.isJson(startupMode)) {
            Map<String, String> offsetMap = ConfigUtil.toStringMap(startupMode);
            if (offsetMap == null || !offsetMap.containsKey(SourceInfo.LSN_KEY)) {
                throw new RuntimeException(
                        "JSON offset for PostgreSQL must contain 'lsn' key, got: " + startupMode);
            }
            configFactory.startupOptions(StartupOptions.specificOffset(offsetMap));
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

        if (cdcConfig.containsKey(DataSourceConfigKeys.SNAPSHOT_SPLIT_KEY)) {
            configFactory.chunkKeyColumn(cdcConfig.get(DataSourceConfigKeys.SNAPSHOT_SPLIT_KEY));
        }

        Properties dbzProps = ConfigUtil.getDefaultDebeziumProps();
        dbzProps.put("interval.handling.mode", "string");

        // Always DISABLED; the publication always pre-exists: Doris creates it for all
        // include_tables
        // in initialize(); user-provided / legacy (dbz_publication) ones are already present on PG.
        // FILTERED would make each split SET TABLE its single table -> flip publication -> data
        // loss.
        String publicationName = resolvePublicationName(cdcConfig, jobId);
        String slotName = resolveSlotName(cdcConfig, jobId);
        dbzProps.put(PostgresConnectorConfig.PUBLICATION_NAME.name(), publicationName);
        dbzProps.put(
                PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE.name(),
                AutoCreateMode.DISABLED.getValue());

        configFactory.debeziumProperties(dbzProps);

        // setting ssl
        if (cdcConfig.containsKey(DataSourceConfigKeys.SSL_MODE)) {
            dbzProps.put("database.sslmode", cdcConfig.get(DataSourceConfigKeys.SSL_MODE));
        }

        if (cdcConfig.containsKey(DataSourceConfigKeys.SSL_ROOTCERT)) {
            String fileName = cdcConfig.get(DataSourceConfigKeys.SSL_ROOTCERT);
            String filePath = SmallFileMgr.getFilePath(fileName);
            LOG.info("Using SSL root cert file path: {}", filePath);
            dbzProps.put("database.sslrootcert", filePath);
        }

        configFactory.serverTimeZone(
                ConfigUtil.getPostgresServerTimeZoneFromProps(props).toString());
        configFactory.slotName(slotName);
        configFactory.decodingPluginName("pgoutput");
        configFactory.heartbeatInterval(
                Duration.ofMillis(Constants.DEBEZIUM_HEARTBEAT_INTERVAL_MS));

        // support scan partition table
        configFactory.setIncludePartitionedTables(true);

        // from-to: FE forces "true" (at-least-once, skip backfill); TVF: absent → false
        // (exactly-once needs backfill).
        configFactory.skipSnapshotBackfill(
                Boolean.parseBoolean(cdcConfig.get(DataSourceConfigKeys.SKIP_SNAPSHOT_BACKFILL)));

        // subtaskId use pg create slot in snapshot phase, slotname is slot_name_subtaskId
        return configFactory.create(subtaskId);
    }

    private String resolveSlotName(Map<String, String> config, String jobId) {
        String name = config.get(DataSourceConfigKeys.SLOT_NAME);
        return StringUtils.isNotBlank(name) ? name : DataSourceConfigKeys.defaultSlotName(jobId);
    }

    // Legacy jobs (created before slot/pub names were persisted) keep no publication_name in
    // sourceProperties; fall back to the pre-PR Debezium default so they continue to use the
    // existing publication on PG.
    private String resolvePublicationName(Map<String, String> config, String jobId) {
        String name = config.get(DataSourceConfigKeys.PUBLICATION_NAME);
        return StringUtils.isNotBlank(name) ? name : DataSourceConfigKeys.LEGACY_PUBLICATION_NAME;
    }

    // Per-resource ownership: Doris owns the resource iff the resolved name equals
    // doris_{cdc|pub}_{jobId}. Users cannot specify this name (jobId is unknown pre-CREATE);
    // legacy publication resolves to dbz_publication and stays user-owned (not dropped).
    private boolean isSlotDorisOwned(Map<String, String> config, String jobId) {
        return DataSourceConfigKeys.defaultSlotName(jobId).equals(resolveSlotName(config, jobId));
    }

    private boolean isPublicationDorisOwned(Map<String, String> config, String jobId) {
        return DataSourceConfigKeys.defaultPublicationName(jobId)
                .equals(resolvePublicationName(config, jobId));
    }

    @Override
    protected IncrementalSourceScanFetcher getSnapshotSplitReader(
            JobBaseConfig config, int subtaskId) {
        PostgresSourceConfig sourceConfig = getSourceConfig(config, subtaskId);
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        PostgresSourceFetchTaskContext taskContext =
                new PostgresSourceFetchTaskContext(sourceConfig, dialect);
        LOG.info(
                "create snapshot reader for job {}, thread tag = debezium-snapshot-reader-{}",
                config.getJobId(),
                subtaskId);
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
        int readerTag = Math.abs(config.getJobId().hashCode());
        LOG.info(
                "create binlog reader for job {}, thread tag = debezium-reader-{}",
                config.getJobId(),
                readerTag);
        IncrementalSourceStreamFetcher binlogReader =
                new IncrementalSourceStreamFetcher(taskContext, readerTag);
        return binlogReader;
    }

    @Override
    protected OffsetFactory getOffsetFactory() {
        return new PostgresOffsetFactory();
    }

    @Override
    protected Offset createOffset(Map<String, ?> offset) {
        // ALTER offset may only contain lsn, supplement ts_usec for PostgresOffsetContext.Loader
        if (offset.containsKey(SourceInfo.LSN_KEY)
                && !offset.containsKey(SourceInfo.TIMESTAMP_USEC_KEY)) {
            Map<String, Object> supplemented = new HashMap<>(offset);
            supplemented.put(SourceInfo.TIMESTAMP_USEC_KEY, "0");
            return PostgresOffset.of(supplemented);
        }
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

    @Override
    protected Class<?> probeSplitKeyClass(
            TableId tableId, Column splitColumn, JobBaseConfig jobConfig) {
        PostgresSourceConfig sourceConfig = getSourceConfig(jobConfig);
        String sql =
                String.format(
                        "SELECT %s FROM %s WHERE 1=0",
                        PostgresQueryUtils.quote(splitColumn.name()),
                        PostgresQueryUtils.quote(tableId));
        try (JdbcConnection jdbc =
                        new PostgresDialect(sourceConfig).openJdbcConnection(sourceConfig);
                Statement st = jdbc.connection().createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            return Class.forName(rs.getMetaData().getColumnClassName(1));
        } catch (Exception e) {
            throw new RuntimeException(
                    "Probe split key class failed for " + tableId + "." + splitColumn.name(), e);
        }
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

    // Detect a replication slot that was dropped (or dropped and recreated) out from under us while
    // the job was paused/retrying. Recreating it silently would resume from a position whose WAL is
    // already gone -> data loss. Fail with a fixed marker so FE classifies it as non-resumable.
    @Override
    protected void validateStreamSource(
            Map<String, Object> offsetMeta, JobBaseRecordRequest baseReq) throws Exception {
        PostgresSourceConfig sourceConfig = getSourceConfig(baseReq);
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        try (PostgresConnection connection = dialect.openJdbcConnection()) {
            SlotState slotState =
                    connection.getReplicationSlotState(
                            dialect.getSlotName(), dialect.getPluginName());
            if (slotState == null) {
                throw new CdcClientException(
                        String.format(
                                "Replication slot invalidated for job %s: slot %s not found on the"
                                        + " upstream (dropped externally), cannot resume from the"
                                        + " committed position without data loss.",
                                baseReq.getJobId(), dialect.getSlotName()));
            }
            Lsn requestedLsn = extractRequestedLsn(offsetMeta);
            Lsn restartLsn = slotState.slotRestartLsn();
            // restart_lsn must stay <= committed position; a higher one means the slot was
            // recreated
            // and the WAL between them was discarded, so resuming would silently skip data.
            if (requestedLsn != null
                    && requestedLsn.asLong() > 0
                    && restartLsn != null
                    && restartLsn.compareTo(requestedLsn) > 0) {
                throw new CdcClientException(
                        String.format(
                                "Replication slot invalidated for job %s: slot %s restart_lsn %s is"
                                        + " ahead of the committed position %s (slot recreated),"
                                        + " cannot resume without data loss.",
                                baseReq.getJobId(),
                                dialect.getSlotName(),
                                restartLsn,
                                requestedLsn));
            }
        }
    }

    private Lsn extractRequestedLsn(Map<String, Object> offsetMeta) {
        if (offsetMeta == null || offsetMeta.get(SourceInfo.LSN_KEY) == null) {
            return null;
        }
        try {
            return Lsn.valueOf(Long.parseLong(String.valueOf(offsetMeta.get(SourceInfo.LSN_KEY))));
        } catch (NumberFormatException ex) {
            return null;
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

    /**
     * Fetch the current schema for a single table directly from PostgreSQL via JDBC.
     *
     * <p>Called by {@link PostgresDebeziumJsonDeserializer} when a schema change (ADD/DROP column)
     * is detected, to obtain accurate PG column types for DDL generation.
     *
     * @return the fresh {@link TableChanges.TableChange}
     */
    private TableChanges.TableChange refreshSingleTableSchema(
            TableId tableId, Map<String, String> config, String jobId) {
        PostgresSourceConfig sourceConfig = generatePostgresConfig(config, jobId, 0);
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            CustomPostgresSchema customPostgresSchema =
                    new CustomPostgresSchema((PostgresConnection) jdbcConnection, sourceConfig);
            Map<TableId, TableChanges.TableChange> schemas =
                    customPostgresSchema.getTableSchema(Collections.singletonList(tableId));
            return schemas.get(tableId);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
    public void commitSourceOffset(String jobId, SourceSplit sourceSplit) {
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

    /**
     * Strip lsn_proc and lsn_commit from the binlog state offset before it is passed to debezium's
     * WalPositionLocator. In pgoutput non-streaming mode (proto_version=1, used by debezium 1.9.x
     * even on PG14), BEGIN and DML messages within a transaction share the same XLogData.data_start
     * as the transaction's begin_lsn. When begin_lsn equals the previous transaction's commit_lsn
     * (i.e. no other WAL write exists between them), WalPositionLocator adds that lsn to lsnSeen
     * during the find phase and then incorrectly filters the DML as already-processed during actual
     * streaming. Removing these keys sets lastCommitStoredLsn=null, so the find phase exits
     * immediately at the first received message and switch-off happens before any DML is filtered.
     * See https://issues.apache.org/jira/browse/FLINK-39265.
     */
    @Override
    public Map<String, String> extractBinlogStateOffset(Object splitState) {
        Map<String, String> offset = super.extractBinlogStateOffset(splitState);
        offset.remove(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY);
        offset.remove(PostgresOffsetContext.LAST_COMMIT_LSN_KEY);
        return offset;
    }

    @Override
    public void close(JobBaseConfig jobConfig) {
        super.close(jobConfig);
        releaseSourceResources(jobConfig);
    }

    /**
     * Drop the Doris-owned slot/publication. Returns false if either is still present (e.g. a dead
     * BE's stale walsender holds the slot until PG reclaims it), so the caller can retry later.
     */
    @Override
    public boolean releaseSourceResources(JobBaseConfig jobConfig) {
        Map<String, String> config = jobConfig.getConfig();
        String jobId = jobConfig.getJobId();
        String slotName = resolveSlotName(config, jobId);
        String pubName = resolvePublicationName(config, jobId);
        boolean dropSlot = isSlotDorisOwned(config, jobId);
        boolean dropPub = isPublicationDorisOwned(config, jobId);
        if (!dropSlot && !dropPub) {
            LOG.info(
                    "Skipping drop of user-provided slot {} / publication {} for job {}",
                    slotName,
                    pubName,
                    jobId);
            return true;
        }
        PostgresDialect dialect = new PostgresDialect(getSourceConfig(jobConfig));
        boolean cleaned = true;
        if (dropPub) {
            LOG.info("Dropping auto-created publication {} for job {}", pubName, jobId);
            try (PostgresConnection connection = dialect.openJdbcConnection()) {
                connection.execute("DROP PUBLICATION IF EXISTS " + pubName);
            } catch (Exception ex) {
                LOG.warn(
                        "Failed to drop publication {} for job {}: {}",
                        pubName,
                        jobId,
                        ex.getMessage());
            }
            if (publicationExists(dialect, pubName)) {
                LOG.warn(
                        "Publication {} for job {} still present after drop, will retry",
                        pubName,
                        jobId);
                cleaned = false;
            }
        }
        if (dropSlot) {
            LOG.info("Dropping auto-created replication slot {} for job {}", slotName, jobId);
            try {
                dialect.removeSlot(slotName);
            } catch (Exception ex) {
                LOG.warn(
                        "Drop of replication slot {} for job {} failed: {}",
                        slotName,
                        jobId,
                        ex.getMessage());
            }
            if (slotExists(dialect, slotName)) {
                LOG.warn(
                        "Replication slot {} for job {} still present after drop, will retry",
                        slotName,
                        jobId);
                cleaned = false;
            }
        }
        return cleaned;
    }

    private boolean slotExists(PostgresDialect dialect, String slotName) {
        try (PostgresConnection connection = dialect.openJdbcConnection()) {
            return connection.queryAndMap(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = '" + slotName + "'",
                    rs -> rs.next());
        } catch (Exception ex) {
            // Can't verify -> treat as present so the bounded retry keeps trying instead of
            // leaking.
            LOG.warn(
                    "Failed to check replication slot {} existence: {}", slotName, ex.getMessage());
            return true;
        }
    }

    private boolean publicationExists(PostgresDialect dialect, String pubName) {
        try (PostgresConnection connection = dialect.openJdbcConnection()) {
            return connection.queryAndMap(
                    "SELECT 1 FROM pg_publication WHERE pubname = '" + pubName + "'",
                    rs -> rs.next());
        } catch (Exception ex) {
            // Can't verify -> treat as present so the bounded retry keeps trying instead of
            // leaking.
            LOG.warn("Failed to check publication {} existence: {}", pubName, ex.getMessage());
            return true;
        }
    }
}
