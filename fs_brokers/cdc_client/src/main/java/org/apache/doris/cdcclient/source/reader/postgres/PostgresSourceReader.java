package org.apache.doris.cdcclient.source.reader.postgres;

import org.apache.doris.cdcclient.source.reader.JdbcIncrementalSourceReader;
import org.apache.doris.cdcclient.utils.ConfigUtil;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.request.CompareOffsetRequest;
import org.apache.doris.job.cdc.request.JobBaseConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import org.apache.flink.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.fetch.PostgresSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffset;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.source.utils.PostgresTypeUtils;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import lombok.Data;
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
    protected PostgresSourceConfig getSourceConfig(JobBaseConfig config) {
        return generatePostgresConfig(config);
    }

    /** Generate PostgreSQL source config from JobBaseConfig */
    private PostgresSourceConfig generatePostgresConfig(JobBaseConfig config) {
        return generatePostgresConfig(config.getConfig());
    }

    /** Generate PostgreSQL source config from Map config */
    private PostgresSourceConfig generatePostgresConfig(Map<String, String> cdcConfig) {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();

        // Parse JDBC URL to extract connection info
        String jdbcUrl = cdcConfig.get(DataSourceConfigKeys.JDBC_URL);
        Preconditions.checkNotNull(jdbcUrl, "jdbc_url is required");

        // PostgreSQL JDBC URL format: jdbc:postgresql://host:port/database
        java.net.URI uri = java.net.URI.create(jdbcUrl.substring(5)); // Remove "jdbc:"
        String hostname = uri.getHost();
        int port = uri.getPort() == -1 ? 5432 : uri.getPort();
        String database = uri.getPath().substring(1); // Remove leading "/"
        // Driver.parseURL()
        configFactory.hostname(hostname);
        configFactory.port(port);
        configFactory.username(cdcConfig.get(DataSourceConfigKeys.USER));
        configFactory.password(cdcConfig.get(DataSourceConfigKeys.PASSWORD));
        configFactory.database(database);

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
        if (cdcConfig.containsKey(DataSourceConfigKeys.SPLIT_SIZE)) {
            configFactory.splitSize(
                    Integer.parseInt(cdcConfig.get(DataSourceConfigKeys.SPLIT_SIZE)));
        }

        // Set default slot name (can be made configurable later)
        configFactory.slotName("flink_slot_" + ConfigUtil.getServerId(0));

        // Set default plugin name (can be made configurable later)
        configFactory.decodingPluginName("pgoutput");

        return configFactory.create(0);
    }

    @Override
    protected IncrementalSourceScanFetcher getSnapshotSplitReader(JobBaseConfig config) {
        PostgresSourceConfig sourceConfig = getSourceConfig(config);
        IncrementalSourceScanFetcher snapshotReader = this.getSnapshotReader();
        if (snapshotReader == null) {
            PostgresDialect dialect = new PostgresDialect(sourceConfig);
            PostgresSourceFetchTaskContext taskContext =
                    new PostgresSourceFetchTaskContext(sourceConfig, dialect);
            snapshotReader = new IncrementalSourceScanFetcher(taskContext, 0);
            this.setSnapshotReader(snapshotReader);
        }
        return snapshotReader;
    }

    @Override
    protected IncrementalSourceStreamFetcher getBinlogSplitReader(JobBaseConfig config) {
        PostgresSourceConfig sourceConfig = getSourceConfig(config);
        IncrementalSourceStreamFetcher binlogReader = this.getBinlogReader();
        if (binlogReader == null) {
            PostgresDialect dialect = new PostgresDialect(sourceConfig);
            PostgresSourceFetchTaskContext taskContext =
                    new PostgresSourceFetchTaskContext(sourceConfig, dialect);
            binlogReader = new IncrementalSourceStreamFetcher(taskContext, 0);
            this.setBinlogReader(binlogReader);
        }
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

    @Override
    public Map<String, String> getEndOffset(JobBaseConfig jobConfig) {
        PostgresSourceConfig sourceConfig = getSourceConfig(jobConfig);
        try {
            PostgresDialect dialect = new PostgresDialect(sourceConfig);
            Offset currentOffset = dialect.displayCurrentOffset(sourceConfig);
            return currentOffset.getOffset();
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

    private Map<TableId, TableChanges.TableChange> getTableSchemas(JobBaseConfig config) {
        Map<TableId, TableChanges.TableChange> schemas = this.getTableSchemas();
        if (schemas == null) {
            schemas = discoverTableSchemas(config);
            this.setTableSchemas(schemas);
        }
        return schemas;
    }

    protected Map<TableId, TableChanges.TableChange> discoverTableSchemas(JobBaseConfig config) {
        PostgresSourceConfig sourceConfig = getSourceConfig(config);
        try {
            PostgresDialect dialect = new PostgresDialect(sourceConfig);
            try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
                // todo: 遍历所有的 tableId，获取
                TableChange tableChange =
                        dialect.queryTableSchema(
                                jdbcConnection, TableId.parse("public.test_table"));
            }
            return new HashMap<>();
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
}
