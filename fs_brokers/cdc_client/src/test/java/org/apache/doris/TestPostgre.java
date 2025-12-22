package org.apache.doris;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.source.assigner.HybridSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.SnapshotSplitAssigner;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test class for PostgreSQL chunk splitting using Flink CDC SnapshotSplitAssigner */
public class TestPostgre {
    private static final Logger LOG = LoggerFactory.getLogger(TestPostgre.class);

    public static void main(String[] args) {
        // Example usage
        try {
            PostgresSourceConfig sourceConfig = createPostgresSourceConfig();
            String database = "test_db";
            String snapshotTable = "t_user_info";

            LOG.info("start split PostgreSQL 表: {}.public.{}", database, snapshotTable);
            List<SnapshotSplit> splits = startSplitChunks(sourceConfig, database, snapshotTable);

            System.out.println("\n=== 切分结果 ===");
            System.out.println("成功生成 " + splits.size() + " 个快照切片");
            for (int i = 0; i < splits.size(); i++) {
                SnapshotSplit split = splits.get(i);
                System.out.println(
                        "切片 "
                                + (i + 1)
                                + ": "
                                + split.splitId()
                                + " (表: "
                                + split.getTableId()
                                + ")");
            }
        } catch (Exception e) {
            LOG.error("切分 PostgreSQL chunks 失败", e);
            System.err.println("错误: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Split PostgreSQL table into chunks similar to MySQL implementation
     *
     * @param sourceConfig PostgreSQL source configuration
     * @param database database name
     * @param snapshotTable table name to split
     * @return list of snapshot splits
     */
    private static List<SnapshotSplit> startSplitChunks(
            PostgresSourceConfig sourceConfig, String database, String snapshotTable)
            throws IOException {
        List<TableId> remainingTables = new ArrayList<>();
        TableId tableId = new TableId(null, "public", snapshotTable);
        remainingTables.add(tableId);
        List<SnapshotSplit> remainingSplits = new ArrayList<>();
        PostgresDialect dialect = new PostgresDialect(sourceConfig);
        OffsetFactory offsetFactory = new PostgresOffsetFactory();
        HybridSplitAssigner<JdbcSourceConfig> splitAssigner =
                new HybridSplitAssigner<>(
                        sourceConfig,
                        1,
                        remainingTables,
                        true,
                        dialect,
                        offsetFactory,
                        new MockSplitEnumeratorContext(1));
        splitAssigner.open();
        try {
            LOG.info("start split for {}.public.{} ...", database, snapshotTable);
            while (true) {
                Optional<SourceSplitBase> split = splitAssigner.getNext();
                if (split.isPresent()) {
                    SnapshotSplit snapshotSplit = split.get().asSnapshotSplit();
                    remainingSplits.add(snapshotSplit);
                    LOG.info("split is {}", snapshotSplit);
                } else {
                    LOG.info("split finished");
                    break;
                }
            }
        } finally {
            closeChunkSplitterOnly(splitAssigner);
            LOG.info("SnapshotSplitAssigner close");
        }
        return remainingSplits;
    }

    /**
     * Close only the chunk splitter to avoid closing shared connection pools Similar to MySQL
     * implementation Note: HybridSplitAssigner wraps SnapshotSplitAssigner, so we need to get the
     * inner assigner first
     */
    private static void closeChunkSplitterOnly(HybridSplitAssigner<?> splitAssigner) {
        try {
            // First, get the inner SnapshotSplitAssigner from HybridSplitAssigner
            java.lang.reflect.Field snapshotAssignerField =
                    HybridSplitAssigner.class.getDeclaredField("snapshotSplitAssigner");
            snapshotAssignerField.setAccessible(true);
            SnapshotSplitAssigner<?> snapshotSplitAssigner =
                    (SnapshotSplitAssigner<?>) snapshotAssignerField.get(splitAssigner);

            if (snapshotSplitAssigner == null) {
                LOG.warn("snapshotSplitAssigner is null in HybridSplitAssigner");
                return;
            }

            // Call closeExecutorService() via reflection
            java.lang.reflect.Method closeExecutorMethod =
                    SnapshotSplitAssigner.class.getDeclaredMethod("closeExecutorService");
            closeExecutorMethod.setAccessible(true);
            closeExecutorMethod.invoke(snapshotSplitAssigner);

            // Call chunkSplitter.close() via reflection
            java.lang.reflect.Field chunkSplitterField =
                    SnapshotSplitAssigner.class.getDeclaredField("chunkSplitter");
            chunkSplitterField.setAccessible(true);
            Object chunkSplitter = chunkSplitterField.get(snapshotSplitAssigner);

            if (chunkSplitter != null) {
                java.lang.reflect.Method closeMethod = chunkSplitter.getClass().getMethod("close");
                closeMethod.invoke(chunkSplitter);
                LOG.info("Closed PostgreSQL chunkSplitter JDBC connection");
            }
        } catch (Exception e) {
            LOG.warn("Failed to close chunkSplitter via reflection", e);
        }
    }

    /**
     * Create PostgreSQL source configuration Adjust these parameters according to your PostgreSQL
     * setup
     */
    private static PostgresSourceConfig createPostgresSourceConfig() {
        PostgresSourceConfigFactory pgFactory = new PostgresSourceConfigFactory();
        pgFactory.hostname("10.16.10.6");
        pgFactory.port(5438);
        pgFactory.database("test_db");
        pgFactory.schemaList(new String[] {"public"});
        pgFactory.tableList("public.t_user_info", "public.t_user");
        pgFactory.username("postgres");
        pgFactory.password("postgres");
        pgFactory.decodingPluginName("pgoutput");
        pgFactory.slotName("flink_slot1");
        pgFactory.splitSize(1);
        pgFactory.assignUnboundedChunkFirst(true);
        return pgFactory.create(0);
    }
}
