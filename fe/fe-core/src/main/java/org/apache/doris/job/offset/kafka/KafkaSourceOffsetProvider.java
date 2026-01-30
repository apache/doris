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

package org.apache.doris.job.offset.kafka;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.kafka.KafkaUtil;
import org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalCatalog;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Offset provider for Kafka streaming jobs.
 * 
 * This provider manages offset tracking for each Kafka partition and supports
 * exactly-once semantics by generating partition-specific tasks.
 */
@Log4j2
@Getter
@Setter
public class KafkaSourceOffsetProvider implements SourceOffsetProvider {
    
    // Kafka TVF parameter names
    public static final String PARAM_CATALOG = "catalog";
    public static final String PARAM_DATABASE = "database";
    public static final String PARAM_TABLE = "table";
    public static final String PARAM_DEFAULT_OFFSETS = "kafka_default_offsets";
    public static final String PARAM_MAX_BATCH_ROWS = "max_batch_rows";
    
    // Offset constants
    public static final String OFFSET_BEGINNING = "OFFSET_BEGINNING";
    public static final String OFFSET_END = "OFFSET_END";
    public static final long OFFSET_BEGINNING_VAL = -2L;
    public static final long OFFSET_END_VAL = -1L;
    
    // Kafka hidden column names
    private static final String PARTITION_COLUMN = "_partition_id";
    private static final String OFFSET_COLUMN = "_partition_offset";
    
    // Current offset state for all partitions
    private KafkaOffset currentOffset;
    
    // Latest available offsets for each partition (from Kafka)
    private Map<Integer, Long> latestOffsets = new HashMap<>();
    
    // Kafka connection parameters
    private String brokerList;
    private String topic;
    private Map<String, String> kafkaClientProps = new HashMap<>();
    
    // Catalog information
    private String catalogName;
    private String databaseName;
    private String tableName;
    
    // Job configuration
    private long maxBatchRows = 100000L;
    private String defaultOffsetsConfig = OFFSET_END;
    
    // Job ID for tracking
    private long jobId;
    
    @Override
    public String getSourceType() {
        return "kafka";
    }
    
    /**
     * Initialize the provider from TVF properties.
     * This should be called when the job is first created.
     */
    public void initFromTvfProperties(Map<String, String> tvfProps) throws UserException {
        this.catalogName = getRequiredProperty(tvfProps, PARAM_CATALOG);
        this.databaseName = tvfProps.getOrDefault(PARAM_DATABASE, "default");
        this.tableName = getRequiredProperty(tvfProps, PARAM_TABLE);
        this.defaultOffsetsConfig = tvfProps.getOrDefault(PARAM_DEFAULT_OFFSETS, OFFSET_END);
        
        String maxBatchRowsStr = tvfProps.get(PARAM_MAX_BATCH_ROWS);
        if (maxBatchRowsStr != null) {
            this.maxBatchRows = Long.parseLong(maxBatchRowsStr);
        }
        
        // Get the Trino Kafka catalog and extract connection parameters
        CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new UserException("Catalog not found: " + catalogName);
        }
        if (!(catalog instanceof TrinoConnectorExternalCatalog)) {
            throw new UserException("Catalog must be a Trino Connector catalog: " + catalogName);
        }
        
        TrinoConnectorExternalCatalog trinoCatalog = (TrinoConnectorExternalCatalog) catalog;
        if (!"kafka".equalsIgnoreCase(trinoCatalog.getConnectorName().toString())) {
            throw new UserException("Catalog must be a Kafka connector: " + catalogName);
        }
        
        // Extract Kafka connection parameters from catalog properties
        Map<String, String> catalogProps = trinoCatalog.getCatalogProperty().getProperties();
        this.brokerList = KafkaPropertiesConverter.extractBrokerList(catalogProps);
        this.topic = tableName;  // In Trino Kafka, table name is the topic name
        this.kafkaClientProps = KafkaPropertiesConverter.convertToKafkaClientProperties(catalogProps);
        
        // Initialize current offset
        this.currentOffset = new KafkaOffset(topic, catalogName, databaseName);
        
        log.info("Initialized KafkaSourceOffsetProvider: catalog={}, topic={}, brokers={}", 
                catalogName, topic, brokerList);
    }
    
    /**
     * Get the next offset for a single partition (standard interface).
     * For Kafka, this returns the KafkaOffset which contains all partition offsets.
     */
    @Override
    public Offset getNextOffset(StreamingJobProperties jobProps, Map<String, String> properties) {
        // Return the current offset state
        // The actual partition-level offsets are managed through getNextPartitionOffsets()
        return currentOffset;
    }
    
    /**
     * Get the next batch of partition offsets for creating parallel tasks.
     * Each KafkaPartitionOffset represents a single partition's work unit.
     */
    public List<KafkaPartitionOffset> getNextPartitionOffsets(StreamingJobProperties jobProps) {
        List<KafkaPartitionOffset> offsets = new ArrayList<>();
        
        if (currentOffset == null || currentOffset.getPartitionOffsets() == null) {
            return offsets;
        }
        
        for (Map.Entry<Integer, Long> entry : currentOffset.getPartitionOffsets().entrySet()) {
            int partitionId = entry.getKey();
            KafkaPartitionOffset partitionOffset = getNextPartitionOffset(partitionId, jobProps);
            if (partitionOffset != null) {
                offsets.add(partitionOffset);
            }
        }
        
        return offsets;
    }
    
    /**
     * Get the next offset range for a single partition.
     * Returns null if there is no more data to consume for this partition.
     * 
     * @param partitionId the Kafka partition ID
     * @param jobProps job properties (not used currently but kept for API consistency)
     * @return the next offset range, or null if no data available
     */
    public KafkaPartitionOffset getNextPartitionOffset(int partitionId, StreamingJobProperties jobProps) {
        if (currentOffset == null || currentOffset.getPartitionOffsets() == null) {
            return null;
        }
        
        long currentPos = currentOffset.getPartitionOffset(partitionId);
        long latestPos = latestOffsets.getOrDefault(partitionId, currentPos);
        
        // No more data if current >= latest
        if (currentPos >= latestPos) {
            log.debug("Partition {} has no more data: current={}, latest={}", 
                    partitionId, currentPos, latestPos);
            return null;
        }
        
        // Calculate the end offset for this batch
        long endOffset = Math.min(currentPos + maxBatchRows, latestPos);
        
        log.debug("Partition {} offset range: [{}, {}), latest: {}", 
                partitionId, currentPos, endOffset, latestPos);
        
        return new KafkaPartitionOffset(partitionId, currentPos, endOffset);
    }
    
    /**
     * Check if a specific partition has more data to consume.
     * 
     * @param partitionId the Kafka partition ID
     * @return true if the partition has unconsumed data
     */
    public boolean hasMoreDataForPartition(int partitionId) {
        if (currentOffset == null || currentOffset.getPartitionOffsets() == null) {
            return false;
        }
        
        long currentPos = currentOffset.getPartitionOffset(partitionId);
        long latestPos = latestOffsets.getOrDefault(partitionId, currentPos);
        
        return currentPos < latestPos;
    }
    
    /**
     * Get all partition IDs that have been initialized.
     * 
     * @return set of partition IDs
     */
    public java.util.Set<Integer> getAllPartitionIds() {
        if (currentOffset == null || currentOffset.getPartitionOffsets() == null) {
            return java.util.Collections.emptySet();
        }
        return currentOffset.getPartitionOffsets().keySet();
    }
    
    @Override
    public String getShowCurrentOffset() {
        if (currentOffset != null) {
            return currentOffset.showRange();
        }
        return "{}";
    }
    
    @Override
    public String getShowMaxOffset() {
        if (latestOffsets != null && !latestOffsets.isEmpty()) {
            String offsetsStr = latestOffsets.entrySet().stream()
                    .map(e -> String.format("p%d=%d", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(", "));
            return "{" + offsetsStr + "}";
        }
        return "{}";
    }
    
    /**
     * Rewrite the TVF-based INSERT command to a direct table query with offset filtering.
     * 
     * Original: INSERT INTO target SELECT * FROM kafka(...)
     * Rewritten: INSERT INTO target SELECT * FROM catalog.db.table 
     *            WHERE _partition = X AND _offset >= Y AND _offset < Z
     */
    @Override
    public InsertIntoTableCommand rewriteTvfParams(InsertIntoTableCommand originCommand, Offset runningOffset) {
        KafkaPartitionOffset partitionOffset = (KafkaPartitionOffset) runningOffset;
        
        // Rewrite the plan tree
        Plan rewritePlan = originCommand.getParsedPlan().get().rewriteUp(plan -> {
            if (plan instanceof LogicalProject) {
                LogicalProject<?> project = (LogicalProject<?>) plan;
                Plan child = project.child();
                
                // Check if the child is a TVF relation (possibly wrapped in a filter)
                if (child instanceof UnboundTVFRelation) {
                    UnboundTVFRelation tvf = (UnboundTVFRelation) child;
                    if ("kafka".equalsIgnoreCase(tvf.getFunctionName())) {
                        // Create table reference and filter
                        Plan tableWithFilter = createKafkaTableWithFilter(tvf, partitionOffset);
                        return project.withChildren(ImmutableList.of(tableWithFilter));
                    }
                }
            } else if (plan instanceof UnboundTVFRelation) {
                UnboundTVFRelation tvf = (UnboundTVFRelation) plan;
                if ("kafka".equalsIgnoreCase(tvf.getFunctionName())) {
                    return createKafkaTableWithFilter(tvf, partitionOffset);
                }
            }
            return plan;
        });
        
        InsertIntoTableCommand newCommand = new InsertIntoTableCommand(
                (LogicalPlan) rewritePlan,
                Optional.empty(), Optional.empty(), Optional.empty(), true, Optional.empty());
        newCommand.setJobId(originCommand.getJobId());
        return newCommand;
    }
    
    /**
     * Create a Kafka table reference with partition and offset filter.
     */
    private Plan createKafkaTableWithFilter(UnboundTVFRelation tvf, KafkaPartitionOffset partitionOffset) {
        // Create UnboundRelation for the Trino Kafka table: catalog.database.table
        List<String> tableParts = ImmutableList.of(catalogName, databaseName, tableName);
        UnboundRelation tableRelation = new UnboundRelation(
                tvf.getRelationId(),
                tableParts
        );
        
        // Create filter expressions:
        // _partition = partitionId AND _offset >= startOffset AND _offset < endOffset
        Expression partitionFilter = new EqualTo(
                new UnboundSlot(PARTITION_COLUMN),
                new IntegerLiteral(partitionOffset.getPartitionId())
        );
        
        Expression offsetGte = new GreaterThanEqual(
                new UnboundSlot(OFFSET_COLUMN),
                new BigIntLiteral(partitionOffset.getStartOffset())
        );
        
        Expression offsetLt = new LessThan(
                new UnboundSlot(OFFSET_COLUMN),
                new BigIntLiteral(partitionOffset.getEndOffset())
        );
        
        // Combine all conditions with AND
        Expression filterExpr = new And(partitionFilter, new And(offsetGte, offsetLt));

        log.info("create offset filter for partition {}: [{}, {})",
                partitionOffset.getPartitionId(), partitionOffset.getStartOffset(), partitionOffset.getEndOffset());
        // Create LogicalFilter with the table relation as child
        return new LogicalFilter<>(ImmutableSet.of(filterExpr), tableRelation);
    }
    
    /**
     * Update the offset for a specific partition after task completion.
     */
    public void updatePartitionOffset(int partitionId, long newOffset) {
        if (currentOffset != null) {
            currentOffset.updatePartitionOffset(partitionId, newOffset);
            log.info("Updated partition {} offset to {}", partitionId, newOffset);
        }
    }
    
    @Override
    public void updateOffset(Offset offset) {
        if (offset instanceof KafkaOffset) {
            this.currentOffset = (KafkaOffset) offset;
        } else if (offset instanceof KafkaPartitionOffset) {
            // Update single partition offset
            KafkaPartitionOffset partitionOffset = (KafkaPartitionOffset) offset;
            updatePartitionOffset(partitionOffset.getPartitionId(), 
                    partitionOffset.getStartOffset() + partitionOffset.getConsumedRows());
        }
    }
    
    /**
     * Fetch latest offsets from Kafka for all partitions.
     */
    @Override
    public void fetchRemoteMeta(Map<String, String> properties) throws Exception {
        try {
            // Get all partitions for the topic
            List<Integer> partitionIds = KafkaUtil.getAllKafkaPartitions(
                    brokerList, topic, kafkaClientProps);
            
            log.info("Fetched {} partitions for topic {}", partitionIds.size(), topic);
            
            // Initialize partition offsets if this is the first run
            if (currentOffset.isEmpty()) {
                initializePartitionOffsets(partitionIds);
            }
            
            // Get latest offsets for all partitions
            List<Pair<Integer, Long>> offsets = KafkaUtil.getLatestOffsets(
                    jobId, UUID.randomUUID(), brokerList, topic, kafkaClientProps, partitionIds);
            
            // Update latest offsets map
            latestOffsets.clear();
            for (Pair<Integer, Long> offset : offsets) {
                latestOffsets.put(offset.first, offset.second);
            }
            
            log.info("Fetched latest offsets: {}", latestOffsets);
            
        } catch (Exception e) {
            log.warn("Failed to fetch Kafka metadata for topic {}", topic, e);
            throw e;
        }
    }
    
    /**
     * Initialize offsets for all partitions based on the default offset configuration.
     */
    private void initializePartitionOffsets(List<Integer> partitionIds) throws Exception {
        long initialOffset;
        
        if (OFFSET_BEGINNING.equalsIgnoreCase(defaultOffsetsConfig)) {
            // Start from the beginning (offset 0 for each partition)
            // We need to fetch the actual beginning offsets
            List<Pair<Integer, Long>> beginningOffsets = getBeginningOffsets(partitionIds);
            for (Pair<Integer, Long> offset : beginningOffsets) {
                currentOffset.updatePartitionOffset(offset.first, offset.second);
            }
        } else if (OFFSET_END.equalsIgnoreCase(defaultOffsetsConfig)) {
            // Start from the end (latest offset for each partition)
            List<Pair<Integer, Long>> endOffsets = KafkaUtil.getLatestOffsets(
                    jobId, UUID.randomUUID(), brokerList, topic, kafkaClientProps, partitionIds);
            for (Pair<Integer, Long> offset : endOffsets) {
                currentOffset.updatePartitionOffset(offset.first, offset.second);
            }
        } else {
            // Try to parse as a specific offset value
            try {
                initialOffset = Long.parseLong(defaultOffsetsConfig);
                for (Integer partitionId : partitionIds) {
                    currentOffset.updatePartitionOffset(partitionId, initialOffset);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid kafka_default_offsets value: " + defaultOffsetsConfig
                        + ". Expected: OFFSET_BEGINNING, OFFSET_END, or a numeric value");
            }
        }
        
        log.info("Initialized partition offsets: {}", currentOffset.getPartitionOffsets());
    }
    
    /**
     * Get beginning offsets (earliest available) for partitions.
     */
    private List<Pair<Integer, Long>> getBeginningOffsets(List<Integer> partitionIds) throws Exception {
        // Use timestamp -2 to get earliest offsets
        List<Pair<Integer, Long>> timestampOffsets = partitionIds.stream()
                .map(p -> Pair.of(p, OFFSET_BEGINNING_VAL))
                .collect(Collectors.toList());
        
        return KafkaUtil.getRealOffsets(brokerList, topic, kafkaClientProps, timestampOffsets);
    }
    
    @Override
    public boolean hasMoreDataToConsume() {
        if (currentOffset == null || currentOffset.isEmpty()) {
            return true;  // Need to initialize
        }
        
        for (Map.Entry<Integer, Long> entry : currentOffset.getPartitionOffsets().entrySet()) {
            int partitionId = entry.getKey();
            long currentPos = entry.getValue();
            long latestPos = latestOffsets.getOrDefault(partitionId, currentPos);
            
            if (currentPos < latestPos) {
                return true;
            }
        }
        
        return false;
    }
    
    @Override
    public Offset deserializeOffset(String offset) {
        return GsonUtils.GSON.fromJson(offset, KafkaOffset.class);
    }
    
    @Override
    public Offset deserializeOffsetProperty(String offset) {
        if (StringUtils.isBlank(offset)) {
            return null;
        }
        
        try {
            // Try to parse as KafkaOffset JSON
            return GsonUtils.GSON.fromJson(offset, KafkaOffset.class);
        } catch (Exception e) {
            log.warn("Failed to deserialize Kafka offset: {}", offset, e);
            return null;
        }
    }
    
    @Override
    public String getPersistInfo() {
        if (currentOffset != null) {
            return currentOffset.toSerializedJson();
        }
        return null;
    }
    
    /**
     * Get a required property value, throwing an exception if not found.
     */
    private String getRequiredProperty(Map<String, String> props, String key) throws UserException {
        String value = props.get(key);
        if (value == null || value.isEmpty()) {
            throw new UserException("Missing required property: " + key);
        }
        return value;
    }
}
