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

package org.apache.doris.cdcloader.mysql.loader;

import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.doris.cdcloader.mysql.config.LoaderOptions;
import org.apache.doris.cdcloader.mysql.constants.LoadConstants;
import org.apache.doris.cdcloader.mysql.exception.CdcLoaderException;
import org.apache.doris.cdcloader.mysql.service.JobService;
import org.apache.doris.cdcloader.mysql.state.HybridState;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlSnapshotSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.SnapshotPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupMode;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.getStartingOffsetOfBinlogSplit;

public class MySqlSplitAssigner implements SplitAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSplitAssigner.class);
    private MySqlSourceConfig sourceConfig;
    private MySqlSnapshotSplitAssigner assigner;
    private List<MySqlSnapshotSplit> remainingSplits = new ArrayList<>();
    private List<MySqlSnapshotSplit> finishedSplits = new ArrayList<>();
    private List<TableId> alreadyProcessedTables = new ArrayList<>();
    private Map<String, MySqlSnapshotSplit> assignedSplits;
    private Map<String, BinlogOffset> splitFinishedOffsets = new HashMap<>();
    private Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();
    private JobService jobService;
    private MySqlBinlogSplit binlogSplit;
    private boolean isBinlogSplitAssigned;
    private long maxBatchRows;
    private long maxBatchSize;
    private HybridState restoreState;
    private HybridState lastState = new HybridState();
    private ExecutorService executor;
    private BlockingQueue<MySqlSplit> splits;
    private AtomicBoolean started;
    private Map<String, MySqlSplitState> splitStates;

    public MySqlSplitAssigner(LoaderOptions options) {
        this.sourceConfig = generateMySqlConfig(options);
        this.maxBatchRows = Long.parseLong(options.getCdcConfig().getOrDefault(LoadConstants.MAX_BATCH_ROWS, "1000"));
        this.maxBatchSize = Long.parseLong(options.getCdcConfig().getOrDefault(LoadConstants.MAX_BATCH_SIZE, "10485760"));
    }

    private MySqlSourceConfig generateMySqlConfig(LoaderOptions options) {
        Map<String, String> cdcConfig = options.getCdcConfig();
        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
        configFactory.hostname(cdcConfig.get(LoadConstants.HOSTNAME));
        configFactory.port(Integer.valueOf(cdcConfig.get(LoadConstants.PORT)));
        configFactory.username(cdcConfig.get(LoadConstants.USERNAME));
        configFactory.password(cdcConfig.get(LoadConstants.PASSWORD));
        configFactory.databaseList(cdcConfig.get(LoadConstants.DATABASE_NAME));
        configFactory.tableList(cdcConfig.get(LoadConstants.TABLE_NAME));
        return configFactory.createConfig(0);
    }

    @Override
    public void prepare() {
        this.remainingSplits = new ArrayList<>();
        this.assignedSplits = new HashMap<>();
        this.splitStates = new HashMap<>();
        this.splits = LoadContext.getInstance().getSplits();
        this.jobService = new JobService();
        this.started = new AtomicBoolean(false);
    }

    @Override
    public void start() {
        if(!started.get()){
            startSplit();
            if(executor == null){
                ThreadFactory threadFactory =
                    new ThreadFactoryBuilder().setNameFormat("split-chunks").build();
                executor = Executors.newSingleThreadExecutor(threadFactory);
                executor.submit(this::sendSplitAsync);
                LOG.info("split chunk finished");
            }
            started.set(true);
        }
    }

    private void sendSplitAsync() {
        try{
            if(!isBinlogSplitAssigned){
                for(MySqlSnapshotSplit split : remainingSplits){
                    splitStates.put(split.splitId(), new MySqlSnapshotSplitState(split));
                    splits.put(split);
                    assignedSplits.put(split.splitId(), split);
                }
                //all splits are processed
                while (started.get()){
                    if(assigner.noMoreSplits()
                        && finishedSplits.size() == splitFinishedOffsets.size()
                        && finishedSplits.size() == assignedSplits.size()){
                        LOG.info("binlog split start");
                        binlogSplit = generateBinlogSplit();
                        isBinlogSplitAssigned = true;
                        splitStates.put(binlogSplit.splitId(), new MySqlBinlogSplitState(binlogSplit));
                        splits.put(binlogSplit);
                        break;
                    }
                    Thread.sleep(100);
                }
            }else{
                splitStates.put(binlogSplit.splitId(), new MySqlBinlogSplitState(binlogSplit));
                splits.put(binlogSplit);
            }
        }catch (Exception ex){
            LOG.error("start split error", ex);
            System.exit(1);
        }
    }

    @Override
    public Map<String, MySqlSplitState> getSplitStates() {
        return splitStates;
    }

    private void startSplit() {
        this.restoreState = jobService.getLastConsumedState();
        LOG.info("restore state: {}", restoreState);
        if(restoreState == null){
            StartupMode startupMode = sourceConfig.getStartupOptions().startupMode;
            if(startupMode.equals(StartupMode.INITIAL) || startupMode.equals(StartupMode.SNAPSHOT)){
                this.assigner =
                    new MySqlSnapshotSplitAssigner(sourceConfig, 1, new ArrayList<>(), false);
                startSplitChunks();
                return;
            }else{
                isBinlogSplitAssigned = true;
                binlogSplit = generateBinlogSplit();
            }
        }else{
            lastState = restoreState;
        }

        if(restoreState!= null && restoreState.getBinlogSplitState() == null){
            //restore from state
            this.assigner =
                new MySqlSnapshotSplitAssigner(sourceConfig, 1, restoreState.getSplitsState());
            SnapshotPendingSplitsState snapshotState = restoreState.getSplitsState();
            this.tableSchemas = snapshotState.getTableSchemas();
            this.remainingSplits = snapshotState.getRemainingSplits().stream().map(m->m.toMySqlSnapshotSplit(tableSchemas.get(m.getTableId()))).collect(Collectors.toList());
            this.assignedSplits = snapshotState.getAssignedSplits().values().stream().map(m->m.toMySqlSnapshotSplit(tableSchemas.get(m.getTableId()))).collect(Collectors.toMap(MySqlSplit::splitId, m->m));
            this.splitFinishedOffsets = snapshotState.getSplitFinishedOffsets();

        }else {
            isBinlogSplitAssigned = true;
            binlogSplit = generateBinlogSplit();
        }
    }

    private void startSplitChunks(){
        assigner.open();
        while (true) {
            Optional<MySqlSplit> mySqlSplit = assigner.getNext();
            if (mySqlSplit.isPresent()) {
                MySqlSnapshotSplit snapshotSplit = mySqlSplit.get().asSnapshotSplit();
                remainingSplits.add(snapshotSplit);
            } else {
                break;
            }
        }
    }

    private MySqlBinlogSplit generateBinlogSplit(){
        MySqlBinlogSplit binlogSplit;
        Map<TableId, TableChanges.TableChange> tableSchemaMap = discoverTableSchemas();
        if(isBinlogSplitAssigned){
            if(restoreState == null || restoreState.getBinlogSplitState() == null){
                if(sourceConfig.getStartupOptions().binlogOffset == null){
                    throw new CdcLoaderException("start mode offset is null");
                }
                binlogSplit = new MySqlBinlogSplit(
                    BINLOG_SPLIT_ID,
                    sourceConfig.getStartupOptions().binlogOffset,
                    BinlogOffset.ofNonStopping(),
                    new ArrayList<>(),
                    tableSchemaMap,
                    0);
            }else{
                MySqlBinlogSplitState binlogSplitState = restoreState.getBinlogSplitState();

                // Continue consuming from the offset in the state
                binlogSplit = binlogSplitState.toMySqlSplit();
                binlogSplit = MySqlBinlogSplit.fillTableSchemas(binlogSplit, tableSchemaMap);
            }
        }else {
            // The full amount has been consumed, but the increment has not yet been consumed.
            // The increment is generated from the split of the full amount.
            SnapshotPendingSplitsState splitsState = lastState.getSplitsState();
            List<MySqlSchemalessSnapshotSplit> assignedSnapshotSplit = splitsState.getAssignedSplits().values().stream()
                .map(m -> m.toSchemalessSnapshotSplit())
                .sorted(Comparator.comparing(MySqlSplit::splitId))
                .collect(Collectors.toList());
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
            for (MySqlSchemalessSnapshotSplit split : assignedSnapshotSplit) {
                BinlogOffset binlogOffset = splitsState.getSplitFinishedOffsets().get(split.splitId());
                finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                        split.getTableId(),
                        split.splitId(),
                        split.getSplitStart(),
                        split.getSplitEnd(),
                        binlogOffset));
            }
            BinlogOffset startingOffset = getStartingOffsetOfBinlogSplit(finishedSnapshotSplitInfos);
            binlogSplit =
                new MySqlBinlogSplit(
                    BINLOG_SPLIT_ID,
                    startingOffset,
                    BinlogOffset.ofNonStopping(),
                    finishedSnapshotSplitInfos,
                    tableSchemaMap,
                    finishedSnapshotSplitInfos.size());
        }
        return binlogSplit;
    }

    private Map<TableId, TableChanges.TableChange> discoverTableSchemas(){
        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            MySqlPartition partition =
                new MySqlPartition(sourceConfig.getMySqlConnectorConfig().getLogicalName());
            return TableDiscoveryUtils.discoverSchemaForCapturedTables(
                partition, sourceConfig, jdbc);
        }catch (SQLException ex){
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void snapshotSplitFinished(MySqlSplit sqlSplit, BinlogOffset highWatermark){
        finishedSplits.add(sqlSplit.asSnapshotSplit());
        splitFinishedOffsets.put(sqlSplit.splitId(), highWatermark);
        SnapshotPendingSplitsState snapshotPendingSplitsState = new SnapshotPendingSplitsState(
            alreadyProcessedTables,
            remainingSplits.stream().map(MySqlSnapshotSplit::toSchemalessSnapshotSplit).collect(Collectors.toList()),
            assignedSplits.values().stream().map(MySqlSnapshotSplit::toSchemalessSnapshotSplit).collect(Collectors.toMap(MySqlSplit::splitId, m -> m)),
            assigner.getTableSchemas(),
            splitFinishedOffsets,
            assigner.getAssignerStatus(),
            new ArrayList<>(), // todo: need from recovery from state
            false,
            false,
            null);
        //todo: save state by fe callback
        lastState.setSplitsState(snapshotPendingSplitsState);
        System.out.println("=====");
        System.out.println(assigner.noMoreSplits());
        System.out.println(finishedSplits.size());
        System.out.println(splitFinishedOffsets.size());
        System.out.println(assignedSplits.size());
        //todo: remove remaining split when call back success
    }

    @Override
    public void close() {
        if(started.get()){
            if(executor != null){
                executor.shutdown();
            }
            started.set(false);
        }
    }
}
