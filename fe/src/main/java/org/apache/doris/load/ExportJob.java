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

package org.apache.doris.load;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.ExportStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.ExportSink;
import org.apache.doris.planner.MysqlScanNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PartitionColumnFilter;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentClient;
import org.apache.doris.thrift.TAgentResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

// NOTE: we must be carefully if we send next request
//       as soon as receiving one instance's report from one BE,
//       because we may change job's member concurrently.
public class ExportJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(ExportJob.class);

    public enum JobState {
        PENDING,
        EXPORTING,
        FINISHED,
        CANCELLED,
    }

    private long id;
    private long dbId;
    private String clusterName;
    private long tableId;
    private BrokerDesc brokerDesc;
    private String exportPath;
    private String columnSeparator;
    private String lineDelimiter;
    private List<String> partitions;

    private TableName tableName;

    private String sql = "";

    private JobState state;
    private long createTimeMs;
    private long startTimeMs;
    private long finishTimeMs;
    // progress has two functions at EXPORTING stage:
    // 1. when progress < 100, it indicates exporting
    // 2. set progress = 100 ONLY when exporting progress is compeletly done
    private int progress;
    private ExportFailMsg failMsg;
    private Set<String> exportedFiles = Sets.newConcurrentHashSet();

    // descriptor used to register all column and table need
    private final DescriptorTable desc;
    private TupleDescriptor exportTupleDesc;

    private ExportSink exportSink;

    private Analyzer analyzer;
    private Table exportTable;

    // be_address => (tablet_id => Coordinator)
    // private Map<TNetworkAddress, Pair<Long, Coordinator>> coordMap = Maps.newHashMap();

    private List<Coordinator> coordList = Lists.newArrayList();

    private AtomicInteger nextId = new AtomicInteger(0);

    private boolean isReplayed = false;

    private Thread doExportingThread;

    private List<TScanRangeLocations> tabletLocations = Lists.newArrayList();
    // backedn_address => snapshot path
    private List<Pair<TNetworkAddress, String>> snapshotPaths = Lists.newArrayList();

    public ExportJob() {
        this.id = -1;
        this.dbId = -1;
        this.tableId = -1;
        this.state = JobState.PENDING;
        this.progress = 0;
        this.createTimeMs = System.currentTimeMillis();
        this.startTimeMs = -1;
        this.finishTimeMs = -1;
        this.failMsg = new ExportFailMsg(ExportFailMsg.CancelType.UNKNOWN, "");
        this.analyzer = new Analyzer(Catalog.getInstance(), null);
        this.desc = new DescriptorTable();
        this.exportPath = "";
        this.columnSeparator = "\t";
        this.lineDelimiter = "\n";
    }

    public ExportJob(long jobId) {
        this();
        this.id = jobId;
    }

    public void setJob(ExportStmt stmt) throws Exception {
        String dbName = stmt.getTblName().getDb();
        Database db = Catalog.getInstance().getDb(dbName);
        Preconditions.checkNotNull(db);
        this.dbId = db.getId();

        Table table = db.getTable(stmt.getTblName().getTbl());
        this.tableId = table.getId();
        this.tableName = stmt.getTblName();

        Preconditions.checkNotNull(stmt.getBrokerDesc());
        this.brokerDesc = stmt.getBrokerDesc();

        this.columnSeparator = stmt.getColumnSeparator();
        this.lineDelimiter = stmt.getLineDelimiter();

        String path = stmt.getPath();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        this.exportPath = path;

        this.partitions = stmt.getPartitions();

        genExecFragment();

        this.sql = stmt.toSql();
    }

    public void genExecFragment() throws Exception {
        Database db = Catalog.getInstance().getDb(dbId);
        db.readLock();
        try {
            exportTable = db.getTable(tableId);

            registerToDesc();
            exportSink = new ExportSink(
                    getExportPath() + "/tmp/", getColumnSeparator(),
                    getLineDelimiter(), brokerDesc);
            plan();
        } finally {
            db.readUnlock();
        }
    }

    private ScanNode genScanNode() throws UserException {
        ScanNode scanNode = null;
        switch (exportTable.getType()) {
            case OLAP:
                scanNode = new OlapScanNode(new PlanNodeId(0), exportTupleDesc, "OlapScanNodeForExport");
                Map<String, PartitionColumnFilter> columnFilters = Maps.newHashMap();
                ((OlapScanNode) scanNode).setColumnFilters(columnFilters);
                ((OlapScanNode) scanNode).setIsPreAggregation(false, "Export");
                ((OlapScanNode) scanNode).setCanTurnOnPreAggr(false);
                break;
            case MYSQL:
                scanNode = new MysqlScanNode(new PlanNodeId(0), exportTupleDesc, (MysqlTable) this.exportTable);
                break;
            default:
                break;
        }
        if (scanNode != null) {
            scanNode.finalize(analyzer);
        }

        return scanNode;
    }

    private OlapScanNode genOlapScanNodeByLocation(List<TScanRangeLocations> locations) {
        OlapScanNode olapScanNode = OlapScanNode.createOlapScanNodeByLocation(
                new PlanNodeId(nextId.getAndIncrement()),
                exportTupleDesc,
                "OlapScanNodeForExport",
                locations);

        return olapScanNode;
    }

    private PlanFragment genPlanFragment(Table.TableType type, ScanNode scanNode) throws UserException {
        PlanFragment fragment = null;
        switch (exportTable.getType()) {
            case OLAP:
                fragment = new PlanFragment(
                        new PlanFragmentId(nextId.getAndIncrement()), scanNode, DataPartition.RANDOM);
                break;
            case MYSQL:
                fragment = new PlanFragment(
                        new PlanFragmentId(nextId.getAndIncrement()), scanNode, DataPartition.UNPARTITIONED);
                break;
            default:
                break;
        }
        fragment.setOutputExprs(createOutputExprs());

        scanNode.setFragmentId(fragment.getFragmentId());
        fragment.setSink(exportSink);
        try {
            fragment.finalize(analyzer, false);
        } catch (Exception e) {
            LOG.info("Fragment finalize failed. e= {}", e);
            throw new UserException("Fragment finalize failed");
        }

        return fragment;
    }

    private List<Expr> createOutputExprs() {
        List<Expr> outputExprs = Lists.newArrayList();
        for (int i = 0; i < exportTupleDesc.getSlots().size(); ++i) {
            SlotDescriptor slotDesc = exportTupleDesc.getSlots().get(i);
            SlotRef slotRef = new SlotRef(slotDesc);
            if (slotDesc.getType().getPrimitiveType() == PrimitiveType.CHAR) {
                slotRef.setType(Type.CHAR);
            }
            outputExprs.add(slotRef);
        }

        return outputExprs;
    }

    private List<Coordinator> genCoordinators(List<PlanFragment> fragments, List<ScanNode> nodes) {
        List<Coordinator> coords = Lists.newArrayList();

        UUID uuid = UUID.randomUUID();
        for (int i = 0; i < fragments.size(); ++i) {
            PlanFragment fragment = fragments.get(i);
            ScanNode scanNode = nodes.get(i);
            TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits() + i, uuid.getLeastSignificantBits());
            Coordinator coord = new Coordinator(
                    id, queryId, desc, Lists.newArrayList(fragment), Lists.newArrayList(scanNode), clusterName);
            coords.add(coord);
            this.coordList.add(coord);
        }

        return coords;
    }

    private void plan() throws Exception {
        List<PlanFragment> fragments = Lists.newArrayList();
        List<ScanNode> scanNodes = Lists.newArrayList();

        // TODO(lingbin): should we construct TabletInfos(include version, id...) manually?
        ScanNode scanNode = genScanNode();
        tabletLocations = scanNode.getScanRangeLocations(0);
        if (tabletLocations == null) {
            // not olap scan node
            PlanFragment fragment = genPlanFragment(exportTable.getType(), scanNode);
            scanNodes.add(scanNode);
            fragments.add(fragment);
        } else {
            for (TScanRangeLocations tablet : tabletLocations) {
                List<TScanRangeLocation> locations = tablet.getLocations();
                Collections.shuffle(locations);
                tablet.setLocations(locations.subList(0, 1));
            }

            int size = tabletLocations.size();
            int tabletNum = Config.export_parallel_tablet_num;
            for (int i = 0; i < size; i += tabletNum) {
                OlapScanNode olapScanNode = null;
                if (i + tabletNum <= size) {
                    olapScanNode = genOlapScanNodeByLocation(tabletLocations.subList(i, i + tabletNum));
                } else {
                    olapScanNode = genOlapScanNodeByLocation(tabletLocations.subList(i, size));
                }
                PlanFragment fragment = genPlanFragment(exportTable.getType(), olapScanNode);

                fragments.add(fragment);
                scanNodes.add(olapScanNode);
            }
        }

        genCoordinators(fragments, scanNodes);
    }

    private void registerToDesc() {
        TableRef ref = new TableRef(tableName, null, partitions);
        BaseTableRef tableRef = new BaseTableRef(ref, exportTable, tableName);
        exportTupleDesc = desc.createTupleDescriptor();
        exportTupleDesc.setTable(exportTable);
        exportTupleDesc.setRef(tableRef);
        for (Column col : exportTable.getBaseSchema()) {
            SlotDescriptor slot = desc.addSlotDescriptor(exportTupleDesc);
            slot.setIsMaterialized(true);
            slot.setColumn(col);
            if (col.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }
        desc.computeMemLayout();
    }

    public long getId() {
        return id;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return this.tableId;
    }

    public JobState getState() {
        return state;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public void setBrokerDesc(BrokerDesc brokerDesc) {
        this.brokerDesc = brokerDesc;
    }

    public String getExportPath() {
        return exportPath;
    }

    public String getColumnSeparator() {
        return this.columnSeparator;
    }

    public String getLineDelimiter() {
        return this.lineDelimiter;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public void setFailMsg(ExportFailMsg failMsg) {
        this.failMsg = failMsg;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public long getFinishTimeMs() {
        return finishTimeMs;
    }

    public ExportFailMsg getFailMsg() {
        return failMsg;
    }

    public Set<String> getExportedFiles() {
        return this.exportedFiles;
    }

    public synchronized void addExportedFiles(List<String> files) {
        exportedFiles.addAll(files);
        LOG.debug("exported files: {}", this.exportedFiles);
    }

    public synchronized Thread getDoExportingThread() {
        return doExportingThread;
    }

    public synchronized void setDoExportingThread(Thread isExportingThread) {
        this.doExportingThread = isExportingThread;
    }

    public List<Coordinator> getCoordList() {
        return coordList;
    }

    public List<TScanRangeLocations> getTabletLocations() {
        return tabletLocations;
    }

    public List<Pair<TNetworkAddress, String>> getSnapshotPaths() {
        return this.snapshotPaths;
    }

    public void addSnapshotPath(Pair<TNetworkAddress, String> snapshotPath) {
        this.snapshotPaths.add(snapshotPath);
    }

    public String getSql() {
        return sql;
    }

    public TableName getTableName() {
        return tableName;
    }

    public synchronized void cancel(ExportFailMsg.CancelType type, String msg) {
        releaseSnapshotPaths();
        failMsg = new ExportFailMsg(type, msg);
        updateState(ExportJob.JobState.CANCELLED, false);
    }

    public synchronized boolean updateState(ExportJob.JobState newState) {
        return this.updateState(newState, false);
    }

    public synchronized boolean updateState(ExportJob.JobState newState, boolean isReplay) {
        state = newState;
        switch (newState) {
            case PENDING:
                progress = 0;
                break;
            case EXPORTING:
                startTimeMs = System.currentTimeMillis();
                break;
            case FINISHED:
            case CANCELLED:
                finishTimeMs = System.currentTimeMillis();
                progress = 100;
                break;
            default:
                Preconditions.checkState(false, "wrong job state: " + newState.name());
                break;
        }
        if (!isReplay) {
            Catalog.getInstance().getEditLog().logExportUpdateState(id, newState);
        }
        return true;
    }

    public Status releaseSnapshotPaths() {
        List<Pair<TNetworkAddress, String>> snapshotPaths = getSnapshotPaths();
        LOG.debug("snapshotPaths:{}", snapshotPaths);
        for (Pair<TNetworkAddress, String> snapshotPath : snapshotPaths) {
            TNetworkAddress address = snapshotPath.first;
            String host = address.getHostname();
            int port = address.getPort();
            Backend backend = Catalog.getCurrentSystemInfo().getBackendWithBePort(host, port);
            if (backend == null) {
                continue;
            }
            long backendId = backend.getId();
            if (!Catalog.getCurrentSystemInfo().checkBackendAvailable(backendId)) {
                continue;
            }

            AgentClient client = new AgentClient(host, port);
            TAgentResult result = client.releaseSnapshot(snapshotPath.second);
            if (result == null || result.getStatus().getStatus_code() != TStatusCode.OK) {
                continue;
            }
        }
        snapshotPaths.clear();
        return Status.OK;
    }

    @Override
    public String toString() {
        return "ExportJob [jobId=" + id
                + ", dbId=" + dbId
                + ", tableId=" + tableId
                + ", state=" + state
                + ", path=" + exportPath
                + ", partitions=(" + StringUtils.join(partitions, ",") + ")"
                + ", progress=" + progress
                + ", createTimeMs=" + TimeUtils.longToTimeString(createTimeMs)
                + ", exportStartTimeMs=" + TimeUtils.longToTimeString(startTimeMs)
                + ", exportFinishTimeMs=" + TimeUtils.longToTimeString(finishTimeMs)
                + ", failMsg=" + failMsg
                + ", files=(" + StringUtils.join(exportedFiles, ",") + ")"
                + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // base infos
        out.writeLong(id);
        out.writeLong(dbId);
        out.writeLong(tableId);
        Text.writeString(out, exportPath);
        Text.writeString(out, columnSeparator);
        Text.writeString(out, lineDelimiter);

        // partitions
        boolean hasPartition = (partitions != null);
        if (hasPartition) {
            out.writeBoolean(true);
            int partitionSize = partitions.size();
            out.writeInt(partitionSize);
            for (String partitionName : partitions) {
                Text.writeString(out, partitionName);
            }
        } else {
            out.writeBoolean(false);
        }

        // task info
        Text.writeString(out, state.name());
        out.writeLong(createTimeMs);
        out.writeLong(startTimeMs);
        out.writeLong(finishTimeMs);
        out.writeInt(progress);
        failMsg.write(out);

        if (brokerDesc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            brokerDesc.write(out);
        }

        tableName.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        isReplayed = true;
        id = in.readLong();
        dbId = in.readLong();
        tableId = in.readLong();
        exportPath = Text.readString(in);
        columnSeparator = Text.readString(in);
        lineDelimiter = Text.readString(in);

        boolean hasPartition = in.readBoolean();
        if (hasPartition) {
            partitions = Lists.newArrayList();
            int partitionSize = in.readInt();
            for (int i = 0; i < partitionSize; ++i) {
                String partitionName = Text.readString(in);
                partitions.add(partitionName);
            }
        }

        state = JobState.valueOf(Text.readString(in));
        createTimeMs = in.readLong();
        startTimeMs = in.readLong();
        finishTimeMs = in.readLong();
        progress = in.readInt();
        failMsg.readFields(in);

        if (in.readBoolean()) {
            brokerDesc = BrokerDesc.read(in);
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_43) {
            tableName = new TableName();
            tableName.readFields(in);
        } else {
            tableName = new TableName("DUMMY", "DUMMY");
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ExportJob)) {
            return false;
        }

        ExportJob job = (ExportJob) obj;

        if (this.id == job.id) {
            return true;
        }

        return false;
    }

    public boolean isReplayed() {
        return isReplayed;
    }

    // for only persist op when switching job state.
    public static class StateTransfer implements Writable {
        long jobId;
        JobState state;

        public StateTransfer() {
            this.jobId = -1;
            this.state = JobState.CANCELLED;
        }

        public StateTransfer(long jobId, JobState state) {
            this.jobId = jobId;
            this.state = state;
        }

        public long getJobId() {
            return jobId;
        }

        public JobState getState() {
            return state;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(jobId);
            Text.writeString(out, state.name());
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            jobId = in.readLong();
            state = JobState.valueOf(Text.readString(in));
        }
    }
}
