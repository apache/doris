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

package org.apache.doris.qe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportWhereStmt;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

// Class used to record state of multi-load operation
public class MultiLoadMgr {
    private static final Logger LOG = LogManager.getLogger(MultiLoadMgr.class);

    private Map<LabelName, MultiLoadDesc> infoMap = Maps.newHashMap();
    private ReadWriteLock lock = new ReentrantReadWriteLock(true);

    // Start multi-load transaction.
    // Label is the only need parameter, maybe other properties?
    public void startMulti(String fullDbName, String label, Map<String, String> properties) throws DdlException {
        if (Strings.isNullOrEmpty(fullDbName)) {
            throw new DdlException("Database is empty");
        }

        if (Strings.isNullOrEmpty(label)) {
            throw new DdlException("Label is empty");
        }

        LoadStmt.checkProperties(properties);
        LabelName multiLabel = new LabelName(fullDbName, label);
        lock.writeLock().lock();
        try {
            if (infoMap.containsKey(multiLabel)) {
                throw new LabelAlreadyUsedException(label);
            }
            BeSelectionPolicy policy = new BeSelectionPolicy.Builder().needLoadAvailable().build();
            List<Long> backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, 1);
            if (backendIds.isEmpty()) {
                throw new DdlException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + " policy: " + policy);
            }
            MultiLoadDesc multiLoadDesc = new MultiLoadDesc(multiLabel, properties);
            multiLoadDesc.setBackendId(backendIds.get(0));
            infoMap.put(multiLabel, multiLoadDesc);
        } finally {
            lock.writeLock().unlock();
        }
        // Register to Load after put into map.
        Env.getCurrentEnv().getLoadManager().createLoadJobV1FromMultiStart(fullDbName, label);
    }

    // Add one load job
    private void load(String fullDbName, String label,
            String subLabel, String table,
            List<Pair<String, Long>> files,
            TNetworkAddress fileAddr,
            Map<String, String> properties,
            long timestamp) throws DdlException {
        LabelName multiLabel = new LabelName(fullDbName, label);
        lock.writeLock().lock();
        try {
            MultiLoadDesc multiLoadDesc = infoMap.get(multiLabel);
            if (multiLoadDesc == null) {
                throw new DdlException("Unknown label(" + multiLabel + ")");
            }
            multiLoadDesc.addFile(subLabel, table, files, fileAddr, properties, timestamp);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void unload(String fullDbName, String label, String subLabel) throws DdlException {
        LabelName multiLabel = new LabelName(fullDbName, label);
        lock.writeLock().lock();
        try {
            MultiLoadDesc multiLoadDesc = infoMap.get(multiLabel);
            if (multiLoadDesc == null) {
                throw new DdlException("Unknown label(" + multiLabel + ")");
            }
            multiLoadDesc.delFile(subLabel);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 'db' and 'label' form a multiLabel used to
    // user can pass commitLabel which use this string commit to jobmgr
    public void commit(String fullDbName, String label) throws DdlException {
        LabelName multiLabel = new LabelName(fullDbName, label);
        List<Long> jobIds = Lists.newArrayList();
        lock.writeLock().lock();
        try {
            MultiLoadDesc multiLoadDesc = infoMap.get(multiLabel);
            if (multiLoadDesc == null) {
                throw new DdlException("Unknown label(" + multiLabel + ")");
            }
            jobIds.add(Env.getCurrentEnv().getLoadManager().createLoadJobFromStmt(multiLoadDesc.toLoadStmt()));
            infoMap.remove(multiLabel);
        } finally {
            lock.writeLock().unlock();
        }
        final long jobId = jobIds.isEmpty() ? -1 : jobIds.get(0);
        Env.getCurrentEnv().getLoadInstance().deregisterMiniLabel(fullDbName, label);
        Env env = Env.getCurrentEnv();
        ConnectContext ctx = ConnectContext.get();
        Awaitility.await().atMost(Config.broker_load_default_timeout_second, TimeUnit.SECONDS).until(() -> {
            ConnectContext.threadLocalInfo.set(ctx);
            LoadJob loadJob = env.getLoadManager().getLoadJob(jobId);
            if (loadJob.getState() == JobState.FINISHED) {
                return true;
            } else if (loadJob.getState() == JobState.PENDING || loadJob.getState() == JobState.LOADING) {
                return false;
            } else {
                throw new DdlException("job failed. ErrorMsg: " + loadJob.getFailMsg().getMsg()
                        + ", URL: " + loadJob.getLoadingStatus().getTrackingUrl()
                        + ", JobDetails: " + loadJob.getLoadStatistic().toJson());
            }
        });
    }

    // Abort a in-progress multi-load job
    public void abort(String fullDbName, String label) throws DdlException {
        LabelName multiLabel = new LabelName(fullDbName, label);
        lock.writeLock().lock();
        try {
            MultiLoadDesc multiLoadDesc = infoMap.get(multiLabel);
            if (multiLoadDesc == null) {
                throw new DdlException("Unknown label(" + multiLabel + ")");
            }
            infoMap.remove(multiLabel);
        } finally {
            lock.writeLock().unlock();
        }
        Env.getCurrentEnv().getLoadInstance().deregisterMiniLabel(fullDbName, label);
    }

    public void desc(String fullDbName, String label, List<String> subLabels) throws DdlException {
        LabelName multiLabel = new LabelName(fullDbName, label);
        lock.readLock().lock();
        try {
            MultiLoadDesc multiLoadDesc = infoMap.get(multiLabel);
            if (multiLoadDesc == null) {
                throw new DdlException("Unknown label(" + multiLabel + ")");
            }
            multiLoadDesc.listLabel(subLabels);
        } finally {
            lock.readLock().unlock();
        }
    }

    // List all in-progress labels in database.
    public void list(String fullDbName, List<String> labels) throws DdlException {
        if (Strings.isNullOrEmpty(fullDbName)) {
            throw new DdlException("No database selected");
        }
        lock.readLock().lock();
        try {
            for (LabelName label : infoMap.keySet()) {
                if (fullDbName.equals(label.getDbName())) {
                    labels.add(label.getLabelName());
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public TNetworkAddress redirectAddr(String fullDbName, String label) throws DdlException {
        LabelName multiLabel = new LabelName(fullDbName, label);
        lock.writeLock().lock();
        try {
            MultiLoadDesc desc = infoMap.get(multiLabel);
            if (desc == null) {
                throw new DdlException("Unknown multiLabel(" + multiLabel + ")");
            }
            Backend backend = Env.getCurrentSystemInfo().getBackend(desc.getBackendId());
            return new TNetworkAddress(backend.getHost(), backend.getHttpPort());
        } finally {
            lock.writeLock().unlock();
        }
    }

    // This is no protect of lock
    private static class MultiLoadDesc {
        private LabelName multiLabel;
        private Map<String, TableLoadDesc> loadDescByLabel;
        private Map<String, TableLoadDesc> loadDescByTable;
        private Long backendId;
        private Map<String, String> properties;

        public MultiLoadDesc(LabelName label, Map<String, String> properties) {
            multiLabel = label;
            loadDescByLabel = Maps.newHashMap();
            loadDescByTable = Maps.newHashMap();
            backendId = -1L;
            this.properties = properties;
        }

        public void addFile(String subLabel, String table, List<Pair<String, Long>> files,
                TNetworkAddress fileAddr,
                Map<String, String> properties,
                long timestamp) throws DdlException {

            if (isSubLabelUsed(subLabel, timestamp)) {
                // sub label is used and this is a retry request.
                // no need to do further operation, just return
                return;
            }

            TableLoadDesc desc = loadDescByLabel.get(subLabel);
            if (desc != null) {
                // Already exists
                throw new LabelAlreadyUsedException(multiLabel.getLabelName(), subLabel);
            }
            desc = loadDescByTable.get(table);
            if (desc == null) {
                desc = new TableLoadDesc(table, subLabel, files, fileAddr, properties, timestamp);
                desc.setBackendId(backendId);
                loadDescByTable.put(table, desc);
            } else {
                if (!desc.canMerge(properties)) {
                    throw new DdlException("Same table have different properties in one multi-load."
                            + "new=" + properties + ",old=" + desc.properties);
                }
                desc.addFiles(subLabel, files);
                desc.addTimestamp(timestamp);
            }
            loadDescByLabel.put(subLabel, desc);
        }

        public void delFile(String label) throws DdlException {
            TableLoadDesc desc = loadDescByLabel.get(label);
            if (desc == null) {
                throw new DdlException("Unknown load label(" + label + ")");
            }
            desc.delFiles(label);
            if (desc.isEmpty()) {
                loadDescByTable.remove(desc.tbl);
            }
            loadDescByLabel.remove(label);
        }

        public void listLabel(List<String> labels) {
            for (String label : loadDescByLabel.keySet()) {
                labels.add(label);
            }
        }

        /*
         * 1. if sub label is already used, and this is not a retry request,
         *    throw exception ("Label already used")
         * 2. if label is already used, but this is a retry request,
         *    return true
         * 3. if label is not used, return false
         * 4. throw exception if encounter error.
         */
        public boolean isSubLabelUsed(String subLabel, long timestamp) throws DdlException {
            if (loadDescByLabel.containsKey(subLabel)) {
                if (timestamp == -1) {
                    // for compatibility
                    throw new LabelAlreadyUsedException(multiLabel.getLabelName(), subLabel);
                } else {
                    TableLoadDesc tblLoadDesc = loadDescByLabel.get(subLabel);
                    if (tblLoadDesc.containsTimestamp(timestamp)) {
                        LOG.info("get a retry request with label: {}, sub label: {}, timestamp: {}. return ok",
                                multiLabel.getLabelName(), subLabel, timestamp);
                        return true;
                    } else {
                        throw new LabelAlreadyUsedException(multiLabel.getLabelName(), subLabel);
                    }
                }
            }
            return false;
        }

        public void setBackendId(long backendId) {
            this.backendId = backendId;
        }

        public long getBackendId() {
            return backendId;
        }

        public LoadStmt toLoadStmt() throws DdlException {
            LabelName commitLabel = multiLabel;

            List<DataDescription> dataDescriptions = Lists.newArrayList();
            for (TableLoadDesc desc : loadDescByTable.values()) {
                dataDescriptions.add(desc.toDataDesc());
            }
            Map<String, String> brokerProperties = Maps.newHashMap();
            brokerProperties.put(BrokerDesc.MULTI_LOAD_BROKER_BACKEND_KEY, backendId.toString());
            BrokerDesc brokerDesc = new BrokerDesc(BrokerDesc.MULTI_LOAD_BROKER, brokerProperties);

            String comment = "multi load";
            if (properties.containsKey(LoadStmt.KEY_COMMENT)) {
                comment = properties.get(LoadStmt.KEY_COMMENT);
                properties.remove(LoadStmt.KEY_COMMENT);
            }

            properties.remove(LoadStmt.KEY_COMMENT);
            LoadStmt loadStmt = new LoadStmt(commitLabel, dataDescriptions, brokerDesc, properties, comment);
            loadStmt.setEtlJobType(EtlJobType.BROKER);
            loadStmt.setOrigStmt(new OriginStatement("", 0));
            loadStmt.setUserInfo(ConnectContext.get().getCurrentUserIdentity());
            Analyzer analyzer = new Analyzer(ConnectContext.get().getEnv(), ConnectContext.get());
            try {
                loadStmt.analyze(analyzer);
            } catch (UserException e) {
                throw new DdlException(e.getMessage());
            }
            return loadStmt;
        }
    }

    public static class TableLoadDesc {
        // identity of this load
        private String tbl;
        private Map<String, List<Pair<String, Long>>> filesByLabel;
        private TNetworkAddress address;

        private Long backendId;
        private Map<String, String> properties;
        // 2 or more files may be loaded to same table with different sub labels.
        // So we use Set to save all timestamp of all different sub labels
        private Set<Long> timestamps = Sets.newHashSet();

        public TableLoadDesc(String tbl, String label, List<Pair<String, Long>> files,
                TNetworkAddress address, Map<String, String> properties,
                long timestamp) {
            this.tbl = tbl;
            this.filesByLabel = Maps.newLinkedHashMap();

            this.address = address;
            this.properties = properties;
            filesByLabel.put(label, files);
            this.timestamps.add(timestamp);
        }

        public boolean canMerge(Map<String, String> properties) {
            return Maps.difference(this.properties, properties).areEqual();
        }

        public boolean isEmpty() {
            return filesByLabel.isEmpty();
        }

        public void addFiles(String label, List<Pair<String, Long>> files) {
            filesByLabel.put(label, files);
        }

        public void delFiles(String label) {
            filesByLabel.remove(label);
        }

        public boolean containsTimestamp(long timestamp) {
            return timestamps.contains(timestamp);
        }

        public void addTimestamp(long timestamp) {
            timestamps.add(timestamp);
        }

        public Long getBackendId() {
            return backendId;
        }

        public void setBackendId(Long backendId) {
            this.backendId = backendId;
        }

        // TODO(zc):
        public DataDescription toDataDesc() throws DdlException {
            List<String> files = Lists.newArrayList();
            List<Long> fileSizes = Lists.newArrayList();
            Iterator<Map.Entry<String, List<Pair<String, Long>>>> it = filesByLabel.entrySet().iterator();
            while (it.hasNext()) {
                List<Pair<String, Long>> value = it.next().getValue();
                value.forEach(pair -> {
                    files.add(pair.first);
                    fileSizes.add(pair.second);
                });
            }
            Separator columnSeparator = null;
            PartitionNames partitionNames = null;
            String fileFormat = properties.get(LoadStmt.KEY_IN_PARAM_FORMAT_TYPE);
            boolean isNegative = properties.get(LoadStmt.KEY_IN_PARAM_NEGATIVE) == null ? false :
                    Boolean.parseBoolean(properties.get(LoadStmt.KEY_IN_PARAM_NEGATIVE));
            Expr whereExpr = null;
            LoadTask.MergeType mergeType = LoadTask.MergeType.APPEND;
            Expr deleteCondition = null;
            String sequenceColName = properties.get(LoadStmt.KEY_IN_PARAM_SEQUENCE_COL);
            String colString = null;
            Backend backend = null;
            boolean stripOuterArray = false;
            String jsonPaths = "";
            String jsonRoot = "";
            boolean fuzzyParse = false;
            if (properties != null) {
                colString = properties.get(LoadStmt.KEY_IN_PARAM_COLUMNS);
                String columnSeparatorStr = properties.get(LoadStmt.KEY_IN_PARAM_COLUMN_SEPARATOR);
                if (columnSeparatorStr != null) {
                    columnSeparator = new Separator(columnSeparatorStr);
                    try {
                        columnSeparator.analyze();
                    } catch (AnalysisException e) {
                        throw new DdlException(e.getMessage());
                    }
                }
                if (properties.get(LoadStmt.KEY_IN_PARAM_PARTITIONS) != null) {
                    String[] splitPartNames = properties.get(LoadStmt.KEY_IN_PARAM_PARTITIONS).trim().split(",");
                    List<String> partNames = Arrays.stream(splitPartNames).map(String::trim)
                            .collect(Collectors.toList());
                    partitionNames = new PartitionNames(false, partNames);
                } else if (properties.get(LoadStmt.KEY_IN_PARAM_TEMP_PARTITIONS) != null) {
                    String[] splitTempPartNames = properties.get(LoadStmt.KEY_IN_PARAM_TEMP_PARTITIONS).trim()
                            .split(",");
                    List<String> tempPartNames = Arrays.stream(splitTempPartNames).map(String::trim)
                            .collect(Collectors.toList());
                    partitionNames = new PartitionNames(true, tempPartNames);
                }
                if (properties.get(LoadStmt.KEY_IN_PARAM_MERGE_TYPE) != null) {
                    mergeType = LoadTask.MergeType.valueOf(properties.get(LoadStmt.KEY_IN_PARAM_MERGE_TYPE));
                }
                if (properties.get(LoadStmt.KEY_IN_PARAM_WHERE) != null) {
                    whereExpr = parseWhereExpr(properties.get(LoadStmt.KEY_IN_PARAM_WHERE));
                }
                if (properties.get(LoadStmt.KEY_IN_PARAM_DELETE_CONDITION) != null) {
                    deleteCondition = parseWhereExpr(properties.get(LoadStmt.KEY_IN_PARAM_DELETE_CONDITION));
                }
                if (fileFormat != null && fileFormat.equalsIgnoreCase("json")) {
                    stripOuterArray = Boolean.valueOf(
                            properties.getOrDefault(LoadStmt.KEY_IN_PARAM_STRIP_OUTER_ARRAY, "false"));
                    jsonPaths = properties.getOrDefault(LoadStmt.KEY_IN_PARAM_JSONPATHS, "");
                    jsonRoot = properties.getOrDefault(LoadStmt.KEY_IN_PARAM_JSONROOT, "");
                    fuzzyParse = Boolean.valueOf(
                            properties.getOrDefault(LoadStmt.KEY_IN_PARAM_FUZZY_PARSE, "false"));
                }
            }
            DataDescription dataDescription = new DataDescription(tbl, partitionNames, files, null, columnSeparator,
                    fileFormat, null, isNegative, null, null, whereExpr, mergeType, deleteCondition,
                    sequenceColName, null);
            dataDescription.setColumnDef(colString);
            backend = Env.getCurrentSystemInfo().getBackend(backendId);
            if (backend == null) {
                throw new DdlException("Backend [" + backendId + "] not found. ");
            }
            dataDescription.setBeAddr(new TNetworkAddress(backend.getHost(), backend.getHeartbeatPort()));
            dataDescription.setFileSize(fileSizes);
            dataDescription.setBackendId(backendId);
            dataDescription.setJsonPaths(jsonPaths);
            dataDescription.setJsonRoot(jsonRoot);
            dataDescription.setStripOuterArray(stripOuterArray);
            dataDescription.setFuzzyParse(fuzzyParse);
            return dataDescription;
        }

        private Expr parseWhereExpr(String whereString) throws DdlException {
            String whereSQL = "WHERE " + whereString;
            SqlParser parser = new SqlParser(new SqlScanner(new StringReader(whereSQL)));
            ImportWhereStmt whereStmt;
            try {
                whereStmt = (ImportWhereStmt) SqlParserUtils.getFirstStmt(parser);
            } catch (Error e) {
                LOG.warn("error happens when parsing where header, sql={}", whereSQL, e);
                throw new DdlException("failed to parsing where header, maybe contain unsupported character");
            } catch (DdlException e) {
                LOG.warn("analyze where statement failed, sql={}, error={}",
                        whereSQL, parser.getErrorMsg(whereSQL), e);
                String errorMessage = parser.getErrorMsg(whereSQL);
                if (errorMessage == null) {
                    throw e;
                } else {
                    throw new DdlException(errorMessage, e);
                }
            } catch (Exception e) {
                LOG.warn("failed to parse where header, sql={}", whereSQL, e);
                throw new DdlException("parse columns header failed", e);
            }
            return whereStmt.getExpr();
        }
    }
}
