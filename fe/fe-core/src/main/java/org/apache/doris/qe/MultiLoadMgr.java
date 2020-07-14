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

import org.apache.doris.analysis.ColumnSeparator;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.thrift.TMiniLoadRequest;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
            infoMap.put(multiLabel, new MultiLoadDesc(multiLabel, properties));
        } finally {
            lock.writeLock().unlock();
        }
        // Register to Load after put into map.
        Catalog.getCurrentCatalog().getLoadManager().createLoadJobV1FromMultiStart(fullDbName, label);
    }

    public void load(TMiniLoadRequest request) throws DdlException {
        load(request.getDb(), request.getLabel(), request.getSubLabel(), request.getTbl(),
             request.getFiles(), request.getBackend(), request.getProperties(), request.getTimestamp());
    }

    // Add one load job
    private void load(String fullDbName, String label,
                     String subLabel, String table,
                     List<String> files,
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
        lock.writeLock().lock();
        try {
            MultiLoadDesc multiLoadDesc = infoMap.get(multiLabel);
            if (multiLoadDesc == null) {
                throw new DdlException("Unknown label(" + multiLabel + ")");
            }
            Catalog.getCurrentCatalog().getLoadInstance().addLoadJob(
                    multiLoadDesc.toLoadStmt(),
                    EtlJobType.MINI,
                    System.currentTimeMillis());
            infoMap.remove(multiLabel);
        } finally {
            lock.writeLock().unlock();
        }
        Catalog.getCurrentCatalog().getLoadInstance().deregisterMiniLabel(fullDbName, label);
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
        Catalog.getCurrentCatalog().getLoadInstance().deregisterMiniLabel(fullDbName, label);
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

    public TNetworkAddress redirectAddr(String fullDbName, String label, String tbl, TNetworkAddress defaultAddr)
            throws DdlException {
        LabelName multiLabel = new LabelName(fullDbName, label);
        lock.writeLock().lock();
        try {
            MultiLoadDesc desc = infoMap.get(multiLabel);
            if (desc == null) {
                throw new DdlException("Unknown multiLabel(" + multiLabel + ")");
            }
            return desc.getHost(tbl, defaultAddr);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // This is no protect of lock
    private static class MultiLoadDesc {
        private LabelName multiLabel;
        private Map<String, TableLoadDesc> loadDescByLabel;
        private Map<String, TableLoadDesc> loadDescByTable;
        private Map<String, TNetworkAddress> addressByTable;
        private Map<String, String> properties;

        public MultiLoadDesc(LabelName label, Map<String, String> properties) {
            multiLabel = label;
            loadDescByLabel = Maps.newHashMap();
            loadDescByTable = Maps.newHashMap();
            addressByTable = Maps.newHashMap();
            this.properties = properties;
        }

        public void addFile(String subLabel, String table, List<String> files,
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

        public TNetworkAddress getHost(String table, TNetworkAddress defaultAddr) {
            TNetworkAddress address = addressByTable.get(table);
            if (address != null) {
                return address;
            }
            addressByTable.put(table, defaultAddr);
            return defaultAddr;
        }

        public LoadStmt toLoadStmt() throws DdlException {
            LabelName commitLabel = multiLabel;

            List<DataDescription> dataDescriptions = Lists.newArrayList();
            for (TableLoadDesc desc : loadDescByTable.values()) {
                dataDescriptions.add(desc.toDataDesc());
            }

            return new LoadStmt(commitLabel, dataDescriptions, null, null, properties);
        }
    }

    public static class TableLoadDesc {
        // identity of this load
        private String tbl;
        private Map<String, List<String>> filesByLabel;
        private TNetworkAddress address;
        private Map<String, String> properties;
        // 2 or more files may be loaded to same table with different sub labels.
        // So we use Set to save all timestamp of all diffrent sub labels
        private Set<Long> timestamps = Sets.newHashSet();

        public TableLoadDesc(String tbl, String label, List<String> files,
                             TNetworkAddress address, Map<String, String> properties,
                             long timestamp) {
            this.tbl = tbl;
            this.filesByLabel = Maps.newHashMap();
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

        public void addFiles(String label, List<String> files) {
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

        // TODO(zc):
        public DataDescription toDataDesc() throws DdlException {
            List<String> files = Lists.newArrayList();
            for (List<String> fileOfLable : filesByLabel.values()) {
                files.addAll(fileOfLable);
            }
            List<String> columns = null;
            ColumnSeparator columnSeparator = null;
            if (properties != null) {
                String colString = properties.get(LoadStmt.KEY_IN_PARAM_COLUMNS);
                if (colString != null) {
                    columns = Arrays.asList(colString.split(","));
                }
                String columnSeparatorStr = properties.get(LoadStmt.KEY_IN_PARAM_COLUMN_SEPARATOR);
                if (columnSeparatorStr != null) {
                    columnSeparator = new ColumnSeparator(columnSeparatorStr);
                    try {
                        columnSeparator.analyze();
                    } catch (AnalysisException e) {
                        throw new DdlException(e.getMessage());
                    }
                }
            }

            DataDescription dataDescription = new DataDescription(
                    tbl, null, files, columns, columnSeparator, null, false, null);

            dataDescription.setBeAddr(address);
            return dataDescription;
        }
    }
}

