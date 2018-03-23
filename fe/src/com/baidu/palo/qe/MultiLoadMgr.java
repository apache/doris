// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.qe;

import com.baidu.palo.analysis.ColumnSeparator;
import com.baidu.palo.analysis.DataDescription;
import com.baidu.palo.analysis.LabelName;
import com.baidu.palo.analysis.LoadStmt;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.load.LoadJob.EtlJobType;
import com.baidu.palo.thrift.TMiniLoadRequest;
import com.baidu.palo.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Class used to record state of multi-load operation
public class MultiLoadMgr {
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
                throw new DdlException("Label(" + multiLabel + ") already exists.");
            }
            infoMap.put(multiLabel, new MultiLoadDesc(multiLabel, properties));
        } finally {
            lock.writeLock().unlock();
        }
        // Register to Load after put into map.
        Catalog.getInstance().getLoadInstance().registerMiniLabel(fullDbName, label, System.currentTimeMillis());
    }

    public void load(TMiniLoadRequest request) throws DdlException {
        load(request.getDb(), request.getLabel(), request.getSubLabel(), request.getTbl(),
                request.getFiles(), request.getBackend(), request.getProperties());
    }

    // Add one load job, we have
    private void load(String fullDbName, String label,
                     String subLabel, String table,
                     List<String> files,
                     TNetworkAddress fileAddr,
                     Map<String, String> properties) throws DdlException {
        LabelName multiLabel = new LabelName(fullDbName, label);
        lock.writeLock().lock();
        try {
            MultiLoadDesc multiLoadDesc = infoMap.get(multiLabel);
            if (multiLoadDesc == null) {
                throw new DdlException("Unknown label(" + multiLabel + ")");
            }
            multiLoadDesc.addFile(subLabel, table, files, fileAddr, properties);
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
            Catalog.getInstance().getLoadInstance().addLoadJob(
                    multiLoadDesc.toLoadStmt(),
                    EtlJobType.MINI,
                    System.currentTimeMillis());
            infoMap.remove(multiLabel);
        } finally {
            lock.writeLock().unlock();
        }
        Catalog.getInstance().getLoadInstance().deregisterMiniLabel(fullDbName, label);
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
        Catalog.getInstance().getLoadInstance().deregisterMiniLabel(fullDbName, label);
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

        public void addFile(String label, String table, List<String> files,
                            TNetworkAddress fileAddr,
                            Map<String, String> properties) throws DdlException {
            TableLoadDesc desc = loadDescByLabel.get(label);
            if (desc != null) {
                // Already exists
                throw new DdlException("Sub label(" + label + ") already exists.");
            }
            desc = loadDescByTable.get(table);
            if (desc == null) {
                desc = new TableLoadDesc(table, label, files, fileAddr, properties);
                loadDescByTable.put(table, desc);
            } else {
                if (!desc.canMerge(properties)) {
                    throw new DdlException("Same table have different properties in one multi-load."
                            + "new=" + properties + ",old=" + desc.properties);
                }
                desc.addFiles(label, files);
            }
            loadDescByLabel.put(label, desc);
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

        public TableLoadDesc(String tbl, String label, List<String> files,
                             TNetworkAddress address, Map<String, String> properties) {
            this.tbl = tbl;
            this.filesByLabel = Maps.newHashMap();
            this.address = address;
            this.properties = properties;
            filesByLabel.put(label, files);
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
                    tbl, null, files, columns, columnSeparator, false, null);

            dataDescription.setBeAddr(address);
            return dataDescription;
        }
    }
}
