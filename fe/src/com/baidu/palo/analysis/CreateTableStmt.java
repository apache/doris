// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.AggregateType;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.KeysType;
import com.baidu.palo.catalog.PartitionType;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.FeMetaVersion;
import com.baidu.palo.common.FeNameFormat;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.common.util.KuduUtil;
import com.baidu.palo.common.util.PrintableMap;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CreateTableStmt extends DdlStmt implements Writable {
    private static final Logger LOG = LogManager.getLogger(CreateTableStmt.class);

    private static final String DEFAULT_ENGINE_NAME = "olap";

    private boolean ifNotExists;
    private boolean isExternal;
    private TableName tableName;
    private List<Column> columns;
    private KeysDesc keysDesc;
    private PartitionDesc partitionDesc;
    private DistributionDesc distributionDesc;
    private Map<String, String> properties;
    private Map<String, String> extProperties;
    private String engineName;

    private static Set<String> engineNames;

    static {
        engineNames = Sets.newHashSet();
        engineNames.add("olap");
        engineNames.add("mysql");
        engineNames.add("kudu");
        engineNames.add("broker");
    }

    // for backup. set to -1 for normal use
    private int tableSignature;

    public CreateTableStmt() {
        // for persist
        tableName = new TableName();
        columns = Lists.newArrayList();
    }

    public CreateTableStmt(boolean ifNotExists,
                           boolean isExternal,
                           TableName tableName,
                           List<Column> columnDefinitions,
                           String engineName, 
                           KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           Map<String, String> properties,
                           Map<String, String> extProperties) {
        this.tableName = tableName;
        if (columnDefinitions == null) {
            this.columns = Lists.newArrayList();
        } else {
            this.columns = columnDefinitions;
        }
        if (Strings.isNullOrEmpty(engineName)) {
            this.engineName = DEFAULT_ENGINE_NAME;
        } else {
            this.engineName = engineName;
        }

        this.keysDesc = keysDesc;
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
        this.extProperties = extProperties;
        this.isExternal = isExternal;
        this.ifNotExists = ifNotExists;

        this.tableSignature = -1;
    }

    public void addColumn(Column col) {
        columns.add(col);
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public TableName getDbTbl() {
        return tableName;
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public List<Column> getColumns() {
        return this.columns;
    }

    public KeysDesc getKeysDesc() {
        return this.keysDesc;
    }

    public PartitionDesc getPartitionDesc() {
        return this.partitionDesc;
    }

    public DistributionDesc getDistributionDesc() {
        return this.distributionDesc;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public Map<String, String> getExtProperties() {
        return this.extProperties;
    }

    public String getEngineName() {
        return engineName;
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public void setTableSignature(int tableSignature) {
        this.tableSignature = tableSignature;
    }

    public int getTableSignature() {
        return tableSignature;
    }

    public void setTableName(String newTableName) {
        tableName = new TableName(tableName.getDb(), newTableName);
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);
        tableName.analyze(analyzer);
        FeNameFormat.checkTableName(tableName.getTbl());

        // check authenticate
        if (!analyzer.getCatalog().getUserMgr()
                .checkAccess(analyzer.getUser(), tableName.getDb(), AccessPrivilege.READ_WRITE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, analyzer.getUser(), tableName.getDb());
        }

        analyzeEngineName();

        // analyze key desc
        if (!(engineName.equals("mysql") || engineName.equals("broker"))) {
            if (engineName.equals("kudu")) {
                if (keysDesc == null) {
                    throw new AnalysisException("create kudu table should contains keys desc");
                }
                KuduUtil.analyzeKeyDesc(keysDesc);
            } else {
                // olap table
                if (keysDesc == null) {
                    List<String> keysColumnNames = Lists.newArrayList();
                    for (Column column : columns) {
                        if (column.getAggregationType() == null) {
                            keysColumnNames.add(column.getName());
                        }
                    }
                    keysDesc = new KeysDesc(KeysType.AGG_KEYS, keysColumnNames);
                }

                keysDesc.analyze(columns);
                for (int i = 0; i < keysDesc.keysColumnSize(); ++i) {
                    columns.get(i).setIsKey(true);
                }
                if (keysDesc.getKeysType() != KeysType.AGG_KEYS) {
                    AggregateType type = AggregateType.REPLACE;
                    if (keysDesc.getKeysType() == KeysType.DUP_KEYS) {
                        type = AggregateType.NONE;
                    }
                    for (int i = keysDesc.keysColumnSize(); i < columns.size(); ++i) {
                        columns.get(i).setAggregationType(type, true);
                    }
                }
            }
        } else {
            // mysql and broker do not need key desc
            if (keysDesc != null) {
                throw new AnalysisException("Create " + engineName + " table should not contain keys desc");
            }

            for (Column column : columns) {
                column.setIsKey(true);
            }
        }

        // analyze column def
        if (columns == null || columns.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }

        int rowLengthBytes = 0;
        boolean hasHll = false;
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Column col : columns) {
            // if engine is mysql, remove varchar limit
            if (engineName.equals("mysql")) {
                col.setVarcharLimit(false);
            }

            if (engineName.equals("kudu")) {
                KuduUtil.analyzeColumn(col, keysDesc);
            } else {
                col.analyze(engineName.equals("olap"));
            }

            if (col.getType().isHllType()) {
                hasHll = true;
            }

            if (!columnSet.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }

            rowLengthBytes += col.getColumnType().getMemlayoutBytes();
        }

        if (rowLengthBytes > Config.max_layout_length_per_row) {
            throw new AnalysisException("The size of a row (" + rowLengthBytes + ") exceed the maximal row size: "
                    + Config.max_layout_length_per_row);
        }

        if (hasHll && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new AnalysisException("HLL must be used in AGG_KEYS");
        }

        if (engineName.equals("olap")) {
            // analyze partition
            if (partitionDesc != null) {
                if (partitionDesc.getType() != PartitionType.RANGE) {
                    throw new AnalysisException("Currently only support range partition with engine type olap");
                }

                RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
                if (rangePartitionDesc.getPartitionColNames().size() != 1) {
                    throw new AnalysisException("Only allow partitioned by one column");
                }

                rangePartitionDesc.analyze(columns, properties);
            }

            // analyze distribution
            if (distributionDesc == null) {
                throw new AnalysisException("Create olap table should contain distribution desc");
            }
            distributionDesc.analyze(columnSet);
        } else if (engineName.equals("kudu")) {
            KuduUtil.analyzePartitionAndDistributionDesc(keysDesc, partitionDesc, distributionDesc);
        } else {
            if (partitionDesc != null || distributionDesc != null) {
                throw new AnalysisException("Create " + engineName
                        + " table should not contain partition or distribution desc");
            }
        }
    }

    private void analyzeEngineName() throws AnalysisException {
        if (Strings.isNullOrEmpty(engineName)) {
            engineName = "olap";
        }
        engineName = engineName.toLowerCase();

        if (!engineNames.contains(engineName)) {
            throw new AnalysisException("Unknown engine name: " + engineName);
        }

        if (engineName.equals("mysql") || engineName.equals("broker")) {
            if (!isExternal) {
                // this is for compatibility
                isExternal = true;
                LOG.warn("create " + engineName + " table without keyword external");
                // throw new AnalysisException("Only support external table with engine name = mysql or broker");
            }
        } else {
            if (isExternal) {
                throw new AnalysisException("Do not support external table with engine name = olap or kudu");
            }
        }
    }

    public static CreateTableStmt read(DataInput in) throws IOException {
        CreateTableStmt stmt = new CreateTableStmt();
        stmt.readFields(in);
        return stmt;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE ");
        if (isExternal) {
            sb.append("EXTERNAL ");
        }
        sb.append("TABLE ");
        sb.append(tableName.toSql()).append(" (\n");
        int idx = 0;
        for (Column column : columns) {
            if (idx != 0) {
                sb.append(",\n");
            }
            sb.append(column.toSql());
            idx++;
        }
        sb.append("\n)");
        if (engineName != null) {
            sb.append(" ENGINE = ").append(engineName);
        }

        if (keysDesc != null) {
            sb.append("\n").append(keysDesc.toSql());
        }

        if (partitionDesc != null) {
            sb.append("\n").append(partitionDesc.toSql());
        }
        
        if (distributionDesc != null) {
            sb.append("\n").append(distributionDesc.toSql());
        }

        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, " = ", true, true));
            sb.append(")");
        }

        if (extProperties != null && !extProperties.isEmpty()) {
            sb.append("\n").append(engineName.toUpperCase()).append(" PROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, " = ", true, true));
            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(ifNotExists);
        tableName.write(out);
        int count = columns.size();
        out.writeInt(count);
        for (Column columnDefinition : columns) {
            columnDefinition.write(out);
        }

        if (keysDesc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            keysDesc.write(out);
        }

        if (partitionDesc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            partitionDesc.write(out);
        }

        if (distributionDesc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            distributionDesc.write(out);
        }

        if (properties == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            count = properties.size();
            out.writeInt(count);
            for (Map.Entry<String, String> prop : properties.entrySet()) {
                Text.writeString(out, prop.getKey());
                Text.writeString(out, prop.getValue());
            }
        }

        Text.writeString(out, engineName);

        out.writeInt(tableSignature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ifNotExists = in.readBoolean();
        tableName.readFields(in);

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            Column columnDefinition = new Column();
            columnDefinition.readFields(in);
            columns.add(columnDefinition);
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
            boolean has = in.readBoolean();
            if (has) {
                keysDesc = KeysDesc.read(in);
            }
        }

        boolean has = in.readBoolean();
        if (has) {
            partitionDesc = PartitionDesc.read(in);
        }

        has = in.readBoolean();
        if (has) {
            distributionDesc = DistributionDesc.read(in);
        }

        has = in.readBoolean();
        if (has) {
            count = in.readInt();
            properties = Maps.newHashMap();
            for (int i = 0; i < count; i++) {
                String key = Text.readString(in);
                String value = Text.readString(in);
                properties.put(key, value);
            }
        }

        engineName = Text.readString(in);
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_30
                && engineName.equals("olap")) {
            List<String> keysColumnNames = Lists.newArrayList();
            for (Column column : columns) {
                if (column.getAggregationType() == null) {
                    keysColumnNames.add(column.getName());
                }
            }
            keysDesc = new KeysDesc(KeysType.AGG_KEYS, keysColumnNames);
        }

        tableSignature = in.readInt();
    }
}
