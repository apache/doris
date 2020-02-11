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

package org.apache.doris.analysis;

import static org.apache.doris.catalog.AggregateType.BITMAP_UNION;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.KuduUtil;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.external.EsUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class CreateTableStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CreateTableStmt.class);

    private static final String DEFAULT_ENGINE_NAME = "olap";

    private boolean ifNotExists;
    private boolean isExternal;
    private TableName tableName;
    private List<ColumnDef> columnDefs;
    private List<IndexDef> indexDefs;
    private KeysDesc keysDesc;
    private PartitionDesc partitionDesc;
    private DistributionDesc distributionDesc;
    private Map<String, String> properties;
    private Map<String, String> extProperties;
    private String engineName;
    private String comment;
    private List<AlterClause> rollupAlterClauseList;

    private static Set<String> engineNames;

    // set in analyze
    private List<Column> columns = Lists.newArrayList();

    private List<Index> indexes = Lists.newArrayList();

    static {
        engineNames = Sets.newHashSet();
        engineNames.add("olap");
        engineNames.add("mysql");
        engineNames.add("kudu");
        engineNames.add("broker");
        engineNames.add("elasticsearch");
    }

    // for backup. set to -1 for normal use
    private int tableSignature;

    public CreateTableStmt() {
        // for persist
        tableName = new TableName();
        columnDefs = Lists.newArrayList();
    }

    public CreateTableStmt(boolean ifNotExists,
                           boolean isExternal,
                           TableName tableName,
                           List<ColumnDef> columnDefinitions,
                           String engineName,
                           KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           Map<String, String> properties,
                           Map<String, String> extProperties,
                           String comment) {
        this(ifNotExists, isExternal, tableName, columnDefinitions, null, engineName, keysDesc, partitionDesc,
                distributionDesc, properties, extProperties, comment, null);
    }

    public CreateTableStmt(boolean ifNotExists,
                           boolean isExternal,
                           TableName tableName,
                           List<ColumnDef> columnDefinitions,
                           String engineName,
                           KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           Map<String, String> properties,
                           Map<String, String> extProperties,
                           String comment, List<AlterClause> ops) {
        this(ifNotExists, isExternal, tableName, columnDefinitions, null, engineName, keysDesc, partitionDesc,
                distributionDesc, properties, extProperties, comment, ops);
    }

    public CreateTableStmt(boolean ifNotExists,
                           boolean isExternal,
                           TableName tableName,
                           List<ColumnDef> columnDefinitions,
                           List<IndexDef> indexDefs,
                           String engineName,
                           KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           Map<String, String> properties,
                           Map<String, String> extProperties,
                           String comment, List<AlterClause> rollupAlterClauseList) {
        this.tableName = tableName;
        if (columnDefinitions == null) {
            this.columnDefs = Lists.newArrayList();
        } else {
            this.columnDefs = columnDefinitions;
        }
        this.indexDefs = indexDefs;
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
        this.comment = Strings.nullToEmpty(comment);

        this.tableSignature = -1;
        this.rollupAlterClauseList = rollupAlterClauseList == null ? new ArrayList<>() : rollupAlterClauseList;
    }

    public void addColumnDef(ColumnDef columnDef) { columnDefs.add(columnDef); }

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

    public String getComment() {
        return comment;
    }

    public List<AlterClause> getRollupAlterClauseList() {
        return rollupAlterClauseList;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        tableName.analyze(analyzer);
        FeNameFormat.checkTableName(tableName.getTbl());

        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tableName.getDb(),
                tableName.getTbl(), PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
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
                    int keyLength = 0;
                    boolean hasAggregate = false;
                    for (ColumnDef columnDef : columnDefs) {
                        if (columnDef.getAggregateType() != null) {
                            hasAggregate = true;
                            break;
                        }
                    }
                    if (hasAggregate) {
                        for (ColumnDef columnDef : columnDefs) {
                            if (columnDef.getAggregateType() == null) {
                                keysColumnNames.add(columnDef.getName());
                            }
                        }
                        keysDesc = new KeysDesc(KeysType.AGG_KEYS, keysColumnNames);
                    } else {
                        for (ColumnDef columnDef : columnDefs) {
                            keyLength += columnDef.getType().getStorageLayoutBytes();
                            if (keysColumnNames.size() < FeConstants.shortkey_max_column_count
                                    || keyLength < FeConstants.shortkey_maxsize_bytes) {
                                keysColumnNames.add(columnDef.getName());
                            }
                        }
                        keysDesc = new KeysDesc(KeysType.DUP_KEYS, keysColumnNames);
                    }
                }

                keysDesc.analyze(columnDefs);
                for (int i = 0; i < keysDesc.keysColumnSize(); ++i) {
                    columnDefs.get(i).setIsKey(true);
                }
                if (keysDesc.getKeysType() != KeysType.AGG_KEYS) {
                    AggregateType type = AggregateType.REPLACE;
                    if (keysDesc.getKeysType() == KeysType.DUP_KEYS) {
                        type = AggregateType.NONE;
                    }
                    for (int i = keysDesc.keysColumnSize(); i < columnDefs.size(); ++i) {
                        columnDefs.get(i).setAggregateType(type, true);
                    }
                }
            }
        } else {
            // mysql and broker do not need key desc
            if (keysDesc != null) {
                throw new AnalysisException("Create " + engineName + " table should not contain keys desc");
            }

            for (ColumnDef columnDef : columnDefs) {
                columnDef.setIsKey(true);
            }
        }

        // analyze column def
        if (columnDefs == null || columnDefs.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }

        int rowLengthBytes = 0;
        boolean hasHll = false;
        boolean hasBitmap = false;
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ColumnDef columnDef : columnDefs) {
            if (engineName.equals("kudu")) {
                // KuduUtil.analyzeColumn(columnDef, keysDesc);
                throw new NotImplementedException("");
            } else {
                columnDef.analyze(engineName.equals("olap"));
            }

            if (columnDef.getType().isHllType()) {
                if (columnDef.isKey()) {
                    throw new AnalysisException("HLL can't be used as keys, " +
                            "please specify the aggregation type HLL_UNION");
                }
                hasHll = true;
            }

            if (columnDef.getType().isBitmapType()) {
                if (columnDef.isKey()) {
                    throw new AnalysisException("BITMAP can't be used as keys, ");
                }
            }

            if (columnDef.getAggregateType() == BITMAP_UNION) {
                if (columnDef.isKey()) {
                    throw new AnalysisException("Key column can't has the BITMAP_UNION aggregation type");
                }
                hasBitmap = true;
            }

            if (!columnSet.add(columnDef.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, columnDef.getName());
            }

            rowLengthBytes += columnDef.getType().getStorageLayoutBytes();
        }

        if (rowLengthBytes > Config.max_layout_length_per_row && engineName.equals("olap")) {
            throw new AnalysisException("The size of a row (" + rowLengthBytes + ") exceed the maximal row size: "
                    + Config.max_layout_length_per_row);
        }

        if (hasHll && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new AnalysisException("HLL must be used in AGG_KEYS");
        }

        if (hasBitmap && keysDesc.getKeysType() != KeysType.AGG_KEYS) {
            throw new AnalysisException("BITMAP_UNION must be used in AGG_KEYS");
        }

        if (engineName.equals("olap")) {
            // analyze partition
            if (partitionDesc != null) {
                if (partitionDesc.getType() != PartitionType.RANGE) {
                    throw new AnalysisException("Currently only support range partition with engine type olap");
                }

                RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
                rangePartitionDesc.analyze(columnDefs, properties);
            }

            // analyze distribution
            if (distributionDesc == null) {
                throw new AnalysisException("Create olap table should contain distribution desc");
            }
            distributionDesc.analyze(columnSet);
        } else if (engineName.equals("kudu")) {
            KuduUtil.analyzePartitionAndDistributionDesc(keysDesc, partitionDesc, distributionDesc);
        } else if (engineName.equalsIgnoreCase("elasticsearch")) {
            EsUtil.analyzePartitionAndDistributionDesc(partitionDesc, distributionDesc);
        } else {
            if (partitionDesc != null || distributionDesc != null) {
                throw new AnalysisException("Create " + engineName
                        + " table should not contain partition or distribution desc");
            }
        }

        for (ColumnDef columnDef : columnDefs) {
            Column col = columnDef.toColumn();
            if (keysDesc != null && keysDesc.getKeysType() == KeysType.UNIQUE_KEYS) {
                if (!col.isKey()) {
                    col.setAggregationTypeImplicit(true);
                }
            }
            columns.add(col);
        }

        if (CollectionUtils.isNotEmpty(indexDefs)) {
            Set<String> distinct = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Set<List<String>> distinctCol = new HashSet<>();

            for (IndexDef indexDef : indexDefs) {
                indexDef.analyze();
                if (!engineName.equalsIgnoreCase("olap")) {
                    throw new AnalysisException("index only support in olap engine at current version.");
                }
                for (String indexColName : indexDef.getColumns()) {
                    indexColName = indexColName.trim();
                    boolean found = false;
                    for (Column column : columns) {
                        if (column.getName().equalsIgnoreCase(indexColName)) {
                            indexColName = column.getName();
                            PrimitiveType colType = column.getDataType();

                            // key columns and none/replace aggregate non-key columns support
                            if (indexDef.getIndexType() == IndexDef.IndexType.BITMAP) {
                                    if (!(colType == PrimitiveType.TINYINT || colType == PrimitiveType.SMALLINT
                                                  || colType == PrimitiveType.INT || colType == PrimitiveType.BIGINT ||
                                                  colType == PrimitiveType.CHAR || colType == PrimitiveType.VARCHAR)) {
                                        throw new AnalysisException(colType + " is not supported in bitmap index. "
                                                + "invalid column: " + indexColName);
                                    } else if (column.isKey()
                                            || column.getAggregationType() == AggregateType.NONE
                                            || column.getAggregationType() == AggregateType.REPLACE
                                            || column.getAggregationType() == AggregateType.REPLACE_IF_NOT_NULL) {
                                        found = true;
                                        break;
                                    } else {
                                        // althrough the implemention supports bf for replace non-key column,
                                        // for simplicity and unity, we don't expose that to user.
                                        throw new AnalysisException(
                                                "BITMAP index only used in columns of DUP_KEYS table or "
                                                        + "key columns of UNIQUE_KEYS/AGG_KEYS table. invalid column: "
                                                        + indexColName);
                                    }
                                }
                            }
                        }

                        if (!found) {
                            throw new AnalysisException("BITMAP column does not exist in table. invalid column: "
                                    + indexColName);
                        }
                    }
                indexes.add(new Index(indexDef.getIndexName(), indexDef.getColumns(), indexDef.getIndexType(),
                        indexDef.getComment()));
                distinct.add(indexDef.getIndexName());
                distinctCol.add(indexDef.getColumns().stream().map(String::toUpperCase).collect(Collectors.toList()));
            }
            if (distinct.size() != indexes.size()) {
                throw new AnalysisException("index name must be unique.");
            }
            if (distinctCol.size() != indexes.size()) {
                throw new AnalysisException("same index columns have multiple index name is not allowed.");
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

        if (engineName.equals("mysql") || engineName.equals("broker") 
                || engineName.equals("elasticsearch")) {
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
        throw new RuntimeException("CreateTableStmt serialization is not supported anymore.");
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
        for (ColumnDef columnDef : columnDefs) {
            if (idx != 0) {
                sb.append(",\n");
            }
            sb.append("  ").append(columnDef.toSql());
            idx++;
        }
        if (CollectionUtils.isNotEmpty(indexDefs)) {
            sb.append(",\n");
            for (IndexDef indexDef : indexDefs) {
                sb.append("  ").append(indexDef.toSql());
            }
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

        if (rollupAlterClauseList != null && rollupAlterClauseList.size() != 0) {
            sb.append("\n rollup(");
            StringBuilder opsSb = new StringBuilder();
            for (int i = 0; i < rollupAlterClauseList.size(); i++) {
                opsSb.append(rollupAlterClauseList.get(i).toSql());
                if (i != rollupAlterClauseList.size() - 1) {
                    opsSb.append(",");
                }
            }
            sb.append(opsSb.toString().replace("ADD ROLLUP", "")).append(")");
        }

        // properties may contains password and other sensitive information,
        // so do not print properties.
        // This toSql() method is only used for log, user can see detail info by using show create table stmt,
        // which is implemented in Catalog.getDdlStmt()
        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, " = ", true, true, true));
            sb.append(")");
        }

        if (extProperties != null && !extProperties.isEmpty()) {
            sb.append("\n").append(engineName.toUpperCase()).append(" PROPERTIES (");
            sb.append(new PrintableMap<String, String>(extProperties, " = ", true, true, true));
            sb.append(")");
        }

        if (!Strings.isNullOrEmpty(comment)) {
            sb.append("\nCOMMENT \"").append(comment).append("\"");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean needAuditEncryption() {
        if (!engineName.equals("olap")) {
            return true;
        }
        return false;
    }
}
