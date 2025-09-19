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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.AutoBucketUtils;
import org.apache.doris.common.util.ParseUtil;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Deprecated
public class CreateTableStmt extends DdlStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(CreateTableStmt.class);

    protected static final String DEFAULT_ENGINE_NAME = "olap";
    private static final ImmutableSet<AggregateType> GENERATED_COLUMN_ALLOW_AGG_TYPE =
            ImmutableSet.of(AggregateType.REPLACE, AggregateType.REPLACE_IF_NOT_NULL);

    protected boolean ifNotExists;
    private boolean isExternal;
    private boolean isTemp;
    protected TableName tableName;
    protected List<ColumnDef> columnDefs;
    private List<IndexDef> indexDefs;
    protected KeysDesc keysDesc;
    protected PartitionDesc partitionDesc;
    protected DistributionDesc distributionDesc;
    protected Map<String, String> properties;
    private Map<String, String> extProperties;
    protected String engineName;
    private String comment;
    private List<AlterClause> rollupAlterClauseList = Lists.newArrayList();

    private static Set<String> engineNames;

    // set in analyze
    private List<Column> columns = Lists.newArrayList();
    private List<Index> indexes = Lists.newArrayList();

    static {
        engineNames = Sets.newHashSet();
        engineNames.add("olap");
        engineNames.add("jdbc");
        engineNames.add("elasticsearch");
        engineNames.add("odbc");
        engineNames.add("mysql");
        engineNames.add("broker");
    }

    // if auto bucket enable, rewrite distribution bucket num &&
    // set properties[PropertyAnalyzer.PROPERTIES_AUTO_BUCKET] = "true"
    private static Map<String, String> maybeRewriteByAutoBucket(DistributionDesc distributionDesc,
            Map<String, String> properties) throws AnalysisException {
        if (distributionDesc == null || !distributionDesc.isAutoBucket()) {
            return properties;
        }

        // auto bucket is enable
        Map<String, String> newProperties = properties;
        if (newProperties == null) {
            newProperties = new HashMap<String, String>();
        }
        newProperties.put(PropertyAnalyzer.PROPERTIES_AUTO_BUCKET, "true");

        if (!newProperties.containsKey(PropertyAnalyzer.PROPERTIES_ESTIMATE_PARTITION_SIZE)) {
            distributionDesc.setBuckets(FeConstants.default_bucket_num);
        } else {
            long partitionSize = ParseUtil
                    .analyzeDataVolume(newProperties.get(PropertyAnalyzer.PROPERTIES_ESTIMATE_PARTITION_SIZE));
            distributionDesc.setBuckets(AutoBucketUtils.getBucketsNum(partitionSize, Config.autobucket_min_buckets));
        }

        return newProperties;
    }

    public CreateTableStmt() {
        // for persist
        tableName = new TableName();
        columnDefs = Lists.newArrayList();
    }

    public CreateTableStmt(boolean ifNotExists, boolean isExternal, TableName tableName,
            List<ColumnDef> columnDefinitions, String engineName, KeysDesc keysDesc, PartitionDesc partitionDesc,
            DistributionDesc distributionDesc, Map<String, String> properties, Map<String, String> extProperties,
            String comment) {
        this(ifNotExists, isExternal, tableName, columnDefinitions, null, engineName, keysDesc, partitionDesc,
                distributionDesc, properties, extProperties, comment, null);
    }

    public CreateTableStmt(boolean ifNotExists, boolean isExternal, TableName tableName,
            List<ColumnDef> columnDefinitions, String engineName, KeysDesc keysDesc, PartitionDesc partitionDesc,
            DistributionDesc distributionDesc, Map<String, String> properties, Map<String, String> extProperties,
            String comment, List<AlterClause> ops) {
        this(ifNotExists, isExternal, tableName, columnDefinitions, null, engineName, keysDesc, partitionDesc,
                distributionDesc, properties, extProperties, comment, ops);
    }

    public CreateTableStmt(boolean ifNotExists, boolean isExternal, TableName tableName,
            List<ColumnDef> columnDefinitions, List<IndexDef> indexDefs, String engineName, KeysDesc keysDesc,
            PartitionDesc partitionDesc, DistributionDesc distributionDesc, Map<String, String> properties,
            Map<String, String> extProperties, String comment, List<AlterClause> rollupAlterClauseList) {
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
        PropertyAnalyzer.getInstance().rewriteForceProperties(this.properties);
        this.extProperties = extProperties;
        this.isExternal = isExternal;
        this.ifNotExists = ifNotExists;
        this.comment = Strings.nullToEmpty(comment);

        this.rollupAlterClauseList = (rollupAlterClauseList == null) ? Lists.newArrayList() : rollupAlterClauseList;
    }

    // for Nereids
    public CreateTableStmt(boolean ifNotExists, boolean isExternal, boolean isTemp, TableName tableName,
            List<Column> columns, List<Index> indexes, String engineName, KeysDesc keysDesc,
            PartitionDesc partitionDesc, DistributionDesc distributionDesc, Map<String, String> properties,
            Map<String, String> extProperties, String comment,
            List<AlterClause> rollupAlterClauseList, Void unused) {
        this.ifNotExists = ifNotExists;
        this.isExternal = isExternal;
        this.isTemp = isTemp;
        this.tableName = tableName;
        this.columns = columns;
        this.indexes = indexes;
        this.engineName = engineName;
        this.keysDesc = keysDesc;
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
        PropertyAnalyzer.getInstance().rewriteForceProperties(this.properties);
        this.extProperties = extProperties;
        this.columnDefs = Lists.newArrayList();
        this.comment = Strings.nullToEmpty(comment);
        this.rollupAlterClauseList = (rollupAlterClauseList == null) ? Lists.newArrayList() : rollupAlterClauseList;
    }

    public void addColumnDef(ColumnDef columnDef) {
        columnDefs.add(columnDef);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public boolean isTemp() {
        return isTemp;
    }

    public void setTemp(boolean temp) {
        isTemp = temp;
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

    public void setDistributionDesc(DistributionDesc desc) {
        this.distributionDesc = desc;
    }

    public Map<String, String> getProperties() {
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        return this.properties;
    }

    public Map<String, String> getExtProperties() {
        return this.extProperties;
    }

    public String getEngineName() {
        return engineName;
    }

    public String getCatalogName() {
        return tableName.getCtl();
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public void setTableName(String newTableName) {
        tableName = new TableName(tableName.getCtl(), tableName.getDb(), newTableName);
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
    public void analyze() throws UserException {
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE ");
        if (isTemp) {
            sb.append("TEMPORARY ");
        }
        if (isExternal) {
            sb.append("EXTERNAL ");
        }
        sb.append("TABLE ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }
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
        sb.append(" ENGINE = ").append(engineName.toLowerCase());

        if (keysDesc != null) {
            sb.append("\n").append(keysDesc.toSql());
        }

        if (!Strings.isNullOrEmpty(comment)) {
            sb.append("\nCOMMENT \"").append(comment).append("\"");
        }

        if (partitionDesc != null) {
            sb.append("\n").append(partitionDesc.toSql());
        }

        if (distributionDesc != null) {
            sb.append("\n").append(distributionDesc.toSql());
        }

        if (rollupAlterClauseList != null && !rollupAlterClauseList.isEmpty()) {
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
            sb.append(new PrintableMap<>(properties, " = ", true, true, true));
            sb.append(")");
        }

        if (extProperties != null && !extProperties.isEmpty()) {
            sb.append("\n").append(engineName.toUpperCase()).append(" PROPERTIES (");
            sb.append(new PrintableMap<>(extProperties, " = ", true, true, true));
            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean needAuditEncryption() {
        return !engineName.equals("olap");
    }


    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
