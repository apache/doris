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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.CreateMTMVStmt;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mtmv.EnvInfo;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * MTMV info in creating MTMV.
 */
public class CreateMTMVInfo {
    private final boolean ifNotExists;
    private final TableNameInfo mvName;
    private List<String> keys;
    private final String comment;
    private final DistributionDescriptor distribution;
    private Map<String, String> properties;
    private Map<String, String> mvProperties = Maps.newHashMap();

    private final LogicalPlan logicalQuery;
    private final String querySql;
    private final MTMVRefreshInfo refreshInfo;
    private final List<ColumnDefinition> columns = Lists.newArrayList();
    private final List<SimpleColumnDefinition> simpleColumnDefinitions;
    private final EnvInfo envInfo;

    /**
     * constructor for create MTMV
     */
    public CreateMTMVInfo(boolean ifNotExists, TableNameInfo mvName,
            List<String> keys, String comment,
            DistributionDescriptor distribution, Map<String, String> properties,
            LogicalPlan logicalQuery, String querySql,
            MTMVRefreshInfo refreshInfo,
            List<SimpleColumnDefinition> simpleColumnDefinitions) {
        this.ifNotExists = Objects.requireNonNull(ifNotExists, "require ifNotExists object");
        this.mvName = Objects.requireNonNull(mvName, "require mvName object");
        this.keys = Utils.copyRequiredList(keys);
        this.comment = comment;
        this.distribution = Objects.requireNonNull(distribution, "require distribution object");
        this.properties = properties;
        this.logicalQuery = Objects.requireNonNull(logicalQuery, "require logicalQuery object");
        this.querySql = Objects.requireNonNull(querySql, "require querySql object");
        this.refreshInfo = Objects.requireNonNull(refreshInfo, "require refreshInfo object");
        this.simpleColumnDefinitions = simpleColumnDefinitions;
        this.envInfo = new EnvInfo(ConnectContext.get().getDefaultCatalog(), ConnectContext.get().getDatabase());
    }

    /**
     * analyze create table info
     */
    public void analyze(ConnectContext ctx) {
        // analyze table name
        mvName.analyze(ctx);
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), mvName.getDb(),
                mvName.getTbl(), PrivPredicate.CREATE)) {
            throw new AnalysisException("Access denied");
        }
        analyzeProperties();
        analyzeQuery(ctx);
        // analyze column
        final boolean finalEnableMergeOnWrite = false;
        Set<String> keysSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        keysSet.addAll(keys);
        columns.forEach(c -> c.validate(keysSet, finalEnableMergeOnWrite, KeysType.DUP_KEYS));

        if (distribution == null) {
            throw new AnalysisException("Create MTMV should contain distribution desc");
        }

        if (properties == null) {
            properties = Maps.newHashMap();
        }

        // analyze distribute
        Map<String, ColumnDefinition> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        columns.forEach(c -> columnMap.put(c.getName(), c));
        distribution.updateCols(columns.get(0).getName());
        distribution.validate(columnMap, KeysType.DUP_KEYS);
        refreshInfo.validate();

        analyzeProperties();
    }

    private void analyzeProperties() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD)) {
            String gracePeriod = properties.get(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD);
            try {
                Long.parseLong(gracePeriod);
            } catch (NumberFormatException e) {
                throw new AnalysisException(
                        "valid grace_period: " + properties.get(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD));
            }
            mvProperties.put(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD, gracePeriod);
            properties.remove(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD);
        }
    }

    /**
     * analyzeQuery
     */
    public void analyzeQuery(ConnectContext ctx) {
    }

    /**
     * translate to catalog CreateMultiTableMaterializedViewStmt
     */
    public CreateMTMVStmt translateToLegacyStmt() {
        TableName tableName = mvName.transferToTableName();
        KeysDesc keysDesc = new KeysDesc(KeysType.DUP_KEYS, keys);
        List<Column> catalogColumns = columns.stream()
                .map(ColumnDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());
        return new CreateMTMVStmt(ifNotExists, tableName, catalogColumns, refreshInfo, keysDesc,
                distribution.translateToCatalogStyle(), properties, mvProperties, querySql, comment, envInfo);
    }
}
