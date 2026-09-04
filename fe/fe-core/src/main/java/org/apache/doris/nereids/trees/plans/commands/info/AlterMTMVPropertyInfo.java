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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVPropertyUtil;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * rename
 */
public class AlterMTMVPropertyInfo extends AlterMTMVInfo {
    private final Map<String, String> properties;

    /**
     * constructor for alter MTMV
     */
    public AlterMTMVPropertyInfo(TableNameInfo mvName, Map<String, String> properties) {
        super(mvName);
        this.properties = Objects.requireNonNull(properties, "require properties object");
    }

    public void analyze(ConnectContext ctx) throws AnalysisException {
        super.analyze(ctx);
        analyzeProperties();
        validateIncrementalExcludedTriggerTablesCompat(ctx);
    }

    @Override
    public void run() throws UserException {
        Env.getCurrentEnv().alterMTMVProperty(this);
    }

    private void analyzeProperties() {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_IVM_USE_FULL_KEYS)) {
            throw new AnalysisException("Property '" + PropertyAnalyzer.PROPERTIES_IVM_USE_FULL_KEYS
                    + "' cannot be altered. It is fixed at creation time.");
        }
        for (String key : properties.keySet()) {
            MTMVPropertyUtil.analyzeProperty(key, properties.get(key));
        }
        validateIvmOnlyProperties();
    }

    private void validateIvmOnlyProperties() {
        if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT)) {
            return;
        }
        MTMV mtmv;
        try {
            mtmv = (MTMV) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException(getMvName().getDb())
                    .getTableOrMetaException(getMvName().getTbl(), TableIf.TableType.MATERIALIZED_VIEW);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        if (!mtmv.isIvm()) {
            throw new AnalysisException("Property '" + PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT
                    + "' can only be set on IVM materialized views (REFRESH INCREMENTAL)");
        }
        MTMVRelation relation = mtmv.getRelation();
        if (relation == null || relation.getBaseTables() == null) {
            return;
        }
        Set<TableNameInfo> baseTableNames = relation.getBaseTables().stream()
                .map(baseTableInfo -> new TableNameInfo(baseTableInfo.getCtlName(),
                        baseTableInfo.getDbName(), baseTableInfo.getTableName()))
                .collect(Collectors.toSet());
        Map<TableNameInfo, Integer> windowLimits = MTMVPropertyUtil.getIvmPartitionWindowLimit(properties);
        for (TableNameInfo configured : windowLimits.keySet()) {
            boolean matched = baseTableNames.stream()
                    .anyMatch(baseTableName -> MTMVPartitionUtil.isTableNamelike(configured, baseTableName));
            if (!matched) {
                throw new AnalysisException("valid " + PropertyAnalyzer.PROPERTIES_IVM_PARTITION_WINDOW_LIMIT
                        + ": table '" + configured.getTbl() + "' is not a base table of the materialized view");
            }
        }
        for (TableNameInfo baseTableName : baseTableNames) {
            MTMVPropertyUtil.getPartitionWindowLimit(windowLimits, baseTableName);
        }
    }

    private void validateIncrementalExcludedTriggerTablesCompat(ConnectContext ctx) {
        if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)) {
            return;
        }
        try {
            MTMV mtmv = (MTMV) Env.getCurrentInternalCatalog()
                    .getDbOrDdlException(getMvName().getDb())
                    .getTableOrMetaException(getMvName().getTbl(), TableIf.TableType.MATERIALIZED_VIEW);
            if (!mtmv.isIvm()) {
                return;
            }
            Map<String, String> mergedMvProps = Maps.newHashMap(mtmv.getMvProperties());
            mergedMvProps.putAll(properties);
            MTMVPlanUtil.validateAlterExcludedTriggerTables(mtmv, mergedMvProps, ctx);
        } catch (AnalysisException e) {
            throw e;
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
