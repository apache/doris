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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Create unbound table sink
 */
public class UnboundTableSinkCreator {

    /**
     * create unbound sink without DML command
     */
    public static LogicalSink<? extends Plan> createUnboundTableSink(List<String> nameParts,
                List<String> colNames, List<String> hints, List<String> partitions, Plan query)
            throws UserException {
        String catalogName = RelationUtil.getQualifierName(ConnectContext.get(), nameParts).get(0);
        CatalogIf<?> curCatalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (curCatalog instanceof InternalCatalog) {
            return new UnboundTableSink<>(nameParts, colNames, hints, partitions, query);
        } else if (curCatalog instanceof HMSExternalCatalog) {
            return new UnboundHiveTableSink<>(nameParts, colNames, hints, partitions, query);
        } else if (curCatalog instanceof IcebergExternalCatalog) {
            return new UnboundIcebergTableSink<>(nameParts, colNames, hints, partitions, query);
        }
        throw new UserException("Load data to " + curCatalog.getClass().getSimpleName() + " is not supported.");
    }

    /**
     * create unbound sink for DML plan
     */
    public static LogicalSink<? extends Plan> createUnboundTableSink(List<String> nameParts,
                List<String> colNames, List<String> hints, boolean temporaryPartition, List<String> partitions,
                boolean isPartialUpdate, DMLCommandType dmlCommandType, LogicalPlan plan) {
        String catalogName = RelationUtil.getQualifierName(ConnectContext.get(), nameParts).get(0);
        CatalogIf<?> curCatalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (curCatalog instanceof InternalCatalog) {
            return new UnboundTableSink<>(nameParts, colNames, hints, temporaryPartition, partitions,
                    isPartialUpdate, dmlCommandType, Optional.empty(),
                    Optional.empty(), plan);
        } else if (curCatalog instanceof HMSExternalCatalog) {
            return new UnboundHiveTableSink<>(nameParts, colNames, hints, partitions,
                    dmlCommandType, Optional.empty(), Optional.empty(), plan);
        } else if (curCatalog instanceof IcebergExternalCatalog) {
            return new UnboundIcebergTableSink<>(nameParts, colNames, hints, partitions,
                    dmlCommandType, Optional.empty(), Optional.empty(), plan);
        }
        throw new RuntimeException("Load data to " + curCatalog.getClass().getSimpleName() + " is not supported.");
    }

    /**
     * create unbound sink for DML plan with auto detect overwrite partition enable.
     */
    public static LogicalSink<? extends Plan> createUnboundTableSinkMaybeOverwrite(List<String> nameParts,
            List<String> colNames, List<String> hints, boolean temporaryPartition, List<String> partitions,
            boolean isAutoDetectPartition, boolean isOverwrite, boolean isPartialUpdate, DMLCommandType dmlCommandType,
            LogicalPlan plan) {
        if (isAutoDetectPartition) { // partitions is null
            if (!isOverwrite) {
                throw new ParseException("ASTERISK is only supported in overwrite partition for OLAP table");
            }
            temporaryPartition = false;
            partitions = ImmutableList.of();
        }

        String catalogName = RelationUtil.getQualifierName(ConnectContext.get(), nameParts).get(0);
        CatalogIf<?> curCatalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (curCatalog instanceof InternalCatalog) {
            return new UnboundTableSink<>(nameParts, colNames, hints, temporaryPartition, partitions,
                    isAutoDetectPartition,
                    isPartialUpdate, dmlCommandType, Optional.empty(),
                    Optional.empty(), plan);
        } else if (curCatalog instanceof HMSExternalCatalog && !isAutoDetectPartition) {
            return new UnboundHiveTableSink<>(nameParts, colNames, hints, partitions,
                    dmlCommandType, Optional.empty(), Optional.empty(), plan);
        } else if (curCatalog instanceof IcebergExternalCatalog && !isAutoDetectPartition) {
            return new UnboundIcebergTableSink<>(nameParts, colNames, hints, partitions,
                dmlCommandType, Optional.empty(), Optional.empty(), plan);
        }
        // TODO: we need to support insert into other catalog
        try {
            if (ConnectContext.get() != null) {
                ConnectContext.get().getSessionVariable().enableFallbackToOriginalPlannerOnce();
            }
        } catch (Exception e) {
            // ignore this.
        }
        throw new AnalysisException(
                (isOverwrite ? "insert overwrite" : "insert") + " data to " + curCatalog.getClass().getSimpleName()
                        + " is not supported."
                        + (isAutoDetectPartition
                                ? " PARTITION(*) is only supported in overwrite partition for OLAP table" : ""));
    }
}
