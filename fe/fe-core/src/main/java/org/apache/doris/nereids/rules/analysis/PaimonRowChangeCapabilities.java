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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.plans.commands.info.PaimonRowChangeSpec;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeMatchedClause;
import org.apache.doris.qe.ConnectContext;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** Validates row-change operations against the Paimon table capabilities. */
final class PaimonRowChangeCapabilities {
    private PaimonRowChangeCapabilities() {
    }

    static void check(PaimonWriteTarget target, PaimonRowChangeSpec spec,
            CascadesContext cascadesContext) {
        requireNoDataMask(target, spec, cascadesContext);
        requireCompleteKeyDynamicIndex(target.getTable(), spec);
        if (spec instanceof PaimonRowChangeSpec.Update) {
            checkUpdate(target,
                    updatedColumns(((PaimonRowChangeSpec.Update) spec).getAssignments()));
        } else if (spec instanceof PaimonRowChangeSpec.Delete) {
            checkDelete(target);
        } else if (spec instanceof PaimonRowChangeSpec.Merge) {
            checkMerge(target, (PaimonRowChangeSpec.Merge) spec);
        } else {
            throw new AnalysisException("Unsupported Paimon row-change specification: "
                    + spec.getClass().getSimpleName());
        }
    }

    private static void checkMerge(PaimonWriteTarget target, PaimonRowChangeSpec.Merge merge) {
        Set<String> updatedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        boolean containsUpdate = false;
        boolean containsDelete = false;
        for (MergeMatchedClause clause : merge.getMatchedClauses()) {
            containsDelete |= clause.isDelete();
            containsUpdate |= !clause.isDelete();
            updatedColumns.addAll(updatedColumns(clause.getAssignments()));
        }
        checkMergeCapabilities(target, updatedColumns, containsUpdate, containsDelete);
    }

    private static Set<String> updatedColumns(List<EqualTo> assignments) {
        Set<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (EqualTo assignment : assignments) {
            List<String> parts = ((UnboundSlot) assignment.left()).getNameParts();
            columns.add(parts.get(parts.size() - 1));
        }
        return columns;
    }

    private static void checkUpdate(PaimonWriteTarget target, Collection<String> updatedColumns) {
        FileStoreTable table = target.getTable();
        requirePrimaryKey(table, "UPDATE");
        CoreOptions options = CoreOptions.fromMap(table.options());
        requireNoRowKindField(options, "UPDATE");
        if (options.changelogProducer() == CoreOptions.ChangelogProducer.INPUT) {
            throw new AnalysisException("Paimon UPDATE is not supported when "
                    + "changelog-producer=input because both UPDATE_BEFORE and UPDATE_AFTER "
                    + "records are required");
        }
        Set<String> primaryKeys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        primaryKeys.addAll(table.primaryKeys());
        Set<String> partitionKeys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        partitionKeys.addAll(table.partitionKeys());
        Set<String> sequenceFields = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        sequenceFields.addAll(options.sequenceField());
        for (String column : updatedColumns) {
            if (primaryKeys.contains(column)) {
                throw new AnalysisException("Paimon UPDATE cannot modify primary-key column '"
                        + column + "'");
            }
            if (sequenceFields.contains(column)) {
                throw new AnalysisException("Paimon UPDATE cannot modify sequence-field column '"
                        + column + "'");
            }
            if (partitionKeys.contains(column)) {
                if (options.bucket() != -1) {
                    throw new AnalysisException("Paimon UPDATE cannot modify partition column '"
                            + column + "' unless bucket=-1 because the old partition row cannot be "
                            + "removed without an UPDATE_BEFORE record");
                }
                if (options.ignoreDelete()) {
                    throw new AnalysisException("Paimon UPDATE cannot modify partition column '"
                            + column + "' when ignore-delete=true because removing the old "
                            + "partition row requires a delete record");
                }
            }
        }
        CoreOptions.MergeEngine engine = options.mergeEngine();
        if (engine != CoreOptions.MergeEngine.DEDUPLICATE) {
            throw new AnalysisException("Paimon UPDATE only supports merge-engine=deduplicate; "
                    + "merge-engine=" + engine + " cannot preserve SQL UPDATE semantics");
        }
    }

    private static void checkDelete(PaimonWriteTarget target) {
        FileStoreTable table = target.getTable();
        requirePrimaryKey(table, "DELETE");
        requireNoRowKindField(CoreOptions.fromMap(table.options()), "DELETE");
        Options options = Options.fromMap(table.options());
        if (options.get(CoreOptions.IGNORE_DELETE)) {
            throw new AnalysisException("Paimon DELETE is not supported when ignore-delete=true "
                    + "because the delete record would be ignored");
        }
        CoreOptions.MergeEngine engine = options.get(CoreOptions.MERGE_ENGINE);
        switch (engine) {
            case DEDUPLICATE:
                return;
            case PARTIAL_UPDATE:
                if (options.get(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE)) {
                    return;
                }
                throw new AnalysisException("Paimon DELETE on merge-engine=partial-update requires "
                        + "partial-update.remove-record-on-delete=true because "
                        + "partial-update.remove-record-on-sequence-group does not guarantee "
                        + "whole-row deletion");
            case AGGREGATE:
                if (options.get(CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE)) {
                    return;
                }
                break;
            default:
                break;
        }
        throw new AnalysisException("Paimon DELETE does not support merge-engine=" + engine
                + " with the current table options");
    }

    private static void checkMergeCapabilities(PaimonWriteTarget target,
            Collection<String> updatedColumns,
            boolean containsUpdate, boolean containsDelete) {
        requirePrimaryKey(target.getTable(), "MERGE");
        requireNoRowKindField(CoreOptions.fromMap(target.getTable().options()), "MERGE");
        if (containsUpdate) {
            checkUpdate(target, updatedColumns);
        }
        if (containsDelete) {
            checkDelete(target);
        }
    }

    private static void requirePrimaryKey(FileStoreTable table, String operation) {
        if (table.primaryKeys().isEmpty()) {
            throw new AnalysisException("Paimon " + operation + " requires a primary-key table");
        }
    }

    private static void requireCompleteKeyDynamicIndex(
            FileStoreTable table, PaimonRowChangeSpec spec) {
        CoreOptions options = CoreOptions.fromMap(table.options());
        if (table.bucketMode() == BucketMode.KEY_DYNAMIC
                && options.crossPartitionUpsertIndexTtl() != null) {
            throw new AnalysisException("Paimon " + spec.getDmlCommandType()
                    + " is not supported when cross-partition-upsert.index-ttl is configured "
                    + "because row-change DML requires a complete key-dynamic index");
        }
    }

    private static void requireNoRowKindField(CoreOptions options, String operation) {
        if (options.rowkindField().isPresent()) {
            throw new AnalysisException("Paimon " + operation
                    + " is not supported when rowkind.field is configured because it overrides "
                    + "the row-change operation");
        }
    }

    private static void requireNoDataMask(PaimonWriteTarget target,
            PaimonRowChangeSpec spec, CascadesContext cascadesContext) {
        ConnectContext connectContext = cascadesContext.getConnectContext();
        UserIdentity user = connectContext.getCurrentUserIdentity();
        if (user.isRootUser() || user.isAdminUser()) {
            return;
        }

        PaimonExternalTable table = target.getDorisTable();
        DatabaseIf database = table.getDatabase();
        if (database == null || database.getCatalog() == null) {
            return;
        }
        CatalogIf catalog = database.getCatalog();
        Set<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Column column : target.getSchema()) {
            columns.add(column.getName());
        }
        AccessControllerManager accessManager = connectContext.getEnv().getAccessManager();
        boolean hasDataMask = accessManager.evalDataMaskPolicies(
                user, catalog.getName(), database.getFullName(), table.getName(), columns)
                .values().stream().anyMatch(policy -> policy.isPresent());
        if (hasDataMask) {
            throw new AnalysisException("Paimon " + spec.getDmlCommandType()
                    + " is not supported when data masking policies apply to the target table");
        }
    }
}
