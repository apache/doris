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

import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.nereids.exceptions.AnalysisException;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

/** Validates row-change operations against the Paimon table capabilities. */
final class PaimonRowChangeCapabilities {
    private PaimonRowChangeCapabilities() {
    }

    static void checkUpdate(PaimonWriteTarget target, Collection<String> updatedColumns) {
        FileStoreTable table = target.getTable();
        requirePrimaryKey(table, "UPDATE");
        Set<String> primaryKeys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        primaryKeys.addAll(table.primaryKeys());
        for (String column : updatedColumns) {
            if (primaryKeys.contains(column)) {
                throw new AnalysisException("Paimon UPDATE cannot modify primary-key column '"
                        + column + "'");
            }
        }
        CoreOptions.MergeEngine engine = CoreOptions.fromMap(table.options()).mergeEngine();
        if (engine != CoreOptions.MergeEngine.DEDUPLICATE) {
            throw new AnalysisException("Paimon UPDATE only supports merge-engine=deduplicate; "
                    + "merge-engine=" + engine + " cannot preserve SQL UPDATE semantics");
        }
    }

    static void checkDelete(PaimonWriteTarget target) {
        FileStoreTable table = target.getTable();
        requirePrimaryKey(table, "DELETE");
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
                if (options.get(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE)
                        || options.getOptional(
                                CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP).isPresent()) {
                    return;
                }
                break;
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

    static void checkMerge(PaimonWriteTarget target, Collection<String> updatedColumns,
            boolean containsUpdate, boolean containsDelete) {
        requirePrimaryKey(target.getTable(), "MERGE");
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
}
