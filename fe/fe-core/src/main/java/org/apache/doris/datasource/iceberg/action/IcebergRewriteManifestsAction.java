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

package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ArgumentParsers;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.rewrite.RewriteManifestExecutor;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.Lists;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Action for rewriting Iceberg manifest files to optimize metadata layout
 */
public class IcebergRewriteManifestsAction extends BaseIcebergAction {
    private static final Logger LOG = LogManager.getLogger(IcebergRewriteManifestsAction.class);
    public static final String SPEC_ID = "spec_id";

    public IcebergRewriteManifestsAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition) {
        super("rewrite_manifests", properties, partitionNamesInfo, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        namedArguments.registerOptionalArgument(SPEC_ID,
                "Spec id of the manifests to rewrite (defaults to current spec id)",
                null,
                ArgumentParsers.intRange(SPEC_ID, 0, Integer.MAX_VALUE));
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(TableIf table) throws UserException {
        try {
            Table icebergTable = ((IcebergExternalTable) table).getIcebergTable();
            Snapshot current = icebergTable.currentSnapshot();
            if (current == null) {
                // No current snapshot means the table is empty, no manifests to rewrite
                return Lists.newArrayList("0", "0");
            }

            // Get optional spec_id parameter
            Integer specId = namedArguments.getInt(SPEC_ID);

            // Execute rewrite operation
            RewriteManifestExecutor executor = new RewriteManifestExecutor();
            RewriteManifestExecutor.Result result = executor.execute(
                    icebergTable,
                    (ExternalTable) table,
                    specId);

            return result.toStringList();
        } catch (Exception e) {
            LOG.warn("Failed to rewrite manifests for table: {}", table.getName(), e);
            throw new UserException("Rewrite manifests failed: " + e.getMessage(), e);
        }
    }

    @Override
    protected List<Column> getResultSchema() {
        return Lists.newArrayList(
                new Column("rewritten_manifests_count", Type.INT, false,
                        "Number of manifests which were re-written by this command"),
                new Column("added_manifests_count", Type.INT, false,
                        "Number of new manifest files which were written by this command")
        );
    }

    @Override
    public String getDescription() {
        return "Rewrite Iceberg manifest files to optimize metadata layout";
    }
}
