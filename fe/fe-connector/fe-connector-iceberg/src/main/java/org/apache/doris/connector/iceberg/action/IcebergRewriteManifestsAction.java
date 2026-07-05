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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.foundation.util.ArgumentParsers;

import com.google.common.collect.Lists;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Rewrites the iceberg manifest files to optimize metadata layout. Connector port of legacy
 * {@code IcebergRewriteManifestsAction}, delegating to the connector {@link RewriteManifestExecutor}. Bug-for-bug
 * preserved: an empty table (no current snapshot) short-circuits to {@code ["0", "0"]}, and the executor's own
 * "Failed to rewrite manifests: ..." message is double-wrapped by this body's "Rewrite manifests failed: ..."
 * handler, exactly as legacy.
 */
public class IcebergRewriteManifestsAction extends BaseIcebergAction {
    private static final Logger LOG = LogManager.getLogger(IcebergRewriteManifestsAction.class);
    public static final String SPEC_ID = "spec_id";

    public IcebergRewriteManifestsAction(Map<String, String> properties, List<String> partitionNames,
            ConnectorPredicate whereCondition) {
        super("rewrite_manifests", properties, partitionNames, whereCondition);
    }

    @Override
    protected void registerIcebergArguments() {
        namedArguments.registerOptionalArgument(SPEC_ID,
                "Spec id of the manifests to rewrite (defaults to current spec id)",
                null,
                ArgumentParsers.intRange(SPEC_ID, 0, Integer.MAX_VALUE));
    }

    @Override
    protected void validateIcebergAction() {
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    protected List<String> executeAction(Table icebergTable, ConnectorSession session) {
        try {
            Snapshot current = icebergTable.currentSnapshot();
            if (current == null) {
                // No current snapshot means the table is empty, no manifests to rewrite
                return Lists.newArrayList("0", "0");
            }

            // Get optional spec_id parameter
            Integer specId = namedArguments.getInt(SPEC_ID);

            // Execute rewrite operation
            RewriteManifestExecutor executor = new RewriteManifestExecutor();
            RewriteManifestExecutor.Result result = executor.execute(icebergTable, specId);

            return result.toStringList();
        } catch (Exception e) {
            LOG.warn("Failed to rewrite manifests for table: {}", icebergTable.name(), e);
            throw new DorisConnectorException("Rewrite manifests failed: " + e.getMessage(), e);
        }
    }

    @Override
    protected List<ConnectorColumn> getResultSchema() {
        return Lists.newArrayList(
                new ConnectorColumn("rewritten_manifests_count", ConnectorType.of("INT"),
                        "Number of manifests which were re-written by this command", false, null),
                new ConnectorColumn("added_manifests_count", ConnectorType.of("INT"),
                        "Number of new manifest files which were written by this command", false, null)
        );
    }
}
