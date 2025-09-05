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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;

import java.util.Map;
import java.util.Optional;

/**
 * Iceberg rewrite manifests action implementation.
 * This action rewrites manifest files in Iceberg tables to optimize
 * metadata performance and reduce the number of manifest files.
 */
public class IcebergRewriteManifestsAction extends BaseIcebergAction {
    public static final String USE_CACHING = "use_caching";
    public static final String SPEC_ID = "spec_id";

    public IcebergRewriteManifestsAction(Map<String, String> properties,
            Optional<PartitionNamesInfo> partitionNamesInfo,
            Optional<Expression> whereCondition,
            IcebergExternalTable icebergTable) {
        super("rewrite_manifests", properties, partitionNamesInfo, whereCondition, icebergTable);
    }

    @Override
    protected void validateIcebergAction() throws UserException {
        // Validate use_caching parameter
        if (properties.containsKey(USE_CACHING)) {
            String useCaching = properties.get(USE_CACHING);
            if (!"true".equalsIgnoreCase(useCaching) && !"false".equalsIgnoreCase(useCaching)) {
                throw new DdlException("use_caching must be 'true' or 'false'");
            }
        }

        // Validate spec_id parameter
        if (properties.containsKey(SPEC_ID)) {
            try {
                int specId = Integer.parseInt(properties.get(SPEC_ID));
                if (specId < 0) {
                    throw new DdlException("spec_id must be non-negative");
                }
            } catch (NumberFormatException e) {
                throw new DdlException("Invalid spec_id format: " + properties.get(SPEC_ID));
            }
        }

        // Iceberg procedures don't support partitions or where conditions
        validateNoPartitions();
        validateNoWhereCondition();
    }

    @Override
    public void execute(TableIf table) throws UserException {
        throw new DdlException("Iceberg rewrite_manifests procedure is not implemented yet");
    }

    @Override
    public String getDescription() {
        return "Rewrite Iceberg manifest files to optimize metadata performance";
    }
}
