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

package org.apache.doris.datasource.systable;

import org.apache.doris.analysis.TableValuedFunctionRef;
import org.apache.doris.nereids.trees.expressions.functions.table.IcebergMeta;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.tablefunction.IcebergTableValuedFunction;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

// table$snapshots
public class IcebergSnapshotsSysTable extends SysTable {
    private static final Logger LOG = LogManager.getLogger(IcebergSnapshotsSysTable.class);

    public static final IcebergSnapshotsSysTable INSTANCE = new IcebergSnapshotsSysTable();

    private IcebergSnapshotsSysTable() {
        super("snapshots", "iceberg_meta");
    }

    @Override
    public TableValuedFunction createFunction(String ctlName, String dbName, String sourceNameWithMetaName) {
        List<String> nameParts = Lists.newArrayList(ctlName, dbName,
                getSourceTableName(sourceNameWithMetaName));
        return IcebergMeta.createSnapshots(nameParts);
    }

    @Override
    public TableValuedFunctionRef createFunctionRef(String ctlName, String dbName, String sourceNameWithMetaName) {
        List<String> nameParts = Lists.newArrayList(ctlName, dbName,
                getSourceTableName(sourceNameWithMetaName));
        Map<String, String> params = Maps.newHashMap();
        params.put(IcebergTableValuedFunction.TABLE, Joiner.on(".").join(nameParts));
        params.put(IcebergTableValuedFunction.QUERY_TYPE, "snapshots");
        try {
            return new TableValuedFunctionRef(tvfName, null, params);
        } catch (org.apache.doris.common.AnalysisException e) {
            LOG.warn("should not happen. {}.{}.{}", ctlName, dbName, sourceNameWithMetaName, e);
            return null;
        }
    }
}
