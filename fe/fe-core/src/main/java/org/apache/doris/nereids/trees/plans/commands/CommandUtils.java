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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;

import java.util.List;

/**
 * delete from unique key table.
 */
public class CommandUtils {

    /**
     * check delete target table should unique key olap table. If ok, return it.
     */
    public static OlapTable checkAndGetDeleteTargetTable(ConnectContext ctx, List<String> nameParts) {
        List<String> qualifiedTableName = RelationUtil.getQualifierName(ctx, nameParts);
        TableIf table = RelationUtil.getTable(qualifiedTableName, ctx.getEnv());
        if (!(table instanceof OlapTable)) {
            throw new AnalysisException("table must be olapTable in delete command");
        }
        OlapTable targetTable = ((OlapTable) table);
        if (targetTable.getKeysType() != KeysType.UNIQUE_KEYS) {
            throw new AnalysisException("Nereids only support delete command on unique key table now");
        }
        return targetTable;
    }
}
