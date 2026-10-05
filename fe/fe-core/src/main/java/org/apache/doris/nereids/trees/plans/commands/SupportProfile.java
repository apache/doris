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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;

import java.util.List;
import java.util.Optional;

/**
 * Commands that can generate query profiles.
 */
public interface SupportProfile {

    List<String> getTargetTableNameParts();

    /**
     * Check whether the target table is an OLAP table.
     */
    default boolean isTargetTableOlap(ConnectContext ctx) {
        try {
            List<String> qualifiedTableName = RelationUtil.getQualifierName(ctx, getTargetTableNameParts());
            TableIf table = RelationUtil.getTable(qualifiedTableName, ctx.getEnv(), Optional.empty());
            return table instanceof OlapTable;
        } catch (Exception e) {
            return false;
        }
    }
}
