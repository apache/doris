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

package org.apache.doris.rewrite.mvrewrite;

import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.MaterializedIndexMeta;

public class SlotRefEqualRule implements MVExprEqualRule {

    public static MVExprEqualRule INSTANCE = new SlotRefEqualRule();

    private String normalize(String name) {
        return MaterializedIndexMeta
                .normalizeName(CreateMaterializedViewStmt.mvColumnBreaker(name));
    }

    @Override
    public boolean equal(Expr queryExpr, Expr mvColumnExpr) {
        if ((!(queryExpr instanceof SlotRef)) || (!(mvColumnExpr instanceof SlotRef))) {
            return false;
        }
        SlotRef querySlotRef = (SlotRef) queryExpr;
        SlotRef mvColumnSlotRef = (SlotRef) mvColumnExpr;

        if (querySlotRef.getColumnName() != null
                && normalize(querySlotRef.getColumnName()).equalsIgnoreCase(
                    normalize(mvColumnSlotRef.getColumnName()))) {
            return true;
        }
        return false;
    }
}
