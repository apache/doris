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

package org.apache.doris.nereids;

import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.statistics.model.Statistics;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * White-box access to {@link StatementContext} internals for unit tests: the statement-scope
 * caches (MVCC relations, view infos, connector scope, id→statistics) are reached through
 * reflection instead of production-side test accessors.
 */
public final class StatementContextTestAccess {

    private StatementContextTestAccess() {
    }

    private static Object field(StatementContext context, String name) {
        try {
            Field f = StatementContext.class.getDeclaredField(name);
            f.setAccessible(true);
            return f.get(context);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static int size(StatementContext context, String name) {
        return ((Map<?, ?>) field(context, name)).size();
    }

    /** Whether the connector statement scope is still cached (see C5). */
    public static boolean hasConnectorStatementScope(StatementContext context) {
        return field(context, "connectorStatementScope") != null;
    }

    /** The number of cached MVCC snapshots / fences / scan params (see C5). */
    public static int mvccRelationCacheSize(StatementContext context) {
        return size(context, "snapshots") + size(context, "latestSnapshots")
                + size(context, "latestSnapshotFences") + size(context, "resolvedSnapshotScanParams");
    }

    /** The number of cached resolved view infos (see C5). */
    public static int viewInfoCacheSize(StatementContext context) {
        return size(context, "viewInfos");
    }

    /** The per-statement id → statistics map (planner cache of matched statistics). */
    @SuppressWarnings("unchecked")
    public static Map<RelationId, Statistics> relationIdToStatisticsMap(StatementContext context) {
        return (Map<RelationId, Statistics>) field(context, "relationIdToStatisticsMap");
    }
}
