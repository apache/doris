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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.catalog.OlapTable;

import org.apache.commons.lang3.NotImplementedException;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * AlterOp
 */
public abstract class AlterOp {

    protected AlterOpType opType;

    public AlterOp(AlterOpType opType) {
        this.opType = opType;
    }

    public AlterOpType getOpType() {
        return opType;
    }

    public abstract boolean allowOpMTMV();

    public abstract boolean needChangeMTMVState();

    /**
     * The columns this operation gives the table or takes away from it, for the operations whose effect on
     * a view is decided by that view's own query; empty for every operation whose effect the query does not
     * decide.
     *
     * <p>Marked on the operations whose only way of reaching a view is through a column, in either
     * direction. A column that is dropped or renamed is either one the view's query reaches -- and then the
     * view cannot be computed from the table as it now stands, or is computed from another column that
     * answers to the same name -- or it is one the query never reaches, and the view's rows are the ones
     * they already were. A column that is added can move a name the query reaches: the name is answered by
     * the column nearest to it in the query's scopes, so one added there takes it away from wherever the
     * query reached it. A type change is neither: the query keeps analysing, and re-analysis compares the
     * columns the query produces, so a column the query only filters or joins on can change what the view's
     * rows mean while re-analysis reports nothing.
     *
     * <p>The names are what the judgement is about, rather than the operation as a whole: they are the
     * columns the view's query is asked about, and what the answer is checked against once it has been
     * given. See {@code MTMVRelationManager}, which asks the query and holds the answer to these names.
     *
     * <p>Default empty: the operation invalidates every view that reads the table.
     */
    public Set<String> queryJudgedColumnNames() {
        return Collections.emptySet();
    }

    /**
     * Whether the change this operation asks for has reached the table yet.
     *
     * <p>It is what a judgement about the materialized views that read the table has to wait for. A schema
     * change is applied by a job unless it is a light one, and the hook that judges the views runs where
     * such a job may not have run yet -- the table still holds the column the change takes away, and
     * re-analysing a view's query against it would answer for the table from before the change.
     *
     * <p>Default true: an operation whose effect is in the statement itself has nothing to wait for.
     */
    public boolean hasReachedTheTable(OlapTable table) {
        return true;
    }

    // Whether this alter operation is allowed on tables that enable row binlog.
    // Default is false, and only operations that are explicitly marked as safe
    // for row binlog should override this to return true.
    public boolean allowOpRowBinlog() {
        return false;
    }

    public Map<String, String> getProperties() {
        throw new NotImplementedException("AlterOp.getProperties() is not implemented");
    }
}
