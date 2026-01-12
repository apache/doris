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

package org.apache.doris.nereids.trees.plans.commands.merge;

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * matched clause for merge into.
 */
public class MergeMatchedClause {

    private final Optional<Expression> casePredicate;
    private final List<EqualTo> assignments;
    private final boolean isDelete;

    public MergeMatchedClause(Optional<Expression> casePredicate, List<EqualTo> assignments, boolean isDelete) {
        this.casePredicate = Objects.requireNonNull(casePredicate, "casePredicate should not be null");
        this.assignments = Utils.fastToImmutableList(
                Objects.requireNonNull(assignments, "assignments should not be null"));
        this.isDelete = isDelete;
    }

    public Optional<Expression> getCasePredicate() {
        return casePredicate;
    }

    public List<EqualTo> getAssignments() {
        return assignments;
    }

    public boolean isDelete() {
        return isDelete;
    }
}
