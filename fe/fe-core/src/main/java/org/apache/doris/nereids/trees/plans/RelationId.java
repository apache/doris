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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.common.Id;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;

/**
 * relation id
 */
public class RelationId extends Id<RelationId> {

    public RelationId(int id) {
        super(id);
    }

    /**
     * Should be only called by {@link StatementScopeIdGenerator}.
     */
    public static IdGenerator<RelationId> createGenerator() {
        return new IdGenerator<RelationId>() {
            @Override
            public RelationId getNextId() {
                return new RelationId(nextId++);
            }
        };
    }

    @Override
    public String toString() {
        return "RelationId#" + id;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
