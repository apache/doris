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

/**
 * placeholder id for prepared statement parameters
 */
public class PlaceholderId extends Id<PlaceholderId> implements Comparable<PlaceholderId> {

    public PlaceholderId(int id) {
        super(id);
    }

    /**
     * Should be only called by {@link org.apache.doris.nereids.StatementContext}.
     */
    public static IdGenerator<PlaceholderId> createGenerator() {
        return new IdGenerator<PlaceholderId>() {
            @Override
            public PlaceholderId getNextId() {
                return new PlaceholderId(nextId++);
            }
        };
    }

    @Override
    public String toString() {
        return "PlaceholderId#" + id;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public int compareTo(PlaceholderId o) {
        return this.id - o.id;
    }
}
