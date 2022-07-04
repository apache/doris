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

package org.apache.doris.nereids.memo;

import org.apache.doris.common.Id;
import org.apache.doris.common.IdGenerator;

/**
 * UUID for {@link Group}.
 */
public class GroupId extends Id<GroupId> {
    protected GroupId(int id) {
        super(id);
    }

    /**
     * create a group id generator.
     *
     * @return group id generator
     */
    public static IdGenerator<GroupId> createGenerator() {
        return new IdGenerator<GroupId>() {
            @Override
            public GroupId getNextId() {
                return new GroupId(nextId++);
            }

            @Override
            public GroupId getMaxId() {
                return new GroupId(nextId - 1);
            }
        };
    }

    @Override
    public String toString() {
        return "GroupId#" + id;
    }
}
