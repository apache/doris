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

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * UUID for {@link Group}.
 */
public class GroupId {
    private static final AtomicLong CURRENT_ID = new AtomicLong();
    private static final UUID JVM_ID = UUID.randomUUID();

    private final long id;
    private final UUID jvmId;

    public GroupId(long id, UUID jvmId) {
        this.id = id;
        this.jvmId = jvmId;
    }

    public static GroupId newPlanSetId() {
        return new GroupId(CURRENT_ID.getAndIncrement(), JVM_ID);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupId groupId = (GroupId) o;
        return id == groupId.id && jvmId.equals(groupId.jvmId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jvmId);
    }
}
