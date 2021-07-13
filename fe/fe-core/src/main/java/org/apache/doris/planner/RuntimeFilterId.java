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

package org.apache.doris.planner;

import org.apache.doris.common.Id;
import org.apache.doris.common.IdGenerator;

public class RuntimeFilterId extends Id<RuntimeFilterId> {
    // Construction only allowed via an IdGenerator.
    protected RuntimeFilterId(int id) {
        super(id);
    }

    public static IdGenerator<RuntimeFilterId> createGenerator() {
        return new IdGenerator<RuntimeFilterId>() {
            @Override
            public RuntimeFilterId getNextId() {
                return new RuntimeFilterId(nextId_++);
            }

            @Override
            public RuntimeFilterId getMaxId() {
                return new RuntimeFilterId(nextId_ - 1);
            }
        };
    }

    @Override
    public String toString() {
        return String.format("RF%03d", id);
    }

    @Override
    public int hashCode() {
        return id;
    }

    public int compareTo(RuntimeFilterId cmp) {
        return Integer.compare(id, cmp.id);
    }
}
