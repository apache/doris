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

package org.apache.doris.nereids.processor.post.runtimeFilterV2;

import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TRuntimeFilterType;

import java.util.ArrayList;
import java.util.List;

public class RuntimeFilterV2Context {

    private final List<RuntimeFilterV2> rfsV2 = new ArrayList<>();

    private final List<TRuntimeFilterType> types = new ArrayList<>();

    private final IdGenerator<RuntimeFilterId> idGenerator;

    public void addRuntimeFilterV2(RuntimeFilterV2 rfv2) {
        rfsV2.add(rfv2);
    }

    public RuntimeFilterV2Context(IdGenerator<RuntimeFilterId> runtimeFilterIdGen) {
        int typesInt = 2;
        if (ConnectContext.get() != null) {
            typesInt = ConnectContext.get().getSessionVariable().getRuntimeFilterType();
        }
        for (TRuntimeFilterType type : TRuntimeFilterType.values()) {
            if ((type.getValue() & typesInt) > 0) {
                types.add(type);
            }
        }
        this.idGenerator = runtimeFilterIdGen;
    }

    public RuntimeFilterId nextId() {
        return idGenerator.getNextId();
    }

    public List<TRuntimeFilterType> getTypes() {
        return types;
    }

}
