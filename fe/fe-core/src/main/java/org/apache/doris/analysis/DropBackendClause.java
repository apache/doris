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

package org.apache.doris.analysis;

import lombok.Getter;

import java.util.List;

@Getter
public class DropBackendClause extends BackendClause {
    private final boolean force;

    public DropBackendClause(List<String> params) {
        super(params);
        this.force = true;
    }

    public DropBackendClause(List<String> params, boolean force) {
        super(params);
        this.force = force;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP BACKEND ");
        for (int i = 0; i < params.size(); i++) {
            sb.append("\"").append(params.get(i)).append("\"");
            if (i != params.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
