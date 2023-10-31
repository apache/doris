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

package org.apache.doris.nereids.properties;

import java.util.Objects;

/**
 * statement hint.
 * e.g. set_var(query_timeout='1800', exec_mem_limit='2147483648')
 */
public class StatementHint {
    // e.g. set_var
    private String hintName;

    public StatementHint(String hintName) {
        this.hintName = Objects.requireNonNull(hintName, "hintName can not be null");
    }

    public void setHintName(String hintName) {
        this.hintName = hintName;
    }

    public String getHintName() {
        return hintName;
    }
}
