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

package org.apache.doris.nereids.hint;

import java.util.List;

/**
 * rule hint.
 */
public class UseIndexHint extends Hint {

    private final boolean isNotUseIndex;

    private final boolean isAllIndex;

    private final List<String> parameters;

    public UseIndexHint(String hintName, List<String> parameters, boolean isNotUseIndex, boolean isAllIndex) {
        super(hintName);
        this.parameters = parameters;
        this.isNotUseIndex = isNotUseIndex;
        this.isAllIndex = isAllIndex;
    }

    public boolean isNotUseIndex() {
        return isNotUseIndex;
    }

    public boolean isAllIndex() {
        return isAllIndex;
    }

    public List<String> getParameters() {
        return parameters;
    }

    @Override
    public String getExplainString() {
        StringBuilder out = new StringBuilder();
        if (isNotUseIndex) {
            out.append("no_use_index");
        } else {
            out.append("use_index");
        }
        if (!parameters.isEmpty()) {
            out.append("(");
            out.append(parameters);
            out.append(")");
        }

        return out.toString();
    }
}
