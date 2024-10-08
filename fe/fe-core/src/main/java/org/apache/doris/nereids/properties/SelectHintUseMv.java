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

import java.util.List;

/**
 * select hint UseMv.
 */
public class SelectHintUseMv extends SelectHint {
    private final List<String> parameters;

    private final boolean isUseMv;

    public SelectHintUseMv(String hintName, List<String> parameters, boolean isUseMv) {
        super(hintName);
        this.parameters = parameters;
        this.isUseMv = isUseMv;
    }

    public List<String> getParameters() {
        return parameters;
    }

    public boolean isUseMv() {
        return isUseMv;
    }

    @Override
    public String getHintName() {
        return super.getHintName();
    }

    @Override
    public String toString() {
        return super.getHintName();
    }
}
