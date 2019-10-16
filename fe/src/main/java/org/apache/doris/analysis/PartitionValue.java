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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

public class PartitionValue {
    public static final PartitionValue MAX_VALUE = new PartitionValue();

    private String value;

    private PartitionValue() {

    }

    public PartitionValue(String value) {
        this.value = value;
    }

    public LiteralExpr getValue(Type type) throws AnalysisException {
        if (isMax()) {
            return LiteralExpr.createInfinity(type, true);
        } else {
            return LiteralExpr.create(value, type);
        }
    }

    public boolean isMax() {
        return this == MAX_VALUE;
    }

    public String getStringValue() {
        if (isMax()) {
            return "MAXVALUE";
        } else {
            return value;
        }
    }
}
