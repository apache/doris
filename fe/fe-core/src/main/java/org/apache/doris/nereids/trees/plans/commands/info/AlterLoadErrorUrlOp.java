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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.util.PrintableMap;

import java.util.Map;

/**
 * ALTER SYSTEM SET LOAD ERRORS HUB properties("type" = "xxx");
 */
public class AlterLoadErrorUrlOp extends AlterSystemOp {
    private Map<String, String> properties;

    public AlterLoadErrorUrlOp(Map<String, String> properties) {
        super(AlterOpType.ALTER_OTHER);
        this.properties = properties;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES(");
        PrintableMap<String, String> printableMap = new PrintableMap<>(properties, "=", true, true, true);
        sb.append(printableMap.toString());
        sb.append(")");
        return sb.toString();
    }

}
