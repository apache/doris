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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/TableName.java
// and modified by Doris

package org.apache.doris.nereids.trees.plans.commands.info;

import java.util.Objects;

/**
 * alias info
 */
public class AliasInfo {
    private final String name;
    private final String alias;

    public AliasInfo(String name, String alias) {
        this.name = Objects.requireNonNull(name);
        this.alias = alias;
    }

    public static AliasInfo of(String name, String alias) {
        return new AliasInfo(name, alias);
    }

    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }

    @Override
    public String toString() {
        return alias == null || alias.isEmpty()
                ? String.format("%s", name)
                : String.format("%s AS `%s`", name, alias);
    }
}
