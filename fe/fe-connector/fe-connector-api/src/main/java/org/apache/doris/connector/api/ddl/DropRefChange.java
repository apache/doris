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

package org.apache.doris.connector.api.ddl;

/**
 * Neutral carrier for a {@code DROP BRANCH}/{@code DROP TAG} request, decoupling the connector SPI from the
 * fe-core/nereids {@code DropBranchInfo}/{@code DropTagInfo} types. {@code ifExists} makes the drop a no-op when
 * the named ref is absent.
 */
public final class DropRefChange {

    private final String name;
    private final boolean ifExists;

    public DropRefChange(String name, boolean ifExists) {
        this.name = name;
        this.ifExists = ifExists;
    }

    public String getName() {
        return name;
    }

    public boolean isIfExists() {
        return ifExists;
    }
}
