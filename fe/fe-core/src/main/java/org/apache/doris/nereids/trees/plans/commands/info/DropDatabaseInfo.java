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

import java.util.List;

/**
 * drop database info
 */
public class DropDatabaseInfo {
    private boolean ifExists;
    private String catalogName;
    private String databaseName;
    private boolean force;

    /**
     * Constructor for DropDatabaseInfo.
     */
    public DropDatabaseInfo(boolean ifExists, List<String> databaseNameParts, boolean force) {
        this.ifExists = ifExists;
        if (databaseNameParts.size() == 2) {
            this.catalogName = databaseNameParts.get(0);
            this.databaseName = databaseNameParts.get(1);
        } else if (databaseNameParts.size() == 1) {
            this.catalogName = "";
            this.databaseName = databaseNameParts.get(0);
        }
        this.force = force;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public boolean isForce() {
        return force;
    }
}
