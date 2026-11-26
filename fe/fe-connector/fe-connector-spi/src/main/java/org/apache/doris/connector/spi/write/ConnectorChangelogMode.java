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

package org.apache.doris.connector.spi.write;

import java.util.Objects;

/** Connector-owned encoding of row operations in a changelog write. */
public final class ConnectorChangelogMode {
    private final String operationColumnName;
    private final byte insertValue;
    private final byte updateValue;
    private final byte deleteValue;

    public ConnectorChangelogMode(String operationColumnName,
            byte insertValue, byte updateValue, byte deleteValue) {
        this.operationColumnName = Objects.requireNonNull(operationColumnName, "operationColumnName");
        if (operationColumnName.isEmpty()) {
            throw new IllegalArgumentException("Changelog operation column name must not be empty");
        }
        if (insertValue == updateValue || insertValue == deleteValue || updateValue == deleteValue) {
            throw new IllegalArgumentException("Changelog operation values must be distinct");
        }
        this.insertValue = insertValue;
        this.updateValue = updateValue;
        this.deleteValue = deleteValue;
    }

    public String getOperationColumnName() {
        return operationColumnName;
    }

    public byte getInsertValue() {
        return insertValue;
    }

    public byte getUpdateValue() {
        return updateValue;
    }

    public byte getDeleteValue() {
        return deleteValue;
    }
}
