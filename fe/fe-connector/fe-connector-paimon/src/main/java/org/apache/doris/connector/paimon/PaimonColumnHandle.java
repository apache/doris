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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.handle.ConnectorColumnHandle;

import java.util.Objects;

/**
 * Column handle for Paimon tables.
 * Carries the column name and its index in the Paimon RowType field list.
 */
public class PaimonColumnHandle implements ConnectorColumnHandle {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final int fieldIndex;

    public PaimonColumnHandle(String name, int fieldIndex) {
        this.name = Objects.requireNonNull(name, "name");
        this.fieldIndex = fieldIndex;
    }

    public String getName() {
        return name;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PaimonColumnHandle)) {
            return false;
        }
        PaimonColumnHandle that = (PaimonColumnHandle) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return name + "[" + fieldIndex + "]";
    }
}
