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

package org.apache.doris.sdk.model;

import java.util.List;
import java.util.Objects;

public class Tablet {
    private List<String> routings;
    private int version;
    private long versionHash;
    private long schemaHash;

    public List<String> getRoutings() {
        return routings;
    }

    public int getVersion() {
        return version;
    }

    public long getVersionHash() {
        return versionHash;
    }


    public long getSchemaHash() {
        return schemaHash;
    }

    @Override
    public int hashCode() {
        return Objects.hash(routings, version, versionHash, schemaHash);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tablet tablet = (Tablet) o;
        return version == tablet.version && versionHash == tablet.versionHash && schemaHash == tablet.schemaHash
                && Objects.equals(routings, tablet.routings);
    }
}
