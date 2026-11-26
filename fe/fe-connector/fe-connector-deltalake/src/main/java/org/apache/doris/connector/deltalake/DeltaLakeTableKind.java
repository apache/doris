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


package org.apache.doris.connector.deltalake;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

enum DeltaLakeTableKind {
    DATA(null),
    HISTORY(new DeltaLakeHistorySystemTable()),
    DETAIL(new DeltaLakeDetailSystemTable()),
    FILES(new DeltaLakeFilesSystemTable()),
    PROPERTIES(new DeltaLakePropertiesSystemTable()),
    PARTITIONS(new DeltaLakePartitionsSystemTable());

    final DeltaLakeSystemTable systemTable;

    DeltaLakeTableKind(DeltaLakeSystemTable systemTable) {
        this.systemTable = systemTable;
    }

    static List<String> systemTableNames() {
        return Arrays.stream(values()).filter(kind -> kind.systemTable != null)
                .map(kind -> kind.systemTable.name()).toList();
    }

    static Optional<DeltaLakeTableKind> systemTableNamed(String name) {
        return Arrays.stream(values()).filter(kind -> kind.systemTable != null
                && kind.systemTable.name().equalsIgnoreCase(name)).findFirst();
    }
}
