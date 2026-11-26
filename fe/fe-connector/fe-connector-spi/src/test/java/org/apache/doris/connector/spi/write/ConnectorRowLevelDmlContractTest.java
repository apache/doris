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

import org.apache.doris.connector.spi.handle.WriteOperation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;

class ConnectorRowLevelDmlContractTest {

    @Test
    void changelogModeOwnsOperationColumnAndValues() {
        ConnectorChangelogMode mode = new ConnectorChangelogMode("row_operation", (byte) 3, (byte) 5, (byte) 7);

        Assertions.assertEquals("row_operation", mode.getOperationColumnName());
        Assertions.assertEquals(3, mode.getInsertValue());
        Assertions.assertEquals(5, mode.getUpdateValue());
        Assertions.assertEquals(7, mode.getDeleteValue());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorChangelogMode("row_operation", (byte) 3, (byte) 3, (byte) 7));
    }

    @Test
    void rowLevelRequestCopiesUpdatedColumnsCaseInsensitively() {
        ConnectorRowLevelDmlRequest request = new ConnectorRowLevelDmlRequest(
                WriteOperation.MERGE, new HashSet<>(Arrays.asList("Value", "value")), true, true);

        Assertions.assertEquals(WriteOperation.MERGE, request.getOperation());
        Assertions.assertEquals(1, request.getUpdatedColumns().size());
        Assertions.assertTrue(request.getUpdatedColumns().contains("VALUE"));
        Assertions.assertTrue(request.containsUpdate());
        Assertions.assertTrue(request.containsDelete());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> request.getUpdatedColumns().add("another"));
    }
}
