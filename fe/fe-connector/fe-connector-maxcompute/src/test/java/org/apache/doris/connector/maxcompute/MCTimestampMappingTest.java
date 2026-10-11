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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.spi.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MCTimestampMappingTest {
    @Test
    void preserveTimestampKindInBothDirections() {
        Assertions.assertEquals(ConnectorType.of("TIMESTAMPTZ", 6, 0),
                MCTypeMapping.toConnectorType(com.aliyun.odps.type.TypeInfoFactory.TIMESTAMP));
        Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 6, 0),
                MCTypeMapping.toConnectorType(com.aliyun.odps.type.TypeInfoFactory.TIMESTAMP_NTZ));
        Assertions.assertEquals(com.aliyun.odps.type.TypeInfoFactory.TIMESTAMP,
                MCTypeMapping.toMcType(ConnectorType.of("TIMESTAMPTZ", 6, 0)));
    }
}
