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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IPV4LiteralTest {

    @Test
    public void testValidIPv4() throws AnalysisException {
        new org.apache.doris.analysis.IPv4Literal("0.0.0.0");
        new org.apache.doris.analysis.IPv4Literal("192.168.1.1");
        new org.apache.doris.analysis.IPv4Literal("255.255.255.255");
        new org.apache.doris.analysis.IPv4Literal("10.0.0.1");
    }

    @Test
    public void testInvalidIPv4() {
        Assertions.assertThrows(AnalysisException.class,
                () -> new org.apache.doris.analysis.IPv4Literal("not_an_ip"));
        Assertions.assertThrows(AnalysisException.class,
                () -> new org.apache.doris.analysis.IPv4Literal("256.0.0.0"));
        Assertions.assertThrows(AnalysisException.class,
                () -> new org.apache.doris.analysis.IPv4Literal("999.999.999.999"));
        Assertions.assertThrows(AnalysisException.class,
                () -> new org.apache.doris.analysis.IPv4Literal("1.2.3"));
        Assertions.assertThrows(AnalysisException.class,
                () -> new org.apache.doris.analysis.IPv4Literal("1.2.3.4.5"));
        Assertions.assertThrows(AnalysisException.class,
                () -> new org.apache.doris.analysis.IPv4Literal(""));
    }

    @Test
    public void testValidateDefaultValueIPv4() throws AnalysisException {
        ColumnDef.validateDefaultValue(Type.IPV4, "192.168.0.1", null);
        ColumnDef.validateDefaultValue(Type.IPV4, "0.0.0.0", null);
        ColumnDef.validateDefaultValue(Type.IPV4, "255.255.255.255", null);
    }

    @Test
    public void testValidateDefaultValueIPv4Invalid() {
        Assertions.assertThrows(AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(Type.IPV4, "not_an_ip", null));
        Assertions.assertThrows(AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(Type.IPV4, "999.999.999.999", null));
        Assertions.assertThrows(AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(Type.IPV4, "256.0.0.1", null));
    }
}
