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

package org.apache.doris.nereids.parser;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class LogicalPlanBuilderAssistantTest {

    @Test
    public void testGetStringLiteralAlias() {
        String expected = "A";

        List<String> values = Lists.newArrayList(
                "\tA",
                "\nA",
                "\bA",
                "\rA",
                "\0A",
                "\032A",
                " A",
                "\t\r\b\n\0\032    A",
                "     \t\r\b\n\0\032    A",
                "     \t\r\b\n\0\032    A\0BCDEF"
        );

        for (String value : values) {
            Assertions.assertEquals(expected, LogicalPlanBuilderAssistant.getStringLiteralAlias(value));
        }

        Assertions.assertEquals("A\t\n\b\n\032    A",
                LogicalPlanBuilderAssistant.getStringLiteralAlias("A\t\n\b\n\032    A"));
        Assertions.assertEquals("A\t\n\b",
                LogicalPlanBuilderAssistant.getStringLiteralAlias("A\t\n\b\0\n\032    A"));
        Assertions.assertEquals("A\t\n\b\n\032    A",
                LogicalPlanBuilderAssistant.getStringLiteralAlias("A\t\n\b\n\032    A\0BCDEF"));
    }
}
