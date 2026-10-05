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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.property.fileformat.ParquetFileFormatProperties;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class ExportCommandTest {

    @Test
    void testEnableInt96TimestampsProperty() {
        Map<String, String> properties = ImmutableMap.of(
                ParquetFileFormatProperties.ENABLE_INT96_TIMESTAMPS, "false");
        ExportCommand exportCommand = new ExportCommand(
                Collections.singletonList("test_table"), Collections.emptyList(), Optional.empty(),
                "file:///tmp/export", properties, Optional.empty());

        Assertions.assertDoesNotThrow(
                () -> Deencapsulation.invoke(exportCommand, "checkPropertyKey", properties));
    }
}
