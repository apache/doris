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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.property.metastore.AbstractPaimonProperties;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class PaimonReaderOptionsTest {

    @Test
    void testRejectUnsafeOptionsFromCreateOrAlterProperties() {
        for (Map<String, String> properties : new Map[] {
                ImmutableMap.of(AbstractPaimonProperties.TABLE_OPTION_PREFIX + "branch", "archive"),
                ImmutableMap.of(AbstractPaimonProperties.TABLE_OPTION_PREFIX + "read.batch-size", "0"),
                ImmutableMap.of(AbstractPaimonProperties.TABLE_OPTION_PREFIX
                        + "file-reader-async-threshold", "2 GB")
        }) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PaimonReaderOptions.validateCatalogProperties(properties));
        }
    }

    @Test
    void testRejectUnsafeEffectivePhysicalTableOptions() {
        for (Map<String, String> options : new Map[] {
                ImmutableMap.of("read.batch-size", "0"),
                ImmutableMap.of("file-reader-async-threshold", "512 KB"),
                ImmutableMap.of("scan.manifest.parallelism", "0"),
                ImmutableMap.of("scan.manifest.parallelism",
                        String.valueOf(Runtime.getRuntime().availableProcessors() + 1))
        }) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> PaimonReaderOptions.validateEffectiveTableOptions(options));
        }
    }
}
