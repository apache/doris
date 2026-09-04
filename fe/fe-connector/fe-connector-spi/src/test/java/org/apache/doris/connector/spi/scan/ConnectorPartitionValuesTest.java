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

package org.apache.doris.connector.spi.scan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * A guard rail, not a behaviour snapshot: it exists to stop someone from "tidying up" the canonical NULL
 * partition NAME once its Java symbol no longer carries a source brand.
 *
 * <p>WHY the literal is frozen: a partition name is persisted, user-visible identity. It is baked into view and
 * materialized-view definitions (e.g. a view filtering {@code t_int != "__HIVE_DEFAULT_PARTITION__"}), into
 * {@code partition_values()} table-function output, and into the {@code columns_from_path} bytes BE parses; BE
 * also hardcodes the same literal on the hive write path ({@code vhive_utils.cpp}). Changing the string orphans
 * every object already persisted with it. Renaming the SYMBOL is free; changing the VALUE is not.</p>
 */
public class ConnectorPartitionValuesTest {

    @Test
    public void nullPartitionNameLiteralIsFrozen() {
        Assertions.assertEquals("__HIVE_DEFAULT_PARTITION__", ConnectorPartitionValues.NULL_PARTITION_NAME,
                "the canonical NULL partition name is a persisted identity — rename the symbol, never the value");
    }
}
