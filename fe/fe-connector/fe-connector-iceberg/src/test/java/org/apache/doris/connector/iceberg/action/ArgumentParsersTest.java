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

package org.apache.doris.connector.iceberg.action;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the connector-local port of {@code org.apache.doris.common.ArgumentParsers}.
 *
 * <p><b>WHY this matters:</b> the parser messages compose into the {@code NamedArguments} validation errors
 * the user sees; they must stay byte-identical to legacy (T08 byte-parity). The parsers themselves still
 * throw {@link IllegalArgumentException} (caught and re-wrapped by {@code NamedArguments}); only the
 * top-level validation type changed. These tests freeze the parser-level messages directly.</p>
 */
public class ArgumentParsersTest {

    @Test
    public void positiveLongRejectsNonPositiveWithMessage() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> ArgumentParsers.positiveLong("snapshot_id").parse("-5"));
        Assertions.assertEquals("snapshot_id must be positive, got: -5", e.getMessage());
    }

    @Test
    public void positiveLongRejectsNonNumericWithMessage() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> ArgumentParsers.positiveLong("snapshot_id").parse("abc"));
        Assertions.assertEquals("Invalid snapshot_id format: abc", e.getMessage());
    }

    @Test
    public void positiveLongAcceptsPositiveAndTrims() throws Exception {
        Assertions.assertEquals(Long.valueOf(42L), ArgumentParsers.positiveLong("snapshot_id").parse(" 42 "));
    }

    @Test
    public void nonEmptyStringRejectsBlankWithMessage() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> ArgumentParsers.nonEmptyString("branch").parse("   "));
        Assertions.assertEquals("branch cannot be empty", e.getMessage());
    }

    @Test
    public void booleanValueRejectsNonBooleanWithMessage() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> ArgumentParsers.booleanValue("use_starting_sequence_number").parse("yes"));
        Assertions.assertEquals("use_starting_sequence_number must be 'true' or 'false', got: yes",
                e.getMessage());
    }
}
