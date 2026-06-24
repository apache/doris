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

import org.apache.doris.connector.api.DorisConnectorException;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Pins the connector-local port of {@code org.apache.doris.common.NamedArguments}.
 *
 * <p><b>WHY this matters:</b> §4 = 4-A signed (D-062) — the import gate forbids
 * {@code org.apache.doris.common.NamedArguments}, so iceberg procedure argument validation lives in the
 * connector. The port must keep the legacy validation <em>error strings byte-identical</em> (T08 byte-parity
 * is the hard gate), changing only the thrown type from the (banned) {@code AnalysisException} to the
 * connector's {@link DorisConnectorException}. These tests freeze each message so a parity drift fails loudly.</p>
 */
public class NamedArgumentsTest {

    @Test
    public void unknownArgumentRejectedWithLegacyMessage() {
        NamedArguments args = new NamedArguments();
        args.registerRequiredArgument("snapshot_id", "Snapshot ID",
                ArgumentParsers.positiveLong("snapshot_id"));

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> args.validate(ImmutableMap.of("snapshot_id", "5", "bogus", "1")));
        Assertions.assertEquals("Unknown argument: bogus", e.getMessage());
    }

    @Test
    public void missingRequiredArgumentRejectedWithLegacyMessage() {
        NamedArguments args = new NamedArguments();
        args.registerRequiredArgument("snapshot_id", "Snapshot ID",
                ArgumentParsers.positiveLong("snapshot_id"));

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> args.validate(Collections.emptyMap()));
        Assertions.assertEquals("Missing required argument: snapshot_id", e.getMessage());
    }

    @Test
    public void invalidValueWrapsParserMessageWithLegacyTemplate() {
        NamedArguments args = new NamedArguments();
        args.registerRequiredArgument("snapshot_id", "Snapshot ID",
                ArgumentParsers.positiveLong("snapshot_id"));

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> args.validate(ImmutableMap.of("snapshot_id", "abc")));
        // The "Invalid value for argument '%s': %s. %s" template wrapping the parser's own message.
        Assertions.assertEquals(
                "Invalid value for argument 'snapshot_id': abc. Invalid snapshot_id format: abc",
                e.getMessage());
    }

    @Test
    public void parsedRequiredValueRetrievableTyped() throws Exception {
        NamedArguments args = new NamedArguments();
        args.registerRequiredArgument("snapshot_id", "Snapshot ID",
                ArgumentParsers.positiveLong("snapshot_id"));

        args.validate(ImmutableMap.of("snapshot_id", "42"));
        Assertions.assertEquals(Long.valueOf(42L), args.getLong("snapshot_id"));
    }

    @Test
    public void optionalArgumentFallsBackToDefaultWhenAbsent() throws Exception {
        NamedArguments args = new NamedArguments();
        args.registerOptionalArgument("retain_last", "Retain last N", 1,
                ArgumentParsers.positiveInt("retain_last"));

        args.validate(Collections.emptyMap());
        Assertions.assertEquals(Integer.valueOf(1), args.getInt("retain_last"));
    }

    @Test
    public void allowedArgumentDoesNotTriggerUnknownError() throws Exception {
        NamedArguments args = new NamedArguments();
        args.registerRequiredArgument("snapshot_id", "Snapshot ID",
                ArgumentParsers.positiveLong("snapshot_id"));
        args.addAllowedArgument("output-spec-id");

        // A registered required + an explicitly-allowed framework arg must both validate cleanly.
        args.validate(ImmutableMap.of("snapshot_id", "7", "output-spec-id", "0"));
        Assertions.assertEquals(Long.valueOf(7L), args.getLong("snapshot_id"));
    }
}
