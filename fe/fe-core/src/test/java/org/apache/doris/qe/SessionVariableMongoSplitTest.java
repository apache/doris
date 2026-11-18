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

package org.apache.doris.qe;

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.DdlException;
import org.apache.doris.connector.ConnectorSessionBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class SessionVariableMongoSplitTest {
    private static void set(SessionVariable variables, String name, String value) throws DdlException {
        VariableMgr.setVar(variables, new SetVar(SetType.SESSION, name, new StringLiteral(value)));
    }

    @Test
    void defaultsAndForwardingPinSplitSettingsInTheStatement() throws Exception {
        var context = new ConnectContext();
        var before = ConnectorSessionBuilder.from(context).build();
        Assertions.assertEquals("single", before.getSessionProperties().get("mongodb_split_strategy"));
        var settings = Map.of("mongodb_split_strategy", "range", "mongodb_split_field", "sequence",
                "mongodb_split_count", "8", "mongodb_split_max_count", "32", "mongodb_split_sample_rows", "512",
                "mongodb_split_target_bytes", "1048576", "mongodb_split_plan_timeout_ms", "2000");
        for (var setting : settings.entrySet()) {
            set(context.getSessionVariable(), setting.getKey(), setting.getValue());
        }
        var after = ConnectorSessionBuilder.from(context).build();
        for (var setting : settings.entrySet()) {
            Assertions.assertEquals(setting.getValue(), after.getSessionProperties().get(setting.getKey()));
            Assertions.assertEquals(setting.getValue(), context.getSessionVariable().getForwardVariables()
                    .get(setting.getKey()));
        }
        Assertions.assertEquals("single", before.getSessionProperties().get("mongodb_split_strategy"));
    }

    @Test
    void rejectsInvalidBoundsAndAmbiguousFields() {
        var variables = new SessionVariable();
        for (String name : List.of("mongodb_split_count", "mongodb_split_max_count", "mongodb_split_sample_rows",
                "mongodb_split_target_bytes", "mongodb_split_plan_timeout_ms")) {
            for (String value : List.of("-1", "2147483648", "invalid")) {
                Assertions.assertThrows(DdlException.class, () -> set(variables, name, value));
            }
            if (!"mongodb_split_count".equals(name)) {
                Assertions.assertThrows(DdlException.class, () -> set(variables, name, "0"));
            }
        }
        for (String value : List.of("257", "4096")) {
            Assertions.assertThrows(DdlException.class, () -> set(variables, "mongodb_split_count", value));
            Assertions.assertThrows(DdlException.class, () -> set(variables, "mongodb_split_max_count", value));
        }
        Assertions.assertThrows(DdlException.class, () -> set(variables, "mongodb_split_sample_rows", "4097"));
        Assertions.assertThrows(DdlException.class, () -> set(variables, "mongodb_split_plan_timeout_ms", "300001"));
        for (String value : List.of("a.b", "$a", "a$b", "a\0b")) {
            Assertions.assertThrows(DdlException.class, () -> set(variables, "mongodb_split_field", value));
        }
        for (String value : List.of("", "invalid", "RANGE")) {
            Assertions.assertThrows(DdlException.class, () -> set(variables, "mongodb_split_strategy", value));
        }
    }
}
