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
import org.apache.doris.connector.spi.ConnectorSession;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class SessionVariableMongoReadPolicyTest {
    private static void set(SessionVariable variables, String name, String value) throws DdlException {
        VariableMgr.setVar(variables, new SetVar(SetType.SESSION, name, new StringLiteral(value)));
    }

    @Test
    void defaultsInheritAndConnectorSnapshotsAreSessionIsolated() throws Exception {
        ConnectContext context = new ConnectContext();
        SessionVariable variables = context.getSessionVariable();
        ConnectorSession inherited = ConnectorSessionBuilder.from(context).build();
        Assertions.assertEquals("inherit", inherited.getSessionProperties().get("mongodb_read_preference"));
        Assertions.assertEquals("inherit", inherited.getSessionProperties().get("mongodb_read_concern"));
        set(variables, "mongodb_read_preference", "secondaryPreferred");
        set(variables, "mongodb_read_concern", "local");
        ConnectorSession overridden = ConnectorSessionBuilder.from(context).build();
        Assertions.assertEquals("secondaryPreferred", overridden.getSessionProperties().get("mongodb_read_preference"));
        Assertions.assertEquals("local", overridden.getSessionProperties().get("mongodb_read_concern"));
        Assertions.assertEquals("inherit", inherited.getSessionProperties().get("mongodb_read_preference"));
        Assertions.assertEquals("inherit", VariableMgr.toMap(new SessionVariable()).get("mongodb_read_concern"));
    }

    @Test
    void acceptsSupportedValuesAndForwardsPolicies() throws Exception {
        SessionVariable variables = new SessionVariable();
        for (String value : List.of("inherit", "primary", "primaryPreferred", "secondary", "secondaryPreferred",
                "nearest", "SECONDARY", "INHERIT")) {
            set(variables, "mongodb_read_preference", value);
            Assertions.assertEquals(value, VariableMgr.toMap(variables).get("mongodb_read_preference"));
        }
        for (String value : List.of("inherit", "local", "majority", "available", "LOCAL", "INHERIT")) {
            set(variables, "mongodb_read_concern", value);
            Assertions.assertEquals(value, VariableMgr.toMap(variables).get("mongodb_read_concern"));
        }
        Map<String, String> forwarded = variables.getForwardVariables();
        Assertions.assertEquals("INHERIT", forwarded.get("mongodb_read_preference"));
        Assertions.assertEquals("INHERIT", forwarded.get("mongodb_read_concern"));
    }

    @Test
    void rejectsUnsupportedValuesWithoutChangingSession() {
        SessionVariable variables = new SessionVariable();
        for (String value : List.of("", "invalid", "secondary_preferred", " primary")) {
            Assertions.assertThrows(DdlException.class, () -> set(variables, "mongodb_read_preference", value));
        }
        for (String value : List.of("", "snapshot", "linearizable", "unknown", " local")) {
            Assertions.assertThrows(DdlException.class, () -> set(variables, "mongodb_read_concern", value));
        }
        Assertions.assertEquals("inherit", VariableMgr.toMap(variables).get("mongodb_read_preference"));
        Assertions.assertEquals("inherit", VariableMgr.toMap(variables).get("mongodb_read_concern"));
    }
}
