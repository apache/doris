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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.NodeType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test for ShowConfigCommand, especially the masking of sensitive config values.
 */
public class ShowConfigCommandTest extends TestWithFeService {

    private static final String MASK_VALUE = ConfigBase.SENSITIVE_CONF_MASK;

    private static final List<String> SENSITIVE_KEYS = List.of(
            "mysql_ssl_default_server_certificate_password",
            "key_store_password",
            "tls_private_key_password",
            "initial_root_password",
            "mysql_ssl_default_ca_certificate_password");

    /**
     * Expected value shown by {@code SHOW FRONTEND CONFIG} for a config: ConfigBase masks a
     * sensitive (non-empty) value, while an empty sensitive value is left as-is.
     */
    private static String expectedValue(String key) throws Exception {
        Field field = Config.class.getField(key);
        String rawValue = ConfigBase.getConfValue(field);
        return rawValue.isEmpty() ? rawValue : MASK_VALUE;
    }

    private ShowResultSet runShowFrontendConfig(String pattern) throws Exception {
        ShowConfigCommand command = new ShowConfigCommand(NodeType.FRONTEND);
        if (pattern != null) {
            command.setPattern(pattern);
        }
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        ArgumentCaptor<ShowResultSet> captor = ArgumentCaptor.forClass(ShowResultSet.class);
        command.run(connectContext, executor);
        Mockito.verify(executor).sendResultSet(captor.capture());
        return captor.getValue();
    }

    @Test
    public void testParseShowConfig() {
        LogicalPlan plan = new NereidsParser().parseSingle("show frontend config");
        Assertions.assertTrue(plan instanceof ShowConfigCommand);
        plan = new NereidsParser().parseSingle("show backend config");
        Assertions.assertTrue(plan instanceof ShowConfigCommand);
    }

    @Test
    public void testMaskSensitiveConfig() throws Exception {
        ShowResultSet resultSet = runShowFrontendConfig(null);
        List<List<String>> rows = resultSet.getResultRows();
        Assertions.assertFalse(rows.isEmpty());

        Map<String, List<String>> keyToRow = rows.stream()
                .collect(Collectors.toMap(row -> row.get(0), row -> row));

        for (String sensitiveKey : SENSITIVE_KEYS) {
            Assertions.assertTrue(keyToRow.containsKey(sensitiveKey),
                    "config '" + sensitiveKey + "' should be present in show frontend config");
            List<String> row = keyToRow.get(sensitiveKey);
            Assertions.assertEquals(ShowConfigCommand.FE_TITLE_NAMES.size(), row.size());
            Assertions.assertEquals(expectedValue(sensitiveKey), row.get(1),
                    "value of config '" + sensitiveKey + "' should be masked with " + MASK_VALUE
                            + " if set, otherwise left empty");
            // Only the value column is masked, the other columns stay intact.
            Assertions.assertEquals("String", row.get(2));
        }

        // A normal config keeps its real value and is never masked.
        Assertions.assertTrue(keyToRow.containsKey("http_port"));
        Assertions.assertEquals(ConfigBase.getConfValue(Config.class.getField("http_port")),
                keyToRow.get("http_port").get(1));
        Assertions.assertNotEquals(MASK_VALUE, keyToRow.get("http_port").get(1));
    }

    @Test
    public void testMaskSensitiveConfigWithPattern() throws Exception {
        ShowResultSet resultSet = runShowFrontendConfig("mysql_ssl_default_ca_certificate_password");
        List<List<String>> rows = resultSet.getResultRows();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("mysql_ssl_default_ca_certificate_password", rows.get(0).get(0));
        Assertions.assertEquals(expectedValue("mysql_ssl_default_ca_certificate_password"), rows.get(0).get(1));
    }

    @Test
    public void testMaskAllPasswordConfigsWithPattern() throws Exception {
        ShowResultSet resultSet = runShowFrontendConfig("%password%");
        List<List<String>> rows = resultSet.getResultRows();
        // The pattern '%password%' matches 6 configs: the 5 sensitive ones plus
        // tls_cert_based_auth_ignore_password which is not a secret.
        Assertions.assertEquals(SENSITIVE_KEYS.size() + 1, rows.size());
        Map<String, String> keyToValue = rows.stream()
                .collect(Collectors.toMap(row -> row.get(0), row -> row.get(1)));
        for (String sensitiveKey : SENSITIVE_KEYS) {
            Assertions.assertEquals(expectedValue(sensitiveKey), keyToValue.get(sensitiveKey),
                    "value of config '" + sensitiveKey + "' should be masked with " + MASK_VALUE
                            + " if set, otherwise left empty");
        }
        // A non-sensitive config containing "password" in its name keeps its real value.
        Assertions.assertEquals("false", keyToValue.get("tls_cert_based_auth_ignore_password"));
    }

    @Test
    public void testShowBackendConfigNotExists() throws Exception {
        ShowConfigCommand command = new ShowConfigCommand(NodeType.BACKEND);
        command.setBackendId(99999L);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> command.run(connectContext, executor));
        Assertions.assertEquals("errCode = 2, detailMessage = Backend 99999 not exists", exception.getMessage());
    }
}
