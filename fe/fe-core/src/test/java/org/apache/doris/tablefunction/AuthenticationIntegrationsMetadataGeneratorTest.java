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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authentication.AuthenticationIntegrationMeta;
import org.apache.doris.authentication.AuthenticationIntegrationMgr;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TMetadataTableRequestParams;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TSchemaTableName;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class AuthenticationIntegrationsMetadataGeneratorTest {

    @Mocked
    private Env env;

    @Mocked
    private AccessControllerManager accessManager;

    @Mocked
    private AuthenticationIntegrationMgr authenticationIntegrationMgr;

    @Test
    public void testAuthenticationIntegrationsMetadataResultWithMasking() throws Exception {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("ldap.server", "ldap://127.0.0.1:389");
        properties.put("ldap.admin_password", "plain-text-pwd");
        AuthenticationIntegrationMeta meta =
                new AuthenticationIntegrationMeta("corp_ldap", "ldap", properties, "ldap integration");
        Map<String, AuthenticationIntegrationMeta> integrationMap = ImmutableMap.of("corp_ldap", meta);

        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkGlobalPriv((UserIdentity) any, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;

                env.getAuthenticationIntegrationMgr();
                minTimes = 0;
                result = authenticationIntegrationMgr;

                authenticationIntegrationMgr.getAuthenticationIntegrations();
                minTimes = 0;
                result = integrationMap;
            }
        };

        TFetchSchemaTableDataResult result = MetadataGenerator.getMetadataTable(buildRequest());
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        Assertions.assertEquals(2, result.getDataBatchSize());

        Map<String, String> propertyToValue = new HashMap<>();
        for (int i = 0; i < result.getDataBatchSize(); i++) {
            String integrationName = result.getDataBatch().get(i).getColumnValue().get(0).getStringVal();
            String type = result.getDataBatch().get(i).getColumnValue().get(1).getStringVal();
            String property = result.getDataBatch().get(i).getColumnValue().get(2).getStringVal();
            String value = result.getDataBatch().get(i).getColumnValue().get(3).getStringVal();
            String comment = result.getDataBatch().get(i).getColumnValue().get(4).getStringVal();

            Assertions.assertEquals("corp_ldap", integrationName);
            Assertions.assertEquals("ldap", type);
            Assertions.assertEquals("ldap integration", comment);
            propertyToValue.put(property, value);
        }

        Assertions.assertEquals("ldap://127.0.0.1:389", propertyToValue.get("ldap.server"));
        Assertions.assertEquals(PrintableMap.PASSWORD_MASK, propertyToValue.get("ldap.admin_password"));
    }

    @Test
    public void testAuthenticationIntegrationsMetadataResultDenied() throws Exception {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                accessManager.checkGlobalPriv((UserIdentity) any, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };

        TFetchSchemaTableDataResult result = MetadataGenerator.getMetadataTable(buildRequest());
        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatusCode());
        Assertions.assertTrue(result.getStatus().getErrorMsgs().get(0).contains("ADMIN"));
    }

    private TFetchSchemaTableDataRequest buildRequest() {
        TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest();
        request.setSchemaTableName(TSchemaTableName.METADATA_TABLE);

        TMetadataTableRequestParams params = new TMetadataTableRequestParams();
        params.setMetadataType(TMetadataType.AUTHENTICATION_INTEGRATIONS);
        params.setCurrentUserIdent(UserIdentity.ROOT.toThrift());
        params.setColumnsName(Arrays.asList("integrationname", "type", "property", "value", "comment"));
        request.setMetadaTableParams(params);
        return request;
    }
}
