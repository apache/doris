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

package org.apache.doris.service;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TSchemaTableName;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;

/**
 * A BE->FE schema-table request from a session-narrowed (SU) session carries the session's active
 * role subset (TSchemaTableRequestParams.current_roles); the handler installs it so MetadataGenerator's
 * privilege checks resolve that set instead of the caller's full role union.
 */
public class NarrowedMetadataRpcTest extends TestWithFeService {

    private final ExecuteEnv exeEnv = Mockito.mock(ExecuteEnv.class);

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createDatabase("perso");
        addUser("carol", true);
        createRole("tenant_c");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'tenant_c';");
        grantRole("GRANT 'tenant_c' TO 'carol'@'%'");
        // a personal (default-role) grant the narrowing must drop
        grantPriv("GRANT SELECT_PRIV ON internal.perso.* TO 'carol'@'%';");
    }

    private static TFetchSchemaTableDataRequest databaseProperties(String db, Set<String> currentRoles) {
        TSchemaTableRequestParams params = new TSchemaTableRequestParams();
        params.setCurrentUserIdent(UserIdentity.createAnalyzedUserIdentWithIp("carol", "%").toThrift());
        params.setCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);
        params.setDbId(Env.getCurrentInternalCatalog().getDbNullable(db).getId());
        if (currentRoles != null) {
            params.setCurrentRoles(currentRoles);
        }
        TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest();
        request.setSchemaTableName(TSchemaTableName.DATABASE_PROPERTIES);
        request.setSchemaTableParams(params);
        return request;
    }

    private static int rows(TFetchSchemaTableDataResult result) {
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        return result.getDataBatch().size();
    }

    @Test
    public void testSchemaTableRequestNarrowsPrivilegeChecks() throws Exception {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        // un-narrowed: the personal grant on perso and the role grant on test both show
        Assertions.assertEquals(1, rows(impl.fetchSchemaTableData(databaseProperties("perso", null))));
        Assertions.assertEquals(1, rows(impl.fetchSchemaTableData(databaseProperties("test", null))));

        // narrowed to the tenant role: the personal grant is gone, the role grant remains
        Set<String> narrowed = Sets.newHashSet("tenant_c");
        Assertions.assertEquals(0, rows(impl.fetchSchemaTableData(databaseProperties("perso", narrowed))));
        Assertions.assertEquals(1, rows(impl.fetchSchemaTableData(databaseProperties("test", narrowed))));

        // the narrowing did not stick to the handler thread
        Assertions.assertEquals(1, rows(impl.fetchSchemaTableData(databaseProperties("perso", null))));
    }

    @Test
    public void testNarrowedRequestWithoutIdentityFailsClosed() throws Exception {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TFetchSchemaTableDataRequest request = databaseProperties("test", Sets.newHashSet("tenant_c"));
        request.getSchemaTableParams().unsetCurrentUserIdent();
        TFetchSchemaTableDataResult result = impl.fetchSchemaTableData(request);
        Assertions.assertNotEquals(TStatusCode.OK, result.getStatus().getStatusCode());
    }
}
