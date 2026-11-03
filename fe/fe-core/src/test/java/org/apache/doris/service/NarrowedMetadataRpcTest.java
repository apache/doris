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
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectPoolMgr;
import org.apache.doris.thrift.TDescribeTablesParams;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetTablesParams;
import org.apache.doris.thrift.TListPrivilegesResult;
import org.apache.doris.thrift.TSchemaTableName;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TShowProcessListRequest;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;

/**
 * A BE->FE schema-table request from a session-narrowed (SU) session carries the session's active
 * role subset (TSchemaTableRequestParams.current_roles); the handler installs it so MetadataGenerator's
 * privilege checks resolve that set instead of the caller's full role union.
 */
public class NarrowedMetadataRpcTest extends TestWithFeService {

    private final ExecuteEnv exeEnv = Mockito.mock(ExecuteEnv.class);

    private static final Set<String> NARROWED = Sets.newHashSet("tenant_c");
    private static final UserIdentity CAROL = UserIdentity.createAnalyzedUserIdentWithIp("carol", "%");

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createDatabase("perso");
        createTable("create table perso.t1 (k int) distributed by hash(k) buckets 1"
                + " properties('replication_num' = '1')");
        createTable("create table test.t1 (k int) distributed by hash(k) buckets 1"
                + " properties('replication_num' = '1')");
        addUser("carol", true);
        createRole("tenant_c");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'tenant_c';");
        grantPriv("GRANT SELECT_PRIV ON internal.test.t1 TO ROLE 'tenant_c';");
        grantRole("GRANT 'tenant_c' TO 'carol'@'%'");
        // personal (default-role) grants the narrowing must drop, one per privilege level
        grantPriv("GRANT SELECT_PRIV ON internal.perso.* TO 'carol'@'%';");
        grantPriv("GRANT SELECT_PRIV ON internal.perso.t1 TO 'carol'@'%';");
        // dave: a plain tenant role and an ADMIN-bearing one the switch leaves out; a bystander session
        addUser("dave", true);
        addUser("bystander_rpc", true);
        createRole("tenant_d");
        createRole("ops_d");
        grantPriv("GRANT ADMIN_PRIV ON *.*.* TO ROLE 'ops_d';");
        grantRole("GRANT 'tenant_d','ops_d' TO 'dave'@'%'");
    }

    /** The caller as the request names it: the identity struct, or the deprecated user/user_ip pair. */
    private static void nameCaller(TGetDbsParams params, boolean byIdentity) {
        if (byIdentity) {
            params.setCurrentUserIdent(CAROL.toThrift());
        } else {
            params.setUser("carol");
            params.setUserIp("%");
        }
    }

    private static void nameCaller(TGetTablesParams params, boolean byIdentity) {
        if (byIdentity) {
            params.setCurrentUserIdent(CAROL.toThrift());
        } else {
            params.setUser("carol");
            params.setUserIp("%");
        }
    }

    private static TGetDbsParams dbs(Set<String> currentRoles, boolean byIdentity) {
        TGetDbsParams params = new TGetDbsParams();
        params.setCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);
        nameCaller(params, byIdentity);
        if (currentRoles != null) {
            params.setCurrentRoles(currentRoles);
        }
        return params;
    }

    private static TGetTablesParams tables(String db, Set<String> currentRoles, boolean byIdentity) {
        TGetTablesParams params = new TGetTablesParams();
        params.setCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);
        params.setDb(db);
        nameCaller(params, byIdentity);
        if (currentRoles != null) {
            params.setCurrentRoles(currentRoles);
        }
        return params;
    }

    /** A privilege-status request (TGetTablesParams without a db) from the named caller. */
    private static TGetTablesParams privileges(String user, Set<String> currentRoles, boolean byIdentity) {
        TGetTablesParams params = new TGetTablesParams();
        if (byIdentity) {
            params.setCurrentUserIdent(UserIdentity.createAnalyzedUserIdentWithIp(user, "%").toThrift());
        } else {
            params.setUser(user);
            params.setUserIp("%");
        }
        if (currentRoles != null) {
            params.setCurrentRoles(currentRoles);
        }
        return params;
    }

    /** The rows of a privilege-status listing on the named schema. */
    private static long rowsOn(TListPrivilegesResult result, String schema) {
        return result.getPrivileges().stream().filter(row -> schema.equals(row.getSchema())).count();
    }

    /** How many tables information_schema.metadata_name_ids lists in db for the caller. */
    private static int metadataNameIds(FrontendServiceImpl impl, String db, Set<String> currentRoles,
            boolean byIdentity) throws Exception {
        return impl.listTableMetadataNameIds(tables(db, currentRoles, byIdentity)).getTables().size();
    }

    private static TDescribeTablesParams describe(String db, Set<String> currentRoles) {
        TDescribeTablesParams params = new TDescribeTablesParams();
        params.setCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);
        params.setDb(db);
        params.setTablesName(Lists.newArrayList("t1"));
        params.setCurrentUserIdent(CAROL.toThrift());
        if (currentRoles != null) {
            params.setCurrentRoles(currentRoles);
        }
        return params;
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

    /**
     * Every one of the eight wrapped name-visibility handlers narrows alike, whether the request
     * names the caller by identity or by the deprecated user/user_ip pair: the personal grants on
     * perso vanish under the tenant role, the role grants on test stay, and nothing sticks to the
     * handler thread. listTableMetadataNameIds decides on an executor thread of its own, and the
     * global privilege listing is gated on the caller's own global SHOW, which dave holds only
     * through the ADMIN-bearing role the switch leaves out.
     */
    @Test
    public void testEveryNameVisibilityHandlerNarrows() throws Exception {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        for (boolean byIdentity : new boolean[] {true, false}) {
            Assertions.assertTrue(impl.getDbNames(dbs(null, byIdentity)).getDbs().contains("perso"));
            Assertions.assertFalse(impl.getDbNames(dbs(NARROWED, byIdentity)).getDbs().contains("perso"));
            Assertions.assertTrue(impl.getDbNames(dbs(NARROWED, byIdentity)).getDbs().contains("test"));

            Assertions.assertEquals(1, impl.getTableNames(tables("perso", null, byIdentity)).getTables().size());
            Assertions.assertEquals(0, impl.getTableNames(tables("perso", NARROWED, byIdentity)).getTables().size());
            Assertions.assertEquals(1, impl.getTableNames(tables("test", NARROWED, byIdentity)).getTables().size());

            Assertions.assertEquals(1, impl.listTableStatus(tables("perso", null, byIdentity)).getTables().size());
            Assertions.assertEquals(0, impl.listTableStatus(tables("perso", NARROWED, byIdentity)).getTables().size());
            Assertions.assertEquals(1, impl.listTableStatus(tables("test", NARROWED, byIdentity)).getTables().size());

            // the check runs on the handler's executor thread: the narrowing must follow it there
            Assertions.assertEquals(1, metadataNameIds(impl, "perso", null, byIdentity));
            Assertions.assertEquals(0, metadataNameIds(impl, "perso", NARROWED, byIdentity));
            Assertions.assertEquals(1, metadataNameIds(impl, "test", NARROWED, byIdentity));

            TGetTablesParams own = privileges("carol", null, byIdentity);
            TGetTablesParams narrowed = privileges("carol", NARROWED, byIdentity);
            Assertions.assertEquals(1, rowsOn(impl.listTablePrivilegeStatus(own), "perso"));
            Assertions.assertEquals(0, rowsOn(impl.listTablePrivilegeStatus(narrowed), "perso"));
            Assertions.assertEquals(1, rowsOn(impl.listTablePrivilegeStatus(narrowed), "test"));
            Assertions.assertEquals(1, rowsOn(impl.listSchemaPrivilegeStatus(own), "perso"));
            Assertions.assertEquals(0, rowsOn(impl.listSchemaPrivilegeStatus(narrowed), "perso"));
            Assertions.assertEquals(1, rowsOn(impl.listSchemaPrivilegeStatus(narrowed), "test"));

            TGetTablesParams daveNarrowed = privileges("dave", Sets.newHashSet("tenant_d"), byIdentity);
            Assertions.assertFalse(impl.listUserPrivilegeStatus(privileges("dave", null, byIdentity))
                    .getPrivileges().isEmpty());
            Assertions.assertTrue(impl.listUserPrivilegeStatus(daveNarrowed).getPrivileges().isEmpty());

            // the narrowing did not stick to the handler thread
            Assertions.assertEquals(1, impl.listTableStatus(tables("perso", null, byIdentity)).getTables().size());
            Assertions.assertEquals(1, metadataNameIds(impl, "perso", null, byIdentity));
            Assertions.assertEquals(1, rowsOn(impl.listTablePrivilegeStatus(own), "perso"));
        }
        Assertions.assertEquals(1, impl.describeTables(describe("perso", null)).getColumns().size());
        Assertions.assertEquals(0, impl.describeTables(describe("perso", NARROWED)).getColumns().size());
        Assertions.assertEquals(1, impl.describeTables(describe("test", NARROWED)).getColumns().size());
    }

    /**
     * The FE->FE processlist fan-out: a target holding an ADMIN-bearing role the switch left out sees
     * every session un-narrowed, and only its own sessions under the narrowing.
     */
    @Test
    public void testShowProcessListNarrowsTheAdminCheck() throws Exception {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        UserIdentity dave = UserIdentity.createAnalyzedUserIdentWithIp("dave", "%");
        ConnectPoolMgr pool = ExecuteEnv.getInstance().getScheduler().getConnectPoolMgr();
        ConnectContext daves = registered(pool, dave, 7001);
        ConnectContext bystanders = registered(pool,
                UserIdentity.createAnalyzedUserIdentWithIp("bystander_rpc", "%"), 7002);
        try {
            TShowProcessListRequest request = new TShowProcessListRequest();
            request.setShowFullSql(true);
            request.setCurrentUserIdent(dave.toThrift());
            Set<String> everyUser = usersListed(impl.showProcessList(request).getProcessList());
            Assertions.assertTrue(everyUser.contains("bystander_rpc"), everyUser.toString());

            request.setCurrentRoles(Sets.newHashSet("tenant_d"));
            Set<String> narrowedUsers = usersListed(impl.showProcessList(request).getProcessList());
            Assertions.assertEquals(Sets.newHashSet("dave"), narrowedUsers);
        } finally {
            pool.unregisterConnection(daves);
            pool.unregisterConnection(bystanders);
        }
    }

    private static ConnectContext registered(ConnectPoolMgr pool, UserIdentity user, int connectionId) {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCurrentUserIdentity(user);
        ctx.setConnectionId(connectionId);
        Assertions.assertEquals(-1, pool.registerConnection(ctx));
        return ctx;
    }

    /** The User column (index 2) of every processlist row. */
    private static Set<String> usersListed(List<List<String>> rows) {
        Set<String> users = Sets.newHashSet();
        for (List<String> row : rows) {
            users.add(row.get(2));
        }
        return users;
    }
}
