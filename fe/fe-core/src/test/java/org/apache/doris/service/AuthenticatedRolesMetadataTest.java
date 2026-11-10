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

import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AccessRequirements;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.mysql.authenticate.ldap.LdapManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.EngineAuthorizationContext;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.mysql.privilege.Role;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.tablefunction.MetadataGenerator;
import org.apache.doris.thrift.TDescribeTablesParams;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetTablesParams;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TSchemaTableName;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TUserIdentity;
import org.apache.doris.utframe.TestWithFeService;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Exercises schema RPCs with real grants after detaching them from the authenticated session. */
public class AuthenticatedRolesMetadataTest extends TestWithFeService {
    private static final String DB = "authenticated_role_metadata";
    private static final String MAPPED_ROLE = "metadata_mapped_role";
    private final FrontendServiceImpl service = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB);
        for (String table : Arrays.asList("mapped_table", "local_table", "hidden_table")) {
            createTable("CREATE TABLE " + DB + "." + table + " (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                    + "PROPERTIES ('replication_num'='1')");
        }
        createRole(MAPPED_ROLE);
        grantPriv("GRANT SELECT_PRIV ON internal." + DB + ".mapped_table TO ROLE '" + MAPPED_ROLE + "'");
        addUser("metadata_local_user", true);
        createRole("metadata_local_role");
        grantPriv("GRANT SELECT_PRIV ON internal." + DB + ".local_table TO ROLE 'metadata_local_role'");
        grantRole("GRANT 'metadata_local_role' TO 'metadata_local_user'@'%'");
    }

    @Test
    public void testMappedRolesSurviveSchemaCallbacksWithoutSession() throws Exception {
        ConnectContext session = mappedSession("metadata_jit_user");
        session.setThreadLocalInfo();
        TUserIdentity identity;
        try {
            Assertions.assertTrue(Env.getCurrentEnv().getAccessManager().checkTblPriv(session,
                    "internal", DB, "mapped_table", PrivPredicate.SELECT));
            Assertions.assertFalse(Env.getCurrentEnv().getAccessManager().checkTblPriv(session,
                    "internal", DB, "hidden_table", PrivPredicate.SELECT));
            identity = roundTrip(session.getCurrentUserIdentityForRpc());
        } finally {
            ConnectContext.remove();
        }
        try {
            assertMetadata(identity, Collections.singletonList("mapped_table"));
            // This is the old request: identity alone cannot recover session role-mapping results.
            identity.unsetAuthenticatedRoles();
            assertMetadata(identity, Collections.emptyList());
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testMappedRolesSupplementLocalGrants() throws Exception {
        ConnectContext session = mappedSession("metadata_local_user");
        for (String table : Arrays.asList("local_table", "mapped_table")) {
            Assertions.assertTrue(Env.getCurrentEnv().getAccessManager().checkTblPriv(session,
                    "internal", DB, table, PrivPredicate.SELECT));
        }
        TUserIdentity identity = roundTrip(session.getCurrentUserIdentityForRpc());
        ConnectContext.remove();
        try {
            assertMetadata(identity, Arrays.asList("local_table", "mapped_table"));
            identity.unsetAuthenticatedRoles();
            assertMetadata(identity, Collections.singletonList("local_table"));
            // Mapping roles must not become persistent bindings on the local account.
            Assertions.assertFalse(Env.getCurrentEnv().getAuth().getRolesByUser(session.getCurrentUserIdentity(), true)
                    .contains(MAPPED_ROLE));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testAllThreeSchemaPlansCarrySessionRoles() throws Exception {
        connectContext.setAuthenticatedRoles(Collections.singleton(MAPPED_ROLE));
        connectContext.setThreadLocalInfo();
        try {
            for (String table : Arrays.asList("schemata", "tables", "columns")) {
                Planner planner = getSQLPlanner("EXPLAIN SELECT * FROM information_schema." + table);
                Assertions.assertFalse(planner.getScanNodes().isEmpty());
                for (ScanNode scan : planner.getScanNodes()) {
                    TPlanNode node = scan.treeToThrift().getNodes().get(0);
                    Assertions.assertEquals(Collections.singleton(MAPPED_ROLE),
                            roundTrip(node.getSchemaScanNode().getCurrentUserIdent()).getAuthenticatedRoles());
                }
            }
        } finally {
            connectContext.setAuthenticatedRoles(Collections.emptySet());
        }
    }

    @Test
    public void testExplicitConnectionDoesNotNeedThreadLocal() throws Exception {
        ConnectContext session = mappedSession("metadata_explicit_context");
        ConnectContext.remove();
        try {
            Assertions.assertTrue(Env.getCurrentEnv().getAccessManager().checkTblPriv(session,
                    "internal", DB, "mapped_table", PrivPredicate.SELECT));
            Assertions.assertFalse(Env.getCurrentEnv().getAccessManager().checkTblPriv(session,
                    "internal", DB, "hidden_table", PrivPredicate.SELECT));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testForwardedRolesDoNotEscapeIntoJobCreator() throws Exception {
        TUserIdentity request = roundTrip(mappedSession("metadata_job_creator").getCurrentUserIdentityForRpc());
        ConnectContext forwarded = new ConnectContext();
        forwarded.setEnv(Env.getCurrentEnv());
        forwarded.setCurrentUserIdentityFromRpc(request);
        InsertJob job = new InsertJob(forwarded, null, "metadata_role_job", Collections.emptyList(),
                Collections.emptySet(), Collections.emptyMap(), "", new JobExecutionConfiguration());
        ConnectContext.remove();
        try {
            Assertions.assertTrue(Env.getCurrentEnv().getAccessManager().checkTblPriv(forwarded,
                    "internal", DB, "mapped_table", PrivPredicate.SELECT));
            Assertions.assertTrue(job.getCreateUser().getAuthenticatedRoles().isEmpty());
            Assertions.assertFalse(Env.getCurrentEnv().getAccessManager().checkTblPriv(job.getCreateUser(),
                    "internal", DB, "mapped_table", PrivPredicate.SELECT));
            InsertJob replayed = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job), InsertJob.class);
            Assertions.assertTrue(replayed.getCreateUser().getAuthenticatedRoles().isEmpty());
            Assertions.assertFalse(Env.getCurrentEnv().getAccessManager().checkTblPriv(replayed.getCreateUser(),
                    "internal", DB, "mapped_table", PrivPredicate.SELECT));
            assertMetadata(roundTrip(forwarded.getCurrentUserIdentityForRpc()),
                    Collections.singletonList("mapped_table"));
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testPluginRolesExcludeSyntheticLdapRoleAndKeepRealSameNameRole() throws Exception {
        createRole(LdapManager.LDAP_DEFAULT_ROLE);
        createRole("metadata_ldap_group_role");
        grantPriv("GRANT SELECT_PRIV ON internal." + DB + ".hidden_table TO ROLE 'metadata_ldap_group_role'");
        addUser("metadata_ldap_alice", true);
        addUser("metadata_ldap_bob", true);
        grantRole("GRANT '" + LdapManager.LDAP_DEFAULT_ROLE + "' TO 'metadata_ldap_alice'@'%'");
        Auth auth = Env.getCurrentEnv().getAuth();
        LdapManager original = auth.getLdapManager();
        String originalType = Config.authentication_type;
        LdapManager ldap = Mockito.mock(LdapManager.class);
        TablePattern informationSchema = new TablePattern(InfoSchemaDb.DATABASE_NAME, "*");
        informationSchema.analyze();
        Role synthetic = new Role(LdapManager.LDAP_DEFAULT_ROLE, informationSchema,
                PrivBitSet.of(Privilege.SELECT_PRIV));
        Role group = auth.getRoleByName("metadata_ldap_group_role");
        Mockito.when(ldap.getUserRoles(Mockito.anyString())).thenReturn(Set.of(synthetic, group));
        Config.authentication_type = "ldap";
        Deencapsulation.setField(auth, "ldapManager", ldap);
        ConnectContext.remove();
        try {
            AccessControllerManager manager = new AccessControllerManager(auth);
            EngineAuthorizationContext engine = new EngineAuthorizationContext(manager, auth);
            AuthorizationPlugin plugin = new AuthorizationPlugin() {
                @Override
                public String name() {
                    return "ldap_named_role_policy";
                }

                @Override
                public void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
                        AccessRequirement requirement, AccessContext context) throws AccessDeniedException {
                    if (!engine.rolesOf(subject).contains(LdapManager.LDAP_DEFAULT_ROLE)) {
                        throw AccessDeniedException.of(subject, resource, requirement, name());
                    }
                }
            };
            engine.servedBy(plugin);
            Deencapsulation.setField(manager, "defaultAccessController", plugin);
            UserIdentity bob = UserIdentity.createAnalyzedUserIdentWithIp("metadata_ldap_bob", "%");
            UserIdentity alice = UserIdentity.createAnalyzedUserIdentWithIp("metadata_ldap_alice", "%");
            Assertions.assertTrue(auth.getRolesByUserWithLdap(bob).contains(synthetic));
            Assertions.assertTrue(Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(bob, "internal", DB, "hidden_table", PrivPredicate.SELECT));
            Assertions.assertFalse(manager.decide(bob, AuthorizedResource.global(), AccessRequirements.SELECT));
            Assertions.assertTrue(manager.decide(alice, AuthorizedResource.global(), AccessRequirements.SELECT));
            Assertions.assertFalse(auth.getRoleNamesForAuthorization(bob).contains(LdapManager.LDAP_DEFAULT_ROLE));
            Assertions.assertTrue(auth.getRoleNamesForAuthorization(alice).contains(LdapManager.LDAP_DEFAULT_ROLE));
        } finally {
            Config.authentication_type = originalType;
            Deencapsulation.setField(auth, "ldapManager", original);
            connectContext.setThreadLocalInfo();
        }
    }

    private ConnectContext mappedSession(String user) {
        ConnectContext session = new ConnectContext();
        session.setEnv(Env.getCurrentEnv());
        session.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(user, "%"));
        session.setAuthenticatedRoles(Collections.singleton(MAPPED_ROLE));
        return session;
    }

    private TUserIdentity roundTrip(TUserIdentity identity) throws Exception {
        TUserIdentity decoded = new TUserIdentity();
        new TDeserializer(new TBinaryProtocol.Factory()).deserialize(decoded,
                new TSerializer(new TBinaryProtocol.Factory()).serialize(identity));
        return decoded;
    }

    private void assertMetadata(TUserIdentity identity, List<String> visibleTables) throws Exception {
        Assertions.assertNull(ConnectContext.get());
        TGetDbsParams dbs = new TGetDbsParams();
        dbs.setCatalog("internal");
        dbs.setPattern("%" + DB);
        dbs.setIncludeDbIds(false);
        dbs.setCurrentUserIdent(identity);
        Assertions.assertEquals(visibleTables.isEmpty() ? 0 : 1, service.getDbNames(dbs).getDbsSize());

        TGetTablesParams tables = new TGetTablesParams();
        tables.setCatalog("internal");
        tables.setDb(DB);
        tables.setCurrentUserIdent(identity);
        Assertions.assertEquals(visibleTables, service.getTableNames(tables).getTables().stream()
                .sorted().collect(Collectors.toList()));
        Assertions.assertEquals(visibleTables, service.listTableStatus(tables).getTables().stream()
                .map(status -> status.getName()).sorted().collect(Collectors.toList()));
        // listTableMetadataNameIds runs its privilege checks on a separate executor thread.
        Assertions.assertEquals(visibleTables, service.listTableMetadataNameIds(tables).getTables().stream()
                .map(table -> table.getName()).sorted().collect(Collectors.toList()));

        for (String table : Arrays.asList("mapped_table", "local_table", "hidden_table")) {
            TDescribeTablesParams columns = new TDescribeTablesParams(Collections.singletonList(table));
            columns.setCatalog("internal");
            columns.setDb(DB);
            columns.setCurrentUserIdent(identity);
            Assertions.assertEquals(visibleTables.contains(table) ? 1 : 0,
                    service.describeTables(columns).getColumnsSize());
        }

        // Other schema tables share the same identity / authorization path.
        TSchemaTableRequestParams params = new TSchemaTableRequestParams();
        params.setCurrentUserIdent(identity);
        params.setCatalog("internal");
        params.setDbId(Env.getCurrentInternalCatalog().getDbOrDdlException(DB).getId());
        params.setColumnsName(Collections.singletonList("table_name"));
        TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest();
        request.setSchemaTableName(TSchemaTableName.TABLE_OPTIONS);
        request.setSchemaTableParams(params);
        TFetchSchemaTableDataResult options = MetadataGenerator.getSchemaTableData(request);
        Assertions.assertEquals(visibleTables, options.getDataBatch().stream()
                .map(row -> row.getColumnValue().get(0).getStringVal()).sorted().collect(Collectors.toList()));
    }
}
