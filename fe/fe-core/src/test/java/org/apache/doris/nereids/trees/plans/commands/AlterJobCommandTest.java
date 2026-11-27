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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AlterJobCommandTest extends TestWithFeService {
    private static final String SOURCE = "S3('uri'='s3://bucket/data.csv', 'format'='csv', "
            + "'csv_schema'='c1:int', 's3.endpoint'='s3.example.com', 's3.region'='us-east-1', "
            + "'s3.access_key'='test', 's3.secret_key'='test')";
    private static final String ORIGINAL = "INSERT INTO target SELECT c1 FROM " + SOURCE;
    private static final String QUERY_ERROR = "The query outside TVF properties cannot be modified";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse("alter_job_priv");
        createTable("CREATE TABLE target (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE private_source (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        createDatabaseAndUse("alter_job_other");
        createTable("CREATE TABLE target (id INT) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        connectContext.setDatabase("alter_job_priv");
        String createRole = "CREATE ROLE 'alter_job_loader'";
        ((CreateRoleCommand) new NereidsParser().parseSingle(createRole))
                .run(connectContext, new StmtExecutor(connectContext, createRole));
        grantPriv("GRANT LOAD_PRIV ON alter_job_priv.target TO ROLE 'alter_job_loader'");
        for (String user : new String[] {"alter_job_editor", "alter_job_wrong_db_editor"}) {
            String createUser = "CREATE USER '" + user + "'";
            ((CreateUserCommand) new NereidsParser().parseSingle(createUser))
                    .run(connectContext, new StmtExecutor(connectContext, createUser));
        }
        grantPriv("GRANT LOAD_PRIV ON alter_job_priv.target TO 'alter_job_editor'");
        grantPriv("GRANT LOAD_PRIV ON alter_job_other.target TO 'alter_job_wrong_db_editor'");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT p.id FROM %s s CROSS JOIN private_source p",
            "SELECT (SELECT max(id) FROM private_source) FROM %s s",
            "SELECT c1 FROM %s UNION ALL SELECT id FROM private_source",
            "SELECT c1 + 1 FROM %s",
            "SELECT c1 FROM %s WHERE c1 > 0",
            "SELECT /*+ SET_VAR(query_timeout=1) */ c1 FROM %s"
    })
    public void testAlterRejectsQueryChanges(String query) throws Exception {
        assertAlter(ORIGINAL, "INSERT INTO target " + String.format(query, SOURCE),
                "alter_job_editor", Collections.emptySet(), QUERY_ERROR);
    }

    @Test
    public void testAlterRejectsUriChanges() throws Exception {
        assertAlter(ORIGINAL, ORIGINAL.replace("s3://bucket/data.csv", "s3://other/data.csv"),
                "alter_job_editor", Collections.emptySet(), "The uri property cannot be modified in ALTER JOB");
        assertAlter(ORIGINAL, ORIGINAL.replace("'uri'='s3://bucket/data.csv', ", ""),
                "alter_job_editor", Collections.emptySet(), "The uri property cannot be modified in ALTER JOB");
    }

    @Test
    public void testAlterRejectsTargetAndAdditionalSourceChanges() throws Exception {
        assertAlter(ORIGINAL, ORIGINAL.replace("INTO target", "INTO private_source"),
                "alter_job_editor", Collections.emptySet(), QUERY_ERROR);
        assertAlter(ORIGINAL, ORIGINAL.replace("FROM S3", "FROM cdc_stream"),
                "alter_job_editor", Collections.emptySet(), QUERY_ERROR);
        assertAlter(ORIGINAL, ORIGINAL + " CROSS JOIN " + SOURCE + " s2",
                "alter_job_editor", Collections.emptySet(), "Only support one source");
    }

    @ParameterizedTest
    @ValueSource(strings = {"s3.access_key", "s3.secret_key"})
    public void testAlterRotatesCredentialsWithoutSourceSelect(String key) throws Exception {
        String original = ORIGINAL + " CROSS JOIN private_source p";
        String changed = original.replace("'" + key + "'='test'", "'" + key + "'='rotated'");
        assertAlter(original, changed, "alter_job_editor", Collections.emptySet(), null);
    }

    @Test
    public void testAlterAcceptsCredentialAdditionAndRemoval() throws Exception {
        String changed = ORIGINAL.replace("'s3.secret_key'='test'",
                "'s3.secret_key'='rotated', 's3.session_token'='token'");
        assertAlter(ORIGINAL, changed, "alter_job_editor", Collections.emptySet(), null);
        assertAlter(changed, ORIGINAL, "alter_job_editor", Collections.emptySet(), null);
    }

    @Test
    public void testAlterAcceptsMutableTvfProperties() throws Exception {
        String changed = ORIGINAL.replace("'format'='csv'", "'format'='json'")
                .replace("'csv_schema'='c1:int', ", "");
        assertAlter(ORIGINAL, changed, "alter_job_editor", Collections.emptySet(), null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"s3.endpoint", "s3.region", "format", "csv_schema"})
    public void testAlterAcceptsOtherS3PropertyChanges(String key) throws Exception {
        String changed = ORIGINAL.replace("'" + key + "'='", "'" + key + "'='changed-");
        assertAlter(ORIGINAL, changed, "alter_job_editor", Collections.emptySet(), null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"provider", "resource"})
    public void testAlterAcceptsOtherS3PropertyAdditionAndRemoval(String key) throws Exception {
        String changed = ORIGINAL.replace("'format'='csv'", "'format'='csv', '" + key + "'='changed'");
        assertAlter(ORIGINAL, changed, "alter_job_editor", Collections.emptySet(), null);
        assertAlter(changed, ORIGINAL, "alter_job_editor", Collections.emptySet(), null);
    }

    @Test
    public void testAlterRejectsCaseConflictingProperties() throws Exception {
        String changed = ORIGINAL.replace("'uri'='s3://bucket/data.csv'",
                "'uri'='s3://bucket/data.csv', 'URI'='s3://other/data.csv'");
        assertAlter(ORIGINAL, changed, "alter_job_editor", Collections.emptySet(), "Duplicate TVF property names");
    }

    @Test
    public void testAlterUsesSavedDatabaseAndCallerRoles() throws Exception {
        String changed = ORIGINAL.replace("'s3.secret_key'='test'", "'s3.secret_key'='rotated'");
        assertAlter(ORIGINAL, changed, "alter_job_wrong_db_editor", Collections.emptySet(), "denied");
        assertAlter(ORIGINAL, changed, "alter_job_wrong_db_editor", Collections.singleton("alter_job_loader"), null);
        // No schema inference is needed, even when the caller has a temporary table with this name.
        createTable("CREATE TEMPORARY TABLE target (unrelated VARCHAR(100)) DISTRIBUTED BY HASH(unrelated) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        assertAlter(ORIGINAL, changed, "alter_job_editor", Collections.emptySet(), null);
    }

    @Test
    public void testAlterPreservesQueryLiteralsAndAcceptsPropertyReordering() throws Exception {
        String original = ORIGINAL.replace("SELECT c1", "SELECT 'test' AS c1");
        assertAlter(original, original.replace("SELECT 'test'", "SELECT 'changed'"),
                "alter_job_editor", Collections.emptySet(), QUERY_ERROR);
        String reordered = ORIGINAL.replace("'s3.access_key'='test', 's3.secret_key'='test'",
                "'s3.secret_key'='test', 's3.access_key'='test'");
        assertAlter(ORIGINAL, reordered, "alter_job_editor", Collections.emptySet(), null);
    }

    @Test
    public void testAlterPreservesUnicodeAndAcceptsWhitespace() throws Exception {
        String original = ORIGINAL.replace("SELECT c1", "SELECT '😀' AS c1");
        String changed = original.replace("'s3.secret_key'='test'", "'s3.secret_key'='rotated'")
                .replace("SELECT ", "SELECT\n  ");
        assertAlter(original, changed, "alter_job_editor", Collections.emptySet(), null);
    }

    @Test
    public void testAlterAcceptsWhitespaceAndComments() throws Exception {
        String changed = ORIGINAL.replace("INSERT INTO", "INSERT /* ordinary comment */ INTO")
                .replace("SELECT c1 FROM", "SELECT\n c1 -- ordinary comment\n FROM")
                .replace("'s3.secret_key'='test'", "'s3.secret_key'='rotated'");
        assertAlter(ORIGINAL, changed, "alter_job_editor", Collections.emptySet(), null);
    }

    @Test
    public void testAlterRejectsKeywordCaseChanges() throws Exception {
        assertAlter(ORIGINAL, ORIGINAL.replace("SELECT", "select"),
                "alter_job_editor", Collections.emptySet(), QUERY_ERROR);
    }

    @Test
    public void testAlterPreservesIdentifierAndLiteralContents() throws Exception {
        assertAlter(ORIGINAL, ORIGINAL.replace("SELECT c1", "SELECT C1"),
                "alter_job_editor", Collections.emptySet(), QUERY_ERROR);
        // COMMENT is a non-reserved keyword, but here it names a column.
        String identifier = ORIGINAL.replace("SELECT c1", "SELECT comment");
        assertAlter(identifier, identifier.replace("SELECT comment", "SELECT COMMENT"),
                "alter_job_editor", Collections.emptySet(), QUERY_ERROR);
        String literal = ORIGINAL.replace("SELECT c1", "SELECT 'A B'");
        assertAlter(literal, literal.replace("'A B'", "'a b'"),
                "alter_job_editor", Collections.emptySet(), QUERY_ERROR);
        assertAlter(literal, literal.replace("'A B'", "'A  B'"),
                "alter_job_editor", Collections.emptySet(), QUERY_ERROR);
    }

    @Test
    public void testAlterLocksCdcSourceAndChecksDriverPrivileges() throws Exception {
        String original = "INSERT INTO target SELECT id FROM cdc_stream("
                + "'type'='postgres', 'jdbc_url'='jdbc:postgresql://host/db', 'driver_url'='driver.jar', "
                + "'driver_class'='org.postgresql.Driver', 'database'='db', 'schema'='public', "
                + "'table'='source', 'offset'='initial', 'user'='reader', 'password'='old')";
        assertAlter(original, original.replace("'old'", "'rotated'"),
                "alter_job_editor", Collections.emptySet(), null);
        for (String key : new String[] {"jdbc_url", "table", "schema"}) {
            assertAlter(original, original.replace("'" + key + "'='", "'" + key + "'='changed-"),
                    "alter_job_editor", Collections.emptySet(), "property cannot be modified");
        }
        for (String key : new String[] {"driver_url", "driver_class"}) {
            assertAlter(original, original.replace("'" + key + "'='", "'" + key + "'='changed-"),
                    "alter_job_editor", Collections.emptySet(), null);
        }
        assertAlter(original, original.replace("driver.jar", "https://example.invalid/driver.jar"),
                "alter_job_editor", Collections.emptySet(), "HTTP/HTTPS CDC driver URLs require ADMIN privilege");
        String tuned = original.replace("'offset'='initial'",
                "'offset'='initial', 'snapshot_parallelism'='4', 'snapshot_split_size'='2048'");
        assertAlter(original, tuned, "alter_job_editor", Collections.emptySet(), null);
    }

    @Test
    public void testAlterRejectsHintInsideTvfProperties() throws Exception {
        String changed = ORIGINAL.replace("'format'", "/*+ PREAGGOPEN */ 'format'");
        assertAlter(ORIGINAL, changed, "alter_job_editor", Collections.emptySet(), QUERY_ERROR);
    }

    @Test
    public void testAlterPreservesHintWhenRotatingCredentials() throws Exception {
        String original = ORIGINAL.replace("'format'", "/*+ PREAGGOPEN */ 'format'");
        String changed = original.replace("'s3.secret_key'='test'", "'s3.secret_key'='rotated'");
        assertAlter(original, changed, "alter_job_editor", Collections.emptySet(), null);
    }

    @Test
    public void testAlterAcceptsHintTextInsideCredentialValue() throws Exception {
        String changed = ORIGINAL.replace("'s3.secret_key'='test'",
                "'s3.secret_key'='rotated/*+ PREAGGOPEN */'");
        assertAlter(ORIGINAL, changed, "alter_job_editor", Collections.emptySet(), null);
        assertAlter(changed, ORIGINAL, "alter_job_editor", Collections.emptySet(), null);
    }

    @Test
    public void testAlterParsesCredentialsInBackgroundSqlMode() throws Exception {
        long originalMode = connectContext.getSessionVariable().getSqlMode();
        try {
            connectContext.getSessionVariable().setSqlMode(originalMode | SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
            // In the editor's mode this is one credential value; the background sees an injected JOIN.
            String changed = "INSERT INTO target SELECT c1 FROM S3("
                    + "\"uri\"='s3://bucket/data.csv', \"format\"='csv', \"csv_schema\"='c1:int', "
                    + "\"s3.endpoint\"='s3.example.com', \"s3.region\"='us-east-1', "
                    + "\"s3.secret_key\"='ends-in-backslash\\', "
                    + "\"s3.access_key\"=') CROSS JOIN private_source p -- ')";
            String original = changed.replace("ends-in-backslash\\", "old")
                    .replace(") CROSS JOIN private_source p -- ", "old");
            assertAlter(original, changed, "alter_job_editor", Collections.emptySet(), QUERY_ERROR);
            Assertions.assertEquals(originalMode | SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES,
                    connectContext.getSessionVariable().getSqlMode());
        } finally {
            connectContext.getSessionVariable().setSqlMode(originalMode);
        }
    }

    @Test
    public void testMetadataUsesJobSqlModeAndRestoresCaller() {
        long callerMode = connectContext.getSessionVariable().getSqlMode();
        try {
            for (long jobMode : new long[] {0, SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES}) {
                long editorMode = jobMode ^ SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES;
                connectContext.getSessionVariable().setSqlMode(editorMode);
                StreamingInsertJob job = Deencapsulation.newInstance(StreamingInsertJob.class);
                job.setCurrentDbName("alter_job_priv");
                job.setCreateUser(connectContext.getCurrentUserIdentity());
                job.setExecuteSql(ORIGINAL.replace("'s3.secret_key'='test'", "'s3.secret_key'='test\\n'"));
                Deencapsulation.setField(job, "properties",
                        Collections.singletonMap("session.sql_mode", Long.toString(jobMode)));

                job.initLogicalPlan(true);
                Map<String, String> props = Deencapsulation.invoke(job, "getOriginTvfProps");

                Assertions.assertEquals(jobMode == 0 ? "test\n" : "test\\n", props.get("s3.secret_key"));
                Assertions.assertSame(connectContext, ConnectContext.get());
                Assertions.assertEquals(editorMode, connectContext.getSessionVariable().getSqlMode());
                Assertions.assertEquals(editorMode != 0, SqlModeHelper.hasNoBackSlashEscapes());
            }
        } finally {
            connectContext.getSessionVariable().setSqlMode(callerMode);
            connectContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testAlterRejectsSqlModeChanges() throws Exception {
        assertAlter(ORIGINAL, null, "alter_job_editor", Collections.emptySet(),
                "session.sql_mode cannot be modified", Collections.singletonMap("session.sql_mode", "2097152"));
    }

    private void assertAlter(String original, String changed, String user, Set<String> roles,
            String expectedError) throws Exception {
        assertAlter(original, changed, user, roles, expectedError, Collections.emptyMap());
    }

    private void assertAlter(String original, String changed, String user, Set<String> roles,
            String expectedError, Map<String, String> properties) throws Exception {
        Env env = Env.getCurrentEnv();
        JobManager<?, ?> originalManager = env.getJobManager();
        JobManager<?, ?> manager = Mockito.mock(JobManager.class);
        StreamingInsertJob job = Mockito.mock(StreamingInsertJob.class);
        JobExecutionConfiguration config = new JobExecutionConfiguration();
        config.setExecuteType(JobExecuteType.STREAMING);
        Mockito.when(job.getJobStatus()).thenReturn(JobStatus.PAUSED);
        Mockito.when(job.getJobConfig()).thenReturn(config);
        Mockito.when(job.getExecuteSql()).thenReturn(original);
        Mockito.when(job.getCurrentDbName()).thenReturn("alter_job_priv");
        Mockito.when(job.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(job.checkPrivilege(Mockito.any())).thenCallRealMethod();
        Mockito.when(job.getSqlMode()).thenCallRealMethod();
        Mockito.doReturn(job).when(manager).getJobByName("alter_priv");
        UserIdentity originalUser = connectContext.getCurrentUserIdentity();
        Set<String> originalRoles = connectContext.getAuthenticatedRoles();
        NameSpaceContext originalNamespace = connectContext.getNameSpaceContext();
        try {
            Deencapsulation.setField(env, "jobManager", manager);
            connectContext.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(user, "%"));
            connectContext.setAuthenticatedRoles(roles);
            // Check LOAD on the saved target even when the editor uses another database.
            connectContext.changeDefaultCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);
            connectContext.setDatabase("alter_job_other");
            connectContext.setThreadLocalInfo();
            String sql = "ALTER JOB alter_priv";
            AlterJobCommand command = new AlterJobCommand("alter_priv", properties, changed,
                    null, null, Collections.emptyMap(), Collections.emptyMap());
            if (expectedError == null) {
                command.run(connectContext, new StmtExecutor(connectContext, sql));
                Mockito.verify(manager).alterJob(command);
            } else {
                Exception denied = Assertions.assertThrows(Exception.class,
                        () -> command.run(connectContext, new StmtExecutor(connectContext, sql)));
                Assertions.assertTrue(denied.getMessage().contains(expectedError), denied.getMessage());
                Mockito.verify(manager, Mockito.never()).alterJob(Mockito.any());
            }
            Assertions.assertSame(connectContext, ConnectContext.get());
            Assertions.assertEquals(InternalCatalog.INTERNAL_CATALOG_NAME, connectContext.getDefaultCatalog());
            Assertions.assertEquals("alter_job_other", connectContext.getDatabase());
            Assertions.assertEquals(UserIdentity.createAnalyzedUserIdentWithIp(user, "%"),
                    connectContext.getCurrentUserIdentity());
            Assertions.assertEquals(roles, connectContext.getAuthenticatedRoles());
        } finally {
            connectContext.restoreNameSpaceContext(originalNamespace);
            connectContext.setAuthenticatedRoles(originalRoles);
            connectContext.setCurrentUserIdentity(originalUser);
            Deencapsulation.setField(env, "jobManager", originalManager);
        }
    }

    @Test
    public void testAlterRefreshesCachedCredentialsForProvider() throws Exception {
        StreamingInsertJob job = Mockito.spy(Deencapsulation.newInstance(StreamingInsertJob.class));
        Mockito.doNothing().when(job).logUpdateOperation();
        job.setJobStatus(JobStatus.PAUSED);
        job.setCurrentDbName("alter_job_priv");
        job.setCreateUser(connectContext.getCurrentUserIdentity());
        job.setExecuteSql(ORIGINAL);
        Map<String, String> properties = new HashMap<>();
        Deencapsulation.setField(job, "properties", properties);
        Deencapsulation.setField(job, "jobProperties", new StreamingJobProperties(properties));
        Deencapsulation.setField(job, "tvfType", "s3");
        SourceOffsetProvider provider = Mockito.mock(SourceOffsetProvider.class);
        Deencapsulation.setField(job, "offsetProvider", provider);
        // Warm both the parsed SQL and the original TVF property cache.
        Deencapsulation.invoke(job, "fetchMeta");
        Mockito.verify(provider).fetchRemoteMeta(Mockito.any(), Mockito.argThat(props ->
                "test".equals(props.get("s3.secret_key"))));
        Mockito.clearInvocations(provider);
        String replacement = ORIGINAL.replace("'s3.secret_key'='test'", "'s3.secret_key'='rotated-test-secret'");
        try {
            job.alterJob(new AlterJobCommand("alter_credentials", Collections.emptyMap(), replacement,
                    null, null, Collections.emptyMap(), Collections.emptyMap()));
        } finally {
            connectContext.setThreadLocalInfo();
        }
        Deencapsulation.invoke(job, "fetchMeta");
        Mockito.verify(provider).fetchRemoteMeta(Mockito.any(), Mockito.argThat(props ->
                "rotated-test-secret".equals(props.get("s3.secret_key"))));
    }
}
