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

package org.apache.doris.datasource.property;

import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.CreateRepositoryStmt;
import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.TableValuedFunctionRef;
import org.apache.doris.backup.Repository;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.property.constants.CosProperties;
import org.apache.doris.datasource.property.constants.MinioProperties;
import org.apache.doris.datasource.property.constants.ObsProperties;
import org.apache.doris.datasource.property.constants.OssProperties;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.tablefunction.S3TableValuedFunction;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PropertyConverterTest extends TestWithFeService {

    private final Set<String> checkSet = new HashSet<>();
    private final Map<String, String> expectedCredential = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    @Override
    protected void runBeforeAll() throws Exception {
        createDorisCluster();
        createDatabase("mock_db");
        useDatabase("mock_db");
        createTable("create table mock_tbl1 \n" + "(k1 int, k2 int) distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1');");

        List<String> withoutPrefix = ImmutableList.of("endpoint", "access_key", "secret_key");
        checkSet.addAll(withoutPrefix);
        checkSet.addAll(S3Properties.Env.REQUIRED_FIELDS);
        expectedCredential.put("access_key", "akk");
        expectedCredential.put("secret_key", "skk");
    }

    @Test
    public void testOutFileS3PropertiesConverter() throws Exception {
        String query = "select * from mock_tbl1 \n"
                + "into outfile 's3://bucket/mock_dir'\n"
                + "format as csv\n"
                + "properties(\n"
                + "    'AWS_ENDPOINT' = 'http://127.0.0.1:9000',\n"
                + "    'AWS_ACCESS_KEY' = 'akk',\n"
                + "    'AWS_SECRET_KEY'='akk',\n"
                + "    'AWS_REGION' = 'mock',\n"
                + "    'use_path_style' = 'true'\n"
                + ");";
        QueryStmt analyzedOutStmt = createStmt(query);
        Assertions.assertTrue(analyzedOutStmt.hasOutFileClause());

        OutFileClause outFileClause = analyzedOutStmt.getOutFileClause();
        boolean isOutFileClauseAnalyzed = Deencapsulation.getField(outFileClause, "isAnalyzed");
        Assertions.assertTrue(isOutFileClauseAnalyzed);

        Assertions.assertEquals(outFileClause.getFileFormatType(), TFileFormatType.FORMAT_CSV_PLAIN);

        String queryNew = "select * from mock_tbl1 \n"
                + "into outfile 's3://bucket/mock_dir'\n"
                + "format as csv\n"
                + "properties(\n"
                + "    's3.endpoint' = 'http://127.0.0.1:9000',\n"
                + "    's3.access_key' = 'akk',\n"
                + "    's3.secret_key'='akk',\n"
                + "    'use_path_style' = 'true'\n"
                + ");";
        QueryStmt analyzedOutStmtNew = createStmt(queryNew);
        Assertions.assertTrue(analyzedOutStmtNew.hasOutFileClause());

        OutFileClause outFileClauseNew = analyzedOutStmtNew.getOutFileClause();
        boolean isNewAnalyzed = Deencapsulation.getField(outFileClauseNew, "isAnalyzed");
        Assertions.assertTrue(isNewAnalyzed);
    }

    @Test
    public void testS3SourcePropertiesConverter() throws Exception {
        String queryOld = "CREATE RESOURCE 'remote_s3'\n"
                + "PROPERTIES\n"
                + "(\n"
                + "   'type' = 's3',\n"
                + "   'AWS_ENDPOINT' = 's3.us-east-1.amazonaws.com',\n"
                + "   'AWS_REGION' = 'us-east-1',\n"
                + "   'AWS_ACCESS_KEY' = 'akk',\n"
                + "   'AWS_SECRET_KEY' = 'skk',\n"
                + "   'AWS_ROOT_PATH' = '/',\n"
                + "   'AWS_BUCKET' = 'bucket',\n"
                + "   's3_validity_check' = 'false'"
                + ");";
        CreateResourceStmt analyzedResourceStmt = createStmt(queryOld);
        Assertions.assertEquals(analyzedResourceStmt.getProperties().size(), 8);
        Resource resource = Resource.fromStmt(analyzedResourceStmt);
        // will add converted properties
        Assertions.assertEquals(resource.getCopiedProperties().size(), 20);

        String queryNew = "CREATE RESOURCE 'remote_new_s3'\n"
                + "PROPERTIES\n"
                + "(\n"
                + "   'type' = 's3',\n"
                + "   's3.endpoint' = 'http://s3.us-east-1.amazonaws.com',\n"
                + "   's3.region' = 'us-east-1',\n"
                + "   's3.access_key' = 'akk',\n"
                + "   's3.secret_key' = 'skk',\n"
                + "   's3.root.path' = '/',\n"
                + "   's3.bucket' = 'bucket',\n"
                + "   's3_validity_check' = 'false'"
                + ");";
        CreateResourceStmt analyzedResourceStmtNew = createStmt(queryNew);
        Assertions.assertEquals(analyzedResourceStmtNew.getProperties().size(), 8);
        Resource newResource = Resource.fromStmt(analyzedResourceStmtNew);
        // will add converted properties
        Assertions.assertEquals(newResource.getCopiedProperties().size(), 14);

    }

    @Test
    public void testS3RepositoryPropertiesConverter() throws Exception {
        FeConstants.runningUnitTest = true;
        String s3Repo = "CREATE REPOSITORY `s3_repo`\n"
                + "WITH S3\n"
                + "ON LOCATION 's3://s3-repo'\n"
                + "PROPERTIES\n"
                + "(\n"
                + "    'AWS_ENDPOINT' = 'http://s3.us-east-1.amazonaws.com',\n"
                + "    'AWS_ACCESS_KEY' = 'akk',\n"
                + "    'AWS_SECRET_KEY'='skk',\n"
                + "    'AWS_REGION' = 'us-east-1'\n"
                + ");";
        CreateRepositoryStmt analyzedStmt = createStmt(s3Repo);
        Assertions.assertEquals(analyzedStmt.getProperties().size(), 4);
        Repository repository = getRepository(analyzedStmt, "s3_repo");
        Assertions.assertEquals(9, repository.getRemoteFileSystem().getProperties().size());

        String s3RepoNew = "CREATE REPOSITORY `s3_repo_new`\n"
                + "WITH S3\n"
                + "ON LOCATION 's3://s3-repo'\n"
                + "PROPERTIES\n"
                + "(\n"
                + "    's3.endpoint' = 'http://s3.us-east-1.amazonaws.com',\n"
                + "    's3.access_key' = 'akk',\n"
                + "    's3.secret_key' = 'skk'\n"
                + ");";
        CreateRepositoryStmt analyzedStmtNew = createStmt(s3RepoNew);
        Assertions.assertEquals(analyzedStmtNew.getProperties().size(), 3);
        Repository repositoryNew = getRepository(analyzedStmtNew, "s3_repo_new");
        Assertions.assertEquals(repositoryNew.getRemoteFileSystem().getProperties().size(), 4);
    }

    private static Repository getRepository(CreateRepositoryStmt analyzedStmt, String name) throws DdlException {
        Env.getCurrentEnv().getBackupHandler().createRepository(analyzedStmt);
        return Env.getCurrentEnv().getBackupHandler().getRepoMgr().getRepo(name);
    }

    @Test
    public void testBosBrokerRepositoryPropertiesConverter() throws Exception {
        FeConstants.runningUnitTest = true;
        String bosBroker = "CREATE REPOSITORY `bos_broker_repo`\n"
                + "WITH BROKER `bos_broker`\n"
                + "ON LOCATION 'bos://backup'\n"
                + "PROPERTIES\n"
                + "(\n"
                + "    'bos_endpoint' = 'http://gz.bcebos.com',\n"
                + "    'bos_accesskey' = 'akk',\n"
                + "    'bos_secret_accesskey'='skk'\n"
                + ");";
        CreateRepositoryStmt analyzedStmt = createStmt(bosBroker);
        analyzedStmt.getProperties();
        Assertions.assertEquals(analyzedStmt.getProperties().size(), 3);

        List<Pair<String, Integer>> brokers = ImmutableList.of(Pair.of("127.0.0.1", 9999));
        Env.getCurrentEnv().getBrokerMgr().addBrokers("bos_broker", brokers);

        Repository repositoryNew = getRepository(analyzedStmt, "bos_broker_repo");
        Assertions.assertEquals(repositoryNew.getRemoteFileSystem().getProperties().size(), 4);
    }

    @Test
    public void testS3TVFPropertiesConverter() throws Exception {
        FeConstants.runningUnitTest = true;
        String queryOld = "select * from s3(\n"
                    + "  'uri' = 'http://s3.us-east-1.amazonaws.com/test.parquet',\n"
                    + "  'access_key' = 'akk',\n"
                    + "  'secret_key' = 'skk',\n"
                    + "  'region' = 'us-east-1',\n"
                    + "  'format' = 'parquet',\n"
                    + "  'use_path_style' = 'true'\n"
                    + ") limit 10;";
        SelectStmt analyzedStmt = createStmt(queryOld);
        Assertions.assertEquals(analyzedStmt.getTableRefs().size(), 1);
        TableValuedFunctionRef oldFuncTable = (TableValuedFunctionRef) analyzedStmt.getTableRefs().get(0);
        S3TableValuedFunction s3Tvf = (S3TableValuedFunction) oldFuncTable.getTableFunction();
        Assertions.assertEquals(s3Tvf.getBrokerDesc().getProperties().size(), 9);

        String queryNew = "select * from s3(\n"
                    + "  'uri' = 'http://s3.us-east-1.amazonaws.com/test.parquet',\n"
                    + "  's3.access_key' = 'akk',\n"
                    + "  's3.secret_key' = 'skk',\n"
                    + "  'format' = 'parquet',\n"
                    + "  'use_path_style' = 'true'\n"
                    + ") limit 10;";
        SelectStmt analyzedStmtNew = createStmt(queryNew);
        Assertions.assertEquals(analyzedStmtNew.getTableRefs().size(), 1);
        TableValuedFunctionRef newFuncTable = (TableValuedFunctionRef) analyzedStmt.getTableRefs().get(0);
        S3TableValuedFunction newS3Tvf = (S3TableValuedFunction) newFuncTable.getTableFunction();
        Assertions.assertEquals(newS3Tvf.getBrokerDesc().getProperties().size(), 9);
    }

    @Test
    public void testAWSOldCatalogPropertiesConverter() throws Exception {
        String queryOld = "create catalog hms_s3_old properties (\n"
                    + "    'type'='hms',\n"
                    + "    'hive.metastore.uris' = 'thrift://172.21.0.44:7004',\n"
                    + "    'AWS_ENDPOINT' = 's3.us-east-1.amazonaws.com',\n"
                    + "    'AWS_REGION' = 'us-east-1',\n"
                    + "    'AWS_ACCESS_KEY' = 'akk',\n"
                    + "    'AWS_SECRET_KEY' = 'skk'\n"
                    + ");";
        CreateCatalogStmt analyzedStmt = createStmt(queryOld);
        HMSExternalCatalog catalog = createAndGetCatalog(analyzedStmt, "hms_s3_old");
        Map<String, String> properties = catalog.getCatalogProperty().getProperties();
        Assertions.assertEquals(properties.size(), 12);

        Map<String, String> hdProps = catalog.getCatalogProperty().getHadoopProperties();
        Assertions.assertEquals(hdProps.size(), 21);
    }

    @Test
    public void testS3CatalogPropertiesConverter() throws Exception {
        String query = "create catalog hms_s3 properties (\n"
                    + "    'type'='hms',\n"
                    + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                    + "    's3.endpoint' = 's3.us-east-1.amazonaws.com',\n"
                    + "    's3.access_key' = 'akk',\n"
                    + "    's3.secret_key' = 'skk'\n"
                    + ");";
        CreateCatalogStmt analyzedStmt = createStmt(query);
        HMSExternalCatalog catalog = createAndGetCatalog(analyzedStmt, "hms_s3");
        Map<String, String> properties = catalog.getCatalogProperty().getProperties();
        Assertions.assertEquals(properties.size(), 11);

        Map<String, String> hdProps = catalog.getCatalogProperty().getHadoopProperties();
        Assertions.assertEquals(hdProps.size(), 20);
    }

    @Test
    public void testGlueCatalogPropertiesConverter() throws Exception {
        String queryOld = "create catalog hms_glue_old properties (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.type'='glue',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'aws.glue.endpoint' = 'glue.us-east-1.amazonaws.com',\n"
                + "    'aws.glue.access-key' = 'akk',\n"
                + "    'aws.glue.secret-key' = 'skk',\n"
                + "    'aws.region' = 'us-east-1'\n"
                + ");";
        String catalogName = "hms_glue_old";
        CreateCatalogStmt analyzedStmt = createStmt(queryOld);
        HMSExternalCatalog catalog = createAndGetCatalog(analyzedStmt, catalogName);
        Map<String, String> properties = catalog.getCatalogProperty().getProperties();
        Assertions.assertEquals(properties.size(), 20);

        Map<String, String> hdProps = catalog.getCatalogProperty().getHadoopProperties();
        Assertions.assertEquals(hdProps.size(), 29);

        String query = "create catalog hms_glue properties (\n"
                    + "    'type'='hms',\n"
                    + "    'hive.metastore.type'='glue',\n"
                    + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                    + "    'glue.endpoint' = 'glue.us-east-1.amazonaws.com',\n"
                    + "    'glue.access_key' = 'akk',\n"
                    + "    'glue.secret_key' = 'skk'\n"
                    + ");";
        catalogName = "hms_glue";
        CreateCatalogStmt analyzedStmtNew = createStmt(query);
        HMSExternalCatalog catalogNew = createAndGetCatalog(analyzedStmtNew, catalogName);
        Map<String, String> propertiesNew = catalogNew.getCatalogProperty().getProperties();
        Assertions.assertEquals(propertiesNew.size(), 20);

        Map<String, String> hdPropsNew = catalogNew.getCatalogProperty().getHadoopProperties();
        Assertions.assertEquals(hdPropsNew.size(), 29);
    }

    @Test
    public void testS3CompatibleCatalogPropertiesConverter() throws Exception {
        String catalogName0 = "hms_cos";
        String query0 = "create catalog " + catalogName0 + " properties (\n"
                    + "    'type'='hms',\n"
                    + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                    + "    'cos.endpoint' = 'cos.ap-beijing.myqcloud.com',\n"
                    + "    'cos.access_key' = 'akk',\n"
                    + "    'cos.secret_key' = 'skk'\n"
                    + ");";
        testS3CompatibleCatalogProperties(catalogName0, CosProperties.COS_PREFIX,
                    "cos.ap-beijing.myqcloud.com", query0, 11, 16);

        String catalogName1 = "hms_oss";
        String query1 = "create catalog " + catalogName1 + " properties (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'oss.endpoint' = 'oss.oss-cn-beijing.aliyuncs.com',\n"
                + "    'oss.access_key' = 'akk',\n"
                + "    'oss.secret_key' = 'skk'\n"
                + ");";
        testS3CompatibleCatalogProperties(catalogName1, OssProperties.OSS_PREFIX,
                    "oss.oss-cn-beijing.aliyuncs.com", query1, 11, 16);

        String catalogName2 = "hms_minio";
        String query2 = "create catalog " + catalogName2 + " properties (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'minio.endpoint' = 'http://127.0.0.1',\n"
                + "    'minio.access_key' = 'akk',\n"
                + "    'minio.secret_key' = 'skk'\n"
                + ");";
        testS3CompatibleCatalogProperties(catalogName2, MinioProperties.MINIO_PREFIX,
                    "http://127.0.0.1", query2, 11, 20);

        String catalogName3 = "hms_obs";
        String query3 = "create catalog hms_obs properties (\n"
                + "    'type'='hms',\n"
                + "    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',\n"
                + "    'obs.endpoint' = 'obs.cn-north-4.myhuaweicloud.com',\n"
                + "    'obs.access_key' = 'akk',\n"
                + "    'obs.secret_key' = 'skk'\n"
                + ");";
        testS3CompatibleCatalogProperties(catalogName3, ObsProperties.OBS_PREFIX,
                "obs.cn-north-4.myhuaweicloud.com", query3, 11, 16);
    }

    private void testS3CompatibleCatalogProperties(String catalogName, String prefix,
                                                   String endpoint, String sql,
                                                   int catalogPropsSize, int bePropsSize) throws Exception {
        Env.getCurrentEnv().getCatalogMgr().dropCatalog(new DropCatalogStmt(true, catalogName));
        CreateCatalogStmt analyzedStmt = createStmt(sql);
        HMSExternalCatalog catalog = createAndGetCatalog(analyzedStmt, catalogName);
        Map<String, String> properties = catalog.getCatalogProperty().getProperties();
        Assertions.assertEquals(properties.size(), catalogPropsSize);

        Map<String, String> hdProps = catalog.getCatalogProperty().getHadoopProperties();
        Assertions.assertEquals(hdProps.size(), bePropsSize);

        Map<String, String> expectedMetaProperties = new HashMap<>();
        expectedMetaProperties.put("endpoint", endpoint);
        expectedMetaProperties.put("AWS_ENDPOINT", endpoint);
        expectedMetaProperties.putAll(expectedCredential);
        checkExpectedProperties(prefix, properties, expectedMetaProperties);
    }

    private void checkExpectedProperties(String prefix, Map<String, String> properties,
                                         Map<String, String> expectedProperties) {
        properties.forEach((key, value) -> {
            if (key.startsWith(prefix)) {
                String keyToCheck = key.replace(prefix, "");
                if (checkSet.contains(keyToCheck)) {
                    Assertions.assertEquals(value, expectedProperties.get(keyToCheck));
                }
            }
        });
    }

    private static HMSExternalCatalog createAndGetCatalog(CreateCatalogStmt analyzedStmt, String name)
            throws UserException {
        Env.getCurrentEnv().getCatalogMgr().createCatalog(analyzedStmt);
        return (HMSExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(name);
    }


    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();
    }
}
