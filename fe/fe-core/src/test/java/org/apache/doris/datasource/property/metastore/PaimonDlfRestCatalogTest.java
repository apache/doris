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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.backup.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.fs.StorageTypeMapper;
import org.apache.doris.fs.remote.S3FileSystem;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Disabled("set aliyun access key, secret key before running the test")
public class PaimonDlfRestCatalogTest {

    private String aliyunAk = "";
    private String aliyunSk = "";

    @Test
    public void testPaimonDlfRestCatalog() throws DatabaseNotExistException, TableNotExistException, UserException {
        org.apache.paimon.catalog.Catalog catalog = initPaimonDlfRestCatalog();
        System.out.println(catalog);
        List<String> dbs = catalog.listDatabases();
        for (String dbName : dbs) {
            System.out.println("test debug get db: " + dbName);
            Database db = catalog.getDatabase(dbName);
            System.out.println("test debug get db instance: " + db.name() + ", " + db.options() + ", " + db.comment());
            List<String> tables = catalog.listTables(dbName);
            for (String tblName : tables) {
                System.out.println("test debug get table: " + tblName);
                if (!tblName.equalsIgnoreCase("users_samples")) {
                    continue;
                }
                org.apache.paimon.table.Table table = catalog.getTable(
                        org.apache.paimon.catalog.Identifier.create(dbName, tblName));
                System.out.println("test debug get table instance: " + table.name() + ", " + table.options() + ", "
                        + table.comment());

                FileIO fileIO = table.fileIO();
                if (fileIO instanceof RESTTokenFileIO) {
                    System.out.println("test debug get file io instance: " + fileIO.getClass().getName());
                    RESTTokenFileIO restTokenFileIO = (RESTTokenFileIO) fileIO;
                    RESTToken restToken = restTokenFileIO.validToken();
                    Map<String, String> tokens = restToken.token();
                    for (Map.Entry<String, String> kv : tokens.entrySet()) {
                        System.out.println("test debug get token: " + kv.getKey() + ", " + kv.getValue());
                    }
                    // String accType = tokens.get("fs.oss.token.access.type");
                    String tmpAk = tokens.get("fs.oss.accessKeyId");
                    String tmpSk = tokens.get("fs.oss.accessKeySecret");
                    String stsToken = tokens.get("fs.oss.securityToken");
                    String endpoint = tokens.get("fs.oss.endpoint");

                    ReadBuilder readBuilder = table.newReadBuilder();
                    List<Split> paimonSplits = readBuilder.newScan().plan().splits();
                    for (Split split : paimonSplits) {
                        System.out.println("test debug get split: " + split);
                        if (split instanceof DataSplit) {
                            DataSplit dataSplit = (DataSplit) split;
                            Optional<List<RawFile>> rawFiles = dataSplit.convertToRawFiles();
                            if (rawFiles.isPresent()) {
                                for (RawFile rawFile : rawFiles.get()) {
                                    System.out.println("test debug get raw file: " + rawFile.path());
                                    readByDorisS3FileSystem(rawFile.path(), tmpAk, tmpSk, stsToken, endpoint,
                                            "oss-cn-beijing");
                                    readByAwsSdkV1(rawFile.path(), tmpAk, tmpSk, stsToken, endpoint, "oss-cn-beijing");
                                    readByAwsSdkV2(rawFile.path(), tmpAk, tmpSk, stsToken, endpoint, "oss-cn-beijing");
                                }
                            } else {
                                System.out.println("test debug no raw files in this data split");
                            }
                        }
                    }
                } else {
                    System.out.println(
                            "test debug fileIO is not RESTTokenFileIO, it is: " + fileIO.getClass().getName());
                }
            }
        }
    }

    /**
     * https://paimon.apache.org/docs/1.1/concepts/rest/dlf/
     * CREATE CATALOG `paimon-rest-catalog`
     * WITH (
     * 'type' = 'paimon',
     * 'uri' = '<catalog server url>',
     * 'metastore' = 'rest',
     * 'warehouse' = 'my_instance_name',
     * 'token.provider' = 'dlf',
     * 'dlf.access-key-id'='<access-key-id>',
     * 'dlf.access-key-secret'='<access-key-secret>',
     * );
     *
     * @return
     */
    private org.apache.paimon.catalog.Catalog initPaimonDlfRestCatalog() {
        HiveConf hiveConf = new HiveConf();
        Options catalogOptions = new Options();
        catalogOptions.set("metastore", "rest");
        catalogOptions.set("warehouse", "new_dfl_paimon_catalog");
        catalogOptions.set("uri", "http://cn-beijing-vpc.dlf.aliyuncs.com");
        catalogOptions.set("token.provider", "dlf");
        catalogOptions.set("dlf.access-key-id", aliyunAk);
        catalogOptions.set("dlf.access-key-secret", aliyunSk);
        CatalogContext catalogContext = CatalogContext.create(catalogOptions, hiveConf);
        return CatalogFactory.createCatalog(catalogContext);
    }

    private void readByDorisS3FileSystem(String path, String tmpAk, String tmpSk, String stsToken, String endpoint,
            String region) {
        // replace "oss://" with "s3://"
        String finalPath = path.startsWith("oss://") ? path.replace("oss://", "s3://") : path;
        System.out.println("test debug final path: " + finalPath);
        Map<String, String> props = Maps.newHashMap();
        props.put("s3.endpoint", endpoint);
        props.put("s3.region", region);
        props.put("s3.access_key", tmpAk);
        props.put("s3.secret_key", tmpSk);
        props.put("s3.session_token", stsToken);
        S3Properties s3Properties = S3Properties.of(props);
        S3FileSystem s3Fs = (S3FileSystem) StorageTypeMapper.create(s3Properties);
        File localFile = new File("/tmp/s3/" + System.currentTimeMillis() + ".data");
        if (localFile.exists()) {
            try {
                Files.delete(localFile.toPath());
            } catch (IOException e) {
                System.err.println("Failed to delete existing local file: " + localFile.getAbsolutePath());
                e.printStackTrace();
            }
        } else {
            localFile.getParentFile().mkdirs(); // Ensure parent directories exist
        }
        Status st = s3Fs.getObjStorage().getObject(finalPath, localFile);
        System.out.println(st);
        if (st.ok()) {
            System.out.println("test debug local path: " + localFile.getAbsolutePath());
        } else {
            Assertions.fail(st.toString());
        }
    }

    private void readByAwsSdkV1(String filePath, String accessKeyId, String secretAccessKey,
            String sessionToken, String endpoint, String region) throws UserException {
        BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
                accessKeyId,
                secretAccessKey,
                sessionToken
        );
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setSignerOverride("AWSS3V4SignerType");

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
                .withClientConfiguration(clientConfig)
                .withPathStyleAccessEnabled(false)
                .build();

        S3URI s3URI = S3URI.create(filePath);
        System.out.println("test debug s3uri: " + s3URI);
        try {
            String content = downloadAndReadFileWithSdkV1(s3Client, s3URI.getBucket(), s3URI.getKey());
            System.out.println("Content: " + content);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail(Util.getRootCauseMessage(e));
        }
    }

    private String downloadAndReadFileWithSdkV1(AmazonS3 s3Client, String bucketName, String objectKey)
            throws IOException {
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, objectKey));
        try (InputStream inputStream = s3Object.getObjectContent();
                InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            StringBuilder content = new StringBuilder();
            char[] buffer = new char[1024];
            int bytesRead;
            while ((bytesRead = reader.read(buffer)) != -1) {
                content.append(buffer, 0, bytesRead);
            }
            return content.toString();
        }
    }

    private void readByAwsSdkV2(String path, String tmpAk, String tmpSk, String stsToken, String endpoint,
            String region) {
        S3Client s3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create(tmpAk, tmpSk,
                        stsToken)))
                .region(Region.of(region))
                .endpointOverride(URI.create("https://" + endpoint))
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(false)
                        .build())
                .build();
        try {
            S3URI s3URI = S3URI.create(path);
            System.out.println("test debug s3uri: " + s3URI);
            downloadAndReadFileWithSdkV2(s3Client, s3URI.getBucket(), s3URI.getKey());
        } catch (Exception e) {
            Assertions.fail(Util.getRootCauseMessage(e));
        } finally {
            s3Client.close();
        }
    }

    private void downloadAndReadFileWithSdkV2(S3Client s3Client, String bucketName, String objectKey)
            throws IOException {
        software.amazon.awssdk.services.s3.model.GetObjectRequest request
                = software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        try (ResponseInputStream inputStream = s3Client.getObject(request);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        }
    }
}
