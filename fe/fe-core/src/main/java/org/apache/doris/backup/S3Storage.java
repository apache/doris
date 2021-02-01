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

package org.apache.doris.backup;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3Storage extends BlobStorage {
    public static final String S3_AK = "AWS_ACCESS_KEY";
    public static final String S3_SK = "AWS_SECRET_KEY";
    public static final String S3_ENDPOINT = "AWS_ENDPOINT";
    public static final String S3_REGION = "AWS_REGION";
    private static final Logger LOG = LogManager.getLogger(S3Storage.class);
    private final CaseInsensitiveMap caseInsensitiveProperties;
    private S3Client client;

    public S3Storage(Map<String, String> properties) {
        caseInsensitiveProperties = new CaseInsensitiveMap();
        client = null;
        setProperties(properties);
        setType(StorageBackend.StorageType.S3);
        setName(StorageBackend.StorageType.S3.name());
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        super.setProperties(properties);
        caseInsensitiveProperties.putAll(properties);

    }
    private void checkS3() throws UserException {
        if (!caseInsensitiveProperties.containsKey(S3_REGION)) {
            throw new UserException("AWS_REGION not found.");
        }
        if (!caseInsensitiveProperties.containsKey(S3_ENDPOINT)) {
            throw new UserException("AWS_ENDPOINT not found.");
        }
        if (!caseInsensitiveProperties.containsKey(S3_AK)) {
            throw new UserException("AWS_ACCESS_KEY not found.");
        }
        if (!caseInsensitiveProperties.containsKey(S3_SK)) {
            throw new UserException("AWS_SECRET_KEY not found.");
        }
    }

    private S3Client getClient() throws UserException {
        if (client == null) {
            checkS3();
            URI endpoint = URI.create(caseInsensitiveProperties.get(S3_ENDPOINT).toString());
            AwsBasicCredentials awsBasic = AwsBasicCredentials.create(
                caseInsensitiveProperties.get(S3_AK).toString(),
                caseInsensitiveProperties.get(S3_SK).toString());
            StaticCredentialsProvider scp = StaticCredentialsProvider.create(awsBasic);
            EqualJitterBackoffStrategy backoffStrategy = EqualJitterBackoffStrategy
                .builder()
                .baseDelay(Duration.ofSeconds(1))
                .maxBackoffTime(Duration.ofMinutes(1))
                .build();
            // retry 3 time with Equal backoff
            RetryPolicy retryPolicy = RetryPolicy
                .builder()
                .numRetries(3)
                .backoffStrategy(backoffStrategy)
                .build();
            ClientOverrideConfiguration clientConf = ClientOverrideConfiguration
                .builder()
                // set retry policy
                .retryPolicy(retryPolicy)
                // using AwsS3V4Signer
                .putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create())
                .build();
            client = S3Client.builder()
                .endpointOverride(endpoint)
                .credentialsProvider(scp)
                .region(Region.of(caseInsensitiveProperties.get(S3_REGION).toString()))
                .overrideConfiguration(clientConf)
                // disable chunkedEncoding because of bos not supported
                .serviceConfiguration(S3Configuration.builder().chunkedEncodingEnabled(false).build())
                .build();
        }
        return client;
    }

    @Override
    public Status downloadWithFileSize(String remoteFilePath, String localFilePath, long fileSize) {
        long start = System.currentTimeMillis();
        S3URI uri = new S3URI(remoteFilePath);
        // Write the data to a local file
        File localFile = new File(localFilePath);
        if (localFile.exists()) {
            try {
                Files.walk(Paths.get(localFilePath), FileVisitOption.FOLLOW_LINKS)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            } catch (IOException e) {
                return new Status(
                    Status.ErrCode.COMMON_ERROR, "failed to delete exist local file: " + localFilePath);
            }
        }
        try {
            GetObjectRequest getObjectRequest =
                GetObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build();
            GetObjectResponse response = getClient().getObject(getObjectRequest, localFile.toPath());
            if (localFile.length() == fileSize) {
                LOG.info(
                    "finished to download from {} to {} with size: {}. cost {} ms",
                    remoteFilePath,
                    localFilePath,
                    fileSize,
                    (System.currentTimeMillis() - start));
                return Status.OK;
            } else {
                return new Status(Status.ErrCode.COMMON_ERROR, response.toString());
            }
        } catch (S3Exception s3Exception) {
            return new Status(
                Status.ErrCode.COMMON_ERROR,
                "get file from s3 error: " + s3Exception.awsErrorDetails().errorMessage());
        } catch (UserException ue) {
            LOG.error("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.toString());
        }
    }

    @Override
    public Status directUpload(String content, String remoteFile) {
        S3URI uri = new S3URI(remoteFile);
        try {
            PutObjectResponse response =
                getClient()
                    .putObject(
                        PutObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build(),
                        RequestBody.fromBytes(content.getBytes()));
            LOG.info("upload content success: " + response.eTag());
            return Status.OK;
        } catch (S3Exception e) {
            LOG.error("write content failed:", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "write content failed: " + e.getMessage());
        } catch (UserException ue) {
            LOG.error("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    public Status copy(String origFilePath, String destFilePath) {
        S3URI origUri = new S3URI(origFilePath);
        S3URI descUri = new S3URI(destFilePath);
        try {
            getClient()
                .copyObject(
                    CopyObjectRequest.builder()
                        .copySource(origUri.getBucket() + "/" + origUri.getKey())
                        .destinationBucket(descUri.getBucket())
                        .destinationKey(descUri.getKey())
                        .build());
            return Status.OK;
        } catch (S3Exception e) {
            LOG.error("copy file failed: ", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "copy file failed: " + e.getMessage());
        } catch (UserException ue) {
            LOG.error("copy to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    @Override
    public Status upload(String localPath, String remotePath) {
        S3URI uri = new S3URI(remotePath);
        try {
            PutObjectResponse response =
                getClient()
                    .putObject(
                        PutObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build(),
                        RequestBody.fromFile(new File(localPath)));
            LOG.info("upload file " + localPath + " success: " + response.eTag());
            return Status.OK;
        } catch (S3Exception e) {
            LOG.error("write file failed:", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "write file failed: " + e.getMessage());
        } catch (UserException ue) {
            LOG.error("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    @Override
    public Status rename(String origFilePath, String destFilePath) {
        Status status = copy(origFilePath, destFilePath);
        if (status.ok()) {
            return delete(origFilePath);
        } else {
            return status;
        }
    }

    @Override
    public Status delete(String remotePath) {
        S3URI uri = new S3URI(remotePath);
        try {
            DeleteObjectResponse response =
                getClient()
                    .deleteObject(
                        DeleteObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build());
            LOG.info("delete file " + remotePath + " success: " + response.toString());
            return Status.OK;
        } catch (S3Exception e) {
            LOG.error("delete file failed: ", e);
            if (e.statusCode() == HttpStatus.SC_NOT_FOUND) {
                return Status.OK;
            }
            return new Status(Status.ErrCode.COMMON_ERROR, "delete file failed: " + e.getMessage());
        } catch (UserException ue) {
            LOG.error("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    @Override
    public Status list(String remotePath, List<RemoteFile> result) {
        return list(remotePath, result, true);
    }

    // broker file pattern glob is too complex, so we use hadoop directly
    public Status list(String remotePath, List<RemoteFile> result, boolean fileNameOnly) {
        try {
            checkS3();
            Configuration conf = new Configuration();
            String s3AK = caseInsensitiveProperties.get(S3_AK).toString();
            String s3Sk = caseInsensitiveProperties.get(S3_SK).toString();
            String s3Endpoint = caseInsensitiveProperties.get(S3_ENDPOINT).toString();
            conf.set("fs.s3a.access.key", s3AK);
            conf.set("fs.s3a.secret.key", s3Sk);
            conf.set("fs.s3a.endpoint", s3Endpoint);
            conf.set("fs.s3a.impl.disable.cache", "true");
            conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            FileSystem s3AFileSystem = FileSystem.get(new URI(remotePath), conf);
            org.apache.hadoop.fs.Path pathPattern = new org.apache.hadoop.fs.Path(remotePath);
            FileStatus[] files = s3AFileSystem.globStatus(pathPattern);
            if (files == null) {
                return Status.OK;
            }
            for (FileStatus fileStatus : files) {
                RemoteFile remoteFile = new RemoteFile(fileNameOnly?fileStatus.getPath().getName():fileStatus.getPath().toString(), !fileStatus.isDirectory(), fileStatus.isDirectory()? -1:fileStatus.getLen());
                result.add(remoteFile);
            }
        } catch (FileNotFoundException e) {
            LOG.info("file not found: " + e.getMessage());
            return new Status(Status.ErrCode.NOT_FOUND, "file not found: " + e.getMessage());
        } catch (Exception e) {
            LOG.error("errors while get file status ", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "errors while get file status " + e.getMessage());
        }
        return Status.OK;
    }

    @Override
    public Status makeDir(String remotePath) {
        if (!remotePath.endsWith("/")) {
            remotePath += "/";
        }
        S3URI uri = new S3URI(remotePath);
        try {
            PutObjectResponse response =
                getClient()
                    .putObject(
                        PutObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build(),
                        RequestBody.empty());
            LOG.info("makeDir success: " + response.eTag());
            return Status.OK;
        } catch (S3Exception e) {
            LOG.error("makeDir failed:", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "makeDir failed: " + e.getMessage());
        } catch (UserException ue) {
            LOG.error("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    @Override
    public Status checkPathExist(String remotePath) {
        S3URI uri = new S3URI(remotePath);
        try {
            getClient()
                .headObject(HeadObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build());
            return Status.OK;
        } catch (S3Exception e) {
            if (e.statusCode() == HttpStatus.SC_NOT_FOUND) {
                return new Status(Status.ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
            } else {
                LOG.error("headObject failed:", e);
                return new Status(Status.ErrCode.COMMON_ERROR, "headObject failed: " + e.getMessage());
            }
        } catch (UserException ue) {
            LOG.error("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    @Override
    public StorageBackend.StorageType getStorageType() {
        return StorageBackend.StorageType.S3;
    }
}
