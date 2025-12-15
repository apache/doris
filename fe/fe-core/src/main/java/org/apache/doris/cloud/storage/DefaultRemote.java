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

package org.apache.doris.cloud.storage;

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Default implementation of {@link RemoteBase} use {@link S3Client}.
 * If one object storage such as OSS can not use {@link S3Client} to access, please override these methods.
 */
public class DefaultRemote extends RemoteBase {
    private static final Logger LOG = LogManager.getLogger(DefaultRemote.class);
    private S3Client s3Client;
    private static int MULTI_PART_UPLOAD_MAX_PART_NUM = 10000;
    private static ThreadPoolExecutor POOL = null;

    public DefaultRemote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public ListObjectsResult listObjects(String continuationToken) throws DdlException {
        return listObjectsInner(normalizePrefix(), continuationToken);
    }

    @Override
    public ListObjectsResult listObjects(String subPrefix, String continuationToken) throws DdlException {
        return listObjectsInner(normalizePrefix(subPrefix), continuationToken);
    }

    @Override
    public ListObjectsResult headObject(String subKey) throws DdlException {
        initClient();
        try {
            String key = normalizePrefix(subKey);
            HeadObjectRequest request = HeadObjectRequest.builder().bucket(obj.getBucket()).key(key)
                    .build();
            HeadObjectResponse response = s3Client.headObject(request);
            ObjectFile objectFile = new ObjectFile(key, getRelativePath(key), response.eTag(),
                    response.contentLength());
            return new ListObjectsResult(Lists.newArrayList(objectFile), false, null);
        } catch (NoSuchKeyException e) {
            LOG.warn("NoSuchKey when head object for S3, subKey={}", subKey);
            return new ListObjectsResult(Lists.newArrayList(), false, null);
        } catch (SdkException e) {
            LOG.warn("Failed to head object for S3, subKey={}", subKey, e);
            throw new DdlException(
                    "Failed to head object for S3, subKey=" + subKey + " Error message=" + e.getMessage());
        }
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        throw new DdlException("Get sts token is unsupported");
    }

    private ListObjectsResult listObjectsInner(String prefix, String continuationToken) throws DdlException {
        initClient();
        try {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder().bucket(obj.getBucket())
                    .prefix(prefix);
            if (!StringUtils.isEmpty(continuationToken)) {
                requestBuilder.continuationToken(continuationToken);
            }
            ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
            List<ObjectFile> objectFiles = new ArrayList<>();
            for (S3Object c : response.contents()) {
                objectFiles.add(new ObjectFile(c.key(), getRelativePath(c.key()), c.eTag(), c.size()));
            }
            return new ListObjectsResult(objectFiles, response.isTruncated(), response.nextContinuationToken());
        } catch (SdkException e) {
            LOG.warn("Failed to list objects for S3, prefix {}", prefix, e);
            throw new DdlException("Failed to list objects for S3, Error message=" + e.getMessage());
        }
    }

    private void initClient() {
        if (s3Client == null) {
            /*
             * https://github.com/aws/aws-sdk-java-v2/blob/master/docs/LaunchChangelog.md#131-client-http-configuration
             * https://github.com/aws/aws-sdk-java-v2/blob/master/docs/LaunchChangelog.md#133-client-override-configuration
             * There are several timeout configuration, please config if needed.
             */
            AwsCredentials credentials;
            if (obj.getToken() != null) {
                credentials = AwsSessionCredentials.create(obj.getAk(), obj.getSk(), obj.getToken());
            } else {
                credentials = AwsBasicCredentials.create(obj.getAk(), obj.getSk());
            }
            StaticCredentialsProvider scp = StaticCredentialsProvider.create(credentials);
            String endpointStr = obj.getEndpoint();
            if (!endpointStr.contains("://")) {
                endpointStr = "http://" + endpointStr;
            }
            URI endpointUri = URI.create(endpointStr);
            s3Client = S3Client.builder().endpointOverride(endpointUri).credentialsProvider(scp)
                    .region(Region.of(obj.getRegion()))
                    .serviceConfiguration(S3Configuration.builder().chunkedEncodingEnabled(false).build())
                    .build();
        }
    }

    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
        }
    }

    @Override
    public void deleteObjects(List<String> keys) throws DdlException {
        checkDeleteKeys(keys);
        initClient();
        try {
            int maxDelete = 1000;
            for (int i = 0; i < keys.size() / maxDelete + 1; i++) {
                ArrayList<ObjectIdentifier> toDelete = new ArrayList<>();
                for (int j = maxDelete * i; j < keys.size() && toDelete.size() < maxDelete; j++) {
                    toDelete.add(ObjectIdentifier.builder().key(keys.get(j)).build());
                }
                DeleteObjectsRequest.Builder requestBuilder = DeleteObjectsRequest.builder().bucket(obj.getBucket())
                        .delete(Delete.builder().objects(toDelete).build());
                LOG.info("Delete objects for bucket={}, keys={}", obj.getBucket(), keys);
                DeleteObjectsResponse response = s3Client.deleteObjects(requestBuilder.build());
                if (!response.errors().isEmpty()) {
                    S3Error error = response.errors().get(0);
                    throw new DdlException(
                            "Failed delete objects, bucket=" + obj.getBucket() + ", key=" + error.key() + ", error="
                                    + error.message() + ", code=" + error.code());
                }
            }
        } catch (SdkException e) {
            LOG.warn("Failed to delete objects for S3", e);
            throw new DdlException("Failed to delete objects for S3, Error message=" + e.getMessage());
        }
    }

    @Override
    public void putObject(File file, String key) throws DdlException {
        initClient();
        try {
            PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder().bucket(obj.getBucket())
                    .key(key);
            s3Client.putObject(requestBuilder.build(), RequestBody.fromFile(file));
            LOG.info("Put object for bucket={}, key={}", obj.getBucket(), key);
        } catch (SdkException e) {
            LOG.warn("Failed to put object for S3", e);
            throw new DdlException("Failed to put object for S3, Error message=" + e.getMessage());
        }
    }

    @Override
    public void multipartUploadObject(File file, String key, Function<String, Pair<Boolean, String>> function)
            throws DdlException {
        long fileSize = file.length();
        if (fileSize <= Config.multi_part_upload_part_size_in_bytes) {
            putObject(file, key);
            return;
        }

        initClient();
        initPool();
        // create multipart upload
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(obj.getBucket()).key(key).build();
        CreateMultipartUploadResponse multipartUpload = s3Client.createMultipartUpload(
                createMultipartUploadRequest);
        String uploadId = multipartUpload.uploadId();

        // calculate part size
        long partSize = Config.multi_part_upload_part_size_in_bytes;
        if (partSize * MULTI_PART_UPLOAD_MAX_PART_NUM < fileSize) {
            partSize = (fileSize + MULTI_PART_UPLOAD_MAX_PART_NUM - 1) / MULTI_PART_UPLOAD_MAX_PART_NUM;
        }
        int totalPartNum = (int) (fileSize / partSize) + (fileSize % partSize == 0 ? 0 : 1);
        LOG.info("multipart upload file: {}, size: {}, part size: {}, total part num: {}, upload id: {}",
                file.getAbsolutePath(), fileSize, partSize, totalPartNum, uploadId);

        try {
            if (function != null) {
                Pair<Boolean, String> result = function.apply(uploadId);
                if (!result.first) {
                    LOG.warn("Failed to multipart upload object, file: {}, key: {}, upload id: {}, reason: {}",
                            file.getAbsolutePath(), key, uploadId, result.second == null ? "" : result.second);
                    throw new DdlException("Failed to multi part upload object, reason: "
                            + (result.second == null ? "" : result.second));
                }
            }

            long start = System.currentTimeMillis();
            List<CompletedPart> parts = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(totalPartNum);
            int partNum = 1;
            long totalUploaded = 0;
            AtomicBoolean failed = new AtomicBoolean(false);

            while (totalUploaded < fileSize && !failed.get()) {
                long nextPartSize = Math.min(partSize, fileSize - totalUploaded);
                int partNumConst = partNum;
                long totalUploadedConst = totalUploaded;
                POOL.submit(() -> {
                    if (failed.get()) {
                        return;
                    }
                    LOG.debug("start multipart upload part id: {} for file: {}, key, {}, upload id: {}",
                            partNumConst, file.getAbsolutePath(), key, uploadId);
                    UploadPartRequest uploadPartRequest = UploadPartRequest.builder().bucket(obj.getBucket())
                            .key(key).uploadId(uploadId).partNumber(partNumConst).build();
                    try (FileInputStream inputStream = new FileInputStream(file)) {
                        long skipped = inputStream.skip(totalUploadedConst);
                        if (skipped < totalUploadedConst) {
                            throw new IOException(
                                    "upload file error, skipped: " + skipped + ", skip: " + totalUploadedConst);
                        }
                        UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadPartRequest,
                                RequestBody.fromInputStream(inputStream, nextPartSize));
                        synchronized (parts) {
                            parts.add(
                                    CompletedPart.builder().partNumber(partNumConst).eTag(uploadPartResponse.eTag())
                                            .build());
                        }
                        LOG.debug("finish multipart upload part id: {} for file: {}, key, {}, upload id: {}",
                                partNumConst, file.getAbsolutePath(), key, uploadId);
                    } catch (Exception e) {
                        LOG.warn("Failed multipart upload part id: {} for file: {}, key, {}, upload id: {}",
                                partNumConst, file.getAbsolutePath(), key, uploadId, e);
                        failed.set(true);
                    } finally {
                        latch.countDown();
                    }
                });
                totalUploaded += nextPartSize;
                partNum++;
            }
            while ((System.currentTimeMillis() - start) / 1000 < Config.multi_part_upload_max_seconds) {
                if (latch.await(10, TimeUnit.SECONDS) || failed.get()) {
                    break;
                }
            }
            if (failed.get() || parts.size() < totalPartNum) {
                throw new DdlException("Failed to multipart upload object for file: " + file.getAbsolutePath()
                        + ", key=" + key + ", upload id: " + uploadId + ", finished part num: " + parts.size()
                        + ", total part num: " + totalPartNum);
            }

            // complete the multipart upload
            parts.sort(Comparator.comparingInt(CompletedPart::partNumber));
            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(obj.getBucket()).key(key).uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build()).build();
            CompleteMultipartUploadResponse completeMultipartUploadResponse = s3Client.completeMultipartUpload(
                    completeMultipartUploadRequest);
            LOG.info("Finish multipart upload file: {}, size: {}, key: {}, upload id: {}, etag: {}, cost {} ms",
                    file.getAbsolutePath(), fileSize, key, uploadId, completeMultipartUploadResponse.eTag(),
                    System.currentTimeMillis() - start);
        } catch (Exception e) {
            LOG.warn("Failed to multipart upload object for file: {}, size: {}, key: {}, upload id: {}",
                    file.getAbsolutePath(), fileSize, key, uploadId, e);
            s3Client.abortMultipartUpload(
                    AbortMultipartUploadRequest.builder().uploadId(uploadId).bucket(obj.getBucket()).key(key)
                            .build());
            throw new DdlException("Failed to multipart upload object, Error message=" + e.getMessage());
        }
    }

    @Override
    public void getObject(String key, String file) throws DdlException {
        initClient();
        try {
            GetObjectRequest.Builder getObjectRequest = GetObjectRequest.builder()
                    .bucket(obj.getBucket()).key(key);
            s3Client.getObject(getObjectRequest.build(), Paths.get(file));
            LOG.info("Get object for bucket={}, key={}, file={}", obj.getBucket(), key, file);
        } catch (SdkException e) {
            LOG.warn("Failed to get object for S3", e);
            throw new DdlException("Failed to get object for S3, Error message=" + e.getMessage());
        }
    }

    private void initPool() {
        if (POOL == null) {
            synchronized (DefaultRemote.class) {
                if (POOL == null) {
                    POOL = ThreadPoolManager.newDaemonThreadPool(Config.multi_part_upload_pool_size,
                            Config.multi_part_upload_pool_size, 5, TimeUnit.SECONDS,
                            new LinkedBlockingQueue<Runnable>(MULTI_PART_UPLOAD_MAX_PART_NUM),
                            new ThreadPoolExecutor.DiscardPolicy(), "multi-part-upload", false);
                    POOL.allowCoreThreadTimeOut(true);
                }
            }
        }
    }
}
