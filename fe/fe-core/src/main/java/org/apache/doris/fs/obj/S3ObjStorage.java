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

package org.apache.doris.fs.obj;

import org.apache.doris.backup.Status;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.datasource.credentials.CloudCredential;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.S3Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class S3ObjStorage implements ObjStorage<S3Client> {
    private static final Logger LOG = LogManager.getLogger(S3ObjStorage.class);
    private S3Client client;

    protected Map<String, String> properties;

    // false: the s3 client will automatically convert endpoint to virtual-hosted style, eg:
    //          endpoint:           http://s3.us-east-2.amazonaws.com
    //          bucket/path:        my_bucket/file.txt
    //          auto convert:       http://my_bucket.s3.us-east-2.amazonaws.com/file.txt
    // true: the s3 client will NOT automatically convert endpoint to virtual-hosted style, we need to do some tricks:
    //          endpoint:           http://cos.ap-beijing.myqcloud.com
    //          bucket/path:        my_bucket/file.txt
    //          convert manually:   See S3URI()
    private boolean forceHostedStyle = false;

    public S3ObjStorage(Map<String, String> properties) {
        this.properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        setProperties(properties);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    protected void setProperties(Map<String, String> properties) {
        this.properties.putAll(properties);
        try {
            S3Properties.requiredS3Properties(this.properties);
        } catch (DdlException e) {
            throw new IllegalArgumentException(e);
        }
        // Virtual hosted-style is recommended in the s3 protocol.
        // The path-style has been abandoned, but for some unexplainable reasons,
        // the s3 client will determine whether the endpiont starts with `s3`
        // when generating a virtual hosted-sytle request.
        // If not, it will not be converted ( https://github.com/aws/aws-sdk-java-v2/pull/763),
        // but the endpoints of many cloud service providers for object storage do not start with s3,
        // so they cannot be converted to virtual hosted-sytle.
        // Some of them, such as aliyun's oss, only support virtual hosted-style,
        // and some of them(ceph) may only support
        // path-style, so we need to do some additional conversion.
        //
        //          use_path_style          |     !use_path_style
        //   S3     forceHostedStyle=false  |     forceHostedStyle=false
        //  !S3     forceHostedStyle=false  |     forceHostedStyle=true
        //
        // That is, for S3 endpoint, ignore the `use_path_style` property, and the s3 client will automatically use
        // virtual hosted-sytle.
        // And for other endpoint, if `use_path_style` is true, use path style. Otherwise, use virtual hosted-sytle.
        // 'forceHostedStyle==false' means that use path style.
        if (!this.properties.get(S3Properties.ENDPOINT).toLowerCase().contains(S3Properties.S3_PREFIX)) {
            String usePathStyle = this.properties.getOrDefault(PropertyConverter.USE_PATH_STYLE, "false");
            boolean isUsePathStyle = usePathStyle.equalsIgnoreCase("true");
            // when it's path style, we will not use virtual hosted-style
            forceHostedStyle = !isUsePathStyle;
        } else {
            forceHostedStyle = false;
        }
    }

    @Override
    public S3Client getClient(String bucket) throws UserException {
        if (client == null) {
            URI tmpEndpoint = URI.create(properties.get(S3Properties.ENDPOINT));
            URI endpoint = StringUtils.isEmpty(bucket) ? tmpEndpoint :
                    URI.create(new URIBuilder(tmpEndpoint).setHost(bucket + "." + tmpEndpoint.getHost()).toString());
            CloudCredential credential = new CloudCredential();
            credential.setAccessKey(properties.get(S3Properties.ACCESS_KEY));
            credential.setSecretKey(properties.get(S3Properties.SECRET_KEY));
            if (properties.containsKey(S3Properties.SESSION_TOKEN)) {
                credential.setSessionToken(properties.get(S3Properties.SESSION_TOKEN));
            }
            client = S3Util.buildS3Client(endpoint, properties.get(S3Properties.REGION), credential);
        }
        return client;
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        return null;
    }

    @Override
    public Status headObject(String remotePath) {
        try {
            S3URI uri = S3URI.create(remotePath, forceHostedStyle);
            HeadObjectResponse response = getClient(uri.getVirtualBucket())
                    .headObject(HeadObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build());
            LOG.info("head file " + remotePath + " success: " + response.toString());
            return Status.OK;
        } catch (S3Exception e) {
            if (e.statusCode() == HttpStatus.SC_NOT_FOUND) {
                return new Status(Status.ErrCode.NOT_FOUND, "remote path does not exist: " + remotePath);
            } else {
                LOG.warn("headObject failed:", e);
                return new Status(Status.ErrCode.COMMON_ERROR, "headObject failed: " + e.getMessage());
            }
        } catch (UserException ue) {
            LOG.warn("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    @Override
    public Status getObject(String remoteFilePath, File localFile) {
        try {
            S3URI uri = S3URI.create(remoteFilePath, forceHostedStyle);
            GetObjectResponse response = getClient(uri.getVirtualBucket()).getObject(
                    GetObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build(), localFile.toPath());
            LOG.info("get file " + remoteFilePath + " success: " + response.toString());
            return Status.OK;
        } catch (S3Exception s3Exception) {
            return new Status(
                    Status.ErrCode.COMMON_ERROR,
                    "get file from s3 error: " + s3Exception.awsErrorDetails().errorMessage());
        } catch (UserException ue) {
            LOG.warn("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        } catch (Exception e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.toString());
        }
    }

    @Override
    public Status putObject(String remotePath, @Nullable RequestBody requestBody) {
        try {
            S3URI uri = S3URI.create(remotePath, forceHostedStyle);
            PutObjectResponse response =
                    getClient(uri.getVirtualBucket())
                            .putObject(
                                    PutObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build(),
                                    requestBody);
            LOG.info("put object success: " + response.toString());
            return Status.OK;
        } catch (S3Exception e) {
            LOG.error("put object failed:", e);
            return new Status(Status.ErrCode.COMMON_ERROR, "put object failed: " + e.getMessage());
        } catch (Exception ue) {
            LOG.error("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    @Override
    public Status deleteObject(String remotePath) {
        try {
            S3URI uri = S3URI.create(remotePath, forceHostedStyle);
            DeleteObjectResponse response =
                    getClient(uri.getVirtualBucket())
                            .deleteObject(
                                    DeleteObjectRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build());
            LOG.info("delete file " + remotePath + " success: " + response.toString());
            return Status.OK;
        } catch (S3Exception e) {
            LOG.warn("delete file failed: ", e);
            if (e.statusCode() == HttpStatus.SC_NOT_FOUND) {
                return Status.OK;
            }
            return new Status(Status.ErrCode.COMMON_ERROR, "delete file failed: " + e.getMessage());
        } catch (UserException ue) {
            LOG.warn("connect to s3 failed: ", ue);
            return new Status(Status.ErrCode.COMMON_ERROR, "connect to s3 failed: " + ue.getMessage());
        }
    }

    @Override
    public Status deleteObjects(String absolutePath) {
        try {
            S3URI baseUri = S3URI.create(absolutePath, forceHostedStyle);
            String continuationToken = "";
            boolean isTruncated = false;
            long totalObjects = 0;
            do {
                RemoteObjects objects = listObjects(absolutePath, continuationToken);
                List<RemoteObject> objectList = objects.getObjectList();
                if (!objectList.isEmpty()) {
                    Delete delete = Delete.builder()
                            .objects(objectList.stream()
                                    .map(RemoteObject::getKey)
                                    .map(k -> ObjectIdentifier.builder().key(k).build())
                                    .collect(Collectors.toList()))
                            .build();
                    DeleteObjectsRequest req = DeleteObjectsRequest.builder()
                            .bucket(baseUri.getBucket())
                            .delete(delete)
                            .build();

                    DeleteObjectsResponse resp = getClient(baseUri.getVirtualBucket()).deleteObjects(req);
                    if (resp.errors().size() > 0) {
                        LOG.warn("{} errors returned while deleting {} objects for dir {}",
                                resp.errors().size(), objectList.size(), absolutePath);
                    }
                    LOG.info("{} of {} objects deleted for dir {}",
                            resp.deleted().size(), objectList.size(), absolutePath);
                    totalObjects += objectList.size();
                }

                isTruncated = objects.isTruncated();
                continuationToken = objects.getContinuationToken();
            } while (isTruncated);
            LOG.info("total delete {} objects for dir {}", totalObjects, absolutePath);
            return Status.OK;
        } catch (DdlException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, "list objects for delete objects failed: " + e.getMessage());
        } catch (Exception e) {
            LOG.warn("delete objects {} failed, force visual host style {}", absolutePath, e, forceHostedStyle);
            return new Status(Status.ErrCode.COMMON_ERROR, "delete objects failed: " + e.getMessage());
        }
    }

    @Override
    public Status copyObject(String origFilePath, String destFilePath) {
        try {
            S3URI origUri = S3URI.create(origFilePath);
            S3URI descUri = S3URI.create(destFilePath, forceHostedStyle);
            CopyObjectResponse response = getClient(descUri.getVirtualBucket())
                    .copyObject(
                            CopyObjectRequest.builder()
                                    .copySource(origUri.getBucket() + "/" + origUri.getKey())
                                    .destinationBucket(descUri.getBucket())
                                    .destinationKey(descUri.getKey())
                                    .build());
            LOG.info("copy file from " + origFilePath + " to " + destFilePath + " success: " + response.toString());
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
    public RemoteObjects listObjects(String absolutePath, String continuationToken) throws DdlException {
        try {
            S3URI uri = S3URI.create(absolutePath, forceHostedStyle);
            String bucket = uri.getBucket();
            String prefix = uri.getKey();
            if (!StringUtils.isEmpty(uri.getVirtualBucket())) {
                // Support s3 compatible service. The generated HTTP request for list objects likes:
                //
                //  GET /<bucket-name>?list-type=2&prefix=<prefix>
                prefix = bucket + "/" + prefix;
                String endpoint = properties.get(S3Properties.ENDPOINT);
                if (endpoint.contains("cos.")) {
                    bucket = "/";
                } else if (endpoint.contains("oss-")) {
                    bucket = uri.getVirtualBucket();
                } else if (endpoint.contains("obs.")) {
                    // FIXME: unlike cos and oss, the obs will report 'The specified key does not exist'.
                    throw new DdlException("obs does not support list objects via s3 sdk. path: " + absolutePath);
                }
            }
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(normalizePrefix(prefix));
            if (!StringUtils.isEmpty(continuationToken)) {
                requestBuilder.continuationToken(continuationToken);
            }
            ListObjectsV2Response response = getClient(uri.getVirtualBucket()).listObjectsV2(requestBuilder.build());
            List<RemoteObject> remoteObjects = new ArrayList<>();
            for (S3Object c : response.contents()) {
                String relativePath = getRelativePath(prefix, c.key());
                remoteObjects.add(new RemoteObject(c.key(), relativePath, c.eTag(), c.size()));
            }
            return new RemoteObjects(remoteObjects, response.isTruncated(), response.nextContinuationToken());
        } catch (Exception e) {
            LOG.warn("Failed to list objects for S3: {}", absolutePath, e);
            throw new DdlException("Failed to list objects for S3, Error message: " + e.getMessage(), e);
        }
    }
}
