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

package org.apache.doris.fs.io.s3;

import org.apache.doris.common.util.S3URI;
import org.apache.doris.fs.io.DorisInput;
import org.apache.doris.fs.io.DorisInputFile;
import org.apache.doris.fs.io.DorisInputStream;
import org.apache.doris.fs.io.DorisPath;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;

public class S3InputFile implements DorisInputFile {
    private final S3Client client;
    private final S3URI s3URI;
    // private final S3Context context;
    // private final RequestPayer requestPayer;
    // private final Optional<EncryptionKey> key;
    private long length;
    private long lastModifiedTime;

    public S3InputFile(S3Client client, S3URI s3URI, Long length, long lastModifiedTime) {
        this.client = Objects.requireNonNull(client, "client is null");
        this.s3URI = Objects.requireNonNull(s3URI, "uri is null");
        // this.context = requireNonNull(context, "context is null");
        // this.requestPayer = context.requestPayer();
        this.length = length;
        this.lastModifiedTime = lastModifiedTime;
        // this.key = requireNonNull(key, "key is null");
        // location.location().verifyValidFileLocation();
    }

    @Override
    public DorisInput newInput() {
        return new S3Input(path(), client, newGetObjectRequest());
    }

    @Override
    public DorisInputStream newStream() {
        return new S3InputStream(path(), client, newGetObjectRequest(), length);
    }

    @Override
    public long length()
            throws IOException {
        if ((length == -1) && !headObject()) {
            throw new FileNotFoundException(s3URI.toString());
        }
        return length;
    }

    @Override
    public long lastModifiedTime()
            throws IOException {
        if ((lastModifiedTime == -1) && !headObject()) {
            throw new FileNotFoundException(s3URI.toString());
        }
        return lastModifiedTime;
    }

    @Override
    public boolean exists()
            throws IOException {
        return headObject();
    }

    @Override
    public DorisPath path() {
        return s3URI.toDorisPath();
    }

    private GetObjectRequest newGetObjectRequest() {
        return GetObjectRequest.builder()
                .bucket(s3URI.getBucket())
                .key(s3URI.getKey())
                .build();
    }

    private HeadObjectRequest newHeadObjectRequest() {
        return HeadObjectRequest.builder()
                .bucket(s3URI.getBucket())
                .key(s3URI.getKey())
                .build();
    }

    private boolean headObject() throws IOException {
        HeadObjectRequest request = newHeadObjectRequest();
        try {
            HeadObjectResponse response = client.headObject(request);
            if (length == -1) {
                length = response.contentLength();
            }
            if (lastModifiedTime == -1) {
                lastModifiedTime = response.lastModified().toEpochMilli();
            }
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (SdkException e) {
            throw new IOException("failed to head object for S3, key=" + s3URI.getKey(), e);
        }
    }
}
