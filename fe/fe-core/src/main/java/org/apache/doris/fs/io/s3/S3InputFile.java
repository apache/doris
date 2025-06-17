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
import org.apache.doris.fs.io.ParsedPath;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;

/**
 * An implementation of DorisInputFile for Amazon S3 storage.
 * This class provides functionality to read files from S3 buckets,
 * check their existence, and retrieve their metadata such as length
 * and last modified time.
 */
public class S3InputFile implements DorisInputFile {
    // The S3 client used for making requests to AWS S3
    private final S3Client s3Client;

    // The parsed S3 URI containing bucket and key information
    private final S3URI s3Uri;

    // The length of the S3 object in bytes, -1 if not yet retrieved
    private long objectLength;

    // The last modified timestamp of the S3 object, -1 if not yet retrieved
    private long lastModified;

    /**
     * Constructs a new S3InputFile with the specified parameters.
     *
     * @param s3Client The S3 client to use for requests
     * @param s3Uri The parsed S3 URI of the object
     * @param length Initial length of the object, or -1 if unknown
     * @param lastModifiedTime Initial last modified time, or -1 if unknown
     */
    public S3InputFile(S3Client s3Client, S3URI s3Uri, long length, long lastModifiedTime) {
        this.s3Client = Objects.requireNonNull(s3Client, "s3Client cannot be null");
        this.s3Uri = Objects.requireNonNull(s3Uri, "s3Uri cannot be null");
        this.objectLength = length;
        this.lastModified = lastModifiedTime;
    }

    // ----------------------------------------
    // DorisInputFile Interface Implementation
    // ----------------------------------------

    @Override
    public DorisInput newInput() {
        return new S3Input(path(), s3Client, createGetObjectRequest());
    }

    @Override
    public DorisInputStream newStream() {
        return new S3InputStream(path(), s3Client, createGetObjectRequest(), objectLength);
    }

    @Override
    public long length() throws IOException {
        if (objectLength == -1 && !fetchObjectMetadata()) {
            throw new FileNotFoundException(s3Uri.toString());
        }
        return objectLength;
    }

    @Override
    public long lastModifiedTime() throws IOException {
        if (lastModified == -1 && !fetchObjectMetadata()) {
            throw new FileNotFoundException(s3Uri.toString());
        }
        return lastModified;
    }

    @Override
    public boolean exists() throws IOException {
        return fetchObjectMetadata();
    }

    @Override
    public ParsedPath path() {
        return s3Uri.toDorisPath();
    }

    // ----------------------------------------
    // Internal Helper Methods
    // ----------------------------------------

    /**
     * Creates a GetObjectRequest for the S3 object.
     *
     * @return The configured GetObjectRequest
     */
    private GetObjectRequest createGetObjectRequest() {
        return GetObjectRequest.builder()
                .bucket(s3Uri.getBucket())
                .key(s3Uri.getKey())
                .build();
    }

    /**
     * Creates a HeadObjectRequest for the S3 object.
     *
     * @return The configured HeadObjectRequest
     */
    private HeadObjectRequest createHeadObjectRequest() {
        return HeadObjectRequest.builder()
                .bucket(s3Uri.getBucket())
                .key(s3Uri.getKey())
                .build();
    }

    /**
     * Fetches metadata for the S3 object using a HEAD request.
     * Updates the object length and last modified time if successful.
     *
     * @return true if the object exists and metadata was fetched, false otherwise
     * @throws IOException if there was an error making the request
     */
    private boolean fetchObjectMetadata() throws IOException {
        try {
            HeadObjectResponse response = s3Client.headObject(createHeadObjectRequest());
            if (objectLength == -1) {
                objectLength = response.contentLength();
            }
            if (lastModified == -1) {
                lastModified = response.lastModified().toEpochMilli();
            }
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (SdkException e) {
            throw new IOException("Failed to fetch S3 object metadata, key=" + s3Uri.getKey(), e);
        }
    }
}
