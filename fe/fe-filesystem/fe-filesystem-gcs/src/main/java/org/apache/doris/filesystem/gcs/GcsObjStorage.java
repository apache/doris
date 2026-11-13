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

package org.apache.doris.filesystem.gcs;

import org.apache.doris.filesystem.s3.S3FileSystemProperties;
import org.apache.doris.filesystem.s3.S3ObjStorage;

import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;

/** S3-compatible GCS client with refreshable Google OAuth credentials. */
final class GcsObjStorage extends S3ObjStorage {
    private final GcsFileSystemProperties properties;

    GcsObjStorage(S3FileSystemProperties delegate, GcsFileSystemProperties properties) {
        super(delegate, properties.getSupportedSchemes());
        this.properties = properties;
    }

    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        if (properties.getAuth().getNativeCredential().isPresent()) {
            throw new UnsupportedOperationException("Presigned URLs are not supported for native GCP OAuth "
                    + "credentials; GCS V4 signing with IAM signBlob is required");
        }
        return super.getPresignedUrl(objectKey);
    }

    @Override
    protected S3Client buildClient() throws IOException {
        if (properties.getAuth().getNativeCredential().isEmpty()) {
            return super.buildClient();
        }
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(GcsObjStorage.class.getClassLoader());
        try {
            return GcpAuth.createS3Client(properties);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
