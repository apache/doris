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
import org.apache.doris.fs.io.DorisOutputFile;
import org.apache.doris.fs.io.ParsedPath;

import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * An implementation of {@link DorisOutputFile} for Amazon S3 storage.
 * This class provides functionality to write data to S3 buckets using the AWS SDK.
 * It supports creating new files and overwriting existing ones through streaming operations.
 */
public class S3OutputFile implements DorisOutputFile {
    // Executor service for handling asynchronous upload operations to S3
    private final Executor uploadExecutor;

    // AWS S3 client for interacting with S3 service
    private final S3Client client;

    // URI object containing S3 bucket and key information
    private final S3URI s3URI;

    /**
     * Constructs a new S3OutputFile with the specified parameters.
     *
     * @param uploadExecutor The executor service for handling upload operations
     * @param client The S3 client instance for AWS operations
     * @param s3URI The S3 URI containing bucket and key information
     * @throws NullPointerException if any of the parameters is null
     */
    public S3OutputFile(Executor uploadExecutor, S3Client client, S3URI s3URI) {
        this.uploadExecutor = Objects.requireNonNull(uploadExecutor, "uploadExecutor is null");
        this.client = Objects.requireNonNull(client, "client is null");
        this.s3URI = Objects.requireNonNull(s3URI, "s3URI is null");
    }

    // ********************************************************************************
    // Interface Implementation Methods
    // ********************************************************************************

    /**
     * Creates a new output stream or overwrites an existing one at the specified S3 location.
     * This method is equivalent to {@link #create()} as S3 automatically handles overwrites.
     *
     * @return A new OutputStream for writing data to S3
     * @throws IOException if there is an error creating the stream
     */
    @Override
    public OutputStream createOrOverwrite() throws IOException {
        return createOutputStream();
    }

    /**
     * Creates a new output stream for writing data to the specified S3 location.
     *
     * @return A new OutputStream for writing data to S3
     */
    @Override
    public OutputStream create() {
        return createOutputStream();
    }

    /**
     * Returns the Doris path representation of the S3 location.
     *
     * @return DorisPath object representing the S3 location
     */
    @Override
    public ParsedPath path() {
        return s3URI.toDorisPath();
    }

    // ********************************************************************************
    // Private Helper Methods
    // ********************************************************************************

    /**
     * Creates a new S3OutputStream instance for writing data.
     * This is a helper method to avoid code duplication between create() and createOrOverwrite().
     *
     * @return A new S3OutputStream instance
     */
    private OutputStream createOutputStream() {
        return new S3OutputStream(uploadExecutor, client, s3URI);
    }
}
