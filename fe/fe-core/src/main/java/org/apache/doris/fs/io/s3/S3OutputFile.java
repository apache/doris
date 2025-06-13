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
import org.apache.doris.fs.io.DorisPath;

import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.Executor;

public class S3OutputFile implements DorisOutputFile {
    private final Executor uploadExecutor;
    private final S3Client client;
    private final S3URI s3URI;

    public S3OutputFile(Executor uploadExecutor, S3Client client, S3URI s3URI) {
        this.uploadExecutor = Objects.requireNonNull(uploadExecutor, "uploadExecutor is null");
        this.client = Objects.requireNonNull(client, "client is null");
        this.s3URI = Objects.requireNonNull(s3URI, "s3URI is null");
    }

    @Override
    public OutputStream createOrOverwrite() throws IOException {
        return create();
    }

    @Override
    public OutputStream create() {
        return new S3OutputStream(uploadExecutor, client, s3URI);
    }

    @Override
    public DorisPath path() {
        return s3URI.toDorisPath();
    }
}
