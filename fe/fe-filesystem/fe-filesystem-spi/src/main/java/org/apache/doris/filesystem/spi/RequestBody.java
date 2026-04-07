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

package org.apache.doris.filesystem.spi;

import java.io.InputStream;

/**
 * Represents the body of a PUT or multipart upload request.
 * Abstracts over SDK-specific request body types (S3RequestBody, etc.).
 */
public final class RequestBody {

    private final InputStream content;
    private final long contentLength;

    public RequestBody(InputStream content, long contentLength) {
        this.content = content;
        this.contentLength = contentLength;
    }

    public InputStream content() {
        return content;
    }

    public long contentLength() {
        return contentLength;
    }

    public static RequestBody of(InputStream content, long contentLength) {
        return new RequestBody(content, contentLength);
    }
}
