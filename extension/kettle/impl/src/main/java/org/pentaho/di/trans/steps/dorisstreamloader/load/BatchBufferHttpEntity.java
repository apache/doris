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

package org.pentaho.di.trans.steps.dorisstreamloader.load;

import org.apache.http.entity.AbstractHttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class BatchBufferHttpEntity extends AbstractHttpEntity {

    private static final Logger LOG = LoggerFactory.getLogger(BatchBufferHttpEntity.class);
    protected static final int OUTPUT_BUFFER_SIZE = 4096;
    private final List<byte[]> buffer;
    private final long contentLength;

    public BatchBufferHttpEntity(BatchRecordBuffer recordBuffer) {
        this.buffer = recordBuffer.getBuffer();
        this.contentLength = recordBuffer.getBufferSizeBytes();
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public boolean isChunked() {
        return false;
    }

    @Override
    public long getContentLength() {
        return contentLength;
    }

    @Override
    public InputStream getContent() {
        return new BatchBufferStream(buffer);
    }

    @Override
    public void writeTo(OutputStream outStream) throws IOException {
        try (InputStream inStream = new BatchBufferStream(buffer)) {
            final byte[] buffer = new byte[OUTPUT_BUFFER_SIZE];
            int readLen;
            while ((readLen = inStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, readLen);
            }
        }
    }

    @Override
    public boolean isStreaming() {
        return false;
    }
}
