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

package org.apache.doris.regression.util

import groovy.transform.CompileStatic

@CompileStatic
class BytesInputStream extends InputStream {
    private Iterator<byte[]> bytesIt
    private ByteArrayInputStream currentStream = new ByteArrayInputStream(new byte[0])

    BytesInputStream(Iterator<byte[]> bytesIt) {
        this.bytesIt = bytesIt
    }

    @Override
    int read() throws IOException {
        int byteValue = currentStream.read()
        if (byteValue == -1) {
            if (bytesIt.hasNext()) {
                currentStream = new ByteArrayInputStream(bytesIt.next())
                return read()
            } else {
                return -1
            }
        }
        return byteValue
    }

    @Override
    int read(byte[] b, int off, int len) throws IOException {
        int readSize = 0

        while (readSize < len) {
            int read = currentStream.read(b, off + readSize, len - readSize)
            if (read == -1) {
                if (bytesIt.hasNext()) {
                    currentStream = new ByteArrayInputStream(bytesIt.next())
                    continue
                } else {
                    return readSize > 0 ? readSize : -1
                }
            } else if (read > 0) {
                readSize += read
            } else if (read == 0) {
                break
            }
        }
        return readSize
    }
}
