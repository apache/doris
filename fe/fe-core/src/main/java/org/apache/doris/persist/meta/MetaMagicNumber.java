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

package org.apache.doris.persist.meta;

import org.apache.doris.common.FeConstants;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;

public class MetaMagicNumber {
    public static final String MAGIC_STR = FeConstants.meta_format.getMagicString();
    public static final byte[] MAGIC = MAGIC_STR.getBytes(Charset.forName("ASCII"));
    private byte[] bytes;

    public static MetaMagicNumber read(RandomAccessFile raf) throws IOException {
        MetaMagicNumber metaMagicNumber = new MetaMagicNumber();
        byte[] magicBytes = new byte[MAGIC_STR.length()];
        raf.readFully(magicBytes);
        metaMagicNumber.setBytes(magicBytes);
        return metaMagicNumber;
    }

    public static void write(RandomAccessFile raf) throws IOException {
        raf.write(MAGIC);
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }
}
