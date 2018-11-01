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

package org.apache.doris.persist;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class EditLogFileInputStream extends EditLogInputStream {
    private File file;
    private FileInputStream fStream;

    public EditLogFileInputStream(File name) throws IOException {
        file = name;
        fStream = new FileInputStream(name);
    }

    String getName() {
        return file.getPath();
    }

    public int available() throws IOException {
        return fStream.available();
    }

    public int read() throws IOException {
        return fStream.read();
    }

    public int read(byte[] buffer, int offset, int len) throws IOException {
        return fStream.read(buffer, offset, len);
    }

    public void close() throws IOException {
        fStream.close();
    }

    long length() throws IOException {
        return file.length();
    }
}
