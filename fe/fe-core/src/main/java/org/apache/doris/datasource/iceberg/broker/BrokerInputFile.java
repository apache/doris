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

package org.apache.doris.datasource.iceberg.broker;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.util.BrokerReader;
import org.apache.doris.thrift.TBrokerFD;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;

public class BrokerInputFile implements InputFile {

    private Long fileLength = null;

    private final String filePath;
    private final BrokerDesc brokerDesc;
    private BrokerReader reader;
    private TBrokerFD fd;

    private BrokerInputFile(String filePath, BrokerDesc brokerDesc) {
        this.filePath = filePath;
        this.brokerDesc = brokerDesc;
    }

    private void init() throws IOException {
        this.reader = BrokerReader.create(this.brokerDesc);
        this.fileLength = this.reader.getFileLength(filePath);
        this.fd = this.reader.open(filePath);
    }

    public static BrokerInputFile create(String filePath, BrokerDesc brokerDesc) throws IOException {
        BrokerInputFile inputFile = new BrokerInputFile(filePath, brokerDesc);
        inputFile.init();
        return inputFile;
    }

    @Override
    public long getLength() {
        return fileLength;
    }

    @Override
    public SeekableInputStream newStream() {
        return new BrokerInputStream(this.reader, this.fd, this.fileLength);
    }

    @Override
    public String location() {
        return filePath;
    }

    @Override
    public boolean exists() {
        return fileLength != null;
    }
}
