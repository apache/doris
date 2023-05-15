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

package org.apache.doris.avro;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSFileReader implements AvroReader {
    private static final Logger LOG = LogManager.getLogger(HDFSFileReader.class);
    private final Path filePath;
    private final String url;
    private DataFileStream<GenericRecord> reader;
    private BufferedInputStream inputStream;

    public HDFSFileReader(String url) {
        this.url = url;
        this.filePath = new Path(url);
    }

    @Override
    public void open(Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(URI.create(url), conf);
            inputStream = new BufferedInputStream(fs.open(filePath));
            reader = new DataFileStream<>(inputStream, new GenericDatumReader<>());
        } catch (IOException e) {
            LOG.warn("Open HDFSFileReader meet some error" + e);
            throw new RuntimeException("Failed to initialize HDFSFileReader", e);
        }
    }

    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public Object getNext() throws IOException {
        return reader.next();
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        reader.close();
    }
}
