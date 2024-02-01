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

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

public class HDFSFileReader extends AvroReader {
    private static final Logger LOG = LogManager.getLogger(HDFSFileReader.class);
    private final String url;
    private AvroWrapper<Pair<Integer, Long>> inputPair;

    public HDFSFileReader(String url) {
        this.url = url;
        this.path = new Path(url);
    }

    @Override
    public void open(AvroFileContext avroFileContext, boolean tableSchema) throws IOException {
        fileSystem = FileSystem.get(URI.create(url), new Configuration());
        openSchemaReader();
        if (!tableSchema) {
            avroFileContext.setSchema(schemaReader.getSchema());
            openDataReader(avroFileContext);
        }
    }

    @Override
    public Schema getSchema() {
        return schemaReader.getSchema();
    }

    @Override
    public boolean hasNext(AvroWrapper<Pair<Integer, Long>> inputPair, NullWritable ignore) throws IOException {
        this.inputPair = inputPair;
        return dataReader.next(this.inputPair, ignore);
    }

    @Override
    public Object getNext() {
        return inputPair.datum();
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(schemaReader)) {
            schemaReader.close();
        }
        if (Objects.nonNull(dataReader)) {
            dataReader.close();
        }
        fileSystem.close();
    }
}
