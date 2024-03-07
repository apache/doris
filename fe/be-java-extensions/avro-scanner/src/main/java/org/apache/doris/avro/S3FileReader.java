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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

public class S3FileReader extends AvroReader {

    private static final Logger LOG = LogManager.getLogger(S3FileReader.class);
    private final String bucketName;
    private final String key;
    private AvroWrapper<Pair<Integer, Long>> inputPair;
    private final String endpoint;
    private final String region;
    private final String accessKey;
    private final String secretKey;
    private final String s3aUri;

    public S3FileReader(String accessKey, String secretKey, String endpoint, String region, String uri)
            throws IOException {
        this.endpoint = endpoint;
        this.region = region;
        S3Utils.parseURI(uri);
        this.bucketName = S3Utils.getBucket();
        this.key = S3Utils.getKey();
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.s3aUri = "s3a://" + bucketName + "/" + key;
    }

    @Override
    public void open(AvroFileContext avroFileContext, boolean tableSchema) throws IOException {
        Configuration conf = new Configuration();
        if (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey)) {
            conf.set(AvroProperties.FS_S3A_ACCESS_KEY, accessKey);
            conf.set(AvroProperties.FS_S3A_SECRET_KEY, secretKey);
        }
        conf.set(AvroProperties.FS_S3A_ENDPOINT, endpoint);
        conf.set(AvroProperties.FS_S3A_REGION, region);
        path = new Path(s3aUri);
        fileSystem = FileSystem.get(URI.create(s3aUri), conf);
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
