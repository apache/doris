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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

public class S3FileReader implements AvroReader {

    private static final Logger LOG = LogManager.getLogger(S3FileReader.class);
    private final String bucketName;
    private final String key;
    private AmazonS3 s3Client;
    private DataFileStream<GenericRecord> reader;
    private InputStream s3ObjectInputStream;
    private final AWSCredentials credentials;
    private final String endpoint;
    private final String region;

    public S3FileReader(String accessKey, String secretKey, String endpoint, String region,
            String bucketName, String key) {
        this.bucketName = bucketName;
        this.key = key;
        this.endpoint = endpoint;
        this.region = region;
        credentials = new BasicAWSCredentials(accessKey, secretKey);
    }

    @Override
    public void open(Configuration conf) {
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .build();
        S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
        s3ObjectInputStream = object.getObjectContent();
        try {
            reader = new DataFileStream<>(s3ObjectInputStream, new GenericDatumReader<>());
        } catch (IOException e) {
            LOG.warn("open s3FileReader meet some error" + e);
            throw new RuntimeException("Failed initialize s3FileReader", e);
        }
    }

    @Override
    public Schema getSchema() {
        return reader.getSchema();
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
        s3ObjectInputStream.close();
        reader.close();
    }
}
