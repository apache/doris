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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroRecordReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public abstract class AvroReader {

    private static final Logger LOG = LogManager.getLogger(AvroReader.class);
    protected AvroRecordReader<Pair<Integer, Long>> dataReader;
    protected DataFileStream<GenericRecord> schemaReader;
    protected Path path;
    protected FileSystem fileSystem;

    public abstract void open(AvroFileContext avroFileContext, boolean tableSchema) throws IOException;

    public abstract Schema getSchema();

    public abstract boolean hasNext(AvroWrapper<Pair<Integer, Long>> inputPair, NullWritable ignore) throws IOException;

    public abstract Object getNext();

    public abstract void close() throws IOException;

    protected void openSchemaReader() throws IOException {
        InputStream inputStream = new BufferedInputStream(fileSystem.open(path));
        schemaReader = new DataFileStream<>(inputStream, new GenericDatumReader<>());
        if (LOG.isDebugEnabled()) {
            LOG.debug("success open avro schema reader.");
        }
    }

    protected void openDataReader(AvroFileContext avroFileContext) throws IOException {
        JobConf job = new JobConf();
        projectionSchema(job, avroFileContext);
        FileSplit fileSplit =
                new FileSplit(path, avroFileContext.getSplitStartOffset(), avroFileContext.getSplitSize(), job);
        dataReader = new AvroRecordReader<>(job, fileSplit);
        if (LOG.isDebugEnabled()) {
            LOG.debug("success open avro data reader.");
        }
    }

    protected void projectionSchema(JobConf job, AvroFileContext avroFileContext) {
        Schema projectionSchema;
        Set<String> filedNames = avroFileContext.getRequiredFields();
        Schema avroSchema = avroFileContext.getSchema();
        // The number of fields that need to be queried is the same as that of the avro file,
        // so no projection is required.
        if (filedNames.size() != avroSchema.getFields().size()) {
            JsonObject schemaJson = new Gson().fromJson(avroSchema.toString(), JsonObject.class);
            JsonArray schemaFields = schemaJson.get("fields").getAsJsonArray();
            JsonObject copySchemaJson = schemaJson.deepCopy();
            JsonArray copySchemaFields = copySchemaJson.get("fields").getAsJsonArray();
            for (int i = 0; i < schemaFields.size(); i++) {
                JsonObject object = schemaFields.get(i).getAsJsonObject();
                String name = object.get("name").getAsString();
                if (filedNames.contains(name)) {
                    continue;
                }
                copySchemaFields.remove(schemaFields.get(i));
            }
            projectionSchema = new Parser().parse(copySchemaJson.toString());
        } else {
            projectionSchema = avroSchema;
        }
        AvroJob.setInputSchema(job, projectionSchema);
        if (LOG.isDebugEnabled()) {
            LOG.debug("projection avro schema is:" + projectionSchema.toString());
        }
    }

}
