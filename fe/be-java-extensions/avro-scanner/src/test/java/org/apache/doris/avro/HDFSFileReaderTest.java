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
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;

public class HDFSFileReaderTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testReadProjectedFile() throws Exception {
        Schema schema = SchemaBuilder.record("row").fields()
                .requiredInt("id").requiredString("name").endRecord();
        File file = temporaryFolder.newFile("records.avro");
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, file);
            GenericRecord record = new GenericData.Record(schema);
            record.put("id", 7);
            record.put("name", "seven");
            writer.append(record);
        }

        AvroFileContext context = new AvroFileContext();
        context.setRequiredFields(Collections.singleton("id"));
        context.setSplitStartOffset(0L);
        context.setSplitSize(file.length());
        HDFSFileReader reader = new HDFSFileReader(file.toURI().toString());
        reader.open(context, false);
        try {
            AvroWrapper<Pair<Integer, Long>> input = new AvroWrapper<>();
            Assert.assertEquals(schema, reader.getSchema());
            Assert.assertTrue(reader.hasNext(input, NullWritable.get()));
            GenericRecord record = (GenericRecord) reader.getNext();
            Assert.assertEquals(7, record.get("id"));
            Assert.assertNull(record.getSchema().getField("name"));
            Assert.assertFalse(reader.hasNext(input, NullWritable.get()));
        } finally {
            reader.close();
        }
    }
}
