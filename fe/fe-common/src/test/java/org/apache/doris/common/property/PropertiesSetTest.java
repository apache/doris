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

package org.apache.doris.common.property;

import org.apache.doris.thrift.TPropertyVal;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class PropertiesSetTest {
    @Test
    public void testReadFromStr() {
        Map<String, String> raw = new HashMap<>();
        raw.put("skip_header", "0");
        PropertiesSet<FileFormat> properties = PropertiesSet.readFromStrMap(FileFormat.get(), raw);
        Assert.assertEquals(FileFormat.Type.CSV, properties.get(FileFormat.FILE_FORMAT_TYPE));
        Assert.assertEquals("\n", properties.get(FileFormat.RECORD_DELIMITER));
        Assert.assertEquals(1, properties.writeToThrift().size());

        properties = PropertiesSet.readFromStrMap(FileFormat.get(), rawVariableProps());
        verifyVariableProps(properties);
        Assert.assertEquals(3, properties.writeToThrift().size());
    }

    @Test
    public void testThriftSerde() {
        PropertiesSet<FileFormat> properties = PropertiesSet.readFromStrMap(FileFormat.get(), rawVariableProps());
        Map<String, TPropertyVal> thriftMap = properties.writeToThrift();
        Assert.assertEquals("JSON", thriftMap.get(FileFormat.FILE_FORMAT_TYPE.getName()).strVal);
        Assert.assertEquals("\r", thriftMap.get(FileFormat.RECORD_DELIMITER.getName()).strVal);
        Assert.assertEquals(3, thriftMap.get(FileFormat.SKIP_HEADER.getName()).intVal);

        properties = PropertiesSet.readFromThrift(FileFormat.get(), thriftMap);
        verifyVariableProps(properties);
    }

    @Test
    public void testDataOutputSerde() throws Exception {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outStream);

        PropertiesSet<FileFormat> properties = PropertiesSet.readFromStrMap(FileFormat.get(), rawVariableProps());
        properties.writeToData(output);

        ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
        DataInput input = new DataInputStream(inStream);

        properties = PropertiesSet.readFromData(FileFormat.get(), input);
        verifyVariableProps(properties);
    }

    @Test
    public void testEmpty() {
        PropertiesSet<FileFormat> p1 = PropertiesSet.empty(FileFormat.get());
        PropertiesSet<FileFormat> p2 = PropertiesSet.empty(FileFormat.get());

        Assert.assertEquals(p1, p2);
    }

    @Test
    public void testModifiedSchemas() {
        PropertiesSet<FileFormat> properties = PropertiesSet.readFromStrMap(FileFormat.get(), rawVariableProps());
        Assert.assertEquals(3, properties.getModifiedSchemas().size());
    }

    @Test
    public void testToString() {
        PropertiesSet<FileFormat> properties = PropertiesSet.readFromStrMap(FileFormat.get(), rawVariableProps());
        String str = properties.toString();

        Assert.assertTrue(str.contains(FileFormat.FILE_FORMAT_TYPE.getName()));
        Assert.assertTrue(str.contains("JSON"));
        Assert.assertTrue(str.contains(FileFormat.RECORD_DELIMITER.getName()));
        Assert.assertTrue(str.contains("\r"));
        Assert.assertTrue(str.contains(FileFormat.SKIP_HEADER.getName()));
        Assert.assertTrue(str.contains("3"));
    }

    private Map<String, String> rawVariableProps() {
        Map<String, String> raw = new HashMap<>();
        raw.put(FileFormat.FILE_FORMAT_TYPE.getName(), "Json");
        raw.put(FileFormat.RECORD_DELIMITER.getName(), "\r");
        raw.put(FileFormat.SKIP_HEADER.getName(), "3");
        return raw;
    }

    @Test
    public void testCheckRequiredOpts() {
        try {
            PropertiesSet.readFromStrMap(FileFormat.get(), Maps.newHashMap());
            Assert.fail("Expected an NoSuchElementException to be thrown");
        } catch (NoSuchElementException e) {
            Assert.assertTrue(e.getMessage().contains("Missing"));
        }
    }

    @SuppressWarnings("unchecked")
    private void verifyVariableProps(PropertiesSet properties) {
        Assert.assertEquals(FileFormat.Type.JSON, properties.get(FileFormat.FILE_FORMAT_TYPE));
        Assert.assertEquals("\r", properties.get(FileFormat.RECORD_DELIMITER));
        Assert.assertEquals(3, properties.get(FileFormat.SKIP_HEADER));
    }

    private static class FileFormat implements PropertySchema.SchemaGroup {
        public static PropertySchema<Type> FILE_FORMAT_TYPE =
                new PropertySchema.EnumProperty<>("type", Type.class)
                        .setDefauleValue(Type.CSV);
        public static PropertySchema<String> RECORD_DELIMITER =
                new PropertySchema.StringProperty("record_delimiter").setDefauleValue("\n");
        public static PropertySchema<String> FIELD_DELIMITER =
                new PropertySchema.StringProperty("field_delimiter").setDefauleValue("|");
        public static PropertySchema<Integer> SKIP_HEADER =
                new PropertySchema.IntProperty("skip_header", true).setMin(0);

        private static final FileFormat INSTANCE = new FileFormat();

        private final ImmutableMap<String, PropertySchema> schemas = PropertySchema.createSchemas(
                FILE_FORMAT_TYPE,
                RECORD_DELIMITER,
                FIELD_DELIMITER,
                SKIP_HEADER);

        public ImmutableMap<String, PropertySchema> getSchemas() {
            return schemas;
        }

        public static FileFormat get() {
            return INSTANCE;
        }

        public enum Type {
            CSV, JSON, ORC, PARQUET
        }
    }
}
