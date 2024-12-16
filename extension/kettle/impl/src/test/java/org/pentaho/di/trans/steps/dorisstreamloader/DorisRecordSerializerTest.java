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

package org.pentaho.di.trans.steps.dorisstreamloader;

import org.junit.Assert;
import org.junit.Test;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaBoolean;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaInternetAddress;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;
import org.pentaho.di.trans.steps.dorisstreamloader.serializer.DorisRecordSerializer;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import javax.mail.internet.InternetAddress;

import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.CSV;

public class DorisRecordSerializerTest {

    @Test
    public void testSerialize() throws Exception {
        ValueMetaInterface[] formatMeta = new ValueMetaInterface[]{
            new ValueMetaBoolean("boolean"),
            new ValueMetaInteger("integer"),
            new ValueMetaNumber("number"),
            new ValueMetaBigNumber("bignumber"),
            new ValueMetaDate("date"),
            new ValueMetaTimestamp("timestamp"),
            new ValueMetaBinary("binary"),
            new ValueMetaString("string"),
            new ValueMetaInternetAddress("address"),
        };

        DorisRecordSerializer serializer = DorisRecordSerializer.builder()
            .setType(CSV)
            .setFieldNames(new String[]{"c_boolean", "c_integer", "c_number", "c_bignumber", "c_date", "c_timestamp", "c_binary", "c_string", "c_internetAddress"})
            .setFormatMeta(formatMeta)
            .setFieldDelimiter(",")
            .setLogChannelInterface(new LogChannel())
            .setDeletable(false)
            .build();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Object[] data = new Object[]{
            true,
            10L,
            123.23,
            new BigDecimal("123456789.1234"),
            dateFormat.parse("2024-01-01"),
            Timestamp.valueOf("2024-01-01 10:11:22.123"),
            "binary",
            "string",
            new InternetAddress("127.0.0.1")};
        String actual = serializer.buildCSVString(data, formatMeta.length);
        String except = "true,10,123.23,123456789.1234,2024-01-01,2024-01-01 10:11:22.123,binary,string,127.0.0.1";
        Assert.assertEquals(except, actual);
    }
}
