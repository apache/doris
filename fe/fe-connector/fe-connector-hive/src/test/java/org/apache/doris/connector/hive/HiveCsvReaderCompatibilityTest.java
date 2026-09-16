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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

class HiveCsvReaderCompatibilityTest {
    @Test
    void testExplicitDefaultEscapeReadsLikeHive() throws Exception {
        for (String quote : new String[] {"\"", "q"}) {
            String record = quote + "left\\" + quote + "middle" + quote + ",tail";
            Map<String, String> properties = Map.of("quoteChar", quote, "escapeChar", "\"suffix");
            List<?> expected = (List<?>) hiveSerde(properties).deserialize(new Text(record));
            Assertions.assertEquals(List.of("left" + quote + "middle", "tail"), expected);
            for (Map<String, String> extracted : List.of(
                    HiveTextProperties.extract(HiveTextProperties.HIVE_OPEN_CSV_SERDE, Map.of(), properties),
                    HiveTextProperties.extract(HiveTextProperties.HIVE_OPEN_CSV_SERDE, properties, Map.of()))) {
                Assertions.assertEquals("\\", extracted.get(ScanNodePropertyKeys.TEXT_ESCAPE));
                try (CSVReader reader = new CSVReader(new StringReader(record),
                        extracted.get(ScanNodePropertyKeys.TEXT_COLUMN_SEPARATOR).charAt(0),
                        extracted.get(ScanNodePropertyKeys.TEXT_ENCLOSE).charAt(0),
                        extracted.get(ScanNodePropertyKeys.TEXT_ESCAPE).charAt(0))) {
                    Assertions.assertEquals(expected, Arrays.asList(reader.readNext()));
                }
            }
        }
    }

    @Test
    void testCharacterTuplesMatchHiveReaderAcceptance() throws Exception {
        // Use Hive's real reader as the oracle, including its writer-default escape sentinel and NUL rules.
        char[] characters = {'\0', ',', '|', '"', '\\', 'q', 'e'};
        for (char separator : characters) {
            for (char quote : characters) {
                for (char escape : characters) {
                    Map<String, String> properties = Map.of("separatorChar", String.valueOf(separator),
                            "quoteChar", String.valueOf(quote), "escapeChar", String.valueOf(escape));
                    OpenCSVSerde serde = hiveSerde(properties);
                    boolean accepted;
                    try {
                        serde.deserialize(new Text(""));
                        accepted = true;
                    } catch (SerDeException e) {
                        Assertions.assertInstanceOf(UnsupportedOperationException.class, e.getCause());
                        accepted = false;
                    }
                    assertAcceptance(accepted, Map.of(), properties);
                    assertAcceptance(accepted, properties, Map.of());
                    assertAcceptance(accepted, Map.of("quoteChar", String.valueOf(quote)),
                            Map.of("separatorChar", String.valueOf(separator), "escapeChar", String.valueOf(escape)));
                }
            }
        }
    }

    private static void assertAcceptance(boolean accepted, Map<String, String> serdeProperties,
            Map<String, String> tableProperties) {
        if (accepted) {
            Assertions.assertDoesNotThrow(() -> HiveTextProperties.extract(
                    HiveTextProperties.HIVE_OPEN_CSV_SERDE, serdeProperties, tableProperties));
        } else {
            Assertions.assertThrows(DorisConnectorException.class, () -> HiveTextProperties.extract(
                    HiveTextProperties.HIVE_OPEN_CSV_SERDE, serdeProperties, tableProperties));
        }
    }

    private static OpenCSVSerde hiveSerde(Map<String, String> properties) throws SerDeException {
        Properties serdeProperties = new Properties();
        serdeProperties.setProperty("columns", "first,second");
        serdeProperties.setProperty("columns.types", "string:string");
        serdeProperties.putAll(properties);
        OpenCSVSerde serde = new OpenCSVSerde();
        serde.initialize(null, serdeProperties);
        return serde;
    }
}
