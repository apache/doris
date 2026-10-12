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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

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

    @Test
    void testRecordOracle() throws Exception {
        // This corpus is also consumed by FileScannerV2. Generate expectations with Hive itself,
        // including null trailing fields and binary NULs, rather than a second hand-written parser.
        List<String> records = new ArrayList<>(List.of("x|  qa|bq|c", "qaqqbq,tail", "eeabc,tail",
                "qleft", "rightq|tail", "abcqleft|rightq|tail", "a\0b,tail", "", ",", "||",
                "left|qunclosed", "qleftq|tail", "qleftqqrightq|tail", "x|\t qa|bq|c",
                "x|\u2003\u2003qa|bq|c", "x|\u00a0qa|bq|c", "éqleft|rightq|tail",
                "😀qleft|rightq|tail", "abcqleftérightqétail", "qéqétail", "\"\"\"x\"\",tail"));
        Random random = new Random(7321);
        String alphabet = "aqe|, \\" + '"' + '\0';
        for (int row = 0; row < 80; row++) {
            StringBuilder record = new StringBuilder();
            for (int i = 0, length = random.nextInt(24); i < length; i++) {
                record.append(alphabet.charAt(random.nextInt(alphabet.length())));
            }
            records.add(record.toString());
        }
        List<String> corpus = new ArrayList<>();
        for (String tuple : List.of("|qe", ",q\0", ",\0e", ",\0\0", ",\"\\", "éqe")) {
            OpenCSVSerde serde = hiveSerde(Map.of("separatorChar", tuple.substring(0, 1),
                    "quoteChar", tuple.substring(1, 2), "escapeChar", tuple.substring(2, 3)), 3);
            for (String record : records) {
                List<?> result = (List<?>) serde.deserialize(new Text(record));
                StringBuilder encoded = new StringBuilder(hex(tuple)).append('\t').append(hex(record));
                for (Object value : result) {
                    encoded.append('\t').append(value == null ? "NULL" : hex(value.toString()));
                }
                corpus.add(encoded.toString());
            }
        }
        String output = System.getProperty("hive.csv.oracle.output");
        if (output != null) {
            Files.write(Path.of(output), corpus, StandardCharsets.UTF_8);
        }
        try (var input = getClass().getResourceAsStream("/hive-csv-oracle.tsv")) {
            Assertions.assertNotNull(input);
            Assertions.assertEquals(corpus,
                    new String(input.readAllBytes(), StandardCharsets.UTF_8).lines().toList());
        }
    }

    private static String hex(String value) {
        return value.isEmpty() ? "EMPTY" : HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8));
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
        return hiveSerde(properties, 2);
    }

    private static OpenCSVSerde hiveSerde(Map<String, String> properties, int columns) throws SerDeException {
        Properties serdeProperties = new Properties();
        serdeProperties.setProperty("columns", columns == 2 ? "first,second" : "first,second,third");
        serdeProperties.setProperty("columns.types", columns == 2 ? "string:string" : "string:string:string");
        serdeProperties.putAll(properties);
        OpenCSVSerde serde = new OpenCSVSerde();
        serde.initialize(null, serdeProperties);
        return serde;
    }
}
