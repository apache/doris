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

package org.apache.doris.datasource.scan;

import org.apache.doris.thrift.TFileFormatType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the connector-agnostic file-format name to the thrift enum BE selects its reader with.
 *
 * <p>WHY this matters beyond the switch being obvious: the fall-through is {@code FORMAT_JNI}, which is a
 * WORKING reader rather than an error. A format name the switch does not know therefore produces no failure
 * here at all — the scan simply lands in the JNI scanner and fails later, in BE, with a message about the
 * wrong reader. Every name a connector may return has to be listed for that reason, not for coverage.</p>
 */
public class PluginDrivenScanNodeFileFormatTest {

    @Test
    public void mapsArrowToTheArrowReader() {
        // A connector that hands BE Arrow record batches instead of a file (adbc). BE gates entry to its
        // Arrow table-format path on FORMAT_ARROW, so mapping this to the JNI default would route the scan
        // to a reader that has no ADBC branch.
        Assertions.assertEquals(TFileFormatType.FORMAT_ARROW,
                PluginDrivenScanNode.mapFileFormatType("arrow"));
    }

    @Test
    public void mapsTheFileBackedFormatsToTheirNativeReaders() {
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET,
                PluginDrivenScanNode.mapFileFormatType("parquet"));
        Assertions.assertEquals(TFileFormatType.FORMAT_ORC,
                PluginDrivenScanNode.mapFileFormatType("orc"));
        Assertions.assertEquals(TFileFormatType.FORMAT_TEXT,
                PluginDrivenScanNode.mapFileFormatType("text"));
        Assertions.assertEquals(TFileFormatType.FORMAT_CSV_PLAIN,
                PluginDrivenScanNode.mapFileFormatType("csv"));
        Assertions.assertEquals(TFileFormatType.FORMAT_JSON,
                PluginDrivenScanNode.mapFileFormatType("json"));
        Assertions.assertEquals(TFileFormatType.FORMAT_AVRO,
                PluginDrivenScanNode.mapFileFormatType("avro"));
        Assertions.assertEquals(TFileFormatType.FORMAT_ES_HTTP,
                PluginDrivenScanNode.mapFileFormatType("es_http"));
    }

    @Test
    public void isCaseInsensitiveBecauseTheNameComesFromAConnector() {
        // The name arrives as a free-form string in the connector's scan-node properties, so the casing is
        // the connector's choice, not the engine's.
        Assertions.assertEquals(TFileFormatType.FORMAT_ARROW,
                PluginDrivenScanNode.mapFileFormatType("ARROW"));
    }

    @Test
    public void fallsBackToJniForAnUnknownName() {
        Assertions.assertEquals(TFileFormatType.FORMAT_JNI,
                PluginDrivenScanNode.mapFileFormatType("jni"));
        Assertions.assertEquals(TFileFormatType.FORMAT_JNI,
                PluginDrivenScanNode.mapFileFormatType("not-a-format"));
    }
}
