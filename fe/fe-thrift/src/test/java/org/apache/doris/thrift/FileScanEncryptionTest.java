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

package org.apache.doris.thrift;

import org.apache.doris.rpc.FileScanEncryption;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class FileScanEncryptionTest {
    @Test
    void absentPayloadMeansPlaintextAndDeletePayloadIsIndependent() {
        TTableFormatFileDesc table = new TTableFormatFileDesc().setIcebergParams(new TIcebergFileDesc());
        Assertions.assertFalse(FileScanEncryption.validate(table));
        table.getIcebergParams().setDeleteFiles(List.of(new TIcebergDeleteFileDesc().setEncryptionInfo(valid())));
        Assertions.assertTrue(FileScanEncryption.validate(table));
        table.getIcebergParams().setEncryptionInfo(valid());
        Assertions.assertTrue(FileScanEncryption.validate(table));
    }

    @Test
    void emptyOrMalformedPayloadNeverBecomesPlaintext() {
        TFileEncryptionInfo missingVersion = valid();
        missingVersion.unsetVersion();
        for (TFileEncryptionInfo info : List.of(new TFileEncryptionInfo(), missingVersion,
                new TFileEncryptionInfo().setVersion(1).setKey(new byte[16]),
                valid().setKey(new byte[15]), valid().setKey(new byte[0]))) {
            IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> FileScanEncryption.validateInfo(info));
            Assertions.assertTrue(error.getMessage().contains("INVALID_DESCRIPTOR"));
        }
    }

    @Test
    void unknownVersionsAreRejectedWithoutPrintingTheDescriptor() {
        IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                () -> FileScanEncryption.validateInfo(valid().setVersion(9)));
        Assertions.assertEquals("File encryption [UNSUPPORTED_VERSION]", error.getMessage());
    }

    @Test
    void emptyAadIsPresentAndValidForAllParquetKeySizes() {
        for (int length : List.of(16, 24, 32)) {
            FileScanEncryption.validateInfo(valid().setKey(new byte[length]));
        }
    }

    private static TFileEncryptionInfo valid() {
        return new TFileEncryptionInfo().setVersion(1).setKey(new byte[16]).setAadPrefix(new byte[0]);
    }
}
