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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

class DorisHiveCatalogTest {

    @Test
    void closesOwnedFileIOOnceAcrossRepeatedRetirement() throws Exception {
        DorisHiveCatalog catalog = new DorisHiveCatalog();
        RecordingFileIO fileIO = new RecordingFileIO();
        Field ownedFileIO = DorisHiveCatalog.class.getDeclaredField("ownedFileIO");
        ownedFileIO.setAccessible(true);
        ownedFileIO.set(catalog, fileIO);

        catalog.close();
        catalog.close();

        Assertions.assertEquals(1, fileIO.closeCount);
    }

    private static final class RecordingFileIO implements FileIO {
        private int closeCount;

        @Override
        public InputFile newInputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closeCount++;
        }
    }
}
