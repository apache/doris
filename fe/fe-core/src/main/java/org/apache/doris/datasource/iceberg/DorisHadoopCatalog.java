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

package org.apache.doris.datasource.iceberg;

import com.google.common.base.Throwables;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileIO;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns and closes the shared FileIO created by one Iceberg HadoopCatalog generation. */
public class DorisHadoopCatalog extends HadoopCatalog {
    private final AtomicBoolean closed = new AtomicBoolean();
    private FileIO ownedFileIO;

    @Override
    public void initialize(String name, Map<String, String> properties) {
        try {
            super.initialize(name, properties);
            ownedFileIO = extractFileIO();
        } catch (RuntimeException | Error failure) {
            closePartiallyInitializedFileIO(failure);
            throw failure;
        }
    }

    private void closePartiallyInitializedFileIO(Throwable initializationFailure) {
        try {
            FileIO fileIO = extractFileIO();
            if (fileIO != null) {
                fileIO.close();
            }
        } catch (Throwable closeFailure) {
            initializationFailure.addSuppressed(closeFailure);
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Throwable closeFailure = null;
        try {
            super.close();
        } catch (Throwable e) {
            closeFailure = e;
        }
        try {
            if (ownedFileIO != null) {
                ownedFileIO.close();
            }
        } catch (Throwable e) {
            if (closeFailure != null) {
                closeFailure.addSuppressed(e);
            } else {
                closeFailure = e;
            }
        } finally {
            ownedFileIO = null;
        }
        if (closeFailure != null) {
            Throwables.throwIfInstanceOf(closeFailure, IOException.class);
            Throwables.throwIfUnchecked(closeFailure);
            throw new IOException(closeFailure);
        }
    }

    private FileIO extractFileIO() {
        try {
            Field fileIOField = HadoopCatalog.class.getDeclaredField("fileIO");
            fileIOField.setAccessible(true);
            return (FileIO) fileIOField.get(this);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to capture Iceberg HadoopCatalog FileIO ownership", e);
        }
    }
}
