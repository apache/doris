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

package org.apache.doris.filesystem;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileSystemUtil {

    public static void asyncRenameFiles(FileSystem fs,
                                        Executor executor,
                                        List<CompletableFuture<?>> renameFileFutures,
                                        AtomicBoolean cancelled,
                                        String origFilePath,
                                        String destFilePath,
                                        List<String> fileNames) {
        for (String fileName : fileNames) {
            String source = joinPath(origFilePath, fileName);
            String target = joinPath(destFilePath, fileName);
            renameFileFutures.add(CompletableFuture.runAsync(() -> {
                if (cancelled.get()) {
                    return;
                }
                try {
                    fs.rename(Location.of(source), Location.of(target));
                } catch (java.io.IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }, executor));
        }
    }

    public static void asyncRenameDir(FileSystem fs,
                                      Executor executor,
                                      List<CompletableFuture<?>> renameFileFutures,
                                      AtomicBoolean cancelled,
                                      String origFilePath,
                                      String destFilePath,
                                      Runnable runWhenPathNotExist) {
        renameFileFutures.add(CompletableFuture.runAsync(() -> {
            if (cancelled.get()) {
                return;
            }
            try {
                fs.renameDirectory(Location.of(origFilePath), Location.of(destFilePath), runWhenPathNotExist);
            } catch (java.io.IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }, executor));
    }

    private static String joinPath(String parent, String child) {
        return parent.endsWith("/") ? parent + child : parent + "/" + child;
    }

    /**
     * Returns the parent directory of the given path (including the trailing slash).
     * If the path contains no slash, returns the path unchanged.
     *
     * <p>Examples:
     * <pre>
     *   "hdfs://nn/a/b/file.csv"  →  "hdfs://nn/a/b/"
     *   "hdfs://nn/file.csv"      →  "hdfs://nn/"
     *   "file.csv"                →  "file.csv"
     * </pre>
     */
    public static String extractParentDirectory(String path) {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash >= 0) {
            return path.substring(0, lastSlash + 1);
        }
        return path;
    }
}
