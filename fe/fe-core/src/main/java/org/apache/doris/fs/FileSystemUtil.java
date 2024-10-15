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

package org.apache.doris.fs;

import org.apache.doris.backup.Status;

import org.apache.hadoop.fs.Path;

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
            Path source = new Path(origFilePath, fileName);
            Path target = new Path(destFilePath, fileName);
            renameFileFutures.add(CompletableFuture.runAsync(() -> {
                if (cancelled.get()) {
                    return;
                }
                Status status = fs.rename(source.toString(), target.toString());
                if (!status.ok()) {
                    throw new RuntimeException(status.getErrMsg());
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
            Status status = fs.renameDir(origFilePath, destFilePath, runWhenPathNotExist);
            if (!status.ok()) {
                throw new RuntimeException(status.getErrMsg());
            }
        }, executor));
    }
}
