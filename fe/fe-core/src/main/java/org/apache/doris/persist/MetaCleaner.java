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

package org.apache.doris.persist;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class MetaCleaner {
    private static final Logger LOG = LogManager.getLogger(MetaCleaner.class);

    private String imageDir;

    public MetaCleaner(String imageDir) {
        this.imageDir = imageDir;
    }

    public void clean() throws IOException {
        Storage storage = new Storage(imageDir);
        long currentVersion = storage.getLatestValidatedImageSeq();
        long imageDeleteVersion = currentVersion - 1;

        File currentImage = storage.getImageFile(currentVersion);
        if (currentImage.exists()) {
            File metaDir = new File(imageDir);
            File[] children = metaDir.listFiles();

            // Iterate all file in metaDir
            for (File file : children) {
                String type = fileType(file);
                if (type == null) {
                    continue;
                }
                String filename = file.getName();
                // Delete all image whose version is less than imageVersionDelete
                if (type.equalsIgnoreCase(Storage.IMAGE)) {
                    if (filename.endsWith(".part")) {
                        filename = filename.substring(0, filename.length() - ".part".length());
                    }
                    long version = Long.parseLong(filename.substring(filename.lastIndexOf('.') + 1));

                    if (version < imageDeleteVersion) {
                        if (file.delete()) {
                            LOG.info(file.getAbsoluteFile() + " deleted.");
                        } else {
                            LOG.warn(file.getAbsoluteFile() + " delete failed.");
                        }
                    }
                }
            }
        }
    }

    public void cleanTheLatestInvalidImageFile(String path) throws IOException {
        File latestInvalidImage = new File(path);
        if (latestInvalidImage.exists()) {
            if (latestInvalidImage.delete()) {
                LOG.info(latestInvalidImage.getAbsoluteFile() + " deleted.");
            } else {
                LOG.warn(latestInvalidImage.getAbsoluteFile() + " delete failed.");
            }
        }
    }

    private String fileType(File file) throws IOException {
        String type = null;
        String filename = file.getName();

        if (filename.equals(Storage.IMAGE_NEW)) {
            type = Storage.IMAGE_NEW;
        } else {
            if (filename.endsWith(".part")) {
                filename = filename.substring(0, filename.length() - ".part".length());
            }

            if (filename.contains(".")) {
                if (filename.startsWith(Storage.IMAGE)) {
                    type = Storage.IMAGE;
                }

                if (filename.startsWith(Storage.EDITS)) {
                    type = Storage.EDITS;
                }
            }
        }

        return type;
    }

}
