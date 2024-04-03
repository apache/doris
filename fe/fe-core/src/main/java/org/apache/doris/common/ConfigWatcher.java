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

package org.apache.doris.common;

import org.apache.doris.common.util.Daemon;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

/*
 * used for watch config changed
 */
public class ConfigWatcher extends Daemon {
    private static final Logger LOG = LogManager.getLogger(ConfigWatcher.class);

    public final Path configPath;

    public ConfigWatcher(String configPathStr) {
        super("config watcher");
        Preconditions.checkState(!Strings.isNullOrEmpty(configPathStr));
        configPath = Paths.get(configPathStr);
    }

    @Override
    protected void runOneCycle() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("start config watcher loop");
        }
        try {
            WatchService watchService = FileSystems.getDefault().newWatchService();
            configPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                                StandardWatchEventKinds.ENTRY_MODIFY,
                                StandardWatchEventKinds.ENTRY_DELETE);
            // start an infinite loop
            while (true) {
                // retrieve and remove the next watch key
                final WatchKey key = watchService.take();
                // get list of pending events for the watch key
                for (WatchEvent<?> watchEvent : key.pollEvents()) {
                    // get the kind of event (create, modify, delete)
                    final Kind<?> kind = watchEvent.kind();
                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }

                    final WatchEvent<Path> watchEventPath = (WatchEvent<Path>) watchEvent;
                    final Path filePath = watchEventPath.context();
                    LOG.info("config watcher [" + kind + " -> " + filePath + "]");

                    if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                        handleCreate(filePath);
                    } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                        handleModify(filePath);
                    } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                        handleDelete(filePath);
                    }
                }

                // reset the key
                boolean valid = key.reset();
                // exit loop if the key is not valid
                if (!valid) {
                    LOG.warn("config watch key is not valid");
                    break;
                }
            } // end while
        } catch (Exception e) {
            LOG.warn("config watcher got exception", e);
        }
    }

    private void handleCreate(Path filePath) {
        // TODO(cmy): implement if needed
    }

    private void handleDelete(Path filePath) {
        // TODO(cmy): implement if needed
    }

    private void handleModify(Path filePath) {
        // TODO(cmy): implement if needed
    }

    // for test
    public static void main(String[] args) throws InterruptedException {
        ConfigWatcher watcher = new ConfigWatcher("./");
        watcher.start();
        Thread.sleep(500000);
    }
}
