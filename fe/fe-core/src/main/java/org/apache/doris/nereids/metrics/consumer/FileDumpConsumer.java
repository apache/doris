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

package org.apache.doris.nereids.metrics.consumer;

import org.apache.doris.nereids.metrics.Event;
import org.apache.doris.nereids.metrics.EventConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * file dump consumer
 */
public class FileDumpConsumer extends EventConsumer {
    private static final Logger LOG = LogManager.getLogger(FileDumpConsumer.class);
    private final FileOutputStream fs;

    public FileDumpConsumer(Class<? extends Event> eventClass, String fileName) throws FileNotFoundException {
        super(eventClass);
        this.fs = new FileOutputStream(fileName);
    }

    @Override
    public void consume(Event event) {
        try {
            fs.write(event.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOG.warn("write to file encounter exception: ", e);
        }
    }

    @Override
    public void close() {
        try {
            fs.close();
        } catch (IOException e) {
            LOG.warn("close file output stream encounter: ", e);
        }
    }
}
