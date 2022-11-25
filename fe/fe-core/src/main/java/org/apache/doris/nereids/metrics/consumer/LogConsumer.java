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

import org.apache.logging.log4j.Logger;

/**
 * log consumer
 */
public class LogConsumer extends EventConsumer {
    private final Logger logger;

    public LogConsumer(Class<? extends Event> targetClass, Logger logger) {
        super(targetClass);
        this.logger = logger;
    }

    @Override
    public void consume(Event e) {
        logger.info(e.toString());
    }
}
