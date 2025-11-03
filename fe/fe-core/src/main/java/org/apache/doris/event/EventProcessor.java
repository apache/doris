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

package org.apache.doris.event;

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.Set;

public class EventProcessor {

    private static final Logger LOG = LogManager.getLogger(EventProcessor.class);

    private Set<EventListener> listeners = Sets.newHashSet();

    public EventProcessor(EventListener... args) {
        for (EventListener listener : args) {
            this.listeners.add(listener);
        }
    }

    public boolean processEvent(Event event) {
        Objects.requireNonNull(event);
        if (LOG.isDebugEnabled()) {
            LOG.debug("processEvent: {}", event);
        }
        boolean result = true;
        for (EventListener listener : listeners) {
            try {
                listener.processEvent(event);
            } catch (EventException e) {
                // A listener processing failure does not affect other listeners
                LOG.warn("[{}] process event failed, event: {}, errMsg: {}", listener.getClass().getName(), event,
                        e.getMessage());
                result = false;
            }
        }
        return result;
    }
}
