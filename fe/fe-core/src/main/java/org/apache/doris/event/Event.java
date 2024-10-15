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

import org.apache.doris.catalog.Env;

import java.util.Objects;

public abstract class Event {
    protected final long eventId;

    // eventTime of the event. Used instead of calling getter on event everytime
    protected final long eventTime;

    // eventType from the NotificationEvent
    protected final EventType eventType;

    protected Event(EventType eventType) {
        Objects.requireNonNull(eventType, "require eventType");
        this.eventId = Env.getCurrentEnv().getNextId();
        this.eventTime = System.currentTimeMillis();
        this.eventType = eventType;
    }

    public long getEventId() {
        return eventId;
    }

    public long getEventTime() {
        return eventTime;
    }

    public EventType getEventType() {
        return eventType;
    }

    @Override
    public String toString() {
        return "Event{"
                + "eventId=" + eventId
                + ", eventTime=" + eventTime
                + ", eventType=" + eventType
                + '}';
    }
}
