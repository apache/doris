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

package org.apache.doris.scheduler.disruptor;

import com.lmax.disruptor.EventFactory;
import lombok.Data;

/**
 * This class represents an event task that can be produced and consumed by the Disruptor.
 * The event task contains the ID of the event job and the ID of the event task itself.
 * The class also provides an event factory to create instances of {@link TimerTaskEvent}.
 * <p>
 * it's used by {@link TimerTaskDisruptor} and {@link TimerTaskExpirationHandler}
 */
@Data
public class TimerTaskEvent {

    private Long jobId;

    public static final EventFactory<TimerTaskEvent> FACTORY = TimerTaskEvent::new;
}
