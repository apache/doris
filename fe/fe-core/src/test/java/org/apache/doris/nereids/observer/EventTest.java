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

package org.apache.doris.nereids.observer;

import org.apache.doris.nereids.observer.event.CounterEvent;
import org.apache.doris.nereids.observer.event.TransformEvent;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

public class EventTest {
    private final EventChannel channel = new EventChannel(
            ImmutableList.of(),
            ImmutableList.of()
    );
    private final List<EventProducer> producers = ImmutableList.of(
            new EventProducer(TransformEvent.class, channel),
            new EventProducer(CounterEvent.class, channel)
    );

    @Test
    public void testEvent() throws Exception {
        channel.start();
        for (int i = 0; i < 10; ++i) {
            producers.get(i % 2).log(1);
        }
        channel.stop();
    }
}
