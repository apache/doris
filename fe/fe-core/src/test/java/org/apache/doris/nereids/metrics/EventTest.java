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

package org.apache.doris.nereids.metrics;

import org.apache.doris.nereids.metrics.consumer.PrintConsumer;
import org.apache.doris.nereids.metrics.event.CounterEvent;
import org.apache.doris.nereids.metrics.event.EnforcerEvent;
import org.apache.doris.nereids.metrics.event.GroupMergeEvent;
import org.apache.doris.nereids.metrics.event.TransformEvent;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;

public class EventTest extends TestWithFeService {
    private EventChannel channel;
    private List<EventProducer> producers;

    private final StringBuilder builder = new StringBuilder();
    private final PrintStream printStream = new PrintStream(new OutputStream() {
        @Override
        public void write(int b) {
            builder.append((char) b);
        }
    });

    @Override
    public void runBeforeAll() {
        connectContext.getSessionVariable().setEnableNereidsTrace(true);
        connectContext.getSessionVariable().setEnableNereidsPlanner(true);
        channel = new EventChannel()
                .addConsumers(ImmutableList.of(
                        new PrintConsumer(CounterEvent.class, printStream),
                        new PrintConsumer(TransformEvent.class, printStream),
                        new PrintConsumer(EnforcerEvent.class, printStream),
                        new PrintConsumer(GroupMergeEvent.class, printStream)))
                .addEnhancers(ImmutableList.of(
                        new EventEnhancer(CounterEvent.class) {
                            @Override
                            public void enhance(Event e) {
                                CounterEvent.updateCounter(((CounterEvent) e).getCounterType());
                            }
                        }
                ));
        producers = ImmutableList.of(
                new EventProducer(CounterEvent.class, ImmutableList.of(
                        new EventFilter(CounterEvent.class) { },
                        new EventFilter(CounterEvent.class) { }),
                        channel),
                new EventProducer(TransformEvent.class, ImmutableList.of(
                        new EventFilter(TransformEvent.class) {
                            @Override
                            public Event checkEvent(Event event) {
                                return ((TransformEvent) event).getGroupExpression() == null ? event : null;
                            }
                        }), channel)
        );
        channel.start();
    }

    @Override
    public void runAfterAll() {
        channel.stop();
        Assertions.assertEquals(
                "CounterEvent{count=1, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                        + "CounterEvent{count=2, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "CounterEvent{count=3, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "CounterEvent{count=4, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "CounterEvent{count=5, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "CounterEvent{count=6, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "TransformEvent{groupExpression=null, before=null, afters=[], ruleType=AGGREGATE_DISASSEMBLE}\n"
                            + "CounterEvent{count=7, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "TransformEvent{groupExpression=null, before=null, afters=[], ruleType=AGGREGATE_DISASSEMBLE}\n"
                            + "CounterEvent{count=8, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "TransformEvent{groupExpression=null, before=null, afters=[], ruleType=AGGREGATE_DISASSEMBLE}\n"
                            + "CounterEvent{count=9, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "TransformEvent{groupExpression=null, before=null, afters=[], ruleType=AGGREGATE_DISASSEMBLE}\n"
                            + "CounterEvent{count=10, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "TransformEvent{groupExpression=null, before=null, afters=[], ruleType=AGGREGATE_DISASSEMBLE}\n"
                            + "CounterEvent{count=11, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "TransformEvent{groupExpression=null, before=null, afters=[], ruleType=AGGREGATE_DISASSEMBLE}\n"
                            + "CounterEvent{count=12, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "TransformEvent{groupExpression=null, before=null, afters=[], ruleType=AGGREGATE_DISASSEMBLE}\n"
                            + "CounterEvent{count=13, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "TransformEvent{groupExpression=null, before=null, afters=[], ruleType=AGGREGATE_DISASSEMBLE}\n"
                            + "CounterEvent{count=14, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "TransformEvent{groupExpression=null, before=null, afters=[], ruleType=AGGREGATE_DISASSEMBLE}\n"
                            + "CounterEvent{count=15, counterType=PLAN_CONSTRUCTOR, group=null, groupExpression=null, plan=null}\n"
                            + "TransformEvent{groupExpression=null, before=null, afters=[], ruleType=AGGREGATE_DISASSEMBLE}\n",
                        builder.toString());
        CounterEvent.clearCounter();
    }

    @Test
    public void testEvent() {
        connectContext.getSessionVariable().setNereidsEventMode("all except transform");
        channel.setConnectContext(connectContext);
        for (int i = 0; i < 10; ++i) {
            producers.get(i % 2).log(i % 2 == 0
                    ? CounterEvent.of(0, CounterType.PLAN_CONSTRUCTOR, null, null, null)
                    : TransformEvent.of(null, null, ImmutableList.of(), RuleType.AGGREGATE_DISASSEMBLE));
        }
        connectContext.getSessionVariable().setNereidsEventMode("all");
        channel.setConnectContext(connectContext);
        for (int i = 0; i < 10; ++i) {
            producers.get(i % 2).log(i % 2 == 0
                    ? CounterEvent.of(0, CounterType.PLAN_CONSTRUCTOR, null, null, null)
                    : TransformEvent.of(null, null, ImmutableList.of(), RuleType.AGGREGATE_DISASSEMBLE));
        }
        connectContext.getSessionVariable().setNereidsEventMode("counter, transform");
        channel.setConnectContext(connectContext);
        for (int i = 0; i < 10; ++i) {
            producers.get(i % 2).log(i % 2 == 0
                    ? CounterEvent.of(0, CounterType.PLAN_CONSTRUCTOR, null, null, null)
                    : TransformEvent.of(null, null, ImmutableList.of(), RuleType.AGGREGATE_DISASSEMBLE));
        }
    }
}
