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

package org.apache.doris.job.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslatorVararg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ThreadFactory;

/**
 * Utility class for creating and managing a Disruptor instance.
 *
 * @param <T> the type of the event handled by the Disruptor
 */
public class TaskDisruptor<T> {
    private static final Logger LOG = LogManager.getLogger(TaskDisruptor.class);
    private final Disruptor<T> disruptor;
    private final EventTranslatorVararg<T> eventTranslator;

    /**
     * Constructs a DisruptorUtil instance.
     *
     * @param eventFactory    the factory for creating events
     * @param ringBufferSize  the size of the ring buffer
     * @param threadFactory   the thread factory to create threads for event handling
     * @param waitStrategy    the wait strategy for the ring buffer
     * @param workHandlers    the work handlers for processing events
     * @param eventTranslator the translator for publishing events with variable arguments
     */
    public TaskDisruptor(EventFactory<T> eventFactory, int ringBufferSize, ThreadFactory threadFactory,
                         WaitStrategy waitStrategy, WorkHandler<T>[] workHandlers,
                         EventTranslatorVararg<T> eventTranslator) {
        disruptor = new Disruptor<>(eventFactory, ringBufferSize, threadFactory,
                ProducerType.MULTI, waitStrategy);
        disruptor.handleEventsWithWorkerPool(workHandlers);
        this.eventTranslator = eventTranslator;
        disruptor.start();
    }

    /**
     * Starts the Disruptor.
     */
    public void start() {
        disruptor.start();
    }

    /**
     * Publishes an event with the provided arguments.
     *
     * @param args the arguments for the event
     */
    public boolean publishEvent(Object... args) {
        try {
            RingBuffer<T> ringBuffer = disruptor.getRingBuffer();
            return ringBuffer.tryPublishEvent(eventTranslator, args);
        } catch (Exception e) {
            LOG.warn("Failed to publish event", e);
            // Handle the exception, e.g., retry or alert
        }
        return false;
    }

    /**
     * Shuts down the Disruptor.
     */
    public void shutdown() {
        disruptor.shutdown();
    }
}
