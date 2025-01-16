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
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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
            // Set the timeout to 1 second, converted to nanoseconds for precision
            long timeoutInNanos = TimeUnit.SECONDS.toNanos(1);  // Timeout set to 1 second
            long startTime = System.nanoTime();  // Record the start time

            // Loop until the timeout is reached
            while (System.nanoTime() - startTime < timeoutInNanos) {
                // Check if there is enough remaining capacity in the ring buffer
                // Adjusting to check if the required capacity is available (instead of hardcoding 1)
                if (disruptor.getRingBuffer().remainingCapacity() > 1) {
                    // Publish the event if there is enough capacity
                    disruptor.getRingBuffer().publishEvent(eventTranslator, args);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("publishEvent success,the remaining buffer size is {}",
                                disruptor.getRingBuffer().remainingCapacity());
                    }
                    return true;
                }

                // Wait for a short period before retrying
                try {
                    Thread.sleep(10);  // Adjust the wait time as needed (maybe increase if not high-frequency)
                } catch (InterruptedException e) {
                    // Log the exception and return false if interrupted
                    Thread.currentThread().interrupt();  // Restore interrupt status
                    LOG.warn("Thread interrupted while waiting to publish event", e);
                    return false;
                }
            }

            // Timeout reached without publishing the event
            LOG.warn("Failed to publish event within the specified timeout (1 second)."
                            + "Queue may be full. the remaining buffer size is {}",
                    disruptor.getRingBuffer().remainingCapacity());
        } catch (Exception e) {
            // Catching general exceptions to handle unexpected errors
            LOG.warn("Failed to publish event due to an unexpected error", e);
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
