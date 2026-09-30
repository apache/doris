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

package org.apache.doris.foundation.util;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class UUIDUtils {
    private static final AtomicLong LAST_V7_TIMESTAMP_AND_COUNTER = new AtomicLong();
    private static final long UUID_V7_COUNTER_MASK = (1L << 12) - 1;

    /**
     * Generates a random RFC‑4122 version 4 UUID using {@link java.util.concurrent.ThreadLocalRandom}.
     *
     * <p>This implementation avoids the shared random source used by
     * {@link java.util.UUID#randomUUID()}, reducing contention in high‑concurrency
     * environments and improving throughput.</p>
     *
     * <p><b>Trade‑off:</b> {@code ThreadLocalRandom} is not cryptographically secure.
     * It should be used for high‑throughput identifiers (e.g. request or trace IDs),
     * not for security‑sensitive tokens.</p>
     */
    public static UUID fastUUID() {
        final Random ng = ThreadLocalRandom.current();
        long mostSigBits = ng.nextLong();
        long leastSigBits = ng.nextLong();
        // format to uuid v4
        mostSigBits &= 0xFFFFFFFFFFFF0FFFL;
        mostSigBits |= 0x0000000000004000L;
        leastSigBits &= 0x3FFFFFFFFFFFFFFFL;
        leastSigBits |= 0x8000000000000000L;

        return new UUID(mostSigBits, leastSigBits);
    }

    public static UUID randomUUID() {
        return UUID.randomUUID();
    }

    /** Generate a monotonically increasing RFC 9562 version 7 UUID. */
    public static UUID uuidV7() {
        Random random = ThreadLocalRandom.current();
        long now = System.currentTimeMillis();
        long initialCounter = random.nextLong() & UUID_V7_COUNTER_MASK;

        long previous;
        long next;
        do {
            previous = LAST_V7_TIMESTAMP_AND_COUNTER.get();
            long previousTimestamp = previous >>> 12;
            next = now > previousTimestamp
                    ? (now << 12) | initialCounter
                    : previous + 1;
        } while (!LAST_V7_TIMESTAMP_AND_COUNTER.compareAndSet(previous, next));

        long timestamp = next >>> 12;
        long counter = next & UUID_V7_COUNTER_MASK;
        long mostSignificantBits = (timestamp << 16) | (7L << 12) | counter;
        long leastSignificantBits = (random.nextLong() & 0x3FFFFFFFFFFFFFFFL)
                | 0x8000000000000000L;
        return new UUID(mostSignificantBits, leastSignificantBits);
    }
}
