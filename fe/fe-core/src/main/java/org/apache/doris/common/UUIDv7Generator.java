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

package org.apache.doris.common;

// Format
// We use UUID v7 (RFC 4122) for generating UUIDs.
// UUIDv7 was chosen for the following benefits:
// 1. Time-ordered - Contains a timestamp component that makes UUIDs sortable by generation time,
//    which is valuable for query tracking, debugging, and performance analysis
// 2. High performance - Efficient generation with minimal overhead
// 3. Global uniqueness - Combines timestamp with random data to ensure uniqueness across
//    distributed systems without coordination
// 4. Database friendly - The time-ordered nature makes it more efficient for database indexing
//    and storage compared to purely random UUIDs (like v4)
// 5. Future-proof - Follows the latest UUID standard with improvements over older versions

// Note: Our implementation differs slightly from the standard UUIDv7 specification by
// using a counter instead of random bits in the "rand_a" field to further enhance
// uniqueness when generating multiple UUIDs in rapid succession.

// 0                   1                   2                   3
// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |                           unix_ts_ms                          |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |          unix_ts_ms           |  ver  |       counter         |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |var|                        rand_b                             |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |                            rand_b                             |
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class UUIDv7Generator {
    private static final UUIDv7Generator INSTANCE = new UUIDv7Generator();
    private static final Random RANDOM = ThreadLocalRandom.current();
    private static final AtomicInteger COUNTER = new AtomicInteger(0);
    private static final long VERSION = 7L << 12;
    private static final long VARIANT = 2L << 62;
    private static final long MAX_SEQUENCE = 0xFFF;
    private static final int SPIN_THRESHOLD = 10;
    private static final long PARK_NANOS = 100L; // 100 nanoseconds

    private final AtomicLong timestampAndSequence;

    private UUIDv7Generator() {
        // Initialize with current timestamp and 0 sequence
        long initialTimestamp = System.currentTimeMillis();
        timestampAndSequence = new AtomicLong((initialTimestamp << 12));
    }

    public static UUIDv7Generator getInstance() {
        return INSTANCE;
    }

    public UUID nextUUID() {
        int spinCount = 0;
        while (true) {
            long currentTimestamp = System.currentTimeMillis();
            long current = timestampAndSequence.get();
            long timestamp = current >>> 12;
            long sequence = current & MAX_SEQUENCE;

            if (currentTimestamp < timestamp || (currentTimestamp == timestamp && sequence >= MAX_SEQUENCE)) {
                if (spinCount < SPIN_THRESHOLD) {
                    spinCount++;
                    continue;
                }
                LockSupport.parkNanos(PARK_NANOS);
                continue;
            }

            long next;
            if (currentTimestamp > timestamp) {
                next = currentTimestamp << 12;
            } else {
                next = current + 1;
            }

            if (timestampAndSequence.compareAndSet(current, next)) {
                return generateUUID(next >>> 12, next & MAX_SEQUENCE);
            }
            spinCount++;
        }
    }

    private UUID generateUUID(long timestamp, long sequence) {
        // Get counter value (12 bits)
        int counter = (int) (sequence & 0xFFF);

        // Generate random bits for the lower part
        long random = RANDOM.nextLong();

        // Build UUID components
        // Timestamp (48 bits) + Version (4 bits) + Counter (12 bits)
        long msb = (timestamp << 16) | VERSION | counter;

        // Variant (2 bits) + Random (62 bits)
        long lsb = VARIANT | (random & 0x3FFFFFFFFFFFFFFFL);

        return new UUID(msb, lsb);
    }
}
