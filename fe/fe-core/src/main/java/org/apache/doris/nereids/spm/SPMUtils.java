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

package org.apache.doris.nereids.spm;

import org.apache.commons.codec.digest.DigestUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/** Digest hashing and timestamp helpers for SPM. */
public final class SPMUtils {

    private SPMUtils() {
    }

    /**
     * Returns the current wall-clock time in epoch microseconds, the unit used for stored SPM timestamps. This
     * clock alone does not guarantee distinct or increasing values. Fence ages and other elapsed-time bounds use
     * milliseconds.
     */
    public static long nowMicros() {
        return ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
    }

    /**
     * Returns the first 64 bits of the MD5 digest of the supplied text. Candidate lookup compares the complete
     * structural digest after the hash filter, so a hash collision does not establish a match.
     */
    public static long hashOf(String digestText) {
        String md5 = DigestUtils.md5Hex(digestText);
        // 64 bits are enough: a bucket collision only costs one extra Level 2 digest
        // comparison, which stays authoritative.
        return Long.parseUnsignedLong(md5.substring(0, 16), 16);
    }
}
