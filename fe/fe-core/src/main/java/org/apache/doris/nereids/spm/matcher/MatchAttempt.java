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

package org.apache.doris.nereids.spm.matcher;

/**
 * Tracks bounded retries of unordered expression pairing during a structural match. The driver reruns the check
 * with different rotation offsets for up to eight recorded nontrivial choices, using a mixed-radix counter. Nested
 * checks share the enclosing thread-local attempt. Exhausting the bounded search can miss a usable baseline; the
 * caller then keeps the original query.
 */
public final class MatchAttempt {
    /** Upper bound of whole-check passes (the first pass plus the retries). */
    public static final int MAX_ATTEMPTS = 8;

    /** Upper bound of recorded unordered choice points per pass. */
    private static final int MAX_CHOICE_POINTS = 8;

    private static final ThreadLocal<MatchAttempt> CURRENT = new ThreadLocal<>();

    private final int[] digits = new int[MAX_CHOICE_POINTS];

    private final int[] options = new int[MAX_CHOICE_POINTS];

    private int cursor;

    private MatchAttempt() {
    }

    /**
     * Starts an attempt on this thread, or returns null when an enclosing match is
     * already running (a NESTED subquery check rides that attempt and must not start
     * retries of its own).
     */
    public static MatchAttempt begin() {
        if (CURRENT.get() != null) {
            return null;
        }
        MatchAttempt attempt = new MatchAttempt();
        CURRENT.set(attempt);
        return attempt;
    }

    /** Ends the attempt of this thread. */
    public void end() {
        CURRENT.remove();
    }

    /**
     * Rotation offset for one unordered choice with count options: 0 keeps the
     * natural order, larger values start the search at a later candidate.
     */
    public static int offset(int count) {
        MatchAttempt attempt = CURRENT.get();
        if (attempt == null || count <= 1) {
            return 0;
        }
        return attempt.nextOffset(count) % count;
    }

    /** Resets the per-pass cursor; the digits (the current combination) survive. */
    public void resetPass() {
        cursor = 0;
    }

    private int nextOffset(int count) {
        if (cursor >= MAX_CHOICE_POINTS) {
            return 0;
        }
        int index = cursor++;
        options[index] = count;
        if (digits[index] >= count) {
            // stale from a previous pass that recorded MORE options here: normalize
            digits[index] = 0;
        }
        return digits[index];
    }

    /**
     * Advances to the next combination of the recorded choices.
     *
     * Returns: false when every recorded combination was already tried
     */
    public boolean advance() {
        for (int i = Math.min(cursor, MAX_CHOICE_POINTS) - 1; i >= 0; i--) {
            if (options[i] > 1 && digits[i] + 1 < options[i]) {
                digits[i]++;
                return true;
            }
            digits[i] = 0;
        }
        return false;
    }
}
