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

package org.apache.doris.datasource.paimon;

import com.google.common.collect.ImmutableSet;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import java.util.Collections;
import java.util.Set;

/** Validation shared by catalog-scoped and relation-scoped Paimon reader tuning. */
public final class PaimonReaderOptions {
    public static final int MIN_READ_BATCH_SIZE = 1;
    public static final int MAX_READ_BATCH_SIZE = 65536;
    public static final long MIN_ASYNC_THRESHOLD_BYTES = 1024L * 1024L;
    public static final long MAX_ASYNC_THRESHOLD_BYTES = 1024L * 1024L * 1024L;

    // Only options which cannot select another table context or mutate write behavior are safe to
    // apply with Table.copy after Doris has bound the table schema.
    private static final Set<String> SUPPORTED_OPTIONS = ImmutableSet.of(
            CoreOptions.READ_BATCH_SIZE.key(),
            CoreOptions.FILE_READER_ASYNC_THRESHOLD.key());

    private PaimonReaderOptions() {
    }

    public static Set<String> supportedOptions() {
        return SUPPORTED_OPTIONS;
    }

    public static void validate(String key, String value) {
        if (!SUPPORTED_OPTIONS.contains(key)) {
            throw new IllegalArgumentException("Unsupported Paimon dynamic reader option '" + key
                    + "'. Supported options are " + SUPPORTED_OPTIONS);
        }

        if (CoreOptions.READ_BATCH_SIZE.key().equals(key)) {
            int batchSize = parse(key, value, CoreOptions.READ_BATCH_SIZE);
            // A zero batch can make Paimon's vectorized reader report success without
            // advancing input; the upper bound also prevents one relation from over-allocating.
            requireRange(key, batchSize, MIN_READ_BATCH_SIZE, MAX_READ_BATCH_SIZE);
        } else {
            MemorySize threshold = parse(key, value, CoreOptions.FILE_READER_ASYNC_THRESHOLD);
            // Bound the trigger on both sides so a query cannot fan out tiny async reads or
            // silently disable asynchronous reading with an effectively infinite threshold.
            requireRange(key, threshold.getBytes(),
                    MIN_ASYNC_THRESHOLD_BYTES, MAX_ASYNC_THRESHOLD_BYTES);
        }
    }

    private static <T> T parse(String key, String value, ConfigOption<T> option) {
        try {
            return new Options(Collections.singletonMap(key, value)).get(option);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value for Paimon option '" + key + "': "
                    + e.getMessage(), e);
        }
    }

    private static void requireRange(String key, long value, long minimum, long maximum) {
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException("Paimon option '" + key + "' must be between "
                    + minimum + " and " + maximum + ", but was " + value);
        }
    }
}
