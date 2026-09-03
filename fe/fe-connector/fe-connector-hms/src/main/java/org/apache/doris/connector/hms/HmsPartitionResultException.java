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

package org.apache.doris.connector.hms;

import java.util.ArrayList;
import java.util.List;

final class HmsPartitionResultException extends HmsClientException {

    private static final int MAX_SAMPLES_PER_TYPE = 10;
    private static final int MAX_SAMPLE_LENGTH = 256;

    private HmsPartitionResultException(Builder builder) {
        super("Invalid HMS partition result: requested=%d, returned=%d, "
                        + "missing=%d, duplicate=%d, unexpected=%d, invalid=%d, "
                        + "missingSamples=%s, duplicateSamples=%s, unexpectedSamples=%s, invalidSamples=%s",
                builder.requestedCount, builder.returnedCount,
                builder.missingCount, builder.duplicateCount, builder.unexpectedCount, builder.invalidCount,
                builder.missingSamples, builder.duplicateSamples,
                builder.unexpectedSamples, builder.invalidSamples);
    }

    static Builder builder(int requestedCount, int returnedCount) {
        return new Builder(requestedCount, returnedCount);
    }

    static final class Builder {
        private final int requestedCount;
        private final int returnedCount;
        private final List<String> missingSamples = new ArrayList<>();
        private final List<String> duplicateSamples = new ArrayList<>();
        private final List<String> unexpectedSamples = new ArrayList<>();
        private final List<String> invalidSamples = new ArrayList<>();
        private int missingCount;
        private int duplicateCount;
        private int unexpectedCount;
        private int invalidCount;

        private Builder(int requestedCount, int returnedCount) {
            this.requestedCount = requestedCount;
            this.returnedCount = returnedCount;
        }

        Builder missing(String sample) {
            missingCount++;
            addSample(missingSamples, sample);
            return this;
        }

        Builder duplicate(String sample) {
            duplicateCount++;
            addSample(duplicateSamples, sample);
            return this;
        }

        Builder unexpected(String sample) {
            unexpectedCount++;
            addSample(unexpectedSamples, sample);
            return this;
        }

        Builder invalid(String sample) {
            invalidCount++;
            addSample(invalidSamples, sample);
            return this;
        }

        boolean hasMismatches() {
            return missingCount + duplicateCount + unexpectedCount + invalidCount > 0;
        }

        HmsPartitionResultException build() {
            return new HmsPartitionResultException(this);
        }

        private static void addSample(List<String> samples, String sample) {
            if (samples.size() < MAX_SAMPLES_PER_TYPE) {
                samples.add(sample.length() <= MAX_SAMPLE_LENGTH
                        ? sample : sample.substring(0, MAX_SAMPLE_LENGTH - 3) + "...");
            }
        }
    }
}
