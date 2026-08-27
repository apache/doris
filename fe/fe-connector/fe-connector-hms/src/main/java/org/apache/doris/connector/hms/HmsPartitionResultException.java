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
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/** Strict result-integrity failure for an HMS getPartitionsByNames batch. */
public class HmsPartitionResultException extends HmsClientException {

    private static final int MAX_SAMPLES_PER_TYPE = 10;
    private static final int MAX_SAMPLE_LENGTH = 256;

    public enum MismatchType {
        MISSING_RESULT,
        DUPLICATE_RESULT,
        UNEXPECTED_RESULT,
        INVALID_RESULT
    }

    private final Set<MismatchType> mismatchTypes;
    private final int requestedCount;
    private final int returnedCount;
    private final int missingCount;
    private final int duplicateCount;
    private final int unexpectedCount;
    private final int invalidCount;
    private final List<String> missingSamples;
    private final List<String> duplicateSamples;
    private final List<String> unexpectedSamples;
    private final List<String> invalidSamples;

    private HmsPartitionResultException(Builder builder) {
        super("Invalid HMS partition result: mismatches=%s, requested=%d, returned=%d, "
                        + "missing=%d, duplicate=%d, unexpected=%d, invalid=%d, "
                        + "missingSamples=%s, duplicateSamples=%s, unexpectedSamples=%s, invalidSamples=%s",
                builder.mismatchTypes, builder.requestedCount, builder.returnedCount,
                builder.missingCount, builder.duplicateCount, builder.unexpectedCount, builder.invalidCount,
                builder.missingSamples, builder.duplicateSamples,
                builder.unexpectedSamples, builder.invalidSamples);
        this.mismatchTypes = Collections.unmodifiableSet(EnumSet.copyOf(builder.mismatchTypes));
        this.requestedCount = builder.requestedCount;
        this.returnedCount = builder.returnedCount;
        this.missingCount = builder.missingCount;
        this.duplicateCount = builder.duplicateCount;
        this.unexpectedCount = builder.unexpectedCount;
        this.invalidCount = builder.invalidCount;
        this.missingSamples = immutableCopy(builder.missingSamples);
        this.duplicateSamples = immutableCopy(builder.duplicateSamples);
        this.unexpectedSamples = immutableCopy(builder.unexpectedSamples);
        this.invalidSamples = immutableCopy(builder.invalidSamples);
    }

    static Builder builder(int requestedCount, int returnedCount) {
        return new Builder(requestedCount, returnedCount);
    }

    public Set<MismatchType> getMismatchTypes() {
        return mismatchTypes;
    }

    public int getRequestedCount() {
        return requestedCount;
    }

    public int getReturnedCount() {
        return returnedCount;
    }

    public int getMissingCount() {
        return missingCount;
    }

    /** Returns the number of distinct response identities that appeared more than once. */
    public int getDuplicateCount() {
        return duplicateCount;
    }

    /** Returns the number of distinct response identities absent from the request. */
    public int getUnexpectedCount() {
        return unexpectedCount;
    }

    /** Returns the number of response objects that could not form a valid identity. */
    public int getInvalidCount() {
        return invalidCount;
    }

    public List<String> getMissingSamples() {
        return missingSamples;
    }

    public List<String> getDuplicateSamples() {
        return duplicateSamples;
    }

    public List<String> getUnexpectedSamples() {
        return unexpectedSamples;
    }

    public List<String> getInvalidSamples() {
        return invalidSamples;
    }

    private static List<String> immutableCopy(List<String> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    static final class Builder {
        private final int requestedCount;
        private final int returnedCount;
        private final EnumSet<MismatchType> mismatchTypes = EnumSet.noneOf(MismatchType.class);
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
            mismatchTypes.add(MismatchType.MISSING_RESULT);
            missingCount++;
            addSample(missingSamples, sample);
            return this;
        }

        Builder duplicate(String sample) {
            mismatchTypes.add(MismatchType.DUPLICATE_RESULT);
            duplicateCount++;
            addSample(duplicateSamples, sample);
            return this;
        }

        Builder unexpected(String sample) {
            mismatchTypes.add(MismatchType.UNEXPECTED_RESULT);
            unexpectedCount++;
            addSample(unexpectedSamples, sample);
            return this;
        }

        Builder invalid(String sample) {
            mismatchTypes.add(MismatchType.INVALID_RESULT);
            invalidCount++;
            addSample(invalidSamples, sample);
            return this;
        }

        boolean hasMismatches() {
            return !mismatchTypes.isEmpty();
        }

        HmsPartitionResultException build() {
            if (mismatchTypes.isEmpty()) {
                throw new IllegalStateException("HMS partition result exception requires a mismatch");
            }
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
