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

package org.apache.doris.statistics.util;

import org.apache.doris.common.io.Hll;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

/**
 * This is an HLL implementation with 128 Buckets.
 * Mainly used for statistics partition ndv cache.
 * We can convert the org.apache.doris.common.io.Hll object with 16K buckets to Hll128 to reduce memory consumption.
 */
public class Hll128 {

    public static final byte HLL_DATA_EMPTY = 0;
    public static final byte HLL_DATA_EXPLICIT = 1;
    public static final byte HLL_DATA_FULL = 3;

    public static final int HLL_COLUMN_PRECISION = 7;
    public static final int HLL_ZERO_COUNT_BITS = (64 - HLL_COLUMN_PRECISION);
    public static final int HLL_EXPLICLIT_INT64_NUM = 160;
    public static final int HLL_REGISTERS_COUNT = 128;

    private int type;
    private Set<Long> hashSet;
    private byte[] registers;

    public Hll128() {
        type = Hll.HLL_DATA_EMPTY;
        this.hashSet = new HashSet<>();
    }

    private void convertExplicitToRegister() {
        assert this.type == HLL_DATA_EXPLICIT;
        registers = new byte[HLL_REGISTERS_COUNT];
        for (Long value : hashSet) {
            updateRegisters(value);
        }
        hashSet.clear();
    }

    private void updateRegisters(long hashValue) {
        int idx;
        // hash value less than zero means we get a unsigned long
        // so need to transfer to BigInter to mod
        if (hashValue < 0) {
            BigInteger unint64HashValue = new BigInteger(Long.toUnsignedString(hashValue));
            unint64HashValue = unint64HashValue.mod(new BigInteger(Long.toUnsignedString(HLL_REGISTERS_COUNT)));
            idx = unint64HashValue.intValue();
        } else {
            idx = (int) (hashValue % HLL_REGISTERS_COUNT);
        }

        hashValue >>>= HLL_COLUMN_PRECISION;
        hashValue |= (1L << HLL_ZERO_COUNT_BITS);
        byte firstOneBit = (byte) (Hll.getLongTailZeroNum(hashValue) + 1);
        registers[idx] = registers[idx] > firstOneBit ? registers[idx] : firstOneBit;
    }

    private void mergeRegisters(byte[] other) {
        for (int i = 0; i < HLL_REGISTERS_COUNT; i++) {
            this.registers[i] = this.registers[i] > other[i] ? this.registers[i] : other[i];
        }
    }

    public void update(long hashValue) {
        switch (this.type) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case HLL_DATA_EMPTY:
                hashSet.add(hashValue);
                type = HLL_DATA_EXPLICIT;
                break;
            case HLL_DATA_EXPLICIT:
                if (hashSet.size() < HLL_EXPLICLIT_INT64_NUM) {
                    hashSet.add(hashValue);
                    break;
                }
                convertExplicitToRegister();
                type = HLL_DATA_FULL;
            case HLL_DATA_FULL:  // CHECKSTYLE IGNORE THIS LINE: fall through
                updateRegisters(hashValue);
                break;
        }
    }

    public void merge(Hll128 other) {
        if (other.type == HLL_DATA_EMPTY) {
            return;
        }
        switch (this.type) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case HLL_DATA_EMPTY:
                this.type = other.type;
                switch (other.type) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                    case HLL_DATA_EXPLICIT:
                        this.hashSet.addAll(other.hashSet);
                        break;
                    case HLL_DATA_FULL:
                        this.registers = new byte[HLL_REGISTERS_COUNT];
                        System.arraycopy(other.registers, 0, this.registers, 0, HLL_REGISTERS_COUNT);
                        break;
                }
                break;
            case HLL_DATA_EXPLICIT:
                switch (other.type) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                    case HLL_DATA_EXPLICIT:
                        this.hashSet.addAll(other.hashSet);
                        if (this.hashSet.size() > HLL_EXPLICLIT_INT64_NUM) {
                            convertExplicitToRegister();
                            this.type = HLL_DATA_FULL;
                        }
                        break;
                    case HLL_DATA_FULL:
                        convertExplicitToRegister();
                        mergeRegisters(other.registers);
                        this.type = HLL_DATA_FULL;
                        break;
                }
                break;
            case HLL_DATA_FULL:
                switch (other.type) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                    case HLL_DATA_EXPLICIT:
                        for (long value : other.hashSet) {
                            update(value);
                        }
                        break;
                    case HLL_DATA_FULL:
                        mergeRegisters(other.registers);
                        break;
                }
                break;
        }
    }

    // use strictfp to force java follow IEEE 754 to deal float point strictly
    public strictfp long estimateCardinality() {
        if (type == HLL_DATA_EMPTY) {
            return 0;
        }
        if (type == HLL_DATA_EXPLICIT) {
            return hashSet.size();
        }

        int numStreams = HLL_REGISTERS_COUNT;
        float alpha = 0.7213f / (1 + 1.079f / numStreams);
        float harmonicMean = 0;
        int numZeroRegisters = 0;

        for (int i = 0; i < HLL_REGISTERS_COUNT; i++) {
            harmonicMean += Math.pow(2.0f, -registers[i]);

            if (registers[i] == 0) {
                numZeroRegisters++;
            }
        }

        harmonicMean = 1.0f / harmonicMean;
        double estimate = alpha * numStreams * numStreams * harmonicMean;

        if (estimate <= numStreams * 2.5 && numZeroRegisters != 0) {
            estimate = numStreams * Math.log(((float) numStreams) / ((float) numZeroRegisters));
        }

        return (long) (estimate + 0.5);
    }

    public int getType() {
        return type;
    }

    public static Hll128 fromHll(Hll hll) {
        Hll128 hll128 = new Hll128();
        if (hll == null || hll.getType() == Hll.HLL_DATA_EMPTY) {
            return hll128;
        }
        if (hll.getType() == Hll.HLL_DATA_EXPLICIT) {
            hll128.type = HLL_DATA_EXPLICIT;
            hll128.hashSet.addAll(hll.getHashSet());
            return hll128;
        }

        byte[] registers = hll.getRegisters();
        byte[] registers128 = new byte[HLL_REGISTERS_COUNT];
        int groupSize = Hll.HLL_REGISTERS_COUNT / HLL_REGISTERS_COUNT;
        for (int i = 0; i < HLL_REGISTERS_COUNT; i++) {
            for (int j = 0; j < groupSize; j++) {
                registers128[i] = registers128[i] < registers[i * groupSize + j]
                    ? registers[i * groupSize + j]
                    : registers128[i];
            }
        }
        hll128.registers = registers128;
        hll128.type = HLL_DATA_FULL;
        return hll128;
    }

}

