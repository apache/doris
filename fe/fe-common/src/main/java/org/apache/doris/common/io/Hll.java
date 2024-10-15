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

package org.apache.doris.common.io;

import org.apache.commons.codec.binary.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

/**
 *  mainly used for Spark Load process to produce hll
 *  Try to keep consistent with be's C++ hll
 *  Whether method estimateCardinality can keep consistent with is a question to be studied
 *
 * use example:
 *  Hll hll = new hll();
 *  Hll.updateWithHash(value);
 *
 */

public class Hll {

    public static final byte HLL_DATA_EMPTY = 0;
    public static final byte HLL_DATA_EXPLICIT = 1;
    public static final byte HLL_DATA_SPARSE = 2;
    public static final byte HLL_DATA_FULL = 3;

    public static final int HLL_COLUMN_PRECISION = 14;
    public static final int HLL_ZERO_COUNT_BITS = (64 - HLL_COLUMN_PRECISION);
    public static final int HLL_EXPLICIT_INT64_NUM = 160;
    public static final int HLL_SPARSE_THRESHOLD = 4096;
    public static final int HLL_REGISTERS_COUNT = 16 * 1024;

    private int type;
    private Set<Long> hashSet;
    private byte[] registers;

    public Hll() {
        type = HLL_DATA_EMPTY;
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
        byte firstOneBit = (byte) (getLongTailZeroNum(hashValue) + 1);
        registers[idx] = registers[idx] > firstOneBit ? registers[idx] : firstOneBit;
    }

    private void mergeRegisters(byte[] other) {
        for (int i = 0; i < HLL_REGISTERS_COUNT; i++) {
            this.registers[i] = this.registers[i] > other[i] ? this.registers[i] : other[i];
        }
    }

    public static byte getLongTailZeroNum(long hashValue) {
        if (hashValue == 0) {
            return 0;
        }
        long value = 1L;
        byte idx = 0;
        for (;; idx++) {
            if ((value & hashValue) != 0) {
                return idx;
            }
            value = value << 1;
            if (idx == 62) {
                break;
            }
        }
        return idx;
    }

    public void updateWithHash(Object value) {
        byte[] v = StringUtils.getBytesUtf8(String.valueOf(value));
        update(hash64(v, v.length, SEED));
    }

    public void update(long hashValue) {
        switch (this.type) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case HLL_DATA_EMPTY:
                hashSet.add(hashValue);
                type = HLL_DATA_EXPLICIT;
                break;
            case HLL_DATA_EXPLICIT:
                if (hashSet.size() < HLL_EXPLICIT_INT64_NUM) {
                    hashSet.add(hashValue);
                    break;
                }
                convertExplicitToRegister();
                type = HLL_DATA_FULL;
            case HLL_DATA_SPARSE: // CHECKSTYLE IGNORE THIS LINE: fall through
            case HLL_DATA_FULL:
                updateRegisters(hashValue);
                break;
        }
    }

    public void merge(Hll other) {
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
                    case HLL_DATA_SPARSE:
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
                        if (this.hashSet.size() > HLL_EXPLICIT_INT64_NUM) {
                            convertExplicitToRegister();
                            this.type = HLL_DATA_FULL;
                        }
                        break;
                    case HLL_DATA_SPARSE:
                    case HLL_DATA_FULL:
                        convertExplicitToRegister();
                        mergeRegisters(other.registers);
                        this.type = HLL_DATA_FULL;
                        break;
                }
                break;
            case HLL_DATA_SPARSE:
            case HLL_DATA_FULL:
                switch (other.type) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                    case HLL_DATA_EXPLICIT:
                        for (long value : other.hashSet) {
                            update(value);
                        }
                        break;
                    case HLL_DATA_SPARSE:
                    case HLL_DATA_FULL:
                        mergeRegisters(other.registers);
                        break;
                }
                break;
        }
    }

    public void serialize(DataOutput output) throws IOException {
        switch (type) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case HLL_DATA_EMPTY:
                output.writeByte(type);
                break;
            case HLL_DATA_EXPLICIT:
                output.writeByte(type);
                output.writeByte(hashSet.size());
                for (long value : hashSet) {
                    output.writeLong(Long.reverseBytes(value));
                }
                break;
            case HLL_DATA_SPARSE:
            case HLL_DATA_FULL:
                int nonZeroRegisterNum = 0;
                for (int i = 0; i < HLL_REGISTERS_COUNT; i++) {
                    if (registers[i] != 0) {
                        nonZeroRegisterNum++;
                    }
                }
                if (nonZeroRegisterNum > HLL_SPARSE_THRESHOLD) {
                    output.writeByte(HLL_DATA_FULL);
                    for (byte value : registers) {
                        output.writeByte(value);
                    }
                } else {
                    output.writeByte(HLL_DATA_SPARSE);
                    output.writeInt(Integer.reverseBytes(nonZeroRegisterNum));
                    for (int i = 0; i < HLL_REGISTERS_COUNT; i++) {
                        if (registers[i] != 0) {
                            output.writeShort(Short.reverseBytes((short) i));
                            output.writeByte(registers[i]);
                        }
                    }
                }
                break;
        }
    }

    public boolean deserialize(DataInput input) throws IOException {
        assert type == HLL_DATA_EMPTY;

        if (input == null) {
            return false;
        }

        this.type = input.readByte();
        switch (this.type) {
            case HLL_DATA_EMPTY:
                break;
            case HLL_DATA_EXPLICIT:
                int hashSetSize = input.readUnsignedByte();
                for (int i = 0; i < hashSetSize; i++) {
                    update(Long.reverseBytes(input.readLong()));
                }
                assert this.type == HLL_DATA_EXPLICIT;
                break;
            case HLL_DATA_SPARSE:
                int sparseDataSize = Integer.reverseBytes(input.readInt());
                this.registers = new byte[HLL_REGISTERS_COUNT];
                for (int i = 0; i < sparseDataSize; i++) {
                    int idx = Short.reverseBytes(input.readShort());
                    byte value = input.readByte();
                    registers[idx] = value;
                }
                break;
            case HLL_DATA_FULL:
                this.registers = new byte[HLL_REGISTERS_COUNT];
                for (int i = 0; i < HLL_REGISTERS_COUNT; i++) {
                    registers[i] = input.readByte();
                }
                break;
            default:
                return false;
        }

        return true;
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
        float alpha = 0;

        if (numStreams == 16) {
            alpha = 0.673f;
        } else if (numStreams == 32) {
            alpha = 0.697f;
        } else if (numStreams == 64) {
            alpha = 0.709f;
        } else {
            alpha = 0.7213f / (1 + 1.079f / numStreams);
        }

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
        } else if (numStreams == 16384 && estimate < 72000) {
            double bias = 5.9119 * 1.0e-18 * (estimate * estimate * estimate * estimate)
                    - 1.4253 * 1.0e-12 * (estimate * estimate * estimate)
                    + 1.2940 * 1.0e-7 * (estimate * estimate)
                    - 5.2921 * 1.0e-3 * estimate
                    + 83.3216;
            estimate -= estimate * (bias / 100);
        }

        return (long) (estimate + 0.5);
    }

    public int maxSerializedSize() {
        switch (type) {
            case HLL_DATA_EMPTY:
            default:
                return 1;
            case HLL_DATA_EXPLICIT:
                return 2 + hashSet.size() * 8;
            case HLL_DATA_SPARSE:
            case HLL_DATA_FULL:
                return 1 + HLL_REGISTERS_COUNT;
        }
    }

    public static final long M64 = 0xc6a4a7935bd1e995L;
    public static final int R64 = 47;
    public static final int SEED = 0xadc83b19;


    private static long getLittleEndianLong(final byte[] data, final int index) {
        return (((long) data[index    ] & 0xff))
                | (((long) data[index + 1] & 0xff) <<  8)
                | (((long) data[index + 2] & 0xff) << 16)
                | (((long) data[index + 3] & 0xff) << 24)
                | (((long) data[index + 4] & 0xff) << 32)
                | (((long) data[index + 5] & 0xff) << 40)
                | (((long) data[index + 6] & 0xff) << 48)
                | (((long) data[index + 7] & 0xff) << 56);
    }

    public static long hash64(final byte[] data, final int length, final int seed) {
        long h = (seed & 0xffffffffL) ^ (length * M64);
        final int nblocks = length >> 3;

        // body
        for (int i = 0; i < nblocks; i++) {
            final int index = (i << 3);
            long k = getLittleEndianLong(data, index);

            k *= M64;
            k ^= k >>> R64;
            k *= M64;

            h ^= k;
            h *= M64;
        }

        final int index = (nblocks << 3);
        switch (length - index) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case 7:
                h ^= ((long) data[index + 6] & 0xff) << 48;
            case 6: // CHECKSTYLE IGNORE THIS LINE: fall through
                h ^= ((long) data[index + 5] & 0xff) << 40;
            case 5: // CHECKSTYLE IGNORE THIS LINE: fall through
                h ^= ((long) data[index + 4] & 0xff) << 32;
            case 4: // CHECKSTYLE IGNORE THIS LINE: fall through
                h ^= ((long) data[index + 3] & 0xff) << 24;
            case 3: // CHECKSTYLE IGNORE THIS LINE: fall through
                h ^= ((long) data[index + 2] & 0xff) << 16;
            case 2: // CHECKSTYLE IGNORE THIS LINE: fall through
                h ^= ((long) data[index + 1] & 0xff) << 8;
            case 1: // CHECKSTYLE IGNORE THIS LINE: fall through
                h ^= ((long) data[index] & 0xff);
                h *= M64;
        }

        h ^= h >>> R64;
        h *= M64;
        h ^= h >>> R64;

        return h;
    }

    // just for ut
    public int getType() {
        return type;
    }

    // For convert to statistics used Hll128
    public byte[] getRegisters() {
        return registers;
    }

    // For convert to statistics used Hll128
    public Set<Long> getHashSet() {
        return hashSet;
    }
}
