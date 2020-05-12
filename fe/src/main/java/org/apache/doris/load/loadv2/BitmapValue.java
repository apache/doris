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

package org.apache.doris.load.loadv2;

import org.roaringbitmap.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 *  doris's own java version bitmap
 *  try to keep compatibility with doris be's bitmap_value.h,but still has some difference from bitmap_value.h
 *  major difference from be:
 *      1. java bitmap support integer range [0, Long.MAX],while be's bitmap support range [0, Long.MAX * 2]
 *          Now Long.MAX_VALUE is enough for doris's spark load and support unsigned integer in java need to pay more
 *      2. getSizeInBytes method is different from fe to be, details description see method comment
 */
public class BitmapValue {

    public static final int EMPTY = 0;
    public static final int SINGLE32 = 1;
    public static final int BITMAP32 = 2;
    public static final int SINGLE64 = 3;
    public static final int BITMAP64 = 4;

    public static final int SINGLE_VALUE = 1;
    public static final int BITMAP_VALUE = 2;

    public static final long UNSIGNED_32BIT_INT_MAX_VALUE = 4294967295L;

    private int bitmapType;
    private long singleValue;
    private Roaring64Map bitmap;

    public BitmapValue() {
        bitmapType = EMPTY;
    }

    public void add(int value) {
        add(Util.toUnsignedLong(value));
    }

    public void add(long value) {
        switch (bitmapType) {
            case EMPTY:
                singleValue = value;
                bitmapType = SINGLE_VALUE;
                break;
            case SINGLE_VALUE:
                if (this.singleValue != value) {
                    bitmap = new Roaring64Map();
                    bitmap.add(value);
                    bitmap.add(singleValue);
                    bitmapType = BITMAP_VALUE;
                }
                break;
            case BITMAP_VALUE:
                bitmap.addLong(value);
                break;
        }
    }

    public boolean contains(int value) {
        return contains(Util.toUnsignedLong(value));
    }

    public boolean contains(long value) {
        switch (bitmapType) {
            case EMPTY:
                return false;
            case SINGLE_VALUE:
                return singleValue == value;
            case BITMAP_VALUE:
                return bitmap.contains(value);
            default:
                return false;
        }
    }

    public long cardinality() {
        switch (bitmapType) {
            case EMPTY:
                return 0;
            case SINGLE_VALUE:
                return 1;
            case BITMAP_VALUE:
                return bitmap.getLongCardinality();
        }
        return 0;
    }

    public void serialize(DataOutput output) throws IOException {
        switch (bitmapType) {
            case EMPTY:
                output.writeByte(EMPTY);
                break;
            case SINGLE_VALUE:
                // is 32-bit enough
                if (isLongValue32bitEnough(singleValue)) {
                    output.write(SINGLE32);
                    output.writeInt((int)singleValue);
                } else {
                    output.writeByte(SINGLE64);
                    output.writeLong(singleValue);
                }
                break;
            case BITMAP_VALUE:
                bitmap.serialize(output);
                break;
        }
    }

    public void deserialize(DataInput input) throws IOException {
        clear();
        int bitmapType = input.readByte();
        switch (bitmapType) {
            case EMPTY:
                break;
            case SINGLE32:
                singleValue = Util.toUnsignedLong(input.readInt());
                this.bitmapType = SINGLE_VALUE;
                break;
            case SINGLE64:
                singleValue = input.readLong();
                this.bitmapType = SINGLE_VALUE;
                break;
            case BITMAP32:
            case BITMAP64:
                bitmap = bitmap == null ? new Roaring64Map() : bitmap;
                bitmap.deserialize(input, bitmapType);
                this.bitmapType = BITMAP_VALUE;
                break;
            default:
                throw new RuntimeException(String.format("unknown bitmap type %s ", bitmapType));
        }
    }

    // In-place bitwise AND (intersection) operation. The current bitmap is modified.
    public void and(BitmapValue other) {
        switch (other.bitmapType) {
            case EMPTY:
                clear();
                break;
            case SINGLE_VALUE:
                switch (this.bitmapType) {
                    case EMPTY:
                        break;
                    case SINGLE_VALUE:
                        if (this.singleValue != other.singleValue) {
                            clear();
                        }
                        break;
                    case BITMAP_VALUE:
                        if (!this.bitmap.contains(other.singleValue)) {
                            clear();
                        } else {
                            clear();
                            this.singleValue = other.singleValue;
                            this.bitmapType = SINGLE_VALUE;
                        }
                        break;
                }
                break;
            case BITMAP_VALUE:
                switch (this.bitmapType) {
                    case EMPTY:
                        break;
                    case SINGLE_VALUE:
                        if (!other.bitmap.contains(this.singleValue)) {
                            clear();
                        }
                        break;
                    case BITMAP_VALUE:
                        this.bitmap.and(other.bitmap);
                        convertToSmallerType();
                        break;
                }
                break;
        }
    }

    // In-place bitwise OR (union) operation. The current bitmap is modified.
    public void or(BitmapValue other) {
        switch (other.bitmapType) {
            case EMPTY:
                break;
            case SINGLE_VALUE:
                add(other.singleValue);
                break;
            case BITMAP_VALUE:
                switch (this.bitmapType) {
                    case EMPTY:
                        this.bitmap = other.bitmap;
                        this.bitmapType = BITMAP_VALUE;
                        break;
                    case SINGLE_VALUE:
                        this.bitmap = other.bitmap;
                        this.bitmap.add(this.singleValue);
                        this.bitmapType = BITMAP_VALUE;
                        break;
                    case BITMAP_VALUE:
                        this.bitmap.or(other.bitmap);
                        break;
                }
                break;
        }
    }

    public boolean equals(BitmapValue other) {
        boolean ret = false;
        if (this.bitmapType != other.bitmapType) {
            return false;
        }
        switch (other.bitmapType) {
            case EMPTY:
                ret = true;
                break;
            case SINGLE_VALUE:
                ret = this.singleValue == other.singleValue;
                break;
            case BITMAP_VALUE:
                ret = bitmap.equals(other.bitmap);
        }
        return ret;
    }

    /**
     *  usage note:
     *      now getSizeInBytes is different from be' impl
     *      The reason is that java's roaring didn't implement method #shrinkToFit but be's getSizeInBytes need it
     *      Implementing java's shrinkToFit means refactor roaring whose fields are all unaccess in Doris Fe's package
     *      That would be an another big project
     */
    // TODO(wb): keep getSizeInBytes consistent with be and refactor roaring
    public long getSizeInBytes() {
        long size = 0;
        switch (bitmapType) {
            case EMPTY:
                size = 1;
                break;
            case SINGLE_VALUE:
                if (isLongValue32bitEnough(singleValue)) {
                    size = 1 + 4;
                } else {
                    size = 1 + 8;
                }
                break;
            case BITMAP_VALUE:
                size = 1 + bitmap.getSizeInBytes();
        }
        return size;
    }

    @Override
    public String toString() {
        String toStringStr = "{}";
        switch (bitmapType) {
            case EMPTY:
                break;
            case SINGLE_VALUE:
                toStringStr = String.format("{%s}", singleValue);
                break;
            case BITMAP_VALUE:
                toStringStr = this.bitmap.toString();
                break;
        }
        return toStringStr;
    }

    public void clear() {
        this.bitmapType = EMPTY;
        this.singleValue = -1;
        this.bitmap = null;
    }

    private void convertToSmallerType() {
        if (bitmapType == BITMAP_VALUE) {
            if (bitmap.getLongCardinality() == 0) {
                this.bitmap = null;
                this.bitmapType = EMPTY;
            } else if (bitmap.getLongCardinality() == 1) {
                this.singleValue = bitmap.select(0);
                this.bitmapType = SINGLE_VALUE;
                this.bitmap = null;
            }
        }
    }

    private boolean isLongValue32bitEnough(long value) {
        return value <= UNSIGNED_32BIT_INT_MAX_VALUE;
    }

    // just for ut
    public int getBitmapType() {
        return bitmapType;
    }

    // just for ut
    public boolean is32BitsEnough() {
        switch (bitmapType) {
            case EMPTY:
                return true;
            case SINGLE_VALUE:
                return isLongValue32bitEnough(singleValue);
            case BITMAP_VALUE:
                return bitmap.is32BitsEnough();
            default:
                return false;
        }
    }


}
