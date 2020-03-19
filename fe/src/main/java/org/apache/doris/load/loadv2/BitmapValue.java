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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 *  doris's own java version bitmap
 *  Keep compatibility with doris be's bitmap_value.h
 */
public class BitmapValue {

    public static final int EMPTY = 0;
    public static final int SINGLE32 = 1;
    public static final int BITMAP32 = 2;
    public static final int SINGLE64 = 3;
    public static final int BITMAP64 = 4;

    public static final int SINGLE_VALUE = 1;
    public static final int BITMAP_VALUE = 2;

    private int bitmapType;
    private long singleValue;
    private Roaring64Map bitmap;

    public BitmapValue() {
        bitmapType = EMPTY;
    }

    // different from as Roaring64Map.addInt
    // type cast here to make sure that positive and negative 32-bit integer 's high 32 bit limited to 0 or -1
    // so that it's easy to tell whether current bitmapValue only contains 32-bit integer
    public void add(int value){
        add((long)value);
    }

    public void add(long value) {
        switch (bitmapType) {
            case EMPTY:
                singleValue = value;
                bitmapType = SINGLE_VALUE;
                break;
            case SINGLE_VALUE:
                if (this.singleValue == value) {
                    break;
                } else {
                    bitmap = new Roaring64Map();
                    bitmap.add(value);
                    bitmap.add(singleValue);
                    bitmapType = BITMAP_VALUE;
                }
                break;
            case BITMAP_VALUE:
                bitmap.add(value);
                break;
        }
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
                boolean is32BitEnough = singleValue > Integer.MAX_VALUE;
                if (is32BitEnough) {
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
                singleValue = input.readInt();
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
        switch (bitmapType) {
            case EMPTY:
                break;
            case SINGLE_VALUE:
                switch (other.bitmapType) {
                    case EMPTY:
                        clear();
                        break;
                    case SINGLE_VALUE:
                        if (this.singleValue == other.singleValue) {
                            break;
                        }
                        clear();
                        break;
                    case BITMAP_VALUE:
                        if (other.bitmap.contains(singleValue)) {
                            break;
                        }
                        clear();
                        break;
                }
                break;
            case BITMAP_VALUE:
                switch (other.bitmapType) {
                    case EMPTY:
                        clear();
                        break;
                    case SINGLE_VALUE:
                        if (this.bitmap.contains(other.singleValue)) {
                            clear();
                            this.bitmapType = SINGLE_VALUE;
                            this.singleValue = other.singleValue;
                        } else {
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
        switch (bitmapType) {
            case EMPTY:
                switch (other.bitmapType) {
                    case EMPTY:
                        break;
                    case SINGLE_VALUE:
                        this.singleValue = other.singleValue;
                        this.bitmapType = SINGLE_VALUE;
                        break;
                    case BITMAP_VALUE:
                        this.bitmap = other.bitmap;
                        this.bitmapType = BITMAP_VALUE;
                        break;
                }
                break;
            case SINGLE_VALUE:
                switch (other.bitmapType) {
                    case EMPTY:
                        break;
                    case SINGLE_VALUE:
                        long thisSingleValue = this.singleValue;
                        clear();
                        this.bitmap = new Roaring64Map();
                        add(thisSingleValue);
                        add(other.singleValue);
                        break;
                    case BITMAP_VALUE:
                        other.bitmap.add(this.singleValue);
                        this.bitmap = other.bitmap;
                        this.bitmapType = BITMAP_VALUE;
                        break;
                }
                break;
            case BITMAP_VALUE:
                switch (other.bitmapType) {
                    case EMPTY:
                        break;
                    case SINGLE_VALUE:
                        this.bitmap.add(other.singleValue);
                        break;
                    case BITMAP_VALUE:
                        this.bitmap.or(other.bitmap);
                        this.bitmapType = BITMAP_VALUE;
                        break;
                }
                break;
        }
    }

    public boolean equals(BitmapValue other) {
        boolean ret = false;
        switch (bitmapType) {
            case EMPTY:
                switch (other.bitmapType) {
                    case EMPTY:
                        ret = true;
                        break;
                    case SINGLE_VALUE:
                    case BITMAP_VALUE:
                        ret = false;
                        break;
                }
                break;
            case SINGLE_VALUE:
                switch (other.bitmapType) {
                    case EMPTY:
                    case BITMAP_VALUE:
                        break;
                    case SINGLE_VALUE:
                        ret = this.singleValue == other.singleValue;
                        break;
                }
                break;
            case BITMAP_VALUE:
                switch (other.bitmapType) {
                    case EMPTY:
                    case SINGLE_VALUE:
                        ret = false;
                        break;
                    case BITMAP_VALUE:
                        ret = bitmap.equals(other.bitmap);
                        break;
                }
                break;
        }
        return ret;
    }

    public long getSizeInBytes() {
        long size = 0;
        switch (bitmapType) {
            case EMPTY:
                size = 1;
                break;
            case SINGLE_VALUE:
                if (singleValue <= Integer.MAX_VALUE) {
                    size = 4;
                } else {
                    size = 8;
                }
                break;
            case BITMAP_VALUE:
                size = bitmap.getSizeInBytes();
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
        this.bitmap = null;
        this.singleValue = 0;
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


    // just for ut
    public int getBitmapType() {
        return bitmapType;
    }

    // just for ut
    public boolean is32BitsEnough() {
        return this.bitmap.is32BitsEnough();
    }


}
