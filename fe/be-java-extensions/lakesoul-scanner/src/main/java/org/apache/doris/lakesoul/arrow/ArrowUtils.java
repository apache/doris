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

package org.apache.doris.lakesoul.arrow;

import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.utils.TypeNativeBytes;

import com.lakesoul.shaded.org.apache.arrow.memory.ArrowBuf;
import com.lakesoul.shaded.org.apache.arrow.util.Preconditions;
import com.lakesoul.shaded.org.apache.arrow.vector.types.pojo.ArrowType;
import com.lakesoul.shaded.org.apache.arrow.vector.types.pojo.Field;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

public class ArrowUtils {
    public static long reloadTimeStampSecVectorBuffer(final ArrowBuf sourceDataBuffer,
                                                      final int valueCount) {
        long address = OffHeap.allocateMemory((long) valueCount << 3 + 1);
        long offset = 0;
        for (int sourceIdx = 0; sourceIdx < valueCount; sourceIdx++) {
            long epochSec = sourceDataBuffer.getLong((long) sourceIdx << 3);
            LocalDateTime v = LocalDateTime.ofEpochSecond(epochSec, 0, ZoneOffset.UTC);
            OffHeap.putLong(null, address + offset,
                    TypeNativeBytes.convertToDateTimeV2(v.getYear(), v.getMonthValue(), v.getDayOfMonth(), v.getHour(),
                            v.getMinute(), v.getSecond(), v.getNano() / 1000));
            offset += 8;

        }
        return address;
    }

    public static long reloadTimeStampMilliVectorBuffer(final ArrowBuf sourceDataBuffer,
                                                        final int valueCount) {
        long address = OffHeap.allocateMemory((long) valueCount << 3 + 1);
        long offset = 0;
        for (int sourceIdx = 0; sourceIdx < valueCount; sourceIdx++) {
            long sourceData = sourceDataBuffer.getLong((long) sourceIdx << 3);
            long epochSec = sourceData / 1000;
            long nanoSec = sourceData % 1000 * 1000000;
            LocalDateTime v = LocalDateTime.ofEpochSecond(epochSec, (int) nanoSec, ZoneOffset.UTC);
            OffHeap.putLong(null, address + offset,
                    TypeNativeBytes.convertToDateTimeV2(v.getYear(), v.getMonthValue(), v.getDayOfMonth(), v.getHour(),
                            v.getMinute(), v.getSecond(), v.getNano() / 1000));
            offset += 8;

        }
        return address;
    }

    public static long reloadTimeStampMicroVectorBuffer(final ArrowBuf sourceDataBuffer,
                                                        final int valueCount) {
        long address = OffHeap.allocateMemory((long) valueCount << 3 + 1);
        long offset = 0;
        for (int sourceIdx = 0; sourceIdx < valueCount; sourceIdx++) {
            long sourceData = sourceDataBuffer.getLong((long) sourceIdx << 3);
            long epochSec = sourceData / 1000000;
            long nanoSec = sourceData % 1000000 * 1000;
            LocalDateTime v = LocalDateTime.ofEpochSecond(epochSec, (int) nanoSec, ZoneOffset.UTC);
            OffHeap.putLong(null, address + offset,
                    TypeNativeBytes.convertToDateTimeV2(v.getYear(), v.getMonthValue(), v.getDayOfMonth(), v.getHour(),
                            v.getMinute(), v.getSecond(), v.getNano() / 1000));
            offset += 8;

        }
        return address;
    }

    public static long reloadTimeStampNanoVectorBuffer(final ArrowBuf sourceDataBuffer,
                                                       final int valueCount) {
        long address = OffHeap.allocateMemory((long) valueCount << 3 + 1);
        long offset = 0;
        for (int sourceIdx = 0; sourceIdx < valueCount; sourceIdx++) {
            long sourceData = sourceDataBuffer.getLong((long) sourceIdx << 3);
            long epochSec = sourceData / 1000000000;
            long nanoSec = sourceData % 1000000000;
            LocalDateTime v = LocalDateTime.ofEpochSecond(epochSec, (int) nanoSec, ZoneOffset.UTC);
            OffHeap.putLong(null, address + offset,
                    TypeNativeBytes.convertToDateTimeV2(v.getYear(), v.getMonthValue(), v.getDayOfMonth(), v.getHour(),
                            v.getMinute(), v.getSecond(), v.getNano() / 1000));
            offset += 8;

        }
        return address;
    }

    public static long reloadDecimal128Buffer(final ArrowBuf sourceDataBuffer,
                                              final int valueCount) {
        long address = OffHeap.allocateMemory((long) valueCount << 3 + 1);
        long offset = 0;
        for (int sourceIdx = 0; sourceIdx < valueCount; sourceIdx++) {
            long sourceData = sourceDataBuffer.getLong((long) sourceIdx << 4);
            OffHeap.putLong(null, address + offset, sourceData);
            offset += 8;

        }
        return address;
    }

    public static long reloadDateDayVectorBuffer(final ArrowBuf sourceDataBuffer,
                                                 final int valueCount) {
        long address = OffHeap.allocateMemory((long) valueCount << 2 + 1);
        long offset = 0;
        for (int sourceIdx = 0; sourceIdx < valueCount; sourceIdx++) {
            int sourceData = sourceDataBuffer.getInt((long) sourceIdx << 2);

            LocalDate v = LocalDate.ofEpochDay(sourceData);
            OffHeap.putInt(null, address + offset,
                    TypeNativeBytes.convertToDateV2(v.getYear(), v.getMonthValue(), v.getDayOfMonth()));
            offset += 4;

        }
        return address;
    }


    public static long reloadBitVectorBuffer(final ArrowBuf sourceDataBuffer,
                                             final int valueCount) {
        long address = OffHeap.allocateMemory(valueCount + 1);
        long offset = 0;
        for (int newIdx = 0, sourceIdx = 0; newIdx < valueCount; newIdx += 8, sourceIdx++) {
            byte sourceByte = sourceDataBuffer.getByte(sourceIdx);
            for (int i = 0; i < 8; i++) {
                OffHeap.putByte(null, address + offset, (byte) (sourceByte & 1));
                sourceByte >>= 1;
                offset++;
                if (offset == valueCount) {
                    break;
                }
            }
        }
        return address;
    }

    public static long loadValidityBuffer(final ArrowBuf sourceValidityBuffer,
                                          final int valueCount,
                                          final boolean nullable) {
        long address = OffHeap.allocateMemory(valueCount + 1);
        if (nullable) {
            long offset = 0;
            for (int newIdx = 0, sourceIdx = 0; newIdx < valueCount; newIdx += 8, sourceIdx++) {
                byte sourceByte = sourceValidityBuffer.getByte(sourceIdx);
                for (int i = 0; i < 8; i++) {
                    OffHeap.putBoolean(null, address + offset, (sourceByte & 1) == 0);
                    sourceByte >>= 1;
                    offset++;
                    if (offset == valueCount) {
                        break;
                    }
                }
            }
        } else {
            OffHeap.setMemory(address, (byte) 1, valueCount);
        }
        return address;
    }

    public static long loadComplexTypeOffsetBuffer(final ArrowBuf sourceOffsetBuffer,
                                                   final int valueCount) {
        int length = valueCount << 4;
        long address = OffHeap.allocateMemory(length);
        long offset = 0;
        for (int sourceIdx = 1; sourceIdx <= valueCount; sourceIdx++) {

            int sourceInt = sourceOffsetBuffer.getInt((long) sourceIdx << 2);
            OffHeap.putLong(null, address + offset, sourceInt);
            offset += 8;

        }
        return address;
    }

    public static String hiveTypeFromArrowField(Field field) {
        StringBuilder hiveType = new StringBuilder(field.getType().accept(ArrowTypeToHiveTypeConverter.INSTANCE));
        List<Field> children = field.getChildren();
        switch (hiveType.toString()) {
            case "array":
                Preconditions.checkArgument(children.size() == 1,
                        "Lists have one child Field. Found: %s", children.isEmpty() ? "none" : children);
                hiveType.append("<").append(hiveTypeFromArrowField(children.get(0))).append(">");
                break;
            case "struct":
                hiveType.append("<");
                boolean first = true;
                for (Field child : children) {
                    if (!first) {
                        hiveType.append(",");
                    } else {
                        first = false;
                    }
                    hiveType.append(child.getName()).append(":").append(hiveTypeFromArrowField(child));
                }
                hiveType.append(">");
                break;
            default:
                break;
        }
        return hiveType.toString();
    }

    private static class ArrowTypeToHiveTypeConverter
            implements ArrowType.ArrowTypeVisitor<String> {

        private static final ArrowTypeToHiveTypeConverter INSTANCE =
                new ArrowTypeToHiveTypeConverter();

        @Override
        public String visit(ArrowType.Null type) {
            return "unsupported";
        }

        @Override
        public String visit(ArrowType.Struct type) {
            return "struct";
        }

        @Override
        public String visit(ArrowType.List type) {
            return "array";
        }

        @Override
        public String visit(ArrowType.LargeList type) {
            return "array";
        }

        @Override
        public String visit(ArrowType.FixedSizeList type) {
            return "array";
        }

        @Override
        public String visit(ArrowType.Union type) {
            return "unsupported";
        }

        @Override
        public String visit(ArrowType.Map type) {
            return "map";
        }

        @Override
        public String visit(ArrowType.Int type) {
            int bitWidth = type.getBitWidth();
            if (bitWidth <= 8) {
                return "tinyint";
            }
            if (bitWidth <= 2 * 8) {
                return "smallint";
            }
            if (bitWidth <= 4 * 8) {
                return "int";
            }
            return "bigint";
        }

        @Override
        public String visit(ArrowType.FloatingPoint type) {
            switch (type.getPrecision()) {
                case HALF:
                case SINGLE:
                    return "float";
                case DOUBLE:
                    return "double";
                default:
                    break;
            }
            return "double";
        }

        @Override
        public String visit(ArrowType.Utf8 type) {
            return "string";
        }

        @Override
        public String visit(ArrowType.LargeUtf8 type) {
            return "unsupported";
        }

        @Override
        public String visit(ArrowType.Binary type) {
            return "binary";
        }

        @Override
        public String visit(ArrowType.LargeBinary type) {
            return "binary";
        }

        @Override
        public String visit(ArrowType.FixedSizeBinary type) {
            return "binary";
        }

        @Override
        public String visit(ArrowType.Bool type) {
            return "boolean";
        }

        @Override
        public String visit(ArrowType.Decimal type) {
            return String.format("decimal64(%d, %d)", type.getPrecision(), type.getScale());
        }

        @Override
        public String visit(ArrowType.Date type) {
            return "datev2";
        }

        @Override
        public String visit(ArrowType.Time type) {
            return "datetimev2";
        }

        @Override
        public String visit(ArrowType.Timestamp type) {
            int precision = 0;
            switch (type.getUnit()) {
                case MILLISECOND:
                    precision = 3;
                    break;
                case MICROSECOND:
                    precision = 6;
                    break;
                case NANOSECOND:
                    precision = 9;
                    break;
                default:
                    break;
            }
            return String.format("timestamp(%d)", precision);
        }

        @Override
        public String visit(ArrowType.Interval type) {
            return "unsupported";
        }

        @Override
        public String visit(ArrowType.Duration type) {
            return "unsupported";
        }
    }


}
