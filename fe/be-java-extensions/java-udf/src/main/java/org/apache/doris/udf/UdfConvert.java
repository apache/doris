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

package org.apache.doris.udf;

import org.apache.doris.common.jni.utils.JNINativeMethod;
import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.utils.UdfUtils;

import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;

public class UdfConvert {
    private static final Logger LOG = Logger.getLogger(UdfConvert.class);

    public static Object[] convertBooleanArg(boolean isNullable, int rowsStart, int rowsEnd, long nullMapAddr,
            long columnAddr) {
        Boolean[] argument = new Boolean[rowsEnd - rowsStart];
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + i) == 0) {
                    argument[i - rowsStart] = UdfUtils.UNSAFE.getBoolean(null, columnAddr + i);
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                argument[i - rowsStart] = UdfUtils.UNSAFE.getBoolean(null, columnAddr + i);
            }
        }
        return argument;
    }

    public static Object[] convertTinyIntArg(boolean isNullable, int rowsStart, int rowsEnd, long nullMapAddr,
            long columnAddr) {
        Byte[] argument = new Byte[rowsEnd - rowsStart];
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + i) == 0) {
                    argument[i - rowsStart] = UdfUtils.UNSAFE.getByte(null, columnAddr + i);
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                argument[i - rowsStart] = UdfUtils.UNSAFE.getByte(null, columnAddr + i);
            }
        }
        return argument;
    }

    public static Object[] convertSmallIntArg(boolean isNullable, int rowsStart, int rowsEnd, long nullMapAddr,
            long columnAddr) {
        Short[] argument = new Short[rowsEnd - rowsStart];
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + i) == 0) {
                    argument[i - rowsStart] = UdfUtils.UNSAFE.getShort(null, columnAddr + (i * 2L));
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                argument[i - rowsStart] = UdfUtils.UNSAFE.getShort(null, columnAddr + (i * 2L));
            }
        }
        return argument;
    }

    public static Object[] convertIntArg(boolean isNullable, int rowsStart, int rowsEnd, long nullMapAddr,
            long columnAddr) {
        Integer[] argument = new Integer[rowsEnd - rowsStart];
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + i) == 0) {
                    argument[i - rowsStart] = UdfUtils.UNSAFE.getInt(null, columnAddr + (i * 4L));
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                argument[i - rowsStart] = UdfUtils.UNSAFE.getInt(null, columnAddr + (i * 4L));
            }
        }
        return argument;
    }

    public static Object[] convertBigIntArg(boolean isNullable, int rowsStart, int rowsEnd, long nullMapAddr,
            long columnAddr) {
        Long[] argument = new Long[rowsEnd - rowsStart];
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + i) == 0) {
                    argument[i - rowsStart] = UdfUtils.UNSAFE.getLong(null, columnAddr + (i * 8L));
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                argument[i - rowsStart] = UdfUtils.UNSAFE.getLong(null, columnAddr + (i * 8L));
            }
        }
        return argument;
    }

    public static Object[] convertFloatArg(boolean isNullable, int rowsStart, int rowsEnd, long nullMapAddr,
            long columnAddr) {
        Float[] argument = new Float[rowsEnd - rowsStart];
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + i) == 0) {
                    argument[i - rowsStart] = UdfUtils.UNSAFE.getFloat(null, columnAddr + (i * 4L));
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                argument[i - rowsStart] = UdfUtils.UNSAFE.getFloat(null, columnAddr + (i * 4L));
            }
        }
        return argument;
    }

    public static Object[] convertDoubleArg(boolean isNullable, int rowsStart, int rowsEnd, long nullMapAddr,
            long columnAddr) {
        Double[] argument = new Double[rowsEnd - rowsStart];
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + i) == 0) {
                    argument[i - rowsStart] = UdfUtils.UNSAFE.getDouble(null, columnAddr + (i * 8L));
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                argument[i - rowsStart] = UdfUtils.UNSAFE.getDouble(null, columnAddr + (i * 8L));
            }
        }
        return argument;
    }

    public static Object[] convertDateArg(Class argTypeClass, boolean isNullable, int rowsStart, int rowsEnd,
            long nullMapAddr, long columnAddr) {
        Object[] argument = (Object[]) Array.newInstance(argTypeClass, rowsEnd - rowsStart);
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + i) == 0) {
                    long value = UdfUtils.UNSAFE.getLong(null, columnAddr + (i * 8L));
                    argument[i - rowsStart] = UdfUtils.convertDateToJavaDate(value, argTypeClass);
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                long value = UdfUtils.UNSAFE.getLong(null, columnAddr + (i * 8L));
                argument[i - rowsStart] = UdfUtils.convertDateToJavaDate(value, argTypeClass);
            }
        }
        return argument;
    }

    public static Object[] convertDateTimeArg(Class argTypeClass, boolean isNullable, int rowsStart, int rowsEnd,
            long nullMapAddr, long columnAddr) {
        Object[] argument = (Object[]) Array.newInstance(argTypeClass, rowsEnd - rowsStart);
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + i) == 0) {
                    long value = UdfUtils.UNSAFE.getLong(null, columnAddr + (i * 8L));
                    argument[i - rowsStart] = UdfUtils
                            .convertDateTimeToJavaDateTime(value, argTypeClass);
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                long value = UdfUtils.UNSAFE.getLong(null, columnAddr + (i * 8L));
                argument[i - rowsStart] = UdfUtils.convertDateTimeToJavaDateTime(value, argTypeClass);
            }
        }
        return argument;
    }

    public static Object[] convertDateV2Arg(Class argTypeClass, boolean isNullable, int rowsStart, int rowsEnd,
            long nullMapAddr, long columnAddr) {
        Object[] argument = (Object[]) Array.newInstance(argTypeClass, rowsEnd - rowsStart);
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + i) == 0) {
                    int value = UdfUtils.UNSAFE.getInt(null, columnAddr + (i * 4L));
                    argument[i - rowsStart] = UdfUtils.convertDateV2ToJavaDate(value, argTypeClass);
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                int value = UdfUtils.UNSAFE.getInt(null, columnAddr + (i * 4L));
                argument[i - rowsStart] = UdfUtils.convertDateV2ToJavaDate(value, argTypeClass);
            }
        }
        return argument;
    }

    public static Object[] convertDateTimeV2Arg(Class argTypeClass, boolean isNullable, int rowsStart, int rowsEnd,
            long nullMapAddr, long columnAddr) {
        Object[] argument = (Object[]) Array.newInstance(argTypeClass, rowsEnd - rowsStart);
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(null, nullMapAddr + i) == 0) {
                    long value = UdfUtils.UNSAFE.getLong(columnAddr + (i * 8L));
                    argument[i - rowsStart] = UdfUtils
                            .convertDateTimeV2ToJavaDateTime(value, argTypeClass);
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                long value = UdfUtils.UNSAFE.getLong(null, columnAddr + (i * 8L));
                argument[i - rowsStart] = UdfUtils
                        .convertDateTimeV2ToJavaDateTime(value, argTypeClass);
            }
        }
        return argument;
    }

    public static Object[] convertLargeIntArg(boolean isNullable, int rowsStart, int rowsEnd, long nullMapAddr,
            long columnAddr) {
        BigInteger[] argument = new BigInteger[rowsEnd - rowsStart];
        byte[] bytes = new byte[16];
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + i) == 0) {
                    UdfUtils.copyMemory(null, columnAddr + (i * 16L), bytes, UdfUtils.BYTE_ARRAY_OFFSET, 16);
                    argument[i - rowsStart] = new BigInteger(UdfUtils.convertByteOrder(bytes));
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                UdfUtils.copyMemory(null, columnAddr + (i * 16L), bytes, UdfUtils.BYTE_ARRAY_OFFSET, 16);
                argument[i - rowsStart] = new BigInteger(UdfUtils.convertByteOrder(bytes));
            }
        }
        return argument;
    }

    public static Object[] convertDecimalArg(int scale, long typeLen, boolean isNullable, int rowsStart, int rowsEnd,
            long nullMapAddr, long columnAddr) {
        BigDecimal[] argument = new BigDecimal[rowsEnd - rowsStart];
        byte[] bytes = new byte[(int) typeLen];
        if (isNullable) {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + i) == 0) {
                    UdfUtils.copyMemory(null, columnAddr + (i * typeLen), bytes, UdfUtils.BYTE_ARRAY_OFFSET, typeLen);
                    BigInteger bigInteger = new BigInteger(UdfUtils.convertByteOrder(bytes));
                    argument[i - rowsStart] = new BigDecimal(bigInteger, scale); //show to pass scale info
                } // else is the current row is null
            }
        } else {
            for (int i = rowsStart; i < rowsEnd; ++i) {
                UdfUtils.copyMemory(null, columnAddr + (i * typeLen), bytes, UdfUtils.BYTE_ARRAY_OFFSET, typeLen);
                BigInteger bigInteger = new BigInteger(UdfUtils.convertByteOrder(bytes));
                argument[i - rowsStart] = new BigDecimal(bigInteger, scale);
            }
        }
        return argument;
    }

    public static Object[] convertStringArg(boolean isNullable, int rowsStart, int rowsEnd, long nullMapAddr,
            long charsAddr, long offsetsAddr) {
        String[] argument = new String[rowsEnd - rowsStart];
        Preconditions.checkState(UdfUtils.UNSAFE.getInt(null, offsetsAddr + 4L * (0 - 1)) == 0,
                "offsetsAddr[-1] should be 0;");
        final int totalLen = UdfUtils.UNSAFE.getInt(null, offsetsAddr + (rowsEnd - 1) * 4L);
        byte[] bytes = new byte[totalLen];
        UdfUtils.copyMemory(null, charsAddr, bytes, UdfUtils.BYTE_ARRAY_OFFSET, totalLen);
        if (isNullable) {
            for (int row = rowsStart; row < rowsEnd; ++row) {
                if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                    int prevOffset = UdfUtils.UNSAFE.getInt(null, offsetsAddr + 4L * (row - 1));
                    int currOffset = UdfUtils.UNSAFE.getInt(null, offsetsAddr + row * 4L);
                    argument[row - rowsStart] = new String(bytes, prevOffset, currOffset - prevOffset,
                            StandardCharsets.UTF_8);
                } // else is the current row is null
            }
        } else {
            for (int row = rowsStart; row < rowsEnd; ++row) {
                int prevOffset = UdfUtils.UNSAFE.getInt(null, offsetsAddr + 4L * (row - 1));
                int currOffset = UdfUtils.UNSAFE.getInt(null, offsetsAddr + 4L * row);
                argument[row - rowsStart] = new String(bytes, prevOffset, currOffset - prevOffset,
                        StandardCharsets.UTF_8);
            }
        }
        return argument;
    }

    /////////////////////////////////////////copyBatch//////////////////////////////////////////////////////////////
    public static void copyBatchBooleanResult(boolean isNullable, int numRows, Boolean[] result, long nullMapAddr,
            long resColumnAddr) {
        byte[] dataArr = new byte[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    dataArr[i] = result[i] ? (byte) 1 : 0;
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                dataArr[i] = result[i] ? (byte) 1 : 0;
            }
        }
        UdfUtils.copyMemory(dataArr, UdfUtils.BYTE_ARRAY_OFFSET, null, resColumnAddr, numRows);
    }

    public static void copyBatchTinyIntResult(boolean isNullable, int numRows, Byte[] result, long nullMapAddr,
            long resColumnAddr) {
        byte[] dataArr = new byte[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    dataArr[i] = result[i];
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                dataArr[i] = result[i];
            }
        }
        UdfUtils.copyMemory(dataArr, UdfUtils.BYTE_ARRAY_OFFSET, null, resColumnAddr, numRows);
    }

    public static void copyBatchSmallIntResult(boolean isNullable, int numRows, Short[] result, long nullMapAddr,
            long resColumnAddr) {
        short[] dataArr = new short[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    dataArr[i] = result[i];
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                dataArr[i] = result[i];
            }
        }
        UdfUtils.copyMemory(dataArr, OffHeap.SHORT_ARRAY_OFFSET, null, resColumnAddr, numRows * 2L);
    }

    public static void copyBatchIntResult(boolean isNullable, int numRows, Integer[] result, long nullMapAddr,
            long resColumnAddr) {
        int[] dataArr = new int[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    dataArr[i] = result[i];
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                dataArr[i] = result[i];
            }
        }
        UdfUtils.copyMemory(dataArr, UdfUtils.INT_ARRAY_OFFSET, null, resColumnAddr, numRows * 4L);
    }

    public static void copyBatchBigIntResult(boolean isNullable, int numRows, Long[] result, long nullMapAddr,
            long resColumnAddr) {
        long[] dataArr = new long[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    dataArr[i] = result[i];
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                dataArr[i] = result[i];
            }
        }
        UdfUtils.copyMemory(dataArr, OffHeap.LONG_ARRAY_OFFSET, null, resColumnAddr, numRows * 8L);
    }

    public static void copyBatchFloatResult(boolean isNullable, int numRows, Float[] result, long nullMapAddr,
            long resColumnAddr) {
        float[] dataArr = new float[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    dataArr[i] = result[i];
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                dataArr[i] = result[i];
            }
        }
        UdfUtils.copyMemory(dataArr, OffHeap.FLOAT_ARRAY_OFFSET, null, resColumnAddr, numRows * 4L);
    }

    public static void copyBatchDoubleResult(boolean isNullable, int numRows, Double[] result, long nullMapAddr,
            long resColumnAddr) {
        double[] dataArr = new double[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    dataArr[i] = result[i];
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                dataArr[i] = result[i];
            }
        }
        UdfUtils.copyMemory(dataArr, OffHeap.DOUBLE_ARRAY_OFFSET, null, resColumnAddr, numRows * 8L);
    }

    public static void copyBatchDateResult(Class retClass, boolean isNullable, int numRows, Object[] result,
            long nullMapAddr,
            long resColumnAddr) {
        long[] dataArr = new long[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    dataArr[i] = UdfUtils.convertToDate(result[i], retClass);
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                dataArr[i] = UdfUtils.convertToDate(result[i], retClass);
            }
        }
        UdfUtils.copyMemory(dataArr, OffHeap.LONG_ARRAY_OFFSET, null, resColumnAddr, numRows * 8L);
    }

    public static void copyBatchDateV2Result(Class retClass, boolean isNullable, int numRows, Object[] result,
            long nullMapAddr,
            long resColumnAddr) {
        int[] dataArr = new int[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    dataArr[i] = UdfUtils.convertToDateV2(result[i], retClass);
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                dataArr[i] = UdfUtils.convertToDateV2(result[i], retClass);
            }
        }
        UdfUtils.copyMemory(dataArr, OffHeap.INT_ARRAY_OFFSET, null, resColumnAddr, numRows * 4L);
    }

    public static void copyBatchDateTimeResult(Class retClass, boolean isNullable, int numRows, Object[] result,
            long nullMapAddr, long resColumnAddr) {
        long[] dataArr = new long[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    dataArr[i] = UdfUtils.convertToDateTime(result[i], retClass);
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                dataArr[i] = UdfUtils.convertToDateTime(result[i], retClass);
            }
        }
        UdfUtils.copyMemory(dataArr, OffHeap.LONG_ARRAY_OFFSET, null, resColumnAddr, numRows * 8L);
    }

    public static void copyBatchDateTimeV2Result(Class retClass, boolean isNullable, int numRows,
            Object[] result, long nullMapAddr, long resColumnAddr) {
        long[] dataArr = new long[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    dataArr[i] = UdfUtils.convertToDateTimeV2(result[i], retClass);
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                dataArr[i] = UdfUtils.convertToDateTimeV2(result[i], retClass);
            }
        }
        UdfUtils.copyMemory(dataArr, OffHeap.LONG_ARRAY_OFFSET, null, resColumnAddr, numRows * 8L);
    }

    public static void copyBatchLargeIntResult(boolean isNullable, int numRows, BigInteger[] result, long nullMapAddr,
            long resColumnAddr) {
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    byte[] bytes = UdfUtils.convertByteOrder(result[i].toByteArray());
                    byte[] value = new byte[16];
                    if (result[i].signum() == -1) {
                        Arrays.fill(value, (byte) -1);
                    }
                    System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
                    UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, resColumnAddr + (i * 16L), 16);
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                byte[] bytes = UdfUtils.convertByteOrder(result[i].toByteArray());
                byte[] value = new byte[16];
                if (result[i].signum() == -1) {
                    Arrays.fill(value, (byte) -1);
                }
                System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, resColumnAddr + (i * 16L), 16);
            }
        }
    }

    public static void copyBatchDecimal32Result(int scale, boolean isNullable, int numRows, BigDecimal[] result,
            long nullMapAddr,
            long columnAddr) {
        BigInteger[] data = new BigInteger[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    data[i] = result[i].setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                data[i] = result[i].setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
            }
        }
        copyBatchDecimalResult(4, isNullable, numRows, data, columnAddr);
    }

    public static void copyBatchDecimal64Result(int scale, boolean isNullable, int numRows, BigDecimal[] result,
            long nullMapAddr,
            long columnAddr) {
        BigInteger[] data = new BigInteger[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    data[i] = result[i].setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                data[i] = result[i].setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
            }
        }
        copyBatchDecimalResult(8, isNullable, numRows, data, columnAddr);
    }


    public static void copyBatchDecimal128Result(int scale, boolean isNullable, int numRows, BigDecimal[] result,
            long nullMapAddr,
            long columnAddr) {
        BigInteger[] data = new BigInteger[numRows];
        if (isNullable) {
            byte[] nulls = new byte[numRows];
            for (int i = 0; i < numRows; i++) {
                if (result[i] == null) {
                    nulls[i] = 1;
                } else {
                    data[i] = result[i].setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
                }
            }
            UdfUtils.copyMemory(nulls, UdfUtils.BYTE_ARRAY_OFFSET, null, nullMapAddr, numRows);
        } else {
            for (int i = 0; i < numRows; i++) {
                data[i] = result[i].setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
            }
        }
        copyBatchDecimalResult(16, isNullable, numRows, data, columnAddr);
    }

    private static void copyBatchDecimalResult(long typeLen, boolean isNullable, int numRows, BigInteger[] result,
            long resColumnAddr) {
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (result[i] != null) {
                    byte[] bytes = UdfUtils.convertByteOrder(result[i].toByteArray());
                    byte[] value = new byte[(int) typeLen];
                    if (result[i].signum() == -1) {
                        Arrays.fill(value, (byte) -1);
                    }
                    System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
                    UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, resColumnAddr + (i * typeLen),
                            value.length);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                byte[] bytes = UdfUtils.convertByteOrder(result[i].toByteArray());
                byte[] value = new byte[(int) typeLen];
                if (result[i].signum() == -1) {
                    Arrays.fill(value, (byte) -1);
                }
                System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, resColumnAddr + (i * typeLen),
                        value.length);
            }
        }
    }

    private static final byte[] emptyBytes = new byte[0];

    public static void copyBatchStringResult(boolean isNullable, int numRows, String[] strResult, long nullMapAddr,
            long charsAddr, long offsetsAddr) {
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (strResult[i] == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    byteRes[i] = ((String) strResult[i]).getBytes(StandardCharsets.UTF_8);
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                byteRes[i] = ((String) strResult[i]).getBytes(StandardCharsets.UTF_8);
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        }
        byte[] bytes = new byte[offsets[numRows - 1]];
        long bytesAddr = JNINativeMethod.resizeStringColumn(charsAddr, offsets[numRows - 1]);
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }

        UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, offsetsAddr, numRows * 4L);
        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
    }


    //////////////////////////////////// copyBatchArray//////////////////////////////////////////////////////////

    public static long copyBatchArrayBooleanResult(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Boolean> data = (ArrayList<Boolean>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    Boolean value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(dataAddr + ((hasPutElementNum + i)), value ? (byte) 1 : 0);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                Boolean value = data.get(i);
                if (value == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putByte(dataAddr + ((hasPutElementNum + i)), value ? (byte) 1 : 0);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayTinyIntResult(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Byte> data = (ArrayList<Byte>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    Byte value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(dataAddr + ((hasPutElementNum + i)), value);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                Byte value = data.get(i);
                if (value == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putByte(dataAddr + ((hasPutElementNum + i)), value);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArraySmallIntResult(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Short> data = (ArrayList<Short>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    Short value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putShort(dataAddr + ((hasPutElementNum + i) * 2L), value);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                Short value = data.get(i);
                if (value == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putShort(dataAddr + ((hasPutElementNum + i) * 2L), value);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayIntResult(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Integer> data = (ArrayList<Integer>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    Integer value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putInt(dataAddr + ((hasPutElementNum + i) * 4L), value);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                Integer value = data.get(i);
                if (value == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putInt(dataAddr + ((hasPutElementNum + i) * 4L), value);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayBigIntResult(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Long> data = (ArrayList<Long>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    Long value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putLong(dataAddr + ((hasPutElementNum + i) * 8L), value);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                Long value = data.get(i);
                if (value == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putLong(dataAddr + ((hasPutElementNum + i) * 8L), value);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayFloatResult(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Float> data = (ArrayList<Float>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    Float value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putFloat(dataAddr + ((hasPutElementNum + i) * 4L), value);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                Float value = data.get(i);
                if (value == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putFloat(dataAddr + ((hasPutElementNum + i) * 4L), value);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayDoubleResult(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Double> data = (ArrayList<Double>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    Double value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putDouble(dataAddr + ((hasPutElementNum + i) * 8L), value);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                Double value = data.get(i);
                if (value == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putDouble(dataAddr + ((hasPutElementNum + i) * 8L), value);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayDateResult(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<LocalDate> data = (ArrayList<LocalDate>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    LocalDate value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        long time = UdfUtils.convertToDate(value, LocalDate.class);
                        UdfUtils.UNSAFE.putLong(dataAddr + ((hasPutElementNum + i) * 8L), time);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                LocalDate value = data.get(i);
                if (value == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    long time = UdfUtils.convertToDate(value, LocalDate.class);
                    UdfUtils.UNSAFE.putLong(dataAddr + ((hasPutElementNum + i) * 8L), time);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayDateTimeResult(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<LocalDateTime> data = (ArrayList<LocalDateTime>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    LocalDateTime value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        long time = UdfUtils.convertToDateTime(value, LocalDateTime.class);
                        UdfUtils.UNSAFE.putLong(dataAddr + ((hasPutElementNum + i) * 8L), time);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                LocalDateTime value = data.get(i);
                if (value == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    long time = UdfUtils.convertToDateTime(value, LocalDateTime.class);
                    UdfUtils.UNSAFE.putLong(dataAddr + ((hasPutElementNum + i) * 8L), time);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayDateV2Result(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<LocalDate> data = (ArrayList<LocalDate>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    LocalDate value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        int time = UdfUtils.convertToDateV2(value, LocalDate.class);
                        UdfUtils.UNSAFE.putInt(dataAddr + ((hasPutElementNum + i) * 4L), time);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                LocalDate value = data.get(i);
                if (value == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    int time = UdfUtils.convertToDateV2(value, LocalDate.class);
                    UdfUtils.UNSAFE.putInt(dataAddr + ((hasPutElementNum + i) * 4L), time);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayDateTimeV2Result(long hasPutElementNum, boolean isNullable, int row,
            Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<LocalDateTime> data = (ArrayList<LocalDateTime>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    LocalDateTime value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        long time = UdfUtils.convertToDateTimeV2(value, LocalDateTime.class);
                        UdfUtils.UNSAFE.putLong(dataAddr + ((hasPutElementNum + i) * 8L), time);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                LocalDateTime value = data.get(i);
                if (value == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    long time = UdfUtils.convertToDateTimeV2(value, LocalDateTime.class);
                    UdfUtils.UNSAFE.putLong(dataAddr + ((hasPutElementNum + i) * 8L), time);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayLargeIntResult(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<BigInteger> data = (ArrayList<BigInteger>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    BigInteger bigInteger = data.get(i);
                    if (bigInteger == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        byte[] bytes = UdfUtils.convertByteOrder(bigInteger.toByteArray());
                        byte[] value = new byte[16];
                        // check data is negative
                        if (bigInteger.signum() == -1) {
                            Arrays.fill(value, (byte) -1);
                        }
                        System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
                        UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                                dataAddr + ((hasPutElementNum + i) * 16L), 16);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                BigInteger bigInteger = data.get(i);
                if (bigInteger == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    byte[] bytes = UdfUtils.convertByteOrder(bigInteger.toByteArray());
                    byte[] value = new byte[16];
                    // check data is negative
                    if (bigInteger.signum() == -1) {
                        Arrays.fill(value, (byte) -1);
                    }
                    System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
                    UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                            dataAddr + ((hasPutElementNum + i) * 16L), 16);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayDecimalResult(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<BigDecimal> data = (ArrayList<BigDecimal>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    BigDecimal bigDecimal = data.get(i);
                    if (bigDecimal == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        BigInteger bigInteger = bigDecimal.setScale(9, RoundingMode.HALF_EVEN).unscaledValue();
                        byte[] bytes = UdfUtils.convertByteOrder(bigInteger.toByteArray());
                        byte[] value = new byte[16];
                        // check data is negative
                        if (bigInteger.signum() == -1) {
                            Arrays.fill(value, (byte) -1);
                        }
                        System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
                        UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                                dataAddr + ((hasPutElementNum + i) * 16L), 16);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                BigDecimal bigDecimal = data.get(i);
                if (bigDecimal == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    BigInteger bigInteger = bigDecimal.setScale(9, RoundingMode.HALF_EVEN).unscaledValue();
                    byte[] bytes = UdfUtils.convertByteOrder(bigInteger.toByteArray());
                    byte[] value = new byte[16];
                    // check data is negative
                    if (bigInteger.signum() == -1) {
                        Arrays.fill(value, (byte) -1);
                    }
                    System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
                    UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                            dataAddr + ((hasPutElementNum + i) * 16L), 16);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayDecimalV3Result(int scale, long typeLen, long hasPutElementNum, boolean isNullable,
            int row,
            Object result,
            long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<BigDecimal> data = (ArrayList<BigDecimal>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                for (int i = 0; i < num; ++i) {
                    BigDecimal bigDecimal = data.get(i);
                    if (bigDecimal == null) {
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        BigInteger bigInteger = bigDecimal.setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
                        byte[] bytes = UdfUtils.convertByteOrder(bigInteger.toByteArray());
                        byte[] value = new byte[(int) typeLen];
                        // check data is negative
                        if (bigInteger.signum() == -1) {
                            Arrays.fill(value, (byte) -1);
                        }
                        System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
                        UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                                dataAddr + ((hasPutElementNum + i) * typeLen), typeLen);
                    }
                }
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            for (int i = 0; i < num; ++i) {
                BigDecimal bigDecimal = data.get(i);
                if (bigDecimal == null) {
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    BigInteger bigInteger = bigDecimal.setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
                    byte[] bytes = UdfUtils.convertByteOrder(bigInteger.toByteArray());
                    byte[] value = new byte[(int) typeLen];
                    // check data is negative
                    if (bigInteger.signum() == -1) {
                        Arrays.fill(value, (byte) -1);
                    }
                    System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
                    UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                            dataAddr + ((hasPutElementNum + i) * typeLen), typeLen);
                }
            }
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    public static long copyBatchArrayStringResult(long hasPutElementNum, boolean isNullable, int row,
            Object result, long nullMapAddr, long offsetsAddr, long nestedNullMapAddr, long dataAddr,
            long strOffsetAddr) {
        ArrayList<String> data = (ArrayList<String>) result;
        if (isNullable) {
            if (data == null) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 1);
            } else {
                int num = data.size();
                int[] offsets = new int[num];
                byte[][] byteRes = new byte[num][];
                int oldOffsetNum = UdfUtils.UNSAFE.getInt(null, strOffsetAddr + ((hasPutElementNum - 1) * 4L));
                int offset = oldOffsetNum;
                for (int i = 0; i < num; ++i) {
                    String value = data.get(i);
                    if (value == null) {
                        byteRes[i] = emptyBytes;
                        UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                    } else {
                        byteRes[i] = value.getBytes(StandardCharsets.UTF_8);
                    }
                    offset += byteRes[i].length;
                    offsets[i] = offset;
                }
                int oldSzie = oldOffsetNum;
                if (num > 0) {
                    oldSzie = offsets[num - 1];
                }
                byte[] bytes = new byte[oldSzie - oldOffsetNum];
                long bytesAddr = JNINativeMethod.resizeStringColumn(dataAddr, oldSzie);
                int dst = 0;
                for (int i = 0; i < num; i++) {
                    for (int j = 0; j < byteRes[i].length; j++) {
                        bytes[dst++] = byteRes[i][j];
                    }
                }
                UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, strOffsetAddr + (4L * hasPutElementNum),
                        num * 4L);
                UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr + oldOffsetNum,
                        oldSzie - oldOffsetNum);
                hasPutElementNum = hasPutElementNum + num;
            }
        } else {
            int num = data.size();
            int[] offsets = new int[num];
            byte[][] byteRes = new byte[num][];
            int oldOffsetNum = UdfUtils.UNSAFE.getInt(null, strOffsetAddr + ((hasPutElementNum - 1) * 4L));
            int offset = oldOffsetNum;
            for (int i = 0; i < num; ++i) {
                String value = data.get(i);
                if (value == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nestedNullMapAddr + (hasPutElementNum + i), (byte) 1);
                } else {
                    byteRes[i] = value.getBytes(StandardCharsets.UTF_8);
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
            int oldSzie = oldOffsetNum;
            if (num > 0) {
                oldSzie = offsets[num - 1];
            }
            byte[] bytes = new byte[oldSzie - oldOffsetNum];
            long bytesAddr = JNINativeMethod.resizeStringColumn(dataAddr, oldSzie);
            int dst = 0;
            for (int i = 0; i < num; i++) {
                for (int j = 0; j < byteRes[i].length; j++) {
                    bytes[dst++] = byteRes[i][j];
                }
            }
            UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, strOffsetAddr + (4L * hasPutElementNum),
                    num * 4L);
            UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr + oldOffsetNum,
                    oldSzie - oldOffsetNum);
            hasPutElementNum = hasPutElementNum + num;
        }
        UdfUtils.UNSAFE.putLong(null, offsetsAddr + 8L * row, hasPutElementNum);
        return hasPutElementNum;
    }

    //////////////////////////////////////////convertArray///////////////////////////////////////////////////////////
    public static ArrayList<Boolean> convertArrayBooleanArg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Boolean> data = null;
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        boolean value = UdfUtils.UNSAFE.getBoolean(null, dataAddr + (offsetRow + offsetStart));
                        data.add(value);
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    boolean value = UdfUtils.UNSAFE.getBoolean(null, dataAddr + (offsetRow + offsetStart));
                    data.add(value);
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<Byte> convertArrayTinyIntArg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Byte> data = null;
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        byte value = UdfUtils.UNSAFE.getByte(null, dataAddr + (offsetRow + offsetStart));
                        data.add(value);
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    byte value = UdfUtils.UNSAFE.getByte(null, dataAddr + (offsetRow + offsetStart));
                    data.add(value);
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<Short> convertArraySmallIntArg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Short> data = null;
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        short value = UdfUtils.UNSAFE.getShort(null, dataAddr + 2L * (offsetRow + offsetStart));
                        data.add(value);
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    short value = UdfUtils.UNSAFE.getShort(null, dataAddr + 2L * (offsetRow + offsetStart));
                    data.add(value);
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<Integer> convertArrayIntArg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Integer> data = null;
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        int value = UdfUtils.UNSAFE.getInt(null, dataAddr + 4L * (offsetRow + offsetStart));
                        data.add(value);
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    int value = UdfUtils.UNSAFE.getInt(null, dataAddr + 4L * (offsetRow + offsetStart));
                    data.add(value);
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<Long> convertArrayBigIntArg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Long> data = null;
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        long value = UdfUtils.UNSAFE.getLong(null, dataAddr + 8L * (offsetRow + offsetStart));
                        data.add(value);
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    long value = UdfUtils.UNSAFE.getLong(null, dataAddr + 8L * (offsetRow + offsetStart));
                    data.add(value);
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<Float> convertArrayFloatArg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Float> data = null;
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        float value = UdfUtils.UNSAFE.getFloat(null, dataAddr + 4L * (offsetRow + offsetStart));
                        data.add(value);
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    float value = UdfUtils.UNSAFE.getFloat(null, dataAddr + 4L * (offsetRow + offsetStart));
                    data.add(value);
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<Double> convertArrayDoubleArg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<Double> data = null;
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        double value = UdfUtils.UNSAFE.getDouble(null, dataAddr + 8L * (offsetRow + offsetStart));
                        data.add(value);
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    double value = UdfUtils.UNSAFE.getDouble(null, dataAddr + 8L * (offsetRow + offsetStart));
                    data.add(value);
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<LocalDate> convertArrayDateArg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<LocalDate> data = null;
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        long value = UdfUtils.UNSAFE.getLong(null, dataAddr + 8L * (offsetRow + offsetStart));
                        LocalDate obj = (LocalDate) UdfUtils.convertDateToJavaDate(value, LocalDate.class);
                        data.add(obj);
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    long value = UdfUtils.UNSAFE.getLong(null, dataAddr + 8L * (offsetRow + offsetStart));
                    // TODO: now argClass[argIdx + argClassOffset] is java.util.ArrayList, can't get
                    // nested class type, so don't know the date type class is LocalDate or others
                    // LocalDate obj = UdfUtils.convertDateToJavaDate(value, argClass[argIdx +
                    // argClassOffset]);
                    LocalDate obj = (LocalDate) UdfUtils.convertDateToJavaDate(value, LocalDate.class);
                    data.add(obj);
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<LocalDateTime> convertArrayDateTimeArg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<LocalDateTime> data = null;
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        long value = UdfUtils.UNSAFE.getLong(null, dataAddr + 8L * (offsetRow + offsetStart));
                        LocalDateTime obj = (LocalDateTime) UdfUtils
                                .convertDateTimeToJavaDateTime(value, LocalDateTime.class);
                        data.add(obj);
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    long value = UdfUtils.UNSAFE.getLong(null, dataAddr + 8L * (offsetRow + offsetStart));
                    LocalDateTime obj = (LocalDateTime) UdfUtils
                            .convertDateTimeToJavaDateTime(value, LocalDateTime.class);
                    data.add(obj);
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<LocalDate> convertArrayDateV2Arg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<LocalDate> data = null;
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        int value = UdfUtils.UNSAFE.getInt(null, dataAddr + 4L * (offsetRow + offsetStart));
                        LocalDate obj = (LocalDate) UdfUtils.convertDateV2ToJavaDate(value, LocalDate.class);
                        data.add(obj);
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    int value = UdfUtils.UNSAFE.getInt(null, dataAddr + 4L * (offsetRow + offsetStart));
                    LocalDate obj = (LocalDate) UdfUtils.convertDateV2ToJavaDate(value, LocalDate.class);
                    data.add(obj);
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<LocalDateTime> convertArrayDateTimeV2Arg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<LocalDateTime> data = null;
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        long value = UdfUtils.UNSAFE.getLong(null, dataAddr + 8L * (offsetRow + offsetStart));
                        LocalDateTime obj = (LocalDateTime) UdfUtils
                                .convertDateTimeV2ToJavaDateTime(value, LocalDateTime.class);
                        data.add(obj);
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    long value = UdfUtils.UNSAFE.getLong(null, dataAddr + 8L * (offsetRow + offsetStart));
                    LocalDateTime obj = (LocalDateTime) UdfUtils
                            .convertDateTimeV2ToJavaDateTime(value, LocalDateTime.class);
                    data.add(obj);
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<BigInteger> convertArrayLargeIntArg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<BigInteger> data = null;
        byte[] bytes = new byte[16];
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        UdfUtils.copyMemory(null, dataAddr + 16L * (offsetRow + offsetStart), bytes,
                                UdfUtils.BYTE_ARRAY_OFFSET, 16);
                        data.add(new BigInteger(UdfUtils.convertByteOrder(bytes)));
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    UdfUtils.copyMemory(null, dataAddr + 16L * (offsetRow + offsetStart), bytes,
                            UdfUtils.BYTE_ARRAY_OFFSET, 16);
                    data.add(new BigInteger(UdfUtils.convertByteOrder(bytes)));
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<BigDecimal> convertArrayDecimalArg(int scale, long typeLen, int row, int currentRowNum,
            long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr) {
        ArrayList<BigDecimal> data = null;
        byte[] bytes = new byte[16];
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                        UdfUtils.copyMemory(null, dataAddr + typeLen * (offsetRow + offsetStart), bytes,
                                UdfUtils.BYTE_ARRAY_OFFSET, typeLen);
                        BigInteger bigInteger = new BigInteger(UdfUtils.convertByteOrder(bytes));
                        data.add(new BigDecimal(bigInteger, scale));
                    } else { // in the array row, current offset is null
                        data.add(null);
                    }
                } // for loop
            } // else is current array row is null
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    UdfUtils.copyMemory(null, dataAddr + typeLen * (offsetRow + offsetStart), bytes,
                            UdfUtils.BYTE_ARRAY_OFFSET, typeLen);
                    BigInteger bigInteger = new BigInteger(UdfUtils.convertByteOrder(bytes));
                    data.add(new BigDecimal(bigInteger, scale));
                } else { // in the array row, current offset is null
                    data.add(null);
                }
            } // for loop
        } // end for all current row
        return data;
    }

    public static ArrayList<String> convertArrayStringArg(int row, int currentRowNum, long offsetStart,
            boolean isNullable, long nullMapAddr, long nestedNullMapAddr, long dataAddr, long strOffsetAddr) {
        ArrayList<String> data = null;
        if (isNullable) {
            if (UdfUtils.UNSAFE.getByte(nullMapAddr + row) == 0) {
                data = new ArrayList<>(currentRowNum);
                for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow))
                            == 0)) {
                        int offset = UdfUtils.UNSAFE
                                .getInt(null, strOffsetAddr + (offsetRow + offsetStart) * 4L);
                        int numBytes = offset - UdfUtils.UNSAFE
                                .getInt(null, strOffsetAddr + 4L * ((offsetRow + offsetStart) - 1));
                        long base = dataAddr + offset - numBytes;
                        byte[] bytes = new byte[numBytes];
                        UdfUtils.copyMemory(null, base, bytes, UdfUtils.BYTE_ARRAY_OFFSET, numBytes);
                        data.add(new String(bytes, StandardCharsets.UTF_8));
                    } else {
                        data.add(null);
                    }
                }
            }
        } else {
            data = new ArrayList<>(currentRowNum);
            for (int offsetRow = 0; offsetRow < currentRowNum; ++offsetRow) {
                if ((UdfUtils.UNSAFE.getByte(null, nestedNullMapAddr + (offsetStart + offsetRow)) == 0)) {
                    int offset = UdfUtils.UNSAFE
                            .getInt(null, strOffsetAddr + (offsetRow + offsetStart) * 4L);
                    int numBytes = offset - UdfUtils.UNSAFE
                            .getInt(null, strOffsetAddr + 4L * ((offsetRow + offsetStart) - 1));
                    long base = dataAddr + offset - numBytes;
                    byte[] bytes = new byte[numBytes];
                    UdfUtils.copyMemory(null, base, bytes, UdfUtils.BYTE_ARRAY_OFFSET, numBytes);
                    data.add(new String(bytes, StandardCharsets.UTF_8));
                } else {
                    data.add(null);
                }
            }
        }
        return data;
    }
}
