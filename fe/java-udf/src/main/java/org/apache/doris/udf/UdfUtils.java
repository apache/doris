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

import com.google.common.base.Preconditions;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class UdfUtils {
    public static final Unsafe UNSAFE;
    private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;
    public static final long BYTE_ARRAY_OFFSET;
    public static final char END_OF_STRING = '\0';

    static {
        UNSAFE = (Unsafe) AccessController.doPrivileged(
                (PrivilegedAction<Object>) () -> {
                    try {
                        Field f = Unsafe.class.getDeclaredField("theUnsafe");
                        f.setAccessible(true);
                        return f.get(null);
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        throw new Error();
                    }
                });
        BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    }

    protected static Pair<Type, Integer> fromThrift(TTypeDesc typeDesc, int nodeIdx) throws InternalException {
        TTypeNode node = typeDesc.getTypes().get(nodeIdx);
        Type type = null;
        switch (node.getType()) {
            case SCALAR: {
                Preconditions.checkState(node.isSetScalarType());
                TScalarType scalarType = node.getScalarType();
                if (scalarType.getType() == TPrimitiveType.CHAR) {
                    Preconditions.checkState(scalarType.isSetLen());
                    type = ScalarType.createCharType(scalarType.getLen());
                } else if (scalarType.getType() == TPrimitiveType.VARCHAR) {
                    Preconditions.checkState(scalarType.isSetLen());
                    type = ScalarType.createVarcharType(scalarType.getLen());
                } else if (scalarType.getType() == TPrimitiveType.DECIMALV2) {
                    Preconditions.checkState(scalarType.isSetPrecision()
                            && scalarType.isSetScale());
                    type = ScalarType.createDecimalV2Type(scalarType.getPrecision(),
                            scalarType.getScale());
                } else {
                    type = ScalarType.createType(
                            PrimitiveType.fromThrift(scalarType.getType()));
                }
                break;
            }
            default:
                throw new InternalException("Return type " + node.getType() + " is not supported now!");
        }
        return new Pair<Type, Integer>(type, nodeIdx);
    }

    protected static long getAddressAtOffset(long base, int offset) {
        return base + 8L * offset;
    }

    public static void copyMemory(
            Object src, long srcOffset, Object dst, long dstOffset, long length) {
        // Check if dstOffset is before or after srcOffset to determine if we should copy
        // forward or backwards. This is necessary in case src and dst overlap.
        if (dstOffset < srcOffset) {
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
                srcOffset += size;
                dstOffset += size;
            }
        } else {
            srcOffset += length;
            dstOffset += length;
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                srcOffset -= size;
                dstOffset -= size;
                UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
            }

        }
    }
}
