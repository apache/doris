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

package org.apache.doris;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.lakesoul.arrow.LakeSoulArrowJniScanner;
import org.junit.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;

public class ArrowTest {

    final static String STRUCT_INT_CHILD = "struct_int_child";
    final static String STRUCT_UTF8_CHILD = "struct_utf8_child";

    @Test
    public void testReadVectorSchemaRoot() throws IOException {
        OffHeap.setTesting();
        BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot batch = createVectorSchemaRoot(allocator);
        LakeSoulArrowJniScanner scanner = new LakeSoulArrowJniScanner(allocator, batch);
        scanner.open();
        System.out.println(scanner.dump());
        scanner.close();
        batch.close();
        allocator.close();
    }

    public static VectorSchemaRoot createVectorSchemaRoot(BufferAllocator allocator) {
        Schema schema = new Schema(
            Arrays.asList(
                new Field("int",  FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("utf8",  FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("decimal",  FieldType.nullable(ArrowType.Decimal.createDecimal(10,3, null)), null),
                new Field("boolean", FieldType.nullable(new ArrowType.Bool()), null),
                new Field("date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
                new Field("datetimeSec", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, ZoneId.of("UTC").toString())), null),
                new Field("datetimeMilli", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneId.of("UTC").toString())), null),
                new Field("list", FieldType.nullable(new ArrowType.List()),
                    Collections.singletonList(new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null))),
                new Field("struct", FieldType.nullable(new ArrowType.Struct()),
                    Arrays.asList(
                        new Field(STRUCT_INT_CHILD, FieldType.nullable(new ArrowType.Int(32, true)), null),
                        new Field(STRUCT_UTF8_CHILD, FieldType.nullable(new ArrowType.Utf8()), null)
                    )
                )
            )
        );
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        int batchSize = 20;
        root.setRowCount(batchSize);
        for (int idx=0; idx < schema.getFields().size(); idx ++) {
            setValue(allocator, root, root.getVector(idx), idx, batchSize);
        }

        System.out.println(root.contentToTSVString());
        return root;
    }

    private static void setValue(BufferAllocator allocator, VectorSchemaRoot root, FieldVector fieldVector, int columnIdx, int batchSize) {
        if (fieldVector instanceof TinyIntVector) {
            TinyIntVector vector = (TinyIntVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7 + i * 3 );
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof SmallIntVector) {
            SmallIntVector vector = (SmallIntVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7 + i * 3 );
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof IntVector) {
            IntVector vector = (IntVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7 + i * 3 );
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof BigIntVector) {
            BigIntVector vector = (BigIntVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7L + i * 3L);
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof BitVector) {
            BitVector vector = (BitVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, (columnIdx * 7 + i * 3) & 1);
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof Float4Vector) {
            Float4Vector vector = (Float4Vector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7 + i * 3);
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof Float8Vector) {
            Float8Vector vector = (Float8Vector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7 + i * 3);
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof VarCharVector) {
            VarCharVector vector = (VarCharVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, new Text(String.valueOf(columnIdx * 101 + i * 3)));
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof FixedSizeBinaryVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));
        } else if (fieldVector instanceof VarBinaryVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));
        } else if (fieldVector instanceof DecimalVector) {
            DecimalVector vector = (DecimalVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7L + i * 3L);
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);

        } else if (fieldVector instanceof DateDayVector) {
            DateDayVector vector = (DateDayVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7 + i * 3);
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        }else if (fieldVector instanceof DateMilliVector) {
            DateMilliVector vector = (DateMilliVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7L + i * 3L);
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);
        } else if (fieldVector instanceof TimeSecVector
            || fieldVector instanceof TimeMilliVector
            || fieldVector instanceof TimeMicroVector
            || fieldVector instanceof TimeNanoVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));
        } else if (fieldVector instanceof TimeStampVector) {
            TimeStampVector vector = (TimeStampVector) fieldVector;
            vector.allocateNew(batchSize);
            for (int i = 0; i <batchSize; i++) {
                vector.set(i, columnIdx * 7L + i * 3L);
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }
            vector.setValueCount(batchSize);

        } else if (fieldVector instanceof MapVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));

        } else if (fieldVector instanceof ListVector) {
            ListVector vector = (ListVector) fieldVector;
            vector.allocateNew();
            UnionListWriter writer = vector.getWriter();
            int count = 0;
            for (int i = 0; i < batchSize; i++) {
                writer.startList();
                int subCount = (columnIdx * 7 + i * 3) % 5;
                writer.setPosition(i);
                for (int j = 0; j < subCount; j++) {
                    writer.writeInt(columnIdx * 7 + i * 3 + j * 11);
                }
                writer.setValueCount(subCount);
                count += subCount;

                writer.endList();
                if ((i + columnIdx) % 5 == 0) {
                    vector.setNull(i);
                }
            }

            vector.setValueCount(count);
        } else if (fieldVector instanceof StructVector) {
            StructVector vector = (StructVector) fieldVector;
            NullableStructWriter writer = vector.getWriter();
            IntWriter intWriter = writer.integer(STRUCT_INT_CHILD);
            VarCharWriter varCharWriter = writer.varChar(STRUCT_UTF8_CHILD);
            for (int i = 0; i < batchSize; i++) {
                writer.setPosition(i);
                writer.start();
                intWriter.writeInt(columnIdx * 7 + i * 3);

                byte[] bytes = new Text(String.valueOf(columnIdx * 101 + i * 3)).getBytes();
                ArrowBuf buf = allocator.buffer(bytes.length);
                buf.writeBytes(bytes);
                varCharWriter.writeVarChar(0, bytes.length, buf);
                buf.close();
                writer.end();
            }

            writer.setValueCount(batchSize);
        } else if (fieldVector instanceof NullVector) {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));
        } else {
            throw new UnsupportedOperationException(
                String.format("Unsupported type %s.", fieldVector.getField()));
        }
    }


}
