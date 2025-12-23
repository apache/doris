/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.bench.core.filter;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.TypeDescription;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

class FilterBenchUtil {
  static final TypeDescription schema = TypeDescription.createStruct()
      .addField("f1", TypeDescription.createLong())
      .addField("f2", TypeDescription.createLong());

  static VectorizedRowBatch createBatch(Random rnd) {
    VectorizedRowBatch b = schema.createRowBatch(1024);
    LongColumnVector f1Vector = (LongColumnVector) b.cols[0];
    LongColumnVector f2Vector = (LongColumnVector) b.cols[1];

    for (int i = 0; i < b.getMaxSize(); i++) {
      f1Vector.vector[b.size] = rnd.nextInt();
      f2Vector.vector[b.size] = rnd.nextInt();
      b.size++;
    }
    return b;
  }

  static Map.Entry<SearchArgument, int[]> createSArg(Random rnd,
                                                     VectorizedRowBatch b,
                                                     int inSize) {
    LongColumnVector f1Vector = (LongColumnVector) b.cols[0];
    LongColumnVector f2Vector = (LongColumnVector) b.cols[1];

    Object[] f1Values = new Object[inSize];
    Object[] f2Values = new Object[inSize];
    Set<Integer> sel = new HashSet<>();

    for (int i = 0; i < f1Values.length; i++) {
      int selIdx = rnd.nextInt(b.getMaxSize());
      f1Values[i] = f1Vector.vector[selIdx];
      sel.add(selIdx);
      selIdx = rnd.nextInt(b.getMaxSize());
      f2Values[i] = f2Vector.vector[selIdx];
      sel.add(selIdx);
    }

    SearchArgument sArg = SearchArgumentFactory.newBuilder()
        .startOr()
        .in("f1", PredicateLeaf.Type.LONG, f1Values)
        .in("f2", PredicateLeaf.Type.LONG, f2Values)
        .end()
        .build();
    int[] s = sel.stream()
      .mapToInt(Integer::intValue)
      .toArray();
    Arrays.sort(s);
    return new AbstractMap.SimpleImmutableEntry<>(sArg, s);
  }

  static Map.Entry<SearchArgument, int[]> createComplexSArg(Random rnd,
                                                            VectorizedRowBatch b,
                                                            int inSize,
                                                            int orSize) {
    LongColumnVector f1Vector = (LongColumnVector) b.cols[0];
    LongColumnVector f2Vector = (LongColumnVector) b.cols[1];

    Object[] f1Values = new Object[inSize];
    Object[] f2Values = new Object[inSize];
    Set<Integer> sel = new HashSet<>();
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
    builder.startOr();
    builder.in("f2", PredicateLeaf.Type.LONG, f2Vector.vector[0], f2Vector.vector[1]);
    sel.add(0);
    sel.add(1);
    int selIdx;
    for (int i = 0; i < orSize; i++) {
      builder.startAnd();
      for (int j = 0; j < inSize; j++) {
        selIdx = rnd.nextInt(b.getMaxSize());
        f1Values[j] = f1Vector.vector[selIdx];
        f2Values[j] = f2Vector.vector[selIdx];
        sel.add(selIdx);
      }
      builder
          .in("f1", PredicateLeaf.Type.LONG, f1Values)
          .in("f2", PredicateLeaf.Type.LONG, f2Values);
      builder.end();
    }
    builder.end();

    int[] s = sel.stream()
      .mapToInt(Integer::intValue)
      .toArray();
    Arrays.sort(s);
    return new AbstractMap.SimpleImmutableEntry<>(builder.build(), s);
  }

  static void unFilterBatch(OrcFilterContext fc) {
    fc.setSelectedInUse(false);
    fc.setSelectedSize(1024);
  }

  static void validate(OrcFilterContext fc, int[] expSel) {
    if (!fc.isSelectedInUse()) {
      throw new IllegalArgumentException("Validation failed: selected is not set");
    }
    if (expSel.length != fc.getSelectedSize()) {
      throw new IllegalArgumentException(String.format(
        "Validation failed: length %s is not equal to expected length %s",
        fc.getSelectedSize(), expSel.length));
    }
    if (!Arrays.equals(expSel, Arrays.copyOf(fc.getSelected(), expSel.length))) {
      throw new IllegalArgumentException("Validation failed: array values are not the same");
    }
  }
}
