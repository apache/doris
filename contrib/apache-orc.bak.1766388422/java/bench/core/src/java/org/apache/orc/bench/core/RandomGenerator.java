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

package org.apache.orc.bench.core;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomGenerator {
  private final TypeDescription schema = TypeDescription.createStruct();
  private final List<Field> fields = new ArrayList<>();
  private final Random random;

  public RandomGenerator(int seed) {
    random = new Random(seed);
  }

  private abstract class ValueGenerator {
    double nullProbability = 0;
    abstract void generate(ColumnVector vector, int valueCount);
  }

  private class RandomBoolean extends ValueGenerator {
    public void generate(ColumnVector v, int valueCount) {
      LongColumnVector vector = (LongColumnVector) v;
      for(int r=0; r < valueCount; ++r) {
        if (nullProbability != 0 && random.nextDouble() < nullProbability) {
          v.noNulls = false;
          v.isNull[r] = true;
        } else {
          vector.vector[r] = random.nextInt(2);
        }
      }
    }
  }

  private class RandomList extends ValueGenerator {
    private final int minSize;
    private final int sizeRange;
    private final Field child;

    RandomList(int minSize, int maxSize, Field child) {
      this.minSize = minSize;
      this.sizeRange = maxSize - minSize + 1;
      this.child = child;
    }

    public void generate(ColumnVector v, int valueCount) {
      ListColumnVector vector = (ListColumnVector) v;
      for(int r=0; r < valueCount; ++r) {
        if (nullProbability != 0 && random.nextDouble() < nullProbability) {
          v.noNulls = false;
          v.isNull[r] = true;
        } else {
          vector.offsets[r] = vector.childCount;
          vector.lengths[r] = random.nextInt(sizeRange) + minSize;
          vector.childCount += vector.lengths[r];
        }
      }
      vector.child.ensureSize(vector.childCount, false);
      child.generator.generate(vector.child, vector.childCount);
    }
  }

  private class RandomStruct extends ValueGenerator {
    private final Field[] children;

    RandomStruct(Field[] children) {
      this.children = children;
    }

    public void generate(ColumnVector v, int valueCount) {
      StructColumnVector vector = (StructColumnVector) v;
      for(int r=0; r < valueCount; ++r) {
        if (nullProbability != 0 && random.nextDouble() < nullProbability) {
          v.noNulls = false;
          v.isNull[r] = true;
        }
      }
      for(int c=0; c < children.length; ++c) {
        children[c].generator.generate(vector.fields[c], valueCount);
      }
    }
  }

  private abstract class IntegerGenerator extends ValueGenerator {
    private final long sign;
    private final long mask;

    private IntegerGenerator(TypeDescription.Category kind) {
      int bits = getIntegerLength(kind);
      mask = bits == 64 ? 0 : -1L << bits;
      sign = 1L << (bits - 1);
    }

    protected void normalize(LongColumnVector vector, int valueCount) {
      // make sure the value stays in range by sign extending it
      for(int r=0; r < valueCount; ++r) {
        if ((vector.vector[r] & sign) == 0) {
          vector.vector[r] &= ~mask;
        } else {
          vector.vector[r] |= mask;
        }
      }
    }
  }

  private class AutoIncrement extends IntegerGenerator {
    private long value;
    private final long increment;

    private AutoIncrement(TypeDescription.Category kind, long start,
                          long increment) {
      super(kind);
      this.value = start;
      this.increment = increment;
    }

    public void generate(ColumnVector v, int valueCount) {
      LongColumnVector vector = (LongColumnVector) v;
      for(int r=0; r < valueCount; ++r) {
        if (nullProbability != 0 && random.nextDouble() >= nullProbability) {
          v.noNulls = false;
          v.isNull[r] = true;
        } else {
          vector.vector[r] = value;
          value += increment;
        }
      }
      normalize(vector, valueCount);
    }
  }

  private class RandomInteger extends IntegerGenerator {

    private RandomInteger(TypeDescription.Category kind) {
      super(kind);
    }

    public void generate(ColumnVector v, int valueCount) {
      LongColumnVector vector = (LongColumnVector) v;
      for(int r=0; r < valueCount; ++r) {
        if (nullProbability != 0 && random.nextDouble() < nullProbability) {
          v.noNulls = false;
          v.isNull[r] = true;
        } else {
          vector.vector[r] = random.nextLong();
        }
      }
      normalize(vector, valueCount);
    }
  }

  private class IntegerRange extends IntegerGenerator {
    private final long minimum;
    private final long range;
    private final long limit;

    private IntegerRange(TypeDescription.Category kind, long minimum,
                         long maximum) {
      super(kind);
      this.minimum = minimum;
      this.range = maximum - minimum + 1;
      if (this.range < 0) {
        throw new IllegalArgumentException("Can't support a negative range "
            + range);
      }
      limit = (Long.MAX_VALUE / range) * range;
    }

    public void generate(ColumnVector v, int valueCount) {
      LongColumnVector vector = (LongColumnVector) v;
      for(int r=0; r < valueCount; ++r) {
        if (nullProbability != 0 && random.nextDouble() < nullProbability) {
          v.noNulls = false;
          v.isNull[r] = true;
        } else {
          long rand;
          do {
            // clear the sign bit
            rand = random.nextLong() & Long.MAX_VALUE;
          } while (rand >= limit);
          vector.vector[r] = (rand % range) + minimum;
        }
      }
      normalize(vector, valueCount);
    }
  }

  private class StringChooser extends ValueGenerator {
    private final byte[][] choices;
    private StringChooser(String[] values) {
      choices = new byte[values.length][];
      for(int e=0; e < values.length; ++e) {
        choices[e] = values[e].getBytes(StandardCharsets.UTF_8);
      }
    }

    public void generate(ColumnVector v, int valueCount) {
      BytesColumnVector vector = (BytesColumnVector) v;
      for(int r=0; r < valueCount; ++r) {
        if (nullProbability != 0 && random.nextDouble() < nullProbability) {
          v.noNulls = false;
          v.isNull[r] = true;
        } else {
          int val = random.nextInt(choices.length);
          vector.setRef(r, choices[val], 0, choices[val].length);
        }
      }
    }
  }

  private static byte[] concat(byte[] left, byte[] right) {
    byte[] result = new byte[left.length + right.length];
    System.arraycopy(left, 0, result, 0, left.length);
    System.arraycopy(right, 0, result, left.length, right.length);
    return result;
  }

  private static byte pickOne(byte[] choices, Random random) {
    return choices[random.nextInt(choices.length)];
  }

  private static final byte[] LOWER_CONSONANTS =
      "bcdfghjklmnpqrstvwxyz".getBytes(StandardCharsets.UTF_8);
  private static final byte[] UPPER_CONSONANTS =
      "BCDFGHJKLMNPQRSTVWXYZ".getBytes(StandardCharsets.UTF_8);
  private static final byte[] CONSONANTS =
      concat(LOWER_CONSONANTS, UPPER_CONSONANTS);
  private static final byte[] LOWER_VOWELS = "aeiou".getBytes(StandardCharsets.UTF_8);
  private static final byte[] UPPER_VOWELS = "AEIOU".getBytes(StandardCharsets.UTF_8);
  private static final byte[] VOWELS = concat(LOWER_VOWELS, UPPER_VOWELS);
  private static final byte[] LOWER_LETTERS =
      concat(LOWER_CONSONANTS, LOWER_VOWELS);
  private static final byte[] UPPER_LETTERS =
      concat(UPPER_CONSONANTS, UPPER_VOWELS);
  private static final byte[] LETTERS = concat(LOWER_LETTERS, UPPER_LETTERS);
  private static final byte[] NATURAL_DIGITS = "123456789".getBytes(StandardCharsets.UTF_8);
  private static final byte[] DIGITS = "0123456789".getBytes(StandardCharsets.UTF_8);

  private class StringPattern extends ValueGenerator {
    private final byte[] buffer;
    private final byte[][] choices;
    private final int[] locations;

    private StringPattern(String pattern) {
      buffer = pattern.getBytes(StandardCharsets.UTF_8);
      int locs = 0;
      for(int i=0; i < buffer.length; ++i) {
        switch (buffer[i]) {
          case 'C':
          case 'c':
          case 'E':
          case 'V':
          case 'v':
          case 'F':
          case 'l':
          case 'L':
          case 'D':
          case 'x':
          case 'X':
            locs += 1;
            break;
          default:
            break;
        }
      }
      locations = new int[locs];
      choices = new byte[locs][];
      locs = 0;
      for(int i=0; i < buffer.length; ++i) {
        switch (buffer[i]) {
          case 'C':
            locations[locs] = i;
            choices[locs++] = UPPER_CONSONANTS;
            break;
          case 'c':
            locations[locs] = i;
            choices[locs++] = LOWER_CONSONANTS;
            break;
          case 'E':
            locations[locs] = i;
            choices[locs++] = CONSONANTS;
            break;
          case 'V':
            locations[locs] = i;
            choices[locs++] = UPPER_VOWELS;
            break;
          case 'v':
            locations[locs] = i;
            choices[locs++] = LOWER_VOWELS;
            break;
          case 'F':
            locations[locs] = i;
            choices[locs++] = VOWELS;
            break;
          case 'l':
            locations[locs] = i;
            choices[locs++] = LOWER_LETTERS;
            break;
          case 'L':
            locations[locs] = i;
            choices[locs++] = UPPER_LETTERS;
            break;
          case 'D':
            locations[locs] = i;
            choices[locs++] = LETTERS;
            break;
          case 'x':
            locations[locs] = i;
            choices[locs++] = NATURAL_DIGITS;
            break;
          case 'X':
            locations[locs] = i;
            choices[locs++] = DIGITS;
            break;
          default:
            break;
        }
      }
    }

    public void generate(ColumnVector v, int valueCount) {
      BytesColumnVector vector = (BytesColumnVector) v;
      for(int r=0; r < valueCount; ++r) {
        if (nullProbability != 0 && random.nextDouble() < nullProbability) {
          v.noNulls = false;
          v.isNull[r] = true;
        } else {
          for(int m=0; m < locations.length; ++m) {
            buffer[locations[m]] = pickOne(choices[m], random);
          }
          vector.setVal(r, buffer, 0, buffer.length);
        }
      }
    }
  }

  private class TimestampRange extends ValueGenerator {
    private final long minimum;
    private final long range;
    private final long limit;

    private TimestampRange(String min, String max) {
      minimum = Timestamp.valueOf(min).getTime();
      range = Timestamp.valueOf(max).getTime() - minimum + 1;
      if (range < 0) {
        throw new IllegalArgumentException("Negative range " + range);
      }
      limit = (Long.MAX_VALUE / range) * range;
    }

    public void generate(ColumnVector v, int valueCount) {
      TimestampColumnVector vector = (TimestampColumnVector) v;
      for(int r=0; r < valueCount; ++r) {
        if (nullProbability != 0 && random.nextDouble() < nullProbability) {
          v.noNulls = false;
          v.isNull[r] = true;
        } else {
          long rand;
          do {
            // clear the sign bit
            rand = random.nextLong() & Long.MAX_VALUE;
          } while (rand >= limit);
          vector.time[r] = (rand % range) + minimum;
          vector.nanos[r] = random.nextInt(1000000);
        }
      }
    }
  }

  private static int getIntegerLength(TypeDescription.Category kind) {
    switch (kind) {
      case BYTE:
        return 8;
      case SHORT:
        return 16;
      case INT:
        return 32;
      case LONG:
        return 64;
      default:
        throw new IllegalArgumentException("Unhandled type " + kind);
    }
  }

  public class Field {
    private final TypeDescription type;
    private Field[] children;
    private ValueGenerator generator;

    private Field(TypeDescription type) {
      this.type = type;
      if (!type.getCategory().isPrimitive()) {
        List<TypeDescription> childrenTypes = type.getChildren();
        children = new Field[childrenTypes.size()];
        for(int c=0; c < children.length; ++c) {
          children[c] = new Field(childrenTypes.get(c));
        }
      }
    }

    public Field addAutoIncrement(long start, long increment) {
      generator = new AutoIncrement(type.getCategory(), start, increment);
      return this;
    }

    public Field addIntegerRange(long min, long max) {
      generator = new IntegerRange(type.getCategory(), min, max);
      return this;
    }

    public Field addRandomInt() {
      generator = new RandomInteger(type.getCategory());
      return this;
    }

    public Field addStringChoice(String... choices) {
      if (type.getCategory() != TypeDescription.Category.STRING) {
        throw new IllegalArgumentException("Must be string - " + type);
      }
      generator = new StringChooser(choices);
      return this;
    }

    public Field addStringPattern(String pattern) {
      if (type.getCategory() != TypeDescription.Category.STRING) {
        throw new IllegalArgumentException("Must be string - " + type);
      }
      generator = new StringPattern(pattern);
      return this;
    }

    public Field addTimestampRange(String start, String end) {
      if (type.getCategory() != TypeDescription.Category.TIMESTAMP) {
        throw new IllegalArgumentException("Must be timestamp - " + type);
      }
      generator = new TimestampRange(start, end);
      return this;
    }

    public Field addBoolean() {
      if (type.getCategory() != TypeDescription.Category.BOOLEAN) {
        throw new IllegalArgumentException("Must be boolean - " + type);
      }
      generator = new RandomBoolean();
      return this;
    }

    public Field hasNulls(double probability) {
      generator.nullProbability = probability;
      return this;
    }

    public Field addStruct() {
      generator = new RandomStruct(children);
      return this;
    }

    public Field addList(int minSize, int maxSize) {
      generator = new RandomList(minSize, maxSize, children[0]);
      return this;
    }

    public Field getChildField(int child) {
      return children[child];
    }
  }

  public Field addField(String name, TypeDescription.Category kind) {
    TypeDescription type = new TypeDescription(kind);
    return addField(name, type);
  }

  public Field addField(String name, TypeDescription type) {
    schema.addField(name, type);
    Field result = new Field(type);
    fields.add(result);
    return result;
  }

  public void generate(VectorizedRowBatch batch, int rowCount) {
    batch.reset();
    for(int c=0; c < batch.cols.length; ++c) {
      fields.get(c).generator.generate(batch.cols[c], rowCount);
    }
    batch.size = rowCount;
  }

  /**
   * Get the schema for the table that is being generated.
   * @return
   */
  public TypeDescription getSchema() {
    return schema;
  }
}
