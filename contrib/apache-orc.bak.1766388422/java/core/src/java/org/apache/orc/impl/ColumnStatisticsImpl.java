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
package org.apache.orc.impl;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.orc.BinaryColumnStatistics;
import org.apache.orc.BooleanColumnStatistics;
import org.apache.orc.CollectionColumnStatistics;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DateColumnStatistics;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcProto;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;
import org.threeten.extra.chrono.HybridChronology;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.chrono.ChronoLocalDate;
import java.time.chrono.Chronology;
import java.time.chrono.IsoChronology;
import java.util.TimeZone;


public class ColumnStatisticsImpl implements ColumnStatistics {

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ColumnStatisticsImpl)) {
      return false;
    }

    ColumnStatisticsImpl that = (ColumnStatisticsImpl) o;

    if (count != that.count) {
      return false;
    }
    if (hasNull != that.hasNull) {
      return false;
    }
    return bytesOnDisk == that.bytesOnDisk;
  }

  @Override
  public int hashCode() {
    int result = (int) (count ^ (count >>> 32));
    result = 31 * result + (hasNull ? 1 : 0);
    return result;
  }

  private static final class BooleanStatisticsImpl extends ColumnStatisticsImpl
      implements BooleanColumnStatistics {
    private long trueCount = 0;

    BooleanStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.BucketStatistics bkt = stats.getBucketStatistics();
      trueCount = bkt.getCount(0);
    }

    BooleanStatisticsImpl() {
    }

    @Override
    public void reset() {
      super.reset();
      trueCount = 0;
    }

    @Override
    public void updateBoolean(boolean value, int repetitions) {
      if (value) {
        trueCount += repetitions;
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof BooleanStatisticsImpl) {
        BooleanStatisticsImpl bkt = (BooleanStatisticsImpl) other;
        trueCount += bkt.trueCount;
      } else {
        if (isStatsExists() && trueCount != 0) {
          throw new IllegalArgumentException("Incompatible merging of boolean column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.BucketStatistics.Builder bucket =
          OrcProto.BucketStatistics.newBuilder();
      bucket.addCount(trueCount);
      builder.setBucketStatistics(bucket);
      return builder;
    }

    @Override
    public long getFalseCount() {
      return getNumberOfValues() - trueCount;
    }

    @Override
    public long getTrueCount() {
      return trueCount;
    }

    @Override
    public String toString() {
      return super.toString() + " true: " + trueCount;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BooleanStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      BooleanStatisticsImpl that = (BooleanStatisticsImpl) o;

      return trueCount == that.trueCount;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (trueCount ^ (trueCount >>> 32));
      return result;
    }
  }

  /**
   * Column statistics for List and Map types.
   */
  private static final class CollectionColumnStatisticsImpl extends ColumnStatisticsImpl
      implements CollectionColumnStatistics {

    protected long minimum = Long.MAX_VALUE;
    protected long maximum = 0;
    protected long sum = 0;

    CollectionColumnStatisticsImpl() {
      super();
    }

    CollectionColumnStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.CollectionStatistics collStat = stats.getCollectionStatistics();

      minimum = collStat.hasMinChildren() ? collStat.getMinChildren() : Long.MAX_VALUE;
      maximum = collStat.hasMaxChildren() ? collStat.getMaxChildren() : 0;
      sum = collStat.hasTotalChildren() ? collStat.getTotalChildren() : 0;
    }

    @Override
    public void updateCollectionLength(final long length) {
      /*
       * Here, minimum = minCollectionLength
       * maximum = maxCollectionLength
       * sum = childCount
       */
      if (length < minimum) {
        minimum = length;
      }
      if (length > maximum) {
        maximum = length;
      }

      this.sum += length;
    }

    @Override
    public void reset() {
      super.reset();
      minimum = Long.MAX_VALUE;
      maximum = 0;
      sum = 0;
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof CollectionColumnStatisticsImpl) {
        CollectionColumnStatisticsImpl otherColl = (CollectionColumnStatisticsImpl) other;

        if(count == 0) {
          minimum = otherColl.minimum;
          maximum = otherColl.maximum;
        } else {
          if (otherColl.minimum < minimum) {
            minimum = otherColl.minimum;
          }
          if (otherColl.maximum > maximum) {
            maximum = otherColl.maximum;
          }
        }
        sum += otherColl.sum;
      } else {
        if (isStatsExists()) {
          throw new IllegalArgumentException("Incompatible merging of collection column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public long getMinimumChildren() {
      return minimum;
    }

    @Override
    public long getMaximumChildren() {
      return maximum;
    }

    @Override
    public long getTotalChildren() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (count != 0) {
        buf.append(" minChildren: ");
        buf.append(minimum);
        buf.append(" maxChildren: ");
        buf.append(maximum);
        if (sum != 0) {
          buf.append(" totalChildren: ");
          buf.append(sum);
        }
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CollectionColumnStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      CollectionColumnStatisticsImpl that = (CollectionColumnStatisticsImpl) o;

      if (minimum != that.minimum) {
        return false;
      }
      if (maximum != that.maximum) {
        return false;
      }
      return sum == that.sum;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (count != 0 ? (int) (minimum ^ (minimum >>> 32)): 0) ;
      result = 31 * result + (count != 0 ? (int) (maximum ^ (maximum >>> 32)): 0);
      result = 31 * result + (sum != 0 ? (int) (sum ^ (sum >>> 32)): 0);
      return result;
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.CollectionStatistics.Builder collectionStats =
          OrcProto.CollectionStatistics.newBuilder();
      if (count != 0) {
        collectionStats.setMinChildren(minimum);
        collectionStats.setMaxChildren(maximum);
      }
      if (sum != 0) {
        collectionStats.setTotalChildren(sum);
      }
      builder.setCollectionStatistics(collectionStats);
      return builder;
    }
  }

  /**
   * Implementation of IntegerColumnStatistics
   */
  private static final class IntegerStatisticsImpl extends ColumnStatisticsImpl
      implements IntegerColumnStatistics {

    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;
    private long sum = 0;
    private boolean hasMinimum = false;
    private boolean overflow = false;

    IntegerStatisticsImpl() {
    }

    IntegerStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.IntegerStatistics intStat = stats.getIntStatistics();
      if (intStat.hasMinimum()) {
        hasMinimum = true;
        minimum = intStat.getMinimum();
      }
      if (intStat.hasMaximum()) {
        maximum = intStat.getMaximum();
      }
      if (intStat.hasSum()) {
        sum = intStat.getSum();
      } else {
        overflow = true;
      }
    }

    @Override
    public void reset() {
      super.reset();
      hasMinimum = false;
      minimum = Long.MAX_VALUE;
      maximum = Long.MIN_VALUE;
      sum = 0;
      overflow = false;
    }

    @Override
    public void updateInteger(long value, int repetitions) {
      if (!hasMinimum) {
        hasMinimum = true;
        minimum = value;
        maximum = value;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      if (!overflow) {
        try {
          long increment = repetitions > 1
              ? Math.multiplyExact(value, repetitions)
              : value;
          sum = Math.addExact(sum, increment);
        } catch (ArithmeticException e) {
          overflow = true;
        }
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof IntegerStatisticsImpl) {
        IntegerStatisticsImpl otherInt = (IntegerStatisticsImpl) other;
        if (!hasMinimum) {
          hasMinimum = otherInt.hasMinimum;
          minimum = otherInt.minimum;
          maximum = otherInt.maximum;
        } else if (otherInt.hasMinimum) {
          if (otherInt.minimum < minimum) {
            minimum = otherInt.minimum;
          }
          if (otherInt.maximum > maximum) {
            maximum = otherInt.maximum;
          }
        }

        overflow |= otherInt.overflow;
        if (!overflow) {
          try {
            sum = Math.addExact(sum, otherInt.sum);
          } catch (ArithmeticException e) {
            overflow = true;
          }
        }
      } else {
        if (isStatsExists() && hasMinimum) {
          throw new IllegalArgumentException("Incompatible merging of integer column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.IntegerStatistics.Builder intb =
          OrcProto.IntegerStatistics.newBuilder();
      if (hasMinimum) {
        intb.setMinimum(minimum);
        intb.setMaximum(maximum);
      }
      if (!overflow) {
        intb.setSum(sum);
      }
      builder.setIntStatistics(intb);
      return builder;
    }

    @Override
    public long getMinimum() {
      return minimum;
    }

    @Override
    public long getMaximum() {
      return maximum;
    }

    @Override
    public boolean isSumDefined() {
      return !overflow;
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (hasMinimum) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
      }
      if (!overflow) {
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IntegerStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      IntegerStatisticsImpl that = (IntegerStatisticsImpl) o;

      if (minimum != that.minimum) {
        return false;
      }
      if (maximum != that.maximum) {
        return false;
      }
      if (sum != that.sum) {
        return false;
      }
      if (hasMinimum != that.hasMinimum) {
        return false;
      }
      return overflow == that.overflow;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (minimum ^ (minimum >>> 32));
      result = 31 * result + (int) (maximum ^ (maximum >>> 32));
      result = 31 * result + (int) (sum ^ (sum >>> 32));
      result = 31 * result + (hasMinimum ? 1 : 0);
      result = 31 * result + (overflow ? 1 : 0);
      return result;
    }
  }

  private static final class DoubleStatisticsImpl extends ColumnStatisticsImpl
       implements DoubleColumnStatistics {
    private boolean hasMinimum = false;
    private double minimum = Double.MAX_VALUE;
    private double maximum = Double.MIN_VALUE;
    private double sum = 0;

    DoubleStatisticsImpl() {
    }

    DoubleStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DoubleStatistics dbl = stats.getDoubleStatistics();
      if (dbl.hasMinimum()) {
        hasMinimum = true;
        minimum = dbl.getMinimum();
      }
      if (dbl.hasMaximum()) {
        maximum = dbl.getMaximum();
      }
      if (dbl.hasSum()) {
        sum = dbl.getSum();
      }
    }

    @Override
    public void reset() {
      super.reset();
      hasMinimum = false;
      minimum = Double.MAX_VALUE;
      maximum = Double.MIN_VALUE;
      sum = 0;
    }

    @Override
    public void updateDouble(double value) {
      if (!hasMinimum) {
        hasMinimum = true;
        minimum = value;
        maximum = value;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      sum += value;
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof DoubleStatisticsImpl) {
        DoubleStatisticsImpl dbl = (DoubleStatisticsImpl) other;
        if (!hasMinimum) {
          hasMinimum = dbl.hasMinimum;
          minimum = dbl.minimum;
          maximum = dbl.maximum;
        } else if (dbl.hasMinimum) {
          if (dbl.minimum < minimum) {
            minimum = dbl.minimum;
          }
          if (dbl.maximum > maximum) {
            maximum = dbl.maximum;
          }
        }
        sum += dbl.sum;
      } else {
        if (isStatsExists() && hasMinimum) {
          throw new IllegalArgumentException("Incompatible merging of double column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.DoubleStatistics.Builder dbl =
          OrcProto.DoubleStatistics.newBuilder();
      if (hasMinimum) {
        dbl.setMinimum(minimum);
        dbl.setMaximum(maximum);
      }
      dbl.setSum(sum);
      builder.setDoubleStatistics(dbl);
      return builder;
    }

    @Override
    public double getMinimum() {
      return minimum;
    }

    @Override
    public double getMaximum() {
      return maximum;
    }

    @Override
    public double getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (hasMinimum) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
      }
      buf.append(" sum: ");
      buf.append(sum);
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DoubleStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      DoubleStatisticsImpl that = (DoubleStatisticsImpl) o;

      if (hasMinimum != that.hasMinimum) {
        return false;
      }
      if (Double.compare(that.minimum, minimum) != 0) {
        return false;
      }
      if (Double.compare(that.maximum, maximum) != 0) {
        return false;
      }
      return Double.compare(that.sum, sum) == 0;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      long temp;
      result = 31 * result + (hasMinimum ? 1 : 0);
      temp = Double.doubleToLongBits(minimum);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      temp = Double.doubleToLongBits(maximum);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      temp = Double.doubleToLongBits(sum);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      return result;
    }
  }

  protected static final class StringStatisticsImpl extends ColumnStatisticsImpl
      implements StringColumnStatistics {
    public static final int MAX_BYTES_RECORDED = 1024;
    private Text minimum = null;
    private Text maximum = null;
    private long sum = 0;

    private boolean isLowerBoundSet = false;
    private boolean isUpperBoundSet = false;

    StringStatisticsImpl() {
    }

    StringStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.StringStatistics str = stats.getStringStatistics();
      if (str.hasMaximum()) {
        maximum = new Text(str.getMaximum());
      } else if (str.hasUpperBound()) {
        maximum = new Text(str.getUpperBound());
        isUpperBoundSet = true;
      }
      if (str.hasMinimum()) {
        minimum = new Text(str.getMinimum());
      } else if (str.hasLowerBound()) {
        minimum = new Text(str.getLowerBound());
        isLowerBoundSet = true;
      }
      if(str.hasSum()) {
        sum = str.getSum();
      }
    }

    @Override
    public void reset() {
      super.reset();
      minimum = null;
      maximum = null;
      isLowerBoundSet = false;
      isUpperBoundSet = false;
      sum = 0;
    }

    @Override
    public void updateString(Text value) {
      updateString(value.getBytes(), 0, value.getLength(), 1);
    }

    @Override
    public void updateString(byte[] bytes, int offset, int length,
                             int repetitions) {
      if (minimum == null) {
        if(length > MAX_BYTES_RECORDED) {
          minimum = truncateLowerBound(bytes, offset);
          maximum = truncateUpperBound(bytes, offset);
          isLowerBoundSet = true;
          isUpperBoundSet = true;
        } else {
          maximum = minimum = new Text();
          maximum.set(bytes, offset, length);
          isLowerBoundSet = false;
          isUpperBoundSet = false;
        }
      } else if (WritableComparator.compareBytes(minimum.getBytes(), 0,
          minimum.getLength(), bytes, offset, length) > 0) {
        if(length > MAX_BYTES_RECORDED) {
          minimum = truncateLowerBound(bytes, offset);
          isLowerBoundSet = true;
        } else {
          minimum = new Text();
          minimum.set(bytes, offset, length);
          isLowerBoundSet = false;
        }
      } else if (WritableComparator.compareBytes(maximum.getBytes(), 0,
          maximum.getLength(), bytes, offset, length) < 0) {
        if(length > MAX_BYTES_RECORDED) {
          maximum = truncateUpperBound(bytes, offset);
          isUpperBoundSet = true;
        } else {
          maximum = new Text();
          maximum.set(bytes, offset, length);
          isUpperBoundSet = false;
        }
      }
      sum += (long)length * repetitions;
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof StringStatisticsImpl) {
        StringStatisticsImpl str = (StringStatisticsImpl) other;
        if (count == 0) {
          if (str.count != 0) {
            minimum = new Text(str.minimum);
            isLowerBoundSet = str.isLowerBoundSet;
            maximum = new Text(str.maximum);
            isUpperBoundSet = str.isUpperBoundSet;
          } else {
            /* both are empty */
            maximum = minimum = null;
            isLowerBoundSet = false;
            isUpperBoundSet = false;
          }
        } else if (str.count != 0) {
          if (minimum.compareTo(str.minimum) > 0) {
            minimum = new Text(str.minimum);
            isLowerBoundSet = str.isLowerBoundSet;
          }
          if (maximum.compareTo(str.maximum) < 0) {
            maximum = new Text(str.maximum);
            isUpperBoundSet = str.isUpperBoundSet;
          }
        }
        sum += str.sum;
      } else {
        if (isStatsExists()) {
          throw new IllegalArgumentException("Incompatible merging of string column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.StringStatistics.Builder str =
          OrcProto.StringStatistics.newBuilder();
      if (getNumberOfValues() != 0) {
        if (isLowerBoundSet) {
          str.setLowerBound(minimum.toString());
        } else {
          str.setMinimum(minimum.toString());
        }
        if (isUpperBoundSet) {
          str.setUpperBound(maximum.toString());
        } else {
          str.setMaximum(maximum.toString());
        }
        str.setSum(sum);
      }
      result.setStringStatistics(str);
      return result;
    }

    @Override
    public String getMinimum() {
      /* if we have lower bound set (in case of truncation)
      getMinimum will be null */
      if(isLowerBoundSet) {
        return null;
      } else {
        return minimum == null ? null : minimum.toString();
      }
    }

    @Override
    public String getMaximum() {
      /* if we have upper bound set (in case of truncation)
      getMaximum will be null */
      if(isUpperBoundSet) {
        return null;
      } else {
        return maximum == null ? null : maximum.toString();
      }
    }

    /**
     * Get the string with
     * length = Min(StringStatisticsImpl.MAX_BYTES_RECORDED, getMinimum())
     *
     * @return lower bound
     */
    @Override
    public String getLowerBound() {
      return minimum == null ? null : minimum.toString();
    }

    /**
     * Get the string with
     * length = Min(StringStatisticsImpl.MAX_BYTES_RECORDED, getMaximum())
     *
     * @return upper bound
     */
    @Override
    public String getUpperBound() {
      return maximum == null ? null : maximum.toString();
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (minimum != null) {
        if (isLowerBoundSet) {
          buf.append(" lower: ");
        } else {
          buf.append(" min: ");
        }
        buf.append(getLowerBound());
        if (isUpperBoundSet) {
          buf.append(" upper: ");
        } else {
          buf.append(" max: ");
        }
        buf.append(getUpperBound());
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StringStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      StringStatisticsImpl that = (StringStatisticsImpl) o;

      if (sum != that.sum) {
        return false;
      }
      if (minimum != null ? !minimum.equals(that.minimum) : that.minimum != null) {
        return false;
      }
      return maximum != null ? maximum.equals(that.maximum) : that.maximum == null;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (minimum != null ? minimum.hashCode() : 0);
      result = 31 * result + (maximum != null ? maximum.hashCode() : 0);
      result = 31 * result + (int) (sum ^ (sum >>> 32));
      return result;
    }

    private static void appendCodePoint(Text result, int codepoint) {
      if (codepoint < 0 || codepoint > 0x1f_ffff) {
        throw new IllegalArgumentException("Codepoint out of range " +
            codepoint);
      }
      byte[] buffer = new byte[4];
      if (codepoint < 0x7f) {
        buffer[0] = (byte) codepoint;
        result.append(buffer, 0, 1);
      } else if (codepoint <= 0x7ff) {
        buffer[0] = (byte) (0xc0 | (codepoint >> 6));
        buffer[1] = (byte) (0x80 | (codepoint & 0x3f));
        result.append(buffer, 0 , 2);
      } else if (codepoint < 0xffff) {
        buffer[0] = (byte) (0xe0 | (codepoint >> 12));
        buffer[1] = (byte) (0x80 | ((codepoint >> 6) & 0x3f));
        buffer[2] = (byte) (0x80 | (codepoint & 0x3f));
        result.append(buffer, 0, 3);
      } else {
        buffer[0] = (byte) (0xf0 | (codepoint >> 18));
        buffer[1] = (byte) (0x80 | ((codepoint >> 12) & 0x3f));
        buffer[2] = (byte) (0x80 | ((codepoint >> 6) & 0x3f));
        buffer[3] = (byte) (0x80 | (codepoint & 0x3f));
        result.append(buffer, 0, 4);
      }
    }

    /**
     * Create a text that is truncated to at most MAX_BYTES_RECORDED at a
     * character boundary with the last code point incremented by 1.
     * The length is assumed to be greater than MAX_BYTES_RECORDED.
     * @param text the text to truncate
     * @param from the index of the first character
     * @return truncated Text value
     */
    private static Text truncateUpperBound(final byte[] text, final int from) {
      int followingChar = Utf8Utils.findLastCharacter(text, from,
          from + MAX_BYTES_RECORDED);
      int lastChar = Utf8Utils.findLastCharacter(text, from, followingChar - 1);
      Text result = new Text();
      result.set(text, from, lastChar - from);
      appendCodePoint(result,
          Utf8Utils.getCodePoint(text, lastChar, followingChar - lastChar) + 1);
      return result;
    }

    /**
     * Create a text that is truncated to at most MAX_BYTES_RECORDED at a
     * character boundary.
     * The length is assumed to be greater than MAX_BYTES_RECORDED.
     * @param text Byte array to truncate
     * @param from This is the index of the first character
     * @return truncated {@link Text}
     */
    private static Text truncateLowerBound(final byte[] text, final int from) {

      int lastChar = Utf8Utils.findLastCharacter(text, from,
          from + MAX_BYTES_RECORDED);
      Text result = new Text();
      result.set(text, from, lastChar - from);
      return result;
    }
  }

  protected static final class BinaryStatisticsImpl extends ColumnStatisticsImpl implements
      BinaryColumnStatistics {

    private long sum = 0;

    BinaryStatisticsImpl() {
    }

    BinaryStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.BinaryStatistics binStats = stats.getBinaryStatistics();
      if (binStats.hasSum()) {
        sum = binStats.getSum();
      }
    }

    @Override
    public void reset() {
      super.reset();
      sum = 0;
    }

    @Override
    public void updateBinary(BytesWritable value) {
      sum += value.getLength();
    }

    @Override
    public void updateBinary(byte[] bytes, int offset, int length,
                             int repetitions) {
      sum += (long)length * repetitions;
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof BinaryColumnStatistics) {
        BinaryStatisticsImpl bin = (BinaryStatisticsImpl) other;
        sum += bin.sum;
      } else {
        if (isStatsExists() && sum != 0) {
          throw new IllegalArgumentException("Incompatible merging of binary column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.BinaryStatistics.Builder bin = OrcProto.BinaryStatistics.newBuilder();
      bin.setSum(sum);
      result.setBinaryStatistics(bin);
      return result;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BinaryStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      BinaryStatisticsImpl that = (BinaryStatisticsImpl) o;

      return sum == that.sum;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (sum ^ (sum >>> 32));
      return result;
    }
  }

  private static final class DecimalStatisticsImpl extends ColumnStatisticsImpl
      implements DecimalColumnStatistics {

    // These objects are mutable for better performance.
    private HiveDecimalWritable minimum = null;
    private HiveDecimalWritable maximum = null;
    private HiveDecimalWritable sum = new HiveDecimalWritable(0);

    DecimalStatisticsImpl() {
    }

    DecimalStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DecimalStatistics dec = stats.getDecimalStatistics();
      if (dec.hasMaximum()) {
        maximum = new HiveDecimalWritable(dec.getMaximum());
      }
      if (dec.hasMinimum()) {
        minimum = new HiveDecimalWritable(dec.getMinimum());
      }
      if (dec.hasSum()) {
        sum = new HiveDecimalWritable(dec.getSum());
      } else {
        sum = null;
      }
    }

    @Override
    public void reset() {
      super.reset();
      minimum = null;
      maximum = null;
      sum = new HiveDecimalWritable(0);
    }

    @Override
    public void updateDecimal(HiveDecimalWritable value) {
      if (minimum == null) {
        minimum = new HiveDecimalWritable(value);
        maximum = new HiveDecimalWritable(value);
      } else if (minimum.compareTo(value) > 0) {
        minimum.set(value);
      } else if (maximum.compareTo(value) < 0) {
        maximum.set(value);
      }
      if (sum != null) {
        sum.mutateAdd(value);
      }
    }

    @Override
    public void updateDecimal64(long value, int scale) {
      HiveDecimalWritable dValue = new HiveDecimalWritable();
      dValue.setFromLongAndScale(value, scale);
      updateDecimal(dValue);
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof DecimalStatisticsImpl) {
        DecimalStatisticsImpl dec = (DecimalStatisticsImpl) other;
        if (minimum == null) {
          minimum = (dec.minimum != null ? new HiveDecimalWritable(dec.minimum) : null);
          maximum = (dec.maximum != null ? new HiveDecimalWritable(dec.maximum) : null);
          sum = dec.sum;
        } else if (dec.minimum != null) {
          if (minimum.compareTo(dec.minimum) > 0) {
            minimum.set(dec.minimum);
          }
          if (maximum.compareTo(dec.maximum) < 0) {
            maximum.set(dec.maximum);
          }
          if (sum == null || dec.sum == null) {
            sum = null;
          } else {
            sum.mutateAdd(dec.sum);
          }
        }
      } else {
        if (isStatsExists() && minimum != null) {
          throw new IllegalArgumentException("Incompatible merging of decimal column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.DecimalStatistics.Builder dec =
          OrcProto.DecimalStatistics.newBuilder();
      if (getNumberOfValues() != 0 && minimum != null) {
        dec.setMinimum(minimum.toString());
        dec.setMaximum(maximum.toString());
      }
      // Check isSet for overflow.
      if (sum != null && sum.isSet()) {
        dec.setSum(sum.toString());
      }
      result.setDecimalStatistics(dec);
      return result;
    }

    @Override
    public HiveDecimal getMinimum() {
      return minimum == null ? null : minimum.getHiveDecimal();
    }

    @Override
    public HiveDecimal getMaximum() {
      return maximum == null ? null : maximum.getHiveDecimal();
    }

    @Override
    public HiveDecimal getSum() {
      return sum == null ? null : sum.getHiveDecimal();
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
        if (sum != null) {
          buf.append(" sum: ");
          buf.append(sum);
        }
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DecimalStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      DecimalStatisticsImpl that = (DecimalStatisticsImpl) o;

      if (minimum != null ? !minimum.equals(that.minimum) : that.minimum != null) {
        return false;
      }
      if (maximum != null ? !maximum.equals(that.maximum) : that.maximum != null) {
        return false;
      }
      return sum != null ? sum.equals(that.sum) : that.sum == null;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (minimum != null ? minimum.hashCode() : 0);
      result = 31 * result + (maximum != null ? maximum.hashCode() : 0);
      result = 31 * result + (sum != null ? sum.hashCode() : 0);
      return result;
    }
  }

  private static final class Decimal64StatisticsImpl extends ColumnStatisticsImpl
      implements DecimalColumnStatistics {

    private final int scale;
    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;
    private boolean hasSum = true;
    private long sum = 0;
    private final HiveDecimalWritable scratch = new HiveDecimalWritable();

    Decimal64StatisticsImpl(int scale) {
      this.scale = scale;
    }

    Decimal64StatisticsImpl(int scale, OrcProto.ColumnStatistics stats) {
      super(stats);
      this.scale = scale;
      OrcProto.DecimalStatistics dec = stats.getDecimalStatistics();
      if (dec.hasMaximum()) {
        maximum = new HiveDecimalWritable(dec.getMaximum()).serialize64(scale);
      } else {
        maximum = Long.MIN_VALUE;
      }
      if (dec.hasMinimum()) {
        minimum = new HiveDecimalWritable(dec.getMinimum()).serialize64(scale);
      } else {
        minimum = Long.MAX_VALUE;
      }
      if (dec.hasSum()) {
        hasSum = true;
        HiveDecimalWritable sumTmp = new HiveDecimalWritable(dec.getSum());
        if (sumTmp.getHiveDecimal().integerDigitCount() + scale <=
            TypeDescription.MAX_DECIMAL64_PRECISION) {
          hasSum = true;
          sum = sumTmp.serialize64(scale);
          return;
        }
      }
      hasSum = false;
    }

    @Override
    public void reset() {
      super.reset();
      minimum = Long.MAX_VALUE;
      maximum = Long.MIN_VALUE;
      hasSum = true;
      sum = 0;
    }

    @Override
    public void updateDecimal(HiveDecimalWritable value) {
      updateDecimal64(value.serialize64(scale), scale);
    }

    @Override
    public void updateDecimal64(long value, int valueScale) {
      // normalize the scale to our desired level
      while (valueScale != scale) {
        if (valueScale > scale) {
          value /= 10;
          valueScale -= 1;
        } else {
          value *= 10;
          valueScale += 1;
        }
      }
      if (value < TypeDescription.MIN_DECIMAL64 ||
          value > TypeDescription.MAX_DECIMAL64) {
        throw new IllegalArgumentException("Out of bounds decimal64 " + value);
      }
      if (minimum > value) {
        minimum = value;
      }
      if (maximum < value) {
        maximum = value;
      }
      if (hasSum) {
        sum += value;
        hasSum = sum <= TypeDescription.MAX_DECIMAL64 &&
              sum >= TypeDescription.MIN_DECIMAL64;
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof Decimal64StatisticsImpl) {
        Decimal64StatisticsImpl dec = (Decimal64StatisticsImpl) other;
        if (getNumberOfValues() == 0) {
          minimum = dec.minimum;
          maximum = dec.maximum;
          sum = dec.sum;
        } else {
          if (minimum > dec.minimum) {
            minimum = dec.minimum;
          }
          if (maximum < dec.maximum) {
            maximum = dec.maximum;
          }
          if (hasSum && dec.hasSum) {
            sum += dec.sum;
            hasSum = sum <= TypeDescription.MAX_DECIMAL64 &&
                  sum >= TypeDescription.MIN_DECIMAL64;
          } else {
            hasSum = false;
          }
        }
      } else {
        if (other.getNumberOfValues() != 0) {
          throw new IllegalArgumentException("Incompatible merging of decimal column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.DecimalStatistics.Builder dec =
          OrcProto.DecimalStatistics.newBuilder();
      if (getNumberOfValues() != 0) {
        scratch.setFromLongAndScale(minimum, scale);
        dec.setMinimum(scratch.toString());
        scratch.setFromLongAndScale(maximum, scale);
        dec.setMaximum(scratch.toString());
      }
      // Check hasSum for overflow.
      if (hasSum) {
        scratch.setFromLongAndScale(sum, scale);
        dec.setSum(scratch.toString());
      }
      result.setDecimalStatistics(dec);
      return result;
    }

    @Override
    public HiveDecimal getMinimum() {
      if (getNumberOfValues() > 0) {
        scratch.setFromLongAndScale(minimum, scale);
        return scratch.getHiveDecimal();
      }
      return null;
    }

    @Override
    public HiveDecimal getMaximum() {
      if (getNumberOfValues() > 0) {
        scratch.setFromLongAndScale(maximum, scale);
        return scratch.getHiveDecimal();
      }
      return null;
    }

    @Override
    public HiveDecimal getSum() {
      if (hasSum) {
        scratch.setFromLongAndScale(sum, scale);
        return scratch.getHiveDecimal();
      }
      return null;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(getMinimum());
        buf.append(" max: ");
        buf.append(getMaximum());
        if (hasSum) {
          buf.append(" sum: ");
          buf.append(getSum());
        }
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Decimal64StatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      Decimal64StatisticsImpl that = (Decimal64StatisticsImpl) o;

      if (minimum != that.minimum ||
          maximum != that.maximum ||
          hasSum != that.hasSum) {
        return false;
      }
      return !hasSum || (sum == that.sum);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      boolean hasValues = getNumberOfValues() > 0;
      result = 31 * result + (hasValues ? (int) minimum : 0);
      result = 31 * result + (hasValues ? (int) maximum : 0);
      result = 31 * result + (hasSum ? (int) sum : 0);
      return result;
    }
  }

  private static final class DateStatisticsImpl extends ColumnStatisticsImpl
      implements DateColumnStatistics {
    private int minimum = Integer.MAX_VALUE;
    private int maximum = Integer.MIN_VALUE;
    private final Chronology chronology;

    static Chronology getInstance(boolean proleptic) {
      return proleptic ? IsoChronology.INSTANCE : HybridChronology.INSTANCE;
    }

    DateStatisticsImpl(boolean convertToProleptic) {
      this.chronology = getInstance(convertToProleptic);
    }

    DateStatisticsImpl(OrcProto.ColumnStatistics stats,
                       boolean writerUsedProlepticGregorian,
                       boolean convertToProlepticGregorian) {
      super(stats);
      this.chronology = getInstance(convertToProlepticGregorian);
      OrcProto.DateStatistics dateStats = stats.getDateStatistics();
      // min,max values serialized/deserialized as int (days since epoch)
      if (dateStats.hasMaximum()) {
        maximum = DateUtils.convertDate(dateStats.getMaximum(),
            writerUsedProlepticGregorian, convertToProlepticGregorian);
      }
      if (dateStats.hasMinimum()) {
        minimum = DateUtils.convertDate(dateStats.getMinimum(),
            writerUsedProlepticGregorian, convertToProlepticGregorian);
      }
    }

    @Override
    public void reset() {
      super.reset();
      minimum = Integer.MAX_VALUE;
      maximum = Integer.MIN_VALUE;
    }

    @Override
    public void updateDate(DateWritable value) {
      if (minimum > value.getDays()) {
        minimum = value.getDays();
      }
      if (maximum < value.getDays()) {
        maximum = value.getDays();
      }
    }

    @Override
    public void updateDate(int value) {
      if (minimum > value) {
        minimum = value;
      }
      if (maximum < value) {
        maximum = value;
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof DateStatisticsImpl) {
        DateStatisticsImpl dateStats = (DateStatisticsImpl) other;
        minimum = Math.min(minimum, dateStats.minimum);
        maximum = Math.max(maximum, dateStats.maximum);
      } else {
        if (isStatsExists() && count != 0) {
          throw new IllegalArgumentException("Incompatible merging of date column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.DateStatistics.Builder dateStats =
          OrcProto.DateStatistics.newBuilder();
      if (count != 0) {
        dateStats.setMinimum(minimum);
        dateStats.setMaximum(maximum);
      }
      result.setDateStatistics(dateStats);
      return result;
    }

    @Override
    public ChronoLocalDate getMinimumLocalDate() {
      return count == 0 ? null : chronology.dateEpochDay(minimum);
    }

    @Override
    public long getMinimumDayOfEpoch() {
      return minimum;
    }

    @Override
    public ChronoLocalDate getMaximumLocalDate() {
      return count == 0 ? null : chronology.dateEpochDay(maximum);
    }

    @Override
    public long getMaximumDayOfEpoch() {
      return maximum;
    }

    @Override
    public Date getMinimum() {
      if (count == 0) {
        return null;
      }
      DateWritable minDate = new DateWritable(minimum);
      return minDate.get();
    }

    @Override
    public Date getMaximum() {
      if (count == 0) {
        return null;
      }
      DateWritable maxDate = new DateWritable(maximum);
      return maxDate.get();
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(getMinimumLocalDate());
        buf.append(" max: ");
        buf.append(getMaximumLocalDate());
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DateStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      DateStatisticsImpl that = (DateStatisticsImpl) o;

      if (minimum != that.minimum) {
        return false;
      }
      return maximum == that.maximum;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + minimum;
      result = 31 * result + maximum;
      return result;
    }
  }

  private static class TimestampStatisticsImpl extends ColumnStatisticsImpl
      implements TimestampColumnStatistics {
    private static final int DEFAULT_MIN_NANOS = 000_000;
    private static final int DEFAULT_MAX_NANOS = 999_999;

    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;

    private int minNanos = DEFAULT_MIN_NANOS;
    private int maxNanos = DEFAULT_MAX_NANOS;

    TimestampStatisticsImpl() {
    }

    TimestampStatisticsImpl(OrcProto.ColumnStatistics stats,
                            boolean writerUsedProlepticGregorian,
                            boolean convertToProlepticGregorian) {
      super(stats);
      OrcProto.TimestampStatistics timestampStats = stats.getTimestampStatistics();
      // min,max values serialized/deserialized as int (milliseconds since epoch)
      if (timestampStats.hasMaximum()) {
        maximum = DateUtils.convertTime(
            SerializationUtils.convertToUtc(TimeZone.getDefault(),
               timestampStats.getMaximum()),
            writerUsedProlepticGregorian, convertToProlepticGregorian, true);
      }
      if (timestampStats.hasMinimum()) {
        minimum = DateUtils.convertTime(
            SerializationUtils.convertToUtc(TimeZone.getDefault(),
                timestampStats.getMinimum()),
            writerUsedProlepticGregorian, convertToProlepticGregorian, true);
      }
      if (timestampStats.hasMaximumUtc()) {
        maximum = DateUtils.convertTime(timestampStats.getMaximumUtc(),
            writerUsedProlepticGregorian, convertToProlepticGregorian, true);
      }
      if (timestampStats.hasMinimumUtc()) {
        minimum = DateUtils.convertTime(timestampStats.getMinimumUtc(),
            writerUsedProlepticGregorian, convertToProlepticGregorian, true);
      }
      if (timestampStats.hasMaximumNanos()) {
        maxNanos = timestampStats.getMaximumNanos() - 1;
      }
      if (timestampStats.hasMinimumNanos()) {
        minNanos = timestampStats.getMinimumNanos() - 1;
      }
    }

    @Override
    public void reset() {
      super.reset();
      minimum = Long.MAX_VALUE;
      maximum = Long.MIN_VALUE;
      minNanos = DEFAULT_MIN_NANOS;
      maxNanos = DEFAULT_MAX_NANOS;
    }

    @Override
    public void updateTimestamp(Timestamp value) {
      long millis = SerializationUtils.convertToUtc(TimeZone.getDefault(),
          value.getTime());
      // prune the last 6 digits for ns precision
      updateTimestamp(millis, value.getNanos() % 1_000_000);
    }

    @Override
    public void updateTimestamp(long value, int nanos) {
      if (minimum > maximum) {
        minimum = value;
        maximum = value;
        minNanos = nanos;
        maxNanos = nanos;
      } else {
        if (minimum >= value) {
          if (minimum > value || nanos < minNanos) {
            minNanos = nanos;
          }
          minimum = value;
        }
        if (maximum <= value) {
          if (maximum < value || nanos > maxNanos) {
            maxNanos = nanos;
          }
          maximum = value;
        }
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof TimestampStatisticsImpl) {
        TimestampStatisticsImpl timestampStats = (TimestampStatisticsImpl) other;
        if (count == 0) {
          if (timestampStats.count != 0) {
            minimum = timestampStats.minimum;
            maximum = timestampStats.maximum;
            minNanos = timestampStats.minNanos;
            maxNanos = timestampStats.maxNanos;
          }
        } else if (timestampStats.count != 0) {
          if (minimum >= timestampStats.minimum) {
            if (minimum > timestampStats.minimum ||
                minNanos > timestampStats.minNanos) {
              minNanos = timestampStats.minNanos;
            }
            minimum = timestampStats.minimum;
          }
          if (maximum <= timestampStats.maximum) {
            if (maximum < timestampStats.maximum ||
                maxNanos < timestampStats.maxNanos) {
              maxNanos = timestampStats.maxNanos;
            }
            maximum = timestampStats.maximum;
          }
        }
      } else {
        if (isStatsExists() && count != 0) {
          throw new IllegalArgumentException("Incompatible merging of timestamp column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.TimestampStatistics.Builder timestampStats = OrcProto.TimestampStatistics
          .newBuilder();
      if (getNumberOfValues() != 0) {
        timestampStats.setMinimumUtc(minimum);
        timestampStats.setMaximumUtc(maximum);
        if (minNanos != DEFAULT_MIN_NANOS) {
          timestampStats.setMinimumNanos(minNanos + 1);
        }
        if (maxNanos != DEFAULT_MAX_NANOS) {
          timestampStats.setMaximumNanos(maxNanos + 1);
        }
      }
      result.setTimestampStatistics(timestampStats);
      return result;
    }

    @Override
    public Timestamp getMinimum() {
      if (minimum > maximum) {
        return null;
      } else {
        Timestamp ts = new Timestamp(SerializationUtils.
            convertFromUtc(TimeZone.getDefault(), minimum));
        ts.setNanos(ts.getNanos() + minNanos);
        return ts;
      }
    }

    @Override
    public Timestamp getMaximum() {
      if (minimum > maximum) {
        return null;
      } else {
        Timestamp ts = new Timestamp(SerializationUtils.convertFromUtc(
            TimeZone.getDefault(), maximum));
        ts.setNanos(ts.getNanos() + maxNanos);
        return ts;
      }
    }

    @Override
    public Timestamp getMinimumUTC() {
      if (minimum > maximum) {
        return null;
      } else {
        Timestamp ts = new Timestamp(minimum);
        ts.setNanos(ts.getNanos() + minNanos);
        return ts;
      }
    }

    @Override
    public Timestamp getMaximumUTC() {
      if (minimum > maximum) {
        return null;
      } else {
        Timestamp ts = new Timestamp(maximum);
        ts.setNanos(ts.getNanos() + maxNanos);
        return ts;
      }
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (minimum <= maximum) {
        buf.append(" min: ");
        buf.append(getMinimum());
        buf.append(" max: ");
        buf.append(getMaximum());
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TimestampStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      TimestampStatisticsImpl that = (TimestampStatisticsImpl) o;

      return minimum == that.minimum && maximum == that.maximum &&
          minNanos == that.minNanos && maxNanos == that.maxNanos;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int) (maximum ^ (maximum >>> 32));
      result = prime * result + (int) (minimum ^ (minimum >>> 32));
      return result;
    }
  }

  private static final class TimestampInstantStatisticsImpl extends TimestampStatisticsImpl {
    TimestampInstantStatisticsImpl() {
    }

    TimestampInstantStatisticsImpl(OrcProto.ColumnStatistics stats,
                                   boolean writerUsedProlepticGregorian,
                                   boolean convertToProlepticGregorian) {
      super(stats, writerUsedProlepticGregorian, convertToProlepticGregorian);
    }

    @Override
    public void updateTimestamp(Timestamp value) {
      updateTimestamp(value.getTime(), value.getNanos() % 1_000_000);
    }

    @Override
    public Timestamp getMinimum() {
      return getMinimumUTC();
    }

    @Override
    public Timestamp getMaximum() {
      return getMaximumUTC();
    }
  }

  protected long count = 0;
  private boolean hasNull = false;
  private long bytesOnDisk = 0;

  ColumnStatisticsImpl(OrcProto.ColumnStatistics stats) {
    if (stats.hasNumberOfValues()) {
      count = stats.getNumberOfValues();
    }

    bytesOnDisk = stats.hasBytesOnDisk() ? stats.getBytesOnDisk() : 0;

    if (stats.hasHasNull()) {
      hasNull = stats.getHasNull();
    } else {
      hasNull = true;
    }
  }

  ColumnStatisticsImpl() {
  }

  public void increment() {
    count += 1;
  }

  public void increment(int count) {
    this.count += count;
  }

  public void updateByteCount(long size) {
    this.bytesOnDisk += size;
  }

  public void setNull() {
    hasNull = true;
  }

  /**
   * Update the collection length for Map and List type.
   * @param value length of collection
   */
  public void updateCollectionLength(final long value) {
    throw new UnsupportedOperationException(
        "Can't update collection count");
  }

  public void updateBoolean(boolean value, int repetitions) {
    throw new UnsupportedOperationException("Can't update boolean");
  }

  public void updateInteger(long value, int repetitions) {
    throw new UnsupportedOperationException("Can't update integer");
  }

  public void updateDouble(double value) {
    throw new UnsupportedOperationException("Can't update double");
  }

  public void updateString(Text value) {
    throw new UnsupportedOperationException("Can't update string");
  }

  public void updateString(byte[] bytes, int offset, int length,
                           int repetitions) {
    throw new UnsupportedOperationException("Can't update string");
  }

  public void updateBinary(BytesWritable value) {
    throw new UnsupportedOperationException("Can't update binary");
  }

  public void updateBinary(byte[] bytes, int offset, int length,
                           int repetitions) {
    throw new UnsupportedOperationException("Can't update string");
  }

  public void updateDecimal(HiveDecimalWritable value) {
    throw new UnsupportedOperationException("Can't update decimal");
  }

  public void updateDecimal64(long value, int scale) {
    throw new UnsupportedOperationException("Can't update decimal");
  }

  public void updateDate(DateWritable value) {
    throw new UnsupportedOperationException("Can't update date");
  }

  public void updateDate(int value) {
    throw new UnsupportedOperationException("Can't update date");
  }

  public void updateTimestamp(Timestamp value) {
    throw new UnsupportedOperationException("Can't update timestamp");
  }

  // has to be extended
  public void updateTimestamp(long value, int nanos) {
    throw new UnsupportedOperationException("Can't update timestamp");
  }

  public boolean isStatsExists() {
    return (count > 0 || hasNull == true);
  }

  public void merge(ColumnStatisticsImpl stats) {
    count += stats.count;
    hasNull |= stats.hasNull;
    bytesOnDisk += stats.bytesOnDisk;
  }

  public void reset() {
    count = 0;
    bytesOnDisk = 0;
    hasNull = false;
  }

  @Override
  public long getNumberOfValues() {
    return count;
  }

  @Override
  public boolean hasNull() {
    return hasNull;
  }

  /**
   * Get the number of bytes for this column.
   *
   * @return the number of bytes
   */
  @Override
  public long getBytesOnDisk() {
    return bytesOnDisk;
  }

  @Override
  public String toString() {
    return "count: " + count + " hasNull: " + hasNull +
        (bytesOnDisk != 0 ? " bytesOnDisk: " + bytesOnDisk : "");
  }

  public OrcProto.ColumnStatistics.Builder serialize() {
    OrcProto.ColumnStatistics.Builder builder =
        OrcProto.ColumnStatistics.newBuilder();
    builder.setNumberOfValues(count);
    builder.setHasNull(hasNull);
    if (bytesOnDisk != 0) {
      builder.setBytesOnDisk(bytesOnDisk);
    }
    return builder;
  }

  public static ColumnStatisticsImpl create(TypeDescription schema) {
    return create(schema, false);
  }

  public static ColumnStatisticsImpl create(TypeDescription schema,
                                            boolean convertToProleptic) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return new BooleanStatisticsImpl();
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new IntegerStatisticsImpl();
      case LIST:
      case MAP:
        return new CollectionColumnStatisticsImpl();
      case FLOAT:
      case DOUBLE:
        return new DoubleStatisticsImpl();
      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringStatisticsImpl();
      case DECIMAL:
        if (schema.getPrecision() <= TypeDescription.MAX_DECIMAL64_PRECISION) {
          return new Decimal64StatisticsImpl(schema.getScale());
        } else {
          return new DecimalStatisticsImpl();
        }
      case DATE:
        return new DateStatisticsImpl(convertToProleptic);
      case TIMESTAMP:
        return new TimestampStatisticsImpl();
      case TIMESTAMP_INSTANT:
        return new TimestampInstantStatisticsImpl();
      case BINARY:
        return new BinaryStatisticsImpl();
      default:
        return new ColumnStatisticsImpl();
    }
  }

  public static ColumnStatisticsImpl deserialize(TypeDescription schema,
                                                 OrcProto.ColumnStatistics stats) {
    return deserialize(schema, stats, true, true);
  }

  public static ColumnStatisticsImpl deserialize(TypeDescription schema,
                                                 OrcProto.ColumnStatistics stats,
                                                 boolean writerUsedProlepticGregorian,
                                                 boolean convertToProlepticGregorian) {
    if (stats.hasBucketStatistics()) {
      return new BooleanStatisticsImpl(stats);
    } else if (stats.hasIntStatistics()) {
      return new IntegerStatisticsImpl(stats);
    } else if (stats.hasCollectionStatistics()) {
      return new CollectionColumnStatisticsImpl(stats);
    } else if (stats.hasDoubleStatistics()) {
      return new DoubleStatisticsImpl(stats);
    } else if (stats.hasStringStatistics()) {
      return new StringStatisticsImpl(stats);
    } else if (stats.hasDecimalStatistics()) {
      if (schema != null &&
          schema.getPrecision() <= TypeDescription.MAX_DECIMAL64_PRECISION) {
        return new Decimal64StatisticsImpl(schema.getScale(), stats);
      } else {
        return new DecimalStatisticsImpl(stats);
      }
    } else if (stats.hasDateStatistics()) {
      return new DateStatisticsImpl(stats, writerUsedProlepticGregorian,
          convertToProlepticGregorian);
    } else if (stats.hasTimestampStatistics()) {
      return schema == null ||
                 schema.getCategory() == TypeDescription.Category.TIMESTAMP ?
                 new TimestampStatisticsImpl(stats,
                     writerUsedProlepticGregorian, convertToProlepticGregorian) :
                 new TimestampInstantStatisticsImpl(stats,
                     writerUsedProlepticGregorian, convertToProlepticGregorian);
    } else if(stats.hasBinaryStatistics()) {
      return new BinaryStatisticsImpl(stats);
    } else {
      return new ColumnStatisticsImpl(stats);
    }
  }
}
