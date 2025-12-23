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
package org.apache.orc.impl.mask;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.DataMask;
import org.apache.orc.TypeDescription;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Masking strategy that hides most string and numeric values based on unicode
 * character categories.
 * <p>
 * Masking Parameters:
 *   character replacements: string of 10 characters one per group below
 *     letter, upper case (default X)
 *     letter, lower case (default x)
 *     number, digit      (default 9)
 *     symbol             (default $)
 *     punctuation        (default .)
 *     separator          (default no masking)
 *     letter, other      (default ª)
 *     mark               (default ः)
 *     number, other      (default ²)
 *     other              (default ۝)
 * <p>
 *   time replacements: string of 6 numbers or _ one per field below
 *     year (0 to 4000, default no masking)
 *     month (1 to 12, default 1)
 *     date (1 to 31, default 1)
 *     hour (0 to 23, default 0)
 *     minute (0 to 59, default 0)
 *     second (0 to 59, default 0)
 * <p>
 * Parameters use "_" for preserve original.
 */
public class RedactMaskFactory extends MaskFactory {

  /**
   * The value to indicate that the value should be preserved.
   */
  private static final int UNMASKED_CHAR = "_".codePointAt(0);
  private static final int UNMASKED_DATE = -1;

  // The default replacements for each character category.
  // I picked a character in the same category so that the masking is
  // idempotent. For non-ascii characters, I mostly picked the first example.
  private static final int DEFAULT_LETTER_UPPER = "X".codePointAt(0);
  private static final int DEFAULT_LETTER_LOWER = "x".codePointAt(0);
  private static final int DEFAULT_NUMBER_DIGIT = 9;
  private static final int DEFAULT_NUMBER_DIGIT_CP =
      Integer.toString(DEFAULT_NUMBER_DIGIT).codePointAt(0);
  private static final int DEFAULT_SYMBOL = "$".codePointAt(0);
  private static final int DEFAULT_PUNCTUATION = ".".codePointAt(0);
  private static final int DEFAULT_SEPARATOR = UNMASKED_CHAR;
  private static final int DEFAULT_LETTER_OTHER = "\u00AA".codePointAt(0);
  private static final int DEFAULT_MARK = "\u0903".codePointAt(0);
  private static final int DEFAULT_NUMBER_OTHER = "\u00B2".codePointAt(0);
  private static final int DEFAULT_OTHER = "\u06DD".codePointAt(0);

  // The replacement codepoint for each character category. We use codepoints
  // here so that we don't have to worry about handling long UTF characters
  // as special cases.
  private final int UPPER_REPLACEMENT;
  private final int LOWER_REPLACEMENT;
  private final int OTHER_LETTER_REPLACEMENT;
  private final int MARK_REPLACEMENT;
  private final int DIGIT_CP_REPLACEMENT;
  private final int OTHER_NUMBER_REPLACEMENT;
  private final int SYMBOL_REPLACEMENT;
  private final int PUNCTUATION_REPLACEMENT;
  private final int SEPARATOR_REPLACEMENT;
  private final int OTHER_REPLACEMENT;

  // numeric replacement
  private final int DIGIT_REPLACEMENT;

  // time replacement
  private final int YEAR_REPLACEMENT;
  private final int MONTH_REPLACEMENT;
  private final int DATE_REPLACEMENT;
  private final int HOUR_REPLACEMENT;
  private final int MINUTE_REPLACEMENT;
  private final int SECOND_REPLACEMENT;
  private final boolean maskDate;
  private final boolean maskTimestamp;

  // index tuples that are not to be masked
  private final SortedMap<Integer,Integer> unmaskIndexRanges = new TreeMap<>();

  public RedactMaskFactory(String... params) {
    ByteBuffer param = params.length < 1 ? ByteBuffer.allocate(0) :
        ByteBuffer.wrap(params[0].getBytes(StandardCharsets.UTF_8));
    UPPER_REPLACEMENT = getNextCodepoint(param, DEFAULT_LETTER_UPPER);
    LOWER_REPLACEMENT = getNextCodepoint(param, DEFAULT_LETTER_LOWER);
    DIGIT_CP_REPLACEMENT = getNextCodepoint(param, DEFAULT_NUMBER_DIGIT_CP);
    DIGIT_REPLACEMENT = getReplacementDigit(DIGIT_CP_REPLACEMENT);
    SYMBOL_REPLACEMENT = getNextCodepoint(param, DEFAULT_SYMBOL);
    PUNCTUATION_REPLACEMENT = getNextCodepoint(param, DEFAULT_PUNCTUATION);
    SEPARATOR_REPLACEMENT = getNextCodepoint(param, DEFAULT_SEPARATOR);
    OTHER_LETTER_REPLACEMENT = getNextCodepoint(param, DEFAULT_LETTER_OTHER);
    MARK_REPLACEMENT = getNextCodepoint(param, DEFAULT_MARK);
    OTHER_NUMBER_REPLACEMENT = getNextCodepoint(param, DEFAULT_NUMBER_OTHER);
    OTHER_REPLACEMENT = getNextCodepoint(param, DEFAULT_OTHER);
    String[] timeParams;
    if (params.length < 2 || StringUtils.isBlank(params[1])) {
      timeParams = null;
    } else {
      timeParams = params[1].split("\\W+");
    }
    YEAR_REPLACEMENT = getDateParam(timeParams, 0, UNMASKED_DATE, 4000);
    MONTH_REPLACEMENT = getDateParam(timeParams, 1, 1, 12);
    DATE_REPLACEMENT = getDateParam(timeParams, 2, 1, 31);
    HOUR_REPLACEMENT = getDateParam(timeParams, 3, 0, 23);
    MINUTE_REPLACEMENT = getDateParam(timeParams, 4, 0, 59);
    SECOND_REPLACEMENT = getDateParam(timeParams, 5, 0, 59);
    maskDate = (YEAR_REPLACEMENT != UNMASKED_DATE) ||
        (MONTH_REPLACEMENT != UNMASKED_DATE) ||
        (DATE_REPLACEMENT != UNMASKED_DATE);
    maskTimestamp = maskDate || (HOUR_REPLACEMENT != UNMASKED_DATE) ||
        (MINUTE_REPLACEMENT != UNMASKED_DATE) ||
        (SECOND_REPLACEMENT != UNMASKED_DATE);

    /* un-mask range */
    if(!(params.length < 3 || StringUtils.isBlank(params[2]))) {
      String[] unmaskIndexes = params[2].split(",");

      for(int i=0; i < unmaskIndexes.length; i++ ) {
        String[] pair = unmaskIndexes[i].trim().split(":");
        unmaskIndexRanges.put(Integer.parseInt(pair[0]), Integer.parseInt(pair[1]));
      }
    }
  }

  @Override
  protected DataMask buildBooleanMask(TypeDescription schema) {
    if (DIGIT_CP_REPLACEMENT == UNMASKED_CHAR) {
      return new LongIdentity();
    } else {
      return new BooleanRedactConverter();
    }
  }

  @Override
  protected DataMask buildLongMask(TypeDescription schema) {
    if (DIGIT_CP_REPLACEMENT == UNMASKED_CHAR) {
      return new LongIdentity();
    } else {
      return new LongRedactConverter(schema.getCategory());
    }
  }

  @Override
  protected DataMask buildDecimalMask(TypeDescription schema) {
    if (DIGIT_CP_REPLACEMENT == UNMASKED_CHAR) {
      return new DecimalIdentity();
    } else {
      return new DecimalRedactConverter();
    }
  }

  @Override
  protected DataMask buildDoubleMask(TypeDescription schema) {
    if (DIGIT_CP_REPLACEMENT == UNMASKED_CHAR) {
      return new DoubleIdentity();
    } else {
      return new DoubleRedactConverter();
    }
  }

  @Override
  protected DataMask buildStringMask(TypeDescription schema) {
    return new StringConverter();
  }

  @Override
  protected DataMask buildDateMask(TypeDescription schema) {
    if (maskDate) {
      return new DateRedactConverter();
    } else {
      return new LongIdentity();
    }
  }

  @Override
  protected DataMask buildTimestampMask(TypeDescription schema) {
    if (maskTimestamp) {
      return new TimestampRedactConverter();
    } else {
      return new TimestampIdentity();
    }
  }

  @Override
  protected DataMask buildBinaryMask(TypeDescription schema) {
    return new NullifyMask();
  }

  class LongRedactConverter implements DataMask {
    final long mask;

    LongRedactConverter(TypeDescription.Category category) {
      switch (category) {
        case BYTE:
          mask = 0xff;
          break;
        case SHORT:
          mask = 0xffff;
          break;
        case INT:
          mask = 0xffff_ffff;
          break;
        default:
        case LONG:
          mask = -1;
          break;
      }
    }

    @Override
    public void maskData(ColumnVector original, ColumnVector masked, int start,
                         int length) {
      LongColumnVector target = (LongColumnVector) masked;
      LongColumnVector source = (LongColumnVector) original;
      target.noNulls = original.noNulls;
      target.isRepeating = original.isRepeating;
      if (original.isRepeating) {
        target.vector[0] = maskLong(source.vector[0]) & mask;
        target.isNull[0] = source.isNull[0];
      } else {
        for(int r = start; r < start + length; ++r) {
          target.vector[r] = maskLong(source.vector[r]) & mask;
          target.isNull[r] = source.isNull[r];
        }
      }
    }
  }

  class BooleanRedactConverter implements DataMask {
    @Override
    public void maskData(ColumnVector original, ColumnVector masked, int start,
                         int length) {
      LongColumnVector target = (LongColumnVector) masked;
      LongColumnVector source = (LongColumnVector) original;
      target.noNulls = original.noNulls;
      target.isRepeating = original.isRepeating;
      if (original.isRepeating) {
        target.vector[0] = DIGIT_REPLACEMENT == 0 ? 0 : 1;
        target.isNull[0] = source.isNull[0];
      } else {
        for(int r = start; r < start + length; ++r) {
          target.vector[r] = DIGIT_REPLACEMENT == 0 ? 0 : 1;
          target.isNull[r] = source.isNull[r];
        }
      }
    }
  }

  class DoubleRedactConverter implements DataMask {
    @Override
    public void maskData(ColumnVector original, ColumnVector masked, int start,
                         int length) {
      DoubleColumnVector target = (DoubleColumnVector) masked;
      DoubleColumnVector source = (DoubleColumnVector) original;
      target.noNulls = original.noNulls;
      target.isRepeating = original.isRepeating;
      if (original.isRepeating) {
        target.vector[0] = maskDouble(source.vector[0]);
        target.isNull[0] = source.isNull[0];
      } else {
        for(int r = start; r < start + length; ++r) {
          target.vector[r] = maskDouble(source.vector[r]);
          target.isNull[r] = source.isNull[r];
        }
      }
    }
  }

  class StringConverter implements DataMask {
    @Override
    public void maskData(ColumnVector original, ColumnVector masked, int start,
                         int length) {
      BytesColumnVector target = (BytesColumnVector) masked;
      BytesColumnVector source = (BytesColumnVector) original;
      target.noNulls = original.noNulls;
      target.isRepeating = original.isRepeating;
      if (original.isRepeating) {
        target.isNull[0] = source.isNull[0];
        if (target.noNulls || !target.isNull[0]) {
          maskString(source, 0, target);
        }
      } else {
        for(int r = start; r < start + length; ++r) {
          target.isNull[r] = source.isNull[r];
          if (target.noNulls || !target.isNull[r]) {
            maskString(source, r, target);
          }
        }
      }
    }
  }

  class DecimalRedactConverter implements DataMask {
    @Override
    public void maskData(ColumnVector original, ColumnVector masked, int start,
                         int length) {
      DecimalColumnVector target = (DecimalColumnVector) masked;
      DecimalColumnVector source = (DecimalColumnVector) original;
      target.noNulls = original.noNulls;
      target.isRepeating = original.isRepeating;
      target.scale = source.scale;
      target.precision = source.precision;
      if (original.isRepeating) {
        target.isNull[0] = source.isNull[0];
        if (target.noNulls || !target.isNull[0]) {
          target.vector[0].set(maskDecimal(source.vector[0]));
        }
      } else {
        for(int r = start; r < start + length; ++r) {
          target.isNull[r] = source.isNull[r];
          if (target.noNulls || !target.isNull[r]) {
            target.vector[r].set(maskDecimal(source.vector[r]));
          }
        }
      }
    }
  }

  class TimestampRedactConverter implements DataMask {

    @Override
    public void maskData(ColumnVector original, ColumnVector masked, int start,
                         int length) {
      TimestampColumnVector target = (TimestampColumnVector) masked;
      TimestampColumnVector source = (TimestampColumnVector) original;
      target.noNulls = original.noNulls;
      target.isRepeating = original.isRepeating;
      if (original.isRepeating) {
        target.isNull[0] = source.isNull[0];
        if (target.noNulls || !target.isNull[0]) {
          target.time[0] = maskTime(source.time[0]);
          target.nanos[0] = 0;
        }
      } else {
        for(int r = start; r < start + length; ++r) {
          target.isNull[r] = source.isNull[r];
          if (target.noNulls || !target.isNull[r]) {
            target.time[r] = maskTime(source.time[r]);
            target.nanos[r] = 0;
          }
        }
      }
    }
  }

  class DateRedactConverter implements DataMask {

    @Override
    public void maskData(ColumnVector original, ColumnVector masked, int start,
                         int length) {
      LongColumnVector target = (LongColumnVector) masked;
      LongColumnVector source = (LongColumnVector) original;
      target.noNulls = original.noNulls;
      target.isRepeating = original.isRepeating;
      if (original.isRepeating) {
        target.isNull[0] = source.isNull[0];
        if (target.noNulls || !target.isNull[0]) {
          target.vector[0] = maskDate((int) source.vector[0]);
        }
      } else {
        for(int r = start; r < start + length; ++r) {
          target.isNull[r] = source.isNull[r];
          if (target.noNulls || !target.isNull[r]) {
            target.vector[r] = maskDate((int) source.vector[r]);
          }
        }
      }
    }
  }

  /**
   * Get the next code point from the ByteBuffer. Moves the position in the
   * ByteBuffer forward to the next code point.
   * @param param the source of bytes
   * @param defaultValue if there are no bytes left, use this value
   * @return the code point that was found at the front of the buffer.
   */
  static int getNextCodepoint(ByteBuffer param, int defaultValue) {
    if (param.remaining() == 0) {
      return defaultValue;
    } else {
      return Text.bytesToCodePoint(param);
    }
  }

  /**
   * Get the replacement digit. This routine supports non-ASCII values for the
   * replacement. For example, if the user gives one of "7", "७", "〧" or "፯"
   * the value is 7.
   * @param digitCodePoint the code point that is replacing digits
   * @return the number from 0 to 9 to use as the numeric replacement
   */
  static int getReplacementDigit(int digitCodePoint) {
    int dig = Character.getNumericValue(digitCodePoint);
    if (dig >= 0 && dig <= 9) {
      return dig;
    } else {
      return DEFAULT_NUMBER_DIGIT;
    }
  }

  static int getDateParam(String[] dateParams, int posn,
                          int myDefault, int max) {
    if (dateParams != null && posn < dateParams.length) {
      if (dateParams[posn].codePointAt(0) == UNMASKED_CHAR) {
        return UNMASKED_DATE;
      } else {
        int result = Integer.parseInt(dateParams[posn]);
        if (result >= -1 && result <= max) {
          return result;
        } else {
          throw new IllegalArgumentException("Invalid date parameter " + posn +
              " of " + dateParams[posn] + " greater than " + max);
        }
      }
    } else {
      return myDefault;
    }
  }

  /**
   * Replace each digit in value with DIGIT_REPLACEMENT scaled to the matching
   * number of digits.
   * @param value the number to mask
   * @return the masked value
   */
  public long maskLong(long value) {

    /* check whether unmasking range provided */
    if (!unmaskIndexRanges.isEmpty()) {
      return maskLongWithUnmasking(value);
    }

    long base;
    if (DIGIT_REPLACEMENT == 0) {
      return 0;
    } else if (value >= 0) {
      base = 1;
    } else {
      base = -1;
      // make sure Long.MIN_VALUE doesn't overflow
      if (value == Long.MIN_VALUE) {
        value = Long.MAX_VALUE;
      } else {
        value = -value;
      }
    }
    if (value < 100_000_000L) {
      if (value < 10_000L) {
        if (value < 100L) {
          if (value < 10L) {
            base *= 1;
          } else {
            base *= 11;
          }
        } else if (value < 1_000L) {
          base *= 111;
        } else {
          base *= 1_111;
        }
      } else if (value < 1_000_000L) {
        if (value < 100_000L) {
          base *= 11_111;
        } else {
          base *= 111_111;
        }
      } else if (value < 10_000_000L) {
        base *= 1_111_111;
      } else {
        base *= 11_111_111;
      }
    } else if (value < 10_000_000_000_000_000L) {
      if (value < 1_000_000_000_000L) {
        if (value < 10_000_000_000L) {
          if (value < 1_000_000_000L) {
            base *= 111_111_111;
          } else {
            base *= 1_111_111_111;
          }
        } else if (value < 100_000_000_000L) {
          base *= 11_111_111_111L;
        } else {
          base *= 111_111_111_111L;
        }
      } else if (value < 100_000_000_000_000L) {
        if (value < 10_000_000_000_000L) {
          base *= 1_111_111_111_111L;
        } else {
          base *= 11_111_111_111_111L;
        }
      } else if (value < 1_000_000_000_000_000L) {
        base *= 111_111_111_111_111L;
      } else {
        base *= 1_111_111_111_111_111L;
      }
    } else if (value < 100_000_000_000_000_000L) {
      base *= 11_111_111_111_111_111L;
    // If the digit is 9, it would overflow at 19 digits, so use 18.
    } else if (value < 1_000_000_000_000_000_000L || DIGIT_REPLACEMENT == 9) {
      base *= 111_111_111_111_111_111L;
    } else {
      base *= 1_111_111_111_111_111_111L;
    }

    return DIGIT_REPLACEMENT * base;
  }

  private static final double[] DOUBLE_POWER_10 = new double[]{
      1e-308, 1e-307, 1e-306, 1e-305, 1e-304, 1e-303, 1e-302, 1e-301, 1e-300,
      1e-299, 1e-298, 1e-297, 1e-296, 1e-295, 1e-294, 1e-293, 1e-292, 1e-291,
      1e-290, 1e-289, 1e-288, 1e-287, 1e-286, 1e-285, 1e-284, 1e-283, 1e-282,
      1e-281, 1e-280, 1e-279, 1e-278, 1e-277, 1e-276, 1e-275, 1e-274, 1e-273,
      1e-272, 1e-271, 1e-270, 1e-269, 1e-268, 1e-267, 1e-266, 1e-265, 1e-264,
      1e-263, 1e-262, 1e-261, 1e-260, 1e-259, 1e-258, 1e-257, 1e-256, 1e-255,
      1e-254, 1e-253, 1e-252, 1e-251, 1e-250, 1e-249, 1e-248, 1e-247, 1e-246,
      1e-245, 1e-244, 1e-243, 1e-242, 1e-241, 1e-240, 1e-239, 1e-238, 1e-237,
      1e-236, 1e-235, 1e-234, 1e-233, 1e-232, 1e-231, 1e-230, 1e-229, 1e-228,
      1e-227, 1e-226, 1e-225, 1e-224, 1e-223, 1e-222, 1e-221, 1e-220, 1e-219,
      1e-218, 1e-217, 1e-216, 1e-215, 1e-214, 1e-213, 1e-212, 1e-211, 1e-210,
      1e-209, 1e-208, 1e-207, 1e-206, 1e-205, 1e-204, 1e-203, 1e-202, 1e-201,
      1e-200, 1e-199, 1e-198, 1e-197, 1e-196, 1e-195, 1e-194, 1e-193, 1e-192,
      1e-191, 1e-190, 1e-189, 1e-188, 1e-187, 1e-186, 1e-185, 1e-184, 1e-183,
      1e-182, 1e-181, 1e-180, 1e-179, 1e-178, 1e-177, 1e-176, 1e-175, 1e-174,
      1e-173, 1e-172, 1e-171, 1e-170, 1e-169, 1e-168, 1e-167, 1e-166, 1e-165,
      1e-164, 1e-163, 1e-162, 1e-161, 1e-160, 1e-159, 1e-158, 1e-157, 1e-156,
      1e-155, 1e-154, 1e-153, 1e-152, 1e-151, 1e-150, 1e-149, 1e-148, 1e-147,
      1e-146, 1e-145, 1e-144, 1e-143, 1e-142, 1e-141, 1e-140, 1e-139, 1e-138,
      1e-137, 1e-136, 1e-135, 1e-134, 1e-133, 1e-132, 1e-131, 1e-130, 1e-129,
      1e-128, 1e-127, 1e-126, 1e-125, 1e-124, 1e-123, 1e-122, 1e-121, 1e-120,
      1e-119, 1e-118, 1e-117, 1e-116, 1e-115, 1e-114, 1e-113, 1e-112, 1e-111,
      1e-110, 1e-109, 1e-108, 1e-107, 1e-106, 1e-105, 1e-104, 1e-103, 1e-102,
      1e-101, 1e-100, 1e-99, 1e-98, 1e-97, 1e-96, 1e-95, 1e-94, 1e-93,
      1e-92, 1e-91, 1e-90, 1e-89, 1e-88, 1e-87, 1e-86, 1e-85, 1e-84,
      1e-83, 1e-82, 1e-81, 1e-80, 1e-79, 1e-78, 1e-77, 1e-76, 1e-75,
      1e-74, 1e-73, 1e-72, 1e-71, 1e-70, 1e-69, 1e-68, 1e-67, 1e-66,
      1e-65, 1e-64, 1e-63, 1e-62, 1e-61, 1e-60, 1e-59, 1e-58, 1e-57,
      1e-56, 1e-55, 1e-54, 1e-53, 1e-52, 1e-51, 1e-50, 1e-49, 1e-48,
      1e-47, 1e-46, 1e-45, 1e-44, 1e-43, 1e-42, 1e-41, 1e-40, 1e-39,
      1e-38, 1e-37, 1e-36, 1e-35, 1e-34, 1e-33, 1e-32, 1e-31, 1e-30,
      1e-29, 1e-28, 1e-27, 1e-26, 1e-25, 1e-24, 1e-23, 1e-22, 1e-21,
      1e-20, 1e-19, 1e-18, 1e-17, 1e-16, 1e-15, 1e-14, 1e-13, 1e-12,
      1e-11, 1e-10, 1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3,
      1e-2, 1e-1, 1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6,
      1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
      1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23, 1e24,
      1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31, 1e32, 1e33,
      1e34, 1e35, 1e36, 1e37, 1e38, 1e39, 1e40, 1e41, 1e42,
      1e43, 1e44, 1e45, 1e46, 1e47, 1e48, 1e49, 1e50, 1e51,
      1e52, 1e53, 1e54, 1e55, 1e56, 1e57, 1e58, 1e59, 1e60,
      1e61, 1e62, 1e63, 1e64, 1e65, 1e66, 1e67, 1e68, 1e69,
      1e70, 1e71, 1e72, 1e73, 1e74, 1e75, 1e76, 1e77, 1e78,
      1e79, 1e80, 1e81, 1e82, 1e83, 1e84, 1e85, 1e86, 1e87,
      1e88, 1e89, 1e90, 1e91, 1e92, 1e93, 1e94, 1e95, 1e96,
      1e97, 1e98, 1e99, 1e100, 1e101, 1e102, 1e103, 1e104, 1e105,
      1e106, 1e107, 1e108, 1e109, 1e110, 1e111, 1e112, 1e113, 1e114,
      1e115, 1e116, 1e117, 1e118, 1e119, 1e120, 1e121, 1e122, 1e123,
      1e124, 1e125, 1e126, 1e127, 1e128, 1e129, 1e130, 1e131, 1e132,
      1e133, 1e134, 1e135, 1e136, 1e137, 1e138, 1e139, 1e140, 1e141,
      1e142, 1e143, 1e144, 1e145, 1e146, 1e147, 1e148, 1e149, 1e150,
      1e151, 1e152, 1e153, 1e154, 1e155, 1e156, 1e157, 1e158, 1e159,
      1e160, 1e161, 1e162, 1e163, 1e164, 1e165, 1e166, 1e167, 1e168,
      1e169, 1e170, 1e171, 1e172, 1e173, 1e174, 1e175, 1e176, 1e177,
      1e178, 1e179, 1e180, 1e181, 1e182, 1e183, 1e184, 1e185, 1e186,
      1e187, 1e188, 1e189, 1e190, 1e191, 1e192, 1e193, 1e194, 1e195,
      1e196, 1e197, 1e198, 1e199, 1e200, 1e201, 1e202, 1e203, 1e204,
      1e205, 1e206, 1e207, 1e208, 1e209, 1e210, 1e211, 1e212, 1e213,
      1e214, 1e215, 1e216, 1e217, 1e218, 1e219, 1e220, 1e221, 1e222,
      1e223, 1e224, 1e225, 1e226, 1e227, 1e228, 1e229, 1e230, 1e231,
      1e232, 1e233, 1e234, 1e235, 1e236, 1e237, 1e238, 1e239, 1e240,
      1e241, 1e242, 1e243, 1e244, 1e245, 1e246, 1e247, 1e248, 1e249,
      1e250, 1e251, 1e252, 1e253, 1e254, 1e255, 1e256, 1e257, 1e258,
      1e259, 1e260, 1e261, 1e262, 1e263, 1e264, 1e265, 1e266, 1e267,
      1e268, 1e269, 1e270, 1e271, 1e272, 1e273, 1e274, 1e275, 1e276,
      1e277, 1e278, 1e279, 1e280, 1e281, 1e282, 1e283, 1e284, 1e285,
      1e286, 1e287, 1e288, 1e289, 1e290, 1e291, 1e292, 1e293, 1e294,
      1e295, 1e296, 1e297, 1e298, 1e299, 1e300, 1e301, 1e302, 1e303,
      1e304, 1e305, 1e306, 1e307};

  /**
   * Replace each digit in value with digit.
   * @param value the number to mask
   * @return the
   */
  public double maskDouble(double value) {

    /* check whether unmasking range provided */
    if (!unmaskIndexRanges.isEmpty()) {
      return maskDoubleWIthUnmasking(value);
    }

    double base;
    // It seems better to mask 0 to 9.99999 rather than 9.99999e-308.
    if (value == 0 || DIGIT_REPLACEMENT == 0) {
      return DIGIT_REPLACEMENT * 1.11111;
    } else if (value > 0) {
      base = 1.11111;
    } else {
      base = -1.11111;
      value = -value;
    }
    int posn = Arrays.binarySearch(DOUBLE_POWER_10, value);
    if (posn < -DOUBLE_POWER_10.length - 2) {
      posn = DOUBLE_POWER_10.length - 1;
    } else if (posn == -1) {
      posn = 0;
    } else if (posn < 0) {
      posn = -posn -2;
    }
    return DIGIT_REPLACEMENT * base * DOUBLE_POWER_10[posn];
  }

  private final Calendar scratch = Calendar.getInstance();

  /**
   * Given the requested masking parameters, redact the given time
   * @param millis the original time
   * @return the millis after it has been masked
   */
  long maskTime(long millis) {
    scratch.setTimeInMillis(millis);
    if (YEAR_REPLACEMENT != UNMASKED_DATE) {
      scratch.set(Calendar.YEAR, YEAR_REPLACEMENT);
    }
    if (MONTH_REPLACEMENT != UNMASKED_DATE) {
      scratch.set(Calendar.MONTH, MONTH_REPLACEMENT - 1);
    }
    if (DATE_REPLACEMENT != UNMASKED_DATE) {
      scratch.set(Calendar.DATE, DATE_REPLACEMENT);
    }
    if (HOUR_REPLACEMENT != UNMASKED_DATE) {
      if (HOUR_REPLACEMENT >= 12) {
        scratch.set(Calendar.HOUR, HOUR_REPLACEMENT - 12);
        scratch.set(Calendar.AM_PM, Calendar.PM);
      } else {
        scratch.set(Calendar.HOUR, HOUR_REPLACEMENT);
        scratch.set(Calendar.AM_PM, Calendar.AM);
      }
    }
    if (MINUTE_REPLACEMENT != UNMASKED_DATE) {
      scratch.set(Calendar.MINUTE, MINUTE_REPLACEMENT);
    }
    if (SECOND_REPLACEMENT != UNMASKED_DATE) {
      scratch.set(Calendar.SECOND, SECOND_REPLACEMENT);
      scratch.set(Calendar.MILLISECOND, 0);
    }
    return scratch.getTimeInMillis();
  }

  private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);

  private final Calendar utcScratch =
      Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  /**
   * Given a date as the number of days since epoch (1 Jan 1970),
   * mask the date given the parameters.
   * @param daysSinceEpoch the number of days after epoch
   * @return the number of days after epoch when masked
   */
  int maskDate(int daysSinceEpoch) {
    utcScratch.setTimeInMillis(daysSinceEpoch * MILLIS_PER_DAY);
    if (YEAR_REPLACEMENT != UNMASKED_DATE) {
      utcScratch.set(Calendar.YEAR, YEAR_REPLACEMENT);
    }
    if (MONTH_REPLACEMENT != UNMASKED_DATE) {
      utcScratch.set(Calendar.MONTH, MONTH_REPLACEMENT - 1);
    }
    if (DATE_REPLACEMENT != UNMASKED_DATE) {
      utcScratch.set(Calendar.DATE, DATE_REPLACEMENT);
    }
    return (int) (utcScratch.getTimeInMillis() / MILLIS_PER_DAY);
  }

  /**
   * Mask a decimal.
   * This is painfully slow because it converts to a string and then back to
   * a decimal. Until HiveDecimalWritable gives us more access, this is
   * the best tradeoff between developer time, functionality, and run time.
   * @param source the value to mask
   * @return the masked value.
   */
  HiveDecimalWritable maskDecimal(HiveDecimalWritable source) {
    return new HiveDecimalWritable(maskNumericString(source.toString()));
  }

  /**
   * Given a UTF code point, find the replacement codepoint
   * @param codepoint a UTF character
   * @return the replacement codepoint
   */
  int getReplacement(int codepoint) {
    switch (Character.getType(codepoint)) {
      case Character.UPPERCASE_LETTER:
        return UPPER_REPLACEMENT;
      case Character.LOWERCASE_LETTER:
        return LOWER_REPLACEMENT;
      case Character.TITLECASE_LETTER:
      case Character.MODIFIER_LETTER:
      case Character.OTHER_LETTER:
        return OTHER_LETTER_REPLACEMENT;
      case Character.NON_SPACING_MARK:
      case Character.ENCLOSING_MARK:
      case Character.COMBINING_SPACING_MARK:
        return MARK_REPLACEMENT;
      case Character.DECIMAL_DIGIT_NUMBER:
        return DIGIT_CP_REPLACEMENT;
      case Character.LETTER_NUMBER:
      case Character.OTHER_NUMBER:
        return OTHER_NUMBER_REPLACEMENT;
      case Character.SPACE_SEPARATOR:
      case Character.LINE_SEPARATOR:
      case Character.PARAGRAPH_SEPARATOR:
        return SEPARATOR_REPLACEMENT;
      case Character.MATH_SYMBOL:
      case Character.CURRENCY_SYMBOL:
      case Character.MODIFIER_SYMBOL:
      case Character.OTHER_SYMBOL:
        return SYMBOL_REPLACEMENT;
      case Character.DASH_PUNCTUATION:
      case Character.START_PUNCTUATION:
      case Character.END_PUNCTUATION:
      case Character.CONNECTOR_PUNCTUATION:
      case Character.OTHER_PUNCTUATION:
        return PUNCTUATION_REPLACEMENT;
      default:
        return OTHER_REPLACEMENT;
    }
  }

  /**
   * Get the number of bytes for each codepoint
   * @param codepoint the codepoint to check
   * @return the number of bytes
   */
  static int getCodepointLength(int codepoint) {
    if (codepoint < 0) {
      throw new IllegalArgumentException("Illegal codepoint " + codepoint);
    } else if (codepoint < 0x80) {
      return 1;
    } else if (codepoint < 0x7ff) {
      return 2;
    } else if (codepoint < 0xffff) {
      return 3;
    } else if (codepoint < 0x10FFFF) {
      return 4;
    } else {
      throw new IllegalArgumentException("Illegal codepoint " + codepoint);
    }
  }

  /**
   * Write the give codepoint to the buffer.
   * @param codepoint the codepoint to write
   * @param buffer the buffer to write into
   * @param offset the first offset to use
   * @param length the number of bytes that will be used
   */
  static void writeCodepoint(int codepoint, byte[] buffer, int offset,
                             int length) {
    switch (length) {
      case 1:
        buffer[offset] = (byte) codepoint;
        break;
      case 2:
        buffer[offset] = (byte)(0xC0 | codepoint >> 6);
        buffer[offset+1] = (byte)(0x80 | (codepoint & 0x3f));
        break;
      case 3:
        buffer[offset] = (byte)(0xE0 | codepoint >> 12);
        buffer[offset+1] = (byte)(0x80 | ((codepoint >> 6) & 0x3f));
        buffer[offset+2] = (byte)(0x80 | (codepoint & 0x3f));
        break;
      case 4:
        buffer[offset] = (byte)(0xF0 | codepoint >> 18);
        buffer[offset+1] = (byte)(0x80 | ((codepoint >> 12) & 0x3f));
        buffer[offset+2] = (byte)(0x80 | ((codepoint >> 6) & 0x3f));
        buffer[offset+3] = (byte)(0x80 | (codepoint & 0x3f));
        break;
      default:
        throw new IllegalArgumentException("Invalid length for codepoint " +
            codepoint + " = " + length);
    }
  }

  /**
   * Mask a string by finding the character category of each character
   * and replacing it with the matching literal.
   * @param source the source column vector
   * @param row the value index
   * @param target the target column vector
   */
  void maskString(BytesColumnVector source, int row, BytesColumnVector target) {
    int expectedBytes = source.length[row];
    ByteBuffer sourceBytes = ByteBuffer.wrap(source.vector[row],
        source.start[row], source.length[row]);
    // ensure we have enough space, if the masked data is the same size
    target.ensureValPreallocated(expectedBytes);
    byte[] outputBuffer = target.getValPreallocatedBytes();
    int outputOffset = target.getValPreallocatedStart();
    int outputStart = outputOffset;

    int index = 0;
    while (sourceBytes.remaining() > 0) {
      int cp = Text.bytesToCodePoint(sourceBytes);

      // Find the replacement for the current character.
      int replacement = getReplacement(cp);
      if (replacement == UNMASKED_CHAR || isIndexInUnmaskRange(index, source.length[row])) {
        replacement = cp;
      }

      // increment index
      index++;

      int len = getCodepointLength(replacement);

      // If the translation will overflow the buffer, we need to resize.
      // This will only happen when the masked size is larger than the original.
      if (len + outputOffset > outputBuffer.length) {
        // Revise estimate how much we are going to need now. We are maximally
        // pesamistic here so that we don't have to expand again for this value.
        int currentOutputStart =  outputStart;
        int currentOutputLength = outputOffset - currentOutputStart;
        expectedBytes = currentOutputLength + len + sourceBytes.remaining() * 4;

        // Expand the buffer to fit the new estimate
        target.ensureValPreallocated(expectedBytes);

        // Copy over the bytes we've already written for this value and move
        // the pointers to the new output buffer.
        byte[] oldBuffer = outputBuffer;
        outputBuffer = target.getValPreallocatedBytes();
        outputOffset = target.getValPreallocatedStart();
        outputStart = outputOffset;
        System.arraycopy(oldBuffer, currentOutputStart, outputBuffer,
            outputOffset, currentOutputLength);
        outputOffset += currentOutputLength;
      }

      // finally copy the bytes
      writeCodepoint(replacement, outputBuffer, outputOffset, len);
      outputOffset += len;
    }
    target.setValPreallocated(row, outputOffset - outputStart);
  }

  static final long OVERFLOW_REPLACEMENT = 111_111_111_111_111_111L;

  /**
   * A function that masks longs when there are unmasked ranges.
   * @param value the original value
   * @return the masked value
   */
  long maskLongWithUnmasking(long value) throws IndexOutOfBoundsException {
    try {
      return Long.parseLong(maskNumericString(Long.toString(value)));
    } catch (NumberFormatException nfe) {
      return OVERFLOW_REPLACEMENT * DIGIT_REPLACEMENT;
    }
  }

  /**
   * A function that masks doubles when there are unmasked ranges.
   * @param value original value
   * @return masked value
   */
  double maskDoubleWIthUnmasking(final double value) {
    try {
      return Double.parseDouble(maskNumericString(Double.toString(value)));
    } catch (NumberFormatException nfe) {
      return OVERFLOW_REPLACEMENT * DIGIT_REPLACEMENT;
    }
  }

  /**
   * Mask the given stringified numeric value excluding the unmask range.
   * Non-digit characters are passed through on the assumption they are
   * markers (eg. one of ",.ef").
   * @param value the original value.
   */
  String maskNumericString(final String value) {
    StringBuilder result = new StringBuilder();
    final int length = value.codePointCount(0, value.length());
    for(int c=0; c < length; ++c) {
      int cp = value.codePointAt(c);
      if (isIndexInUnmaskRange(c, length) ||
          Character.getType(cp) != Character.DECIMAL_DIGIT_NUMBER) {
        result.appendCodePoint(cp);
      } else {
        result.appendCodePoint(DIGIT_CP_REPLACEMENT);
      }
    }
    return result.toString();
  }

  /**
   * Given an index and length of a string
   * find out whether it is in a given un-mask range.
   * @param index the character point index
   * @param length the length of the string in character points
   * @return true if the index is in un-mask range else false.
   */
  private boolean isIndexInUnmaskRange(final int index, final int length) {

    for(final Map.Entry<Integer, Integer> pair : unmaskIndexRanges.entrySet()) {
      int start;
      int end;

      if(pair.getKey() >= 0) {
        // for positive indexes
        start = pair.getKey();
      } else {
        // for negative indexes
        start = length + pair.getKey();
      }

      if(pair.getValue() >= 0) {
        // for positive indexes
        end = pair.getValue();
      } else {
        // for negative indexes
        end = length + pair.getValue();
      }

      // if the given index is in range
      if(index >= start && index <= end ) {
        return true;
      }

    }

    return false;
  }
}
