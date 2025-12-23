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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.io.filter.FilterContext;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.apache.orc.impl.reader.StripePlanner;
import org.apache.orc.impl.reader.tree.TypeReader;
import org.threeten.extra.chrono.HybridChronology;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.chrono.Chronology;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.EnumMap;
import java.util.TimeZone;

/**
 * Convert ORC tree readers.
 */
public class ConvertTreeReaderFactory extends TreeReaderFactory {

  /**
   * Override methods like checkEncoding to pass-thru to the convert TreeReader.
   */
  public static class ConvertTreeReader extends TreeReader {

    TypeReader fromReader;

    ConvertTreeReader(int columnId, TypeReader fromReader, Context context) throws IOException {
      super(columnId, context);
      this.fromReader = fromReader;
    }

    // The ordering of types here is used to determine which numeric types
    // are common/convertible to one another. Probably better to rely on the
    // ordering explicitly defined here than to assume that the enum values
    // that were arbitrarily assigned in PrimitiveCategory work for our purposes.
    private static EnumMap<TypeDescription.Category, Integer> numericTypes =
        new EnumMap<>(TypeDescription.Category.class);

    static {
      registerNumericType(TypeDescription.Category.BOOLEAN, 1);
      registerNumericType(TypeDescription.Category.BYTE, 2);
      registerNumericType(TypeDescription.Category.SHORT, 3);
      registerNumericType(TypeDescription.Category.INT, 4);
      registerNumericType(TypeDescription.Category.LONG, 5);
      registerNumericType(TypeDescription.Category.FLOAT, 6);
      registerNumericType(TypeDescription.Category.DOUBLE, 7);
      registerNumericType(TypeDescription.Category.DECIMAL, 8);
    }

    private static void registerNumericType(TypeDescription.Category kind, int level) {
      numericTypes.put(kind, level);
    }

    static TreeReader getStringGroupTreeReader(int columnId,
                                               TypeDescription fileType,
                                               Context context) throws IOException {
      switch (fileType.getCategory()) {
        case STRING:
          return new StringTreeReader(columnId, context);
        case CHAR:
          return new CharTreeReader(columnId, fileType.getMaxLength(), context);
        case VARCHAR:
          return new VarcharTreeReader(columnId, fileType.getMaxLength(), context);
        default:
          throw new RuntimeException("Unexpected type kind " + fileType.getCategory().name());
      }
    }

    protected void assignStringGroupVectorEntry(BytesColumnVector bytesColVector,
                                                int elementNum,
                                                TypeDescription readerType,
                                                byte[] bytes) {
      assignStringGroupVectorEntry(bytesColVector,
          elementNum, readerType, bytes, 0, bytes.length);
    }

    /*
     * Assign a BytesColumnVector entry when we have a byte array, start, and
     * length for the string group which can be (STRING, CHAR, VARCHAR).
     */
    protected void assignStringGroupVectorEntry(BytesColumnVector bytesColVector,
                                                int elementNum,
                                                TypeDescription readerType,
                                                byte[] bytes,
                                                int start,
                                                int length) {
      switch (readerType.getCategory()) {
        case STRING:
          bytesColVector.setVal(elementNum, bytes, start, length);
          break;
        case CHAR:
          int charAdjustedDownLen =
              StringExpr.rightTrimAndTruncate(bytes, start, length, readerType.getMaxLength());
          bytesColVector.setVal(elementNum, bytes, start, charAdjustedDownLen);
          break;
        case VARCHAR:
          int varcharAdjustedDownLen =
              StringExpr.truncate(bytes, start, length, readerType.getMaxLength());
          bytesColVector.setVal(elementNum, bytes, start, varcharAdjustedDownLen);
          break;
        default:
          throw new RuntimeException("Unexpected type kind " + readerType.getCategory().name());
      }
    }

    protected void convertStringGroupVectorElement(BytesColumnVector bytesColVector,
                                                   int elementNum, TypeDescription readerType) {
      switch (readerType.getCategory()) {
        case STRING:
          // No conversion needed.
          break;
        case CHAR:
          int charLength = bytesColVector.length[elementNum];
          int charAdjustedDownLen = StringExpr
              .rightTrimAndTruncate(bytesColVector.vector[elementNum],
                  bytesColVector.start[elementNum], charLength,
                  readerType.getMaxLength());
          if (charAdjustedDownLen < charLength) {
            bytesColVector.length[elementNum] = charAdjustedDownLen;
          }
          break;
        case VARCHAR:
          int varcharLength = bytesColVector.length[elementNum];
          int varcharAdjustedDownLen = StringExpr
              .truncate(bytesColVector.vector[elementNum],
                  bytesColVector.start[elementNum], varcharLength,
                  readerType.getMaxLength());
          if (varcharAdjustedDownLen < varcharLength) {
            bytesColVector.length[elementNum] = varcharAdjustedDownLen;
          }
          break;
        default:
          throw new RuntimeException("Unexpected type kind " + readerType.getCategory().name());
      }
    }

    private boolean isParseError;

    /*
     * We do this because we want the various parse methods return a primitive.
     *
     * @return true if there was a parse error in the last call to
     * parseLongFromString, etc.
     */
    protected boolean getIsParseError() {
      return isParseError;
    }

    protected long parseLongFromString(String string) {
      try {
        long longValue = Long.parseLong(string);
        isParseError = false;
        return longValue;
      } catch (NumberFormatException e) {
        isParseError = true;
        return 0;
      }
    }

    protected float parseFloatFromString(String string) {
      try {
        float floatValue = Float.parseFloat(string);
        isParseError = false;
        return floatValue;
      } catch (NumberFormatException e) {
        isParseError = true;
        return Float.NaN;
      }
    }

    protected double parseDoubleFromString(String string) {
      try {
        double value = Double.parseDouble(string);
        isParseError = false;
        return value;
      } catch (NumberFormatException e) {
        isParseError = true;
        return Double.NaN;
      }
    }

    /**
     * @param string
     * @return the HiveDecimal parsed, or null if there was a parse error.
     */
    protected HiveDecimal parseDecimalFromString(String string) {
      try {
        HiveDecimal value = HiveDecimal.create(string);
        return value;
      } catch (NumberFormatException e) {
        return null;
      }
    }

    private static final double MIN_LONG_AS_DOUBLE = -0x1p63;
    /*
     * We cannot store Long.MAX_VALUE as a double without losing precision. Instead, we store
     * Long.MAX_VALUE + 1 == -Long.MIN_VALUE, and then offset all comparisons by 1.
     */
    private static final double MAX_LONG_AS_DOUBLE_PLUS_ONE = 0x1p63;

    public boolean doubleCanFitInLong(double doubleValue) {

      // Borrowed from Guava DoubleMath.roundToLong except do not want dependency on Guava and we
      // don't want to catch an exception.

      return ((MIN_LONG_AS_DOUBLE - doubleValue < 1.0) &&
          (doubleValue < MAX_LONG_AS_DOUBLE_PLUS_ONE));
    }

    @Override
    public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      // Pass-thru.
      fromReader.checkEncoding(encoding);
    }

    @Override
    public void startStripe(StripePlanner planner, ReadPhase readPhase) throws IOException {
      // Pass-thru.
      fromReader.startStripe(planner, readPhase);
    }

    @Override
    public void seek(PositionProvider[] index, ReadPhase readPhase) throws IOException {
      // Pass-thru.
      fromReader.seek(index, readPhase);
    }

    @Override
    public void seek(PositionProvider index, ReadPhase readPhase) throws IOException {
      // Pass-thru.
      fromReader.seek(index, readPhase);
    }

    @Override
    public void skipRows(long items, ReadPhase readPhase) throws IOException {
      // Pass-thru.
      fromReader.skipRows(items, readPhase);
    }

    /**
     * Override this to use convertVector.
     * Source and result are member variables in the subclass with the right
     * type.
     * @param elementNum
     * @throws IOException
     */
    // Override this to use convertVector.
    public void setConvertVectorElement(int elementNum) throws IOException {
      throw new RuntimeException("Expected this method to be overridden");
    }

    // Common code used by the conversion.
    public void convertVector(ColumnVector fromColVector,
                              ColumnVector resultColVector,
                              final int batchSize) throws IOException {

      resultColVector.reset();
      if (fromColVector.isRepeating) {
        resultColVector.isRepeating = true;
        if (fromColVector.noNulls || !fromColVector.isNull[0]) {
          setConvertVectorElement(0);
        } else {
          resultColVector.noNulls = false;
          resultColVector.isNull[0] = true;
        }
      } else if (fromColVector.noNulls) {
        for (int i = 0; i < batchSize; i++) {
          setConvertVectorElement(i);
        }
      } else {
        for (int i = 0; i < batchSize; i++) {
          if (!fromColVector.isNull[i]) {
            setConvertVectorElement(i);
          } else {
            resultColVector.noNulls = false;
            resultColVector.isNull[i] = true;
          }
        }
      }
    }

    public void downCastAnyInteger(LongColumnVector longColVector, int elementNum,
                                   TypeDescription readerType) {
      downCastAnyInteger(longColVector, elementNum, longColVector.vector[elementNum], readerType);
    }

    public void downCastAnyInteger(LongColumnVector longColVector, int elementNum, long inputLong,
                                   TypeDescription readerType) {
      long[] vector = longColVector.vector;
      long outputLong;
      Category readerCategory = readerType.getCategory();
      switch (readerCategory) {
        case BOOLEAN:
          // No data loss for boolean.
          vector[elementNum] = inputLong == 0 ? 0 : 1;
          return;
        case BYTE:
          outputLong = (byte) inputLong;
          break;
        case SHORT:
          outputLong = (short) inputLong;
          break;
        case INT:
          outputLong = (int) inputLong;
          break;
        case LONG:
          // No data loss for long.
          vector[elementNum] = inputLong;
          return;
        default:
          throw new RuntimeException("Unexpected type kind " + readerCategory.name());
      }

      if (outputLong != inputLong) {
        // Data loss.
        longColVector.isNull[elementNum] = true;
        longColVector.noNulls = false;
      } else {
        vector[elementNum] = outputLong;
      }
    }

    protected boolean integerDownCastNeeded(TypeDescription fileType, TypeDescription readerType) {
      Integer fileLevel = numericTypes.get(fileType.getCategory());
      Integer schemaLevel = numericTypes.get(readerType.getCategory());
      return (schemaLevel.intValue() < fileLevel.intValue());
    }
  }

  private static TypeReader createFromInteger(int columnId,
                                              TypeDescription fileType,
                                              Context context) throws IOException {
    switch (fileType.getCategory()) {
      case BOOLEAN:
        return new BooleanTreeReader(columnId, context);
      case BYTE:
        return new ByteTreeReader(columnId, context);
      case SHORT:
        return new ShortTreeReader(columnId, context);
      case INT:
        return new IntTreeReader(columnId, context);
      case LONG:
        return new LongTreeReader(columnId, context);
      default:
        throw new RuntimeException("Unexpected type kind " + fileType);
    }
  }

  public static class AnyIntegerFromAnyIntegerTreeReader extends ConvertTreeReader {
    private final TypeDescription readerType;
    private final boolean downCastNeeded;

    AnyIntegerFromAnyIntegerTreeReader(
        int columnId, TypeDescription fileType, TypeDescription readerType,
        Context context) throws IOException {
      super(columnId, createFromInteger(columnId, fileType, context), context);
      this.readerType = readerType;
      downCastNeeded = integerDownCastNeeded(fileType, readerType);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      fromReader.nextVector(previousVector, isNull, batchSize, filterContext, readPhase);
      LongColumnVector resultColVector = (LongColumnVector) previousVector;
      if (downCastNeeded) {
        if (resultColVector.isRepeating) {
          if (resultColVector.noNulls || !resultColVector.isNull[0]) {
            downCastAnyInteger(resultColVector, 0, readerType);
          }
        } else if (resultColVector.noNulls){
          for (int i = 0; i < batchSize; i++) {
            downCastAnyInteger(resultColVector, i, readerType);
          }
        } else {
          for (int i = 0; i < batchSize; i++) {
            if (!resultColVector.isNull[i]) {
              downCastAnyInteger(resultColVector, i, readerType);
            }
          }
        }
      }
    }
  }

  public static class AnyIntegerFromDoubleTreeReader extends ConvertTreeReader {
    private final TypeDescription readerType;
    private DoubleColumnVector doubleColVector;
    private LongColumnVector longColVector;

    AnyIntegerFromDoubleTreeReader(int columnId, TypeDescription fileType,
                                   TypeDescription readerType, Context context)
        throws IOException {
      super(columnId, fileType.getCategory() == Category.DOUBLE ?
          new DoubleTreeReader(columnId, context) :
          new FloatTreeReader(columnId, context), context);
      this.readerType = readerType;
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      double doubleValue = doubleColVector.vector[elementNum];
      if (!doubleCanFitInLong(doubleValue)) {
        longColVector.isNull[elementNum] = true;
        longColVector.noNulls = false;
      } else {
        downCastAnyInteger(longColVector, elementNum, (long) doubleValue, readerType);
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (doubleColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        doubleColVector = new DoubleColumnVector(batchSize);
        longColVector = (LongColumnVector) previousVector;
      } else {
        doubleColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(doubleColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(doubleColVector, longColVector, batchSize);
    }
  }

  public static class AnyIntegerFromDecimalTreeReader extends ConvertTreeReader {
    private final int precision;
    private final int scale;
    private final TypeDescription readerType;
    private DecimalColumnVector decimalColVector;
    private LongColumnVector longColVector;

    AnyIntegerFromDecimalTreeReader(int columnId,
                                    TypeDescription fileType,
                                    TypeDescription readerType,
                                    Context context) throws IOException {
      super(columnId, new DecimalTreeReader(columnId, fileType.getPrecision(),
          fileType.getScale(), context), context);
      this.precision = fileType.getPrecision();
      this.scale = fileType.getScale();
      this.readerType = readerType;
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      HiveDecimalWritable decWritable = decimalColVector.vector[elementNum];
      long[] vector = longColVector.vector;
      Category readerCategory = readerType.getCategory();

      // Check to see if the decimal will fit in the Hive integer data type.
      // If not, set the element to null.
      boolean isInRange;
      switch (readerCategory) {
        case BOOLEAN:
          // No data loss for boolean.
          vector[elementNum] = decWritable.signum() == 0 ? 0 : 1;
          return;
        case BYTE:
          isInRange = decWritable.isByte();
          break;
        case SHORT:
          isInRange = decWritable.isShort();
          break;
        case INT:
          isInRange = decWritable.isInt();
          break;
        case LONG:
          isInRange = decWritable.isLong();
          break;
        default:
          throw new RuntimeException("Unexpected type kind " + readerCategory.name());
      }
      if (!isInRange) {
        longColVector.isNull[elementNum] = true;
        longColVector.noNulls = false;
      } else {
        switch (readerCategory) {
          case BYTE:
            vector[elementNum] = decWritable.byteValue();
            break;
          case SHORT:
            vector[elementNum] = decWritable.shortValue();
            break;
          case INT:
            vector[elementNum] = decWritable.intValue();
            break;
          case LONG:
            vector[elementNum] = decWritable.longValue();
            break;
          default:
            throw new RuntimeException("Unexpected type kind " + readerCategory.name());
        }
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (decimalColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        decimalColVector = new DecimalColumnVector(batchSize, precision, scale);
        longColVector = (LongColumnVector) previousVector;
      } else {
        decimalColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(decimalColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(decimalColVector, longColVector, batchSize);
    }
  }

  public static class AnyIntegerFromStringGroupTreeReader extends ConvertTreeReader {
    private final TypeDescription readerType;
    private BytesColumnVector bytesColVector;
    private LongColumnVector longColVector;

    AnyIntegerFromStringGroupTreeReader(int columnId,
                                        TypeDescription fileType,
                                        TypeDescription readerType,
                                        Context context) throws IOException {
      super(columnId, getStringGroupTreeReader(columnId, fileType, context), context);
      this.readerType = readerType;
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      String string = SerializationUtils.bytesVectorToString(bytesColVector, elementNum);
      long longValue = parseLongFromString(string);
      if (!getIsParseError()) {
        downCastAnyInteger(longColVector, elementNum, longValue, readerType);
      } else {
        longColVector.noNulls = false;
        longColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (bytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        bytesColVector = new BytesColumnVector(batchSize);
        longColVector = (LongColumnVector) previousVector;
      } else {
        bytesColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(bytesColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(bytesColVector, longColVector, batchSize);
    }
  }

  public static class AnyIntegerFromTimestampTreeReader extends ConvertTreeReader {
    private final TypeDescription readerType;
    private TimestampColumnVector timestampColVector;
    private LongColumnVector longColVector;

    AnyIntegerFromTimestampTreeReader(int columnId, TypeDescription readerType,
                                      Context context,
                                      boolean instantType) throws IOException {
      super(columnId, new TimestampTreeReader(columnId, context, instantType), context);
      this.readerType = readerType;
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      long millis = timestampColVector.asScratchTimestamp(elementNum).getTime();
      long longValue = Math.floorDiv(millis, 1000);
      downCastAnyInteger(longColVector, elementNum, longValue, readerType);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (timestampColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        timestampColVector = new TimestampColumnVector(batchSize);
        longColVector = (LongColumnVector) previousVector;
      } else {
        timestampColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(timestampColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(timestampColVector, longColVector, batchSize);
    }
  }

  public static class DoubleFromAnyIntegerTreeReader extends ConvertTreeReader {
    private LongColumnVector longColVector;
    private DoubleColumnVector doubleColVector;

    DoubleFromAnyIntegerTreeReader(int columnId, TypeDescription fileType,
                                   Context context) throws IOException {
      super(columnId, createFromInteger(columnId, fileType, context), context);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {

      double doubleValue = (double) longColVector.vector[elementNum];
      if (!Double.isNaN(doubleValue)) {
        doubleColVector.vector[elementNum] = doubleValue;
      } else {
        doubleColVector.vector[elementNum] = Double.NaN;
        doubleColVector.noNulls = false;
        doubleColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new LongColumnVector(batchSize);
        doubleColVector = (DoubleColumnVector) previousVector;
      } else {
        longColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(longColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(longColVector, doubleColVector, batchSize);
    }
  }

  public static class DoubleFromDecimalTreeReader extends ConvertTreeReader {
    private final int precision;
    private final int scale;
    private DecimalColumnVector decimalColVector;
    private DoubleColumnVector doubleColVector;

    DoubleFromDecimalTreeReader(
        int columnId, TypeDescription fileType, Context context) throws IOException {
      super(columnId, new DecimalTreeReader(columnId, fileType.getPrecision(),
          fileType.getScale(), context), context);
      this.precision = fileType.getPrecision();
      this.scale = fileType.getScale();
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      doubleColVector.vector[elementNum] =
          decimalColVector.vector[elementNum].doubleValue();
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (decimalColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        decimalColVector = new DecimalColumnVector(batchSize, precision, scale);
        doubleColVector = (DoubleColumnVector) previousVector;
      } else {
        decimalColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(decimalColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(decimalColVector, doubleColVector, batchSize);
    }
  }

  public static class DoubleFromStringGroupTreeReader extends ConvertTreeReader {
    private BytesColumnVector bytesColVector;
    private DoubleColumnVector doubleColVector;

    DoubleFromStringGroupTreeReader(int columnId, TypeDescription fileType, Context context)
        throws IOException {
      super(columnId, getStringGroupTreeReader(columnId, fileType, context), context);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      String string = SerializationUtils.bytesVectorToString(bytesColVector, elementNum);
      double doubleValue = parseDoubleFromString(string);
      if (!getIsParseError()) {
        doubleColVector.vector[elementNum] = doubleValue;
      } else {
        doubleColVector.noNulls = false;
        doubleColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (bytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        bytesColVector = new BytesColumnVector(batchSize);
        doubleColVector = (DoubleColumnVector) previousVector;
      } else {
        bytesColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(bytesColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(bytesColVector, doubleColVector, batchSize);
    }
  }

  public static class DoubleFromTimestampTreeReader extends ConvertTreeReader {
    private TimestampColumnVector timestampColVector;
    private DoubleColumnVector doubleColVector;

    DoubleFromTimestampTreeReader(int columnId, Context context,
                                  boolean instantType) throws IOException {
      super(columnId, new TimestampTreeReader(columnId, context, instantType), context);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      Timestamp ts = timestampColVector.asScratchTimestamp(elementNum);
      double result = Math.floorDiv(ts.getTime(), 1000);
      int nano = ts.getNanos();
      if (nano != 0) {
        result += nano / 1_000_000_000.0;
      }
      doubleColVector.vector[elementNum] = result;
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (timestampColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        timestampColVector = new TimestampColumnVector(batchSize);
        doubleColVector = (DoubleColumnVector) previousVector;
      } else {
        timestampColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(timestampColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(timestampColVector, doubleColVector, batchSize);
    }
  }

  public static class FloatFromDoubleTreeReader extends ConvertTreeReader {
    FloatFromDoubleTreeReader(int columnId, Context context) throws IOException {
      super(columnId, new DoubleTreeReader(columnId, context), context);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      // Read present/isNull stream
      fromReader.nextVector(previousVector, isNull, batchSize, filterContext, readPhase);
      DoubleColumnVector vector = (DoubleColumnVector) previousVector;
      if (previousVector.isRepeating) {
        vector.vector[0] = (float) vector.vector[0];
      } else {
        for(int i=0; i < batchSize; ++i) {
          vector.vector[i] = (float) vector.vector[i];
        }
      }
    }
  }

  public static class DecimalFromAnyIntegerTreeReader extends ConvertTreeReader {
    private LongColumnVector longColVector;
    private ColumnVector decimalColVector;
    private final HiveDecimalWritable value;

    DecimalFromAnyIntegerTreeReader(int columnId, TypeDescription fileType, Context context)
        throws IOException {
      super(columnId, createFromInteger(columnId, fileType, context), context);
      value = new HiveDecimalWritable();
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      long longValue = longColVector.vector[elementNum];
      this.value.setFromLong(longValue);
      // The DecimalColumnVector will enforce precision and scale and set the entry to null when out of bounds.
      if (decimalColVector instanceof Decimal64ColumnVector) {
        ((Decimal64ColumnVector) decimalColVector).set(elementNum, value);
      } else {
        ((DecimalColumnVector) decimalColVector).set(elementNum, value);
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new LongColumnVector(batchSize);
        decimalColVector = previousVector;
      } else {
        longColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(longColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(longColVector, decimalColVector, batchSize);
    }
  }

  public static class DecimalFromDoubleTreeReader extends ConvertTreeReader {
    private DoubleColumnVector doubleColVector;
    private ColumnVector decimalColVector;

    DecimalFromDoubleTreeReader(int columnId,
                                TypeDescription fileType,
                                TypeDescription readerType,
                                Context context)
        throws IOException {
      super(columnId, fileType.getCategory() == Category.DOUBLE ?
          new DoubleTreeReader(columnId, context) :
          new FloatTreeReader(columnId, context), context);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      HiveDecimal value =
          HiveDecimal.create(Double.toString(doubleColVector.vector[elementNum]));
      if (value != null) {
        if (decimalColVector instanceof Decimal64ColumnVector) {
          ((Decimal64ColumnVector) decimalColVector).set(elementNum, value);
        } else {
          ((DecimalColumnVector) decimalColVector).set(elementNum, value);
        }
      } else {
        decimalColVector.noNulls = false;
        decimalColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (doubleColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        doubleColVector = new DoubleColumnVector(batchSize);
        decimalColVector = previousVector;
      } else {
        doubleColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(doubleColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(doubleColVector, decimalColVector, batchSize);
    }
  }

  public static class DecimalFromStringGroupTreeReader extends ConvertTreeReader {
    private BytesColumnVector bytesColVector;
    private ColumnVector decimalColVector;

    DecimalFromStringGroupTreeReader(int columnId,
                                     TypeDescription fileType,
                                     TypeDescription readerType,
                                     Context context) throws IOException {
      super(columnId, getStringGroupTreeReader(columnId, fileType, context), context);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      String string = SerializationUtils.bytesVectorToString(bytesColVector, elementNum);
      HiveDecimal value = parseDecimalFromString(string);
      if (value != null) {
        // The DecimalColumnVector will enforce precision and scale and set the entry to null when out of bounds.
        if (decimalColVector instanceof Decimal64ColumnVector) {
          ((Decimal64ColumnVector) decimalColVector).set(elementNum, value);
        } else {
          ((DecimalColumnVector) decimalColVector).set(elementNum, value);
        }
      } else {
        decimalColVector.noNulls = false;
        decimalColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (bytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        bytesColVector = new BytesColumnVector(batchSize);
        decimalColVector = previousVector;
      } else {
        bytesColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(bytesColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(bytesColVector, decimalColVector, batchSize);
    }
  }

  public static class DecimalFromTimestampTreeReader extends ConvertTreeReader {
    private TimestampColumnVector timestampColVector;
    private ColumnVector decimalColVector;

    DecimalFromTimestampTreeReader(int columnId, Context context,
                                   boolean instantType) throws IOException {
      super(columnId, new TimestampTreeReader(columnId, context, instantType), context);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      long seconds = Math.floorDiv(timestampColVector.time[elementNum], 1000);
      long nanos = timestampColVector.nanos[elementNum];
      if (seconds < 0 && nanos > 0) {
        seconds += 1;
        nanos = 1_000_000_000 - nanos;
      }
      BigDecimal secondsBd = new BigDecimal(seconds);
      BigDecimal nanosBd = new BigDecimal(nanos).movePointLeft(9);
      BigDecimal resultBd = (seconds >= 0L) ? secondsBd.add(nanosBd) : secondsBd.subtract(nanosBd);
      HiveDecimal value = HiveDecimal.create(resultBd);
      if (value != null) {
        // The DecimalColumnVector will enforce precision and scale and set the entry to null when out of bounds.
        if (decimalColVector instanceof Decimal64ColumnVector) {
          ((Decimal64ColumnVector) decimalColVector).set(elementNum, value);
        } else {
          ((DecimalColumnVector) decimalColVector).set(elementNum, value);
        }
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (timestampColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        timestampColVector = new TimestampColumnVector(batchSize);
        decimalColVector = previousVector;
      } else {
        timestampColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(timestampColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(timestampColVector, decimalColVector, batchSize);
    }
  }

  public static class DecimalFromDecimalTreeReader extends ConvertTreeReader {
    private DecimalColumnVector fileDecimalColVector;
    private int filePrecision;
    private int fileScale;
    private ColumnVector decimalColVector;

    DecimalFromDecimalTreeReader(
        int columnId, TypeDescription fileType, TypeDescription readerType, Context context)
        throws IOException {
      super(columnId, new DecimalTreeReader(columnId, fileType.getPrecision(),
          fileType.getScale(), context), context);
      filePrecision = fileType.getPrecision();
      fileScale = fileType.getScale();
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {

      if (decimalColVector instanceof Decimal64ColumnVector) {
        ((Decimal64ColumnVector) decimalColVector).set(
            elementNum, fileDecimalColVector.vector[elementNum]);
      } else {
        ((DecimalColumnVector) decimalColVector).set(
            elementNum, fileDecimalColVector.vector[elementNum]);
      }

    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (fileDecimalColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        fileDecimalColVector = new DecimalColumnVector(batchSize, filePrecision, fileScale);
        decimalColVector = previousVector;
      } else {
        fileDecimalColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(fileDecimalColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(fileDecimalColVector, decimalColVector, batchSize);
    }
  }

  public static class StringGroupFromAnyIntegerTreeReader extends ConvertTreeReader {
    protected final TypeDescription readerType;
    protected LongColumnVector longColVector;
    protected BytesColumnVector bytesColVector;

    StringGroupFromAnyIntegerTreeReader(int columnId,
                                        TypeDescription fileType,
                                        TypeDescription readerType,
                                        Context context) throws IOException {
      super(columnId, createFromInteger(columnId, fileType, context), context);
      this.readerType = readerType;
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      byte[] bytes = Long.toString(longColVector.vector[elementNum])
          .getBytes(StandardCharsets.UTF_8);
      assignStringGroupVectorEntry(bytesColVector, elementNum, readerType, bytes);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new LongColumnVector(batchSize);
        bytesColVector = (BytesColumnVector) previousVector;
      } else {
        longColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(longColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(longColVector, bytesColVector, batchSize);
    }
  }

  public static class StringGroupFromBooleanTreeReader extends StringGroupFromAnyIntegerTreeReader {
    private static final byte[] TRUE_BYTES = "TRUE".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FALSE_BYTES = "FALSE".getBytes(StandardCharsets.US_ASCII);

    StringGroupFromBooleanTreeReader(int columnId, TypeDescription fileType,
                                     TypeDescription readerType,
                                     Context context) throws IOException {
      super(columnId, fileType, readerType, context);
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      byte[] bytes = (longColVector.vector[elementNum] != 0 ? TRUE_BYTES : FALSE_BYTES);
      assignStringGroupVectorEntry(bytesColVector, elementNum, readerType, bytes);
    }
  }

  public static class StringGroupFromDoubleTreeReader extends ConvertTreeReader {
    private final TypeDescription readerType;
    private DoubleColumnVector doubleColVector;
    private BytesColumnVector bytesColVector;

    StringGroupFromDoubleTreeReader(int columnId, TypeDescription fileType,
                                    TypeDescription readerType,
                                    Context context) throws IOException {
      super(columnId, fileType.getCategory() == Category.DOUBLE ?
          new DoubleTreeReader(columnId, context) :
          new FloatTreeReader(columnId, context), context);
      this.readerType = readerType;
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      double doubleValue = doubleColVector.vector[elementNum];
      if (!Double.isNaN(doubleValue)) {
        String string = Double.toString(doubleValue);
        byte[] bytes = string.getBytes(StandardCharsets.US_ASCII);
        assignStringGroupVectorEntry(bytesColVector, elementNum, readerType, bytes);
      } else {
        bytesColVector.noNulls = false;
        bytesColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (doubleColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        doubleColVector = new DoubleColumnVector(batchSize);
        bytesColVector = (BytesColumnVector) previousVector;
      } else {
        doubleColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(doubleColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(doubleColVector, bytesColVector, batchSize);
    }
  }


  public static class StringGroupFromDecimalTreeReader extends ConvertTreeReader {
    private int precision;
    private int scale;
    private final TypeDescription readerType;
    private DecimalColumnVector decimalColVector;
    private BytesColumnVector bytesColVector;
    private byte[] scratchBuffer;

    StringGroupFromDecimalTreeReader(int columnId,
                                     TypeDescription fileType,
                                     TypeDescription readerType,
                                     Context context) throws IOException {
      super(columnId, new DecimalTreeReader(columnId, fileType.getPrecision(),
          fileType.getScale(), context), context);
      this.precision = fileType.getPrecision();
      this.scale = fileType.getScale();
      this.readerType = readerType;
      scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      HiveDecimalWritable decWritable = decimalColVector.vector[elementNum];

      // Convert decimal into bytes instead of a String for better performance.
      final int byteIndex = decWritable.toBytes(scratchBuffer);

      assignStringGroupVectorEntry(
          bytesColVector, elementNum, readerType,
          scratchBuffer, byteIndex, HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES - byteIndex);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (decimalColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        decimalColVector = new DecimalColumnVector(batchSize, precision, scale);
        bytesColVector = (BytesColumnVector) previousVector;
      } else {
        decimalColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(decimalColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(decimalColVector, bytesColVector, batchSize);
    }
  }

  /**
   * The format for converting from/to string/date.
   * Eg. "2019-07-09"
   */
  static final DateTimeFormatter DATE_FORMAT =
      new DateTimeFormatterBuilder()
          .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
          .appendLiteral('-')
          .appendValue(ChronoField.MONTH_OF_YEAR, 2)
          .appendLiteral('-')
          .appendValue(ChronoField.DAY_OF_MONTH, 2)
          .toFormatter();

  /**
   * The format for converting from/to string/timestamp.
   * Eg. "2019-07-09 13:11:00"
   */
  static final DateTimeFormatter TIMESTAMP_FORMAT =
      new DateTimeFormatterBuilder()
          .append(DATE_FORMAT)
          .appendLiteral(' ')
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .optionalStart()
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter();

  /**
   * The format for converting from/to string/timestamp with local time zone.
   * Eg. "2019-07-09 13:11:00 America/Los_Angeles"
   */
  static final DateTimeFormatter INSTANT_TIMESTAMP_FORMAT =
      new DateTimeFormatterBuilder()
          .append(TIMESTAMP_FORMAT)
          .appendPattern(" VV")
          .toFormatter();

  static final long MIN_EPOCH_SECONDS = Instant.MIN.getEpochSecond();
  static final long MAX_EPOCH_SECONDS = Instant.MAX.getEpochSecond();

  /**
   * Create an Instant from an entry in a TimestampColumnVector.
   * It assumes that vector.isRepeating and null values have been handled
   * before we get called.
   * @param vector the timestamp column vector
   * @param element the element number
   * @return a timestamp Instant
   */
  static Instant timestampToInstant(TimestampColumnVector vector, int element) {
    return Instant.ofEpochSecond(Math.floorDiv(vector.time[element], 1000),
        vector.nanos[element]);
  }

  /**
   * Convert a decimal to an Instant using seconds & nanos.
   * @param vector the decimal64 column vector
   * @param element the element number to use
   * @param value the writable container to reuse
   * @return the timestamp instant
   */
  static Instant decimalToInstant(DecimalColumnVector vector, int element,
                                  HiveDecimalWritable value) {
    final HiveDecimalWritable writable = vector.vector[element];
    final long seconds = writable.longValue();

    if (seconds < MIN_EPOCH_SECONDS || seconds > MAX_EPOCH_SECONDS) {
      return null;
    } else {
      // copy the value so that we can mutate it
      value.set(writable);
      value.mutateFractionPortion();
      value.mutateScaleByPowerOfTen(9);
      int nanos = (int) value.longValue();
      return Instant.ofEpochSecond(seconds, nanos);
    }
  }

  public static class StringGroupFromTimestampTreeReader extends ConvertTreeReader {
    private final TypeDescription readerType;
    private final ZoneId local;
    private final DateTimeFormatter formatter;
    private TimestampColumnVector timestampColVector;
    private BytesColumnVector bytesColVector;

    StringGroupFromTimestampTreeReader(int columnId, TypeDescription readerType,
                                       Context context,
                                       boolean instantType) throws IOException {
      super(columnId, new TimestampTreeReader(columnId, context, instantType), context);
      this.readerType = readerType;
      local = context.getUseUTCTimestamp() ? ZoneId.of("UTC")
          : ZoneId.systemDefault();
      Chronology chronology = context.useProlepticGregorian()
          ? IsoChronology.INSTANCE : HybridChronology.INSTANCE;
      formatter = (instantType ? INSTANT_TIMESTAMP_FORMAT : TIMESTAMP_FORMAT)
          .withChronology(chronology);
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      String string = timestampToInstant(timestampColVector, elementNum).atZone(local)
          .format(formatter);
      byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
      assignStringGroupVectorEntry(bytesColVector, elementNum, readerType, bytes);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (timestampColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        timestampColVector = new TimestampColumnVector(batchSize);
        bytesColVector = (BytesColumnVector) previousVector;
      } else {
        timestampColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(timestampColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(timestampColVector, bytesColVector, batchSize);
    }
  }

  public static class StringGroupFromDateTreeReader extends ConvertTreeReader {
    private final TypeDescription readerType;
    private DateColumnVector longColVector;
    private BytesColumnVector bytesColVector;
    private final boolean useProlepticGregorian;

    StringGroupFromDateTreeReader(int columnId, TypeDescription readerType,
                                  Context context) throws IOException {
      super(columnId, new DateTreeReader(columnId, context), context);
      this.readerType = readerType;
      useProlepticGregorian = context.useProlepticGregorian();
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      String dateStr = DateUtils.printDate((int) (longColVector.vector[elementNum]),
          useProlepticGregorian);
      byte[] bytes = dateStr.getBytes(StandardCharsets.UTF_8);
      assignStringGroupVectorEntry(bytesColVector, elementNum, readerType, bytes);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new DateColumnVector(batchSize);
        bytesColVector = (BytesColumnVector) previousVector;
      } else {
        longColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(longColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(longColVector, bytesColVector, batchSize);
    }
  }

  public static class StringGroupFromStringGroupTreeReader extends ConvertTreeReader {
    private final TypeDescription readerType;

    StringGroupFromStringGroupTreeReader(int columnId,
                                         TypeDescription fileType,
                                         TypeDescription readerType,
                                         Context context) throws IOException {
      super(columnId, getStringGroupTreeReader(columnId, fileType, context), context);
      this.readerType = readerType;
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      fromReader.nextVector(previousVector, isNull, batchSize, filterContext, readPhase);

      BytesColumnVector resultColVector = (BytesColumnVector) previousVector;

      if (resultColVector.isRepeating) {
        if (resultColVector.noNulls || !resultColVector.isNull[0]) {
          convertStringGroupVectorElement(resultColVector, 0, readerType);
        } else {
          // Remains null.
        }
      } else if (resultColVector.noNulls){
        for (int i = 0; i < batchSize; i++) {
          convertStringGroupVectorElement(resultColVector, i, readerType);
        }
      } else {
        for (int i = 0; i < batchSize; i++) {
          if (!resultColVector.isNull[i]) {
            convertStringGroupVectorElement(resultColVector, i, readerType);
          } else {
            // Remains null.
          }
        }
      }
    }
  }

  public static class StringGroupFromBinaryTreeReader extends ConvertTreeReader {
    private final TypeDescription readerType;
    private BytesColumnVector inBytesColVector;
    private BytesColumnVector outBytesColVector;

    StringGroupFromBinaryTreeReader(int columnId, TypeDescription readerType,
                                    Context context) throws IOException {
      super(columnId, new BinaryTreeReader(columnId, context), context);
      this.readerType = readerType;
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      byte[] bytes = inBytesColVector.vector[elementNum];
      int start = inBytesColVector.start[elementNum];
      int length = inBytesColVector.length[elementNum];
      final byte[] string = (length == 0) ? ArrayUtils.EMPTY_BYTE_ARRAY : new byte[3 * length - 1];
      for(int p = 0; p < string.length; p += 2) {
        if (p != 0) {
          string[p++] = ' ';
        }
        int num = 0xff & bytes[start++];
        int digit = num / 16;
        string[p] = (byte)((digit) + (digit < 10 ? '0' : 'a' - 10));
        digit = num % 16;
        string[p + 1] = (byte)((digit) + (digit < 10 ? '0' : 'a' - 10));
      }
      assignStringGroupVectorEntry(outBytesColVector, elementNum, readerType,
          string, 0, string.length);
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (inBytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        inBytesColVector = new BytesColumnVector(batchSize);
        outBytesColVector = (BytesColumnVector) previousVector;
      } else {
        inBytesColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(inBytesColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(inBytesColVector, outBytesColVector, batchSize);
    }
  }

  public static class TimestampFromAnyIntegerTreeReader extends ConvertTreeReader {
    private LongColumnVector longColVector;
    private TimestampColumnVector timestampColVector;
    private final boolean useUtc;
    private final TimeZone local;
    private final boolean fileUsedProlepticGregorian;
    private final boolean useProlepticGregorian;

    TimestampFromAnyIntegerTreeReader(int columnId, TypeDescription fileType,
                                      Context context,
                                      boolean isInstant) throws IOException {
      super(columnId, createFromInteger(columnId, fileType, context), context);
      this.useUtc = isInstant || context.getUseUTCTimestamp();
      local = TimeZone.getDefault();
      fileUsedProlepticGregorian = context.fileUsedProlepticGregorian();
      useProlepticGregorian = context.useProlepticGregorian();
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      long millis = longColVector.vector[elementNum] * 1000;
      timestampColVector.time[elementNum] = useUtc
          ? millis
          : SerializationUtils.convertFromUtc(local, millis);
      timestampColVector.nanos[elementNum] = 0;
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new LongColumnVector(batchSize);
        timestampColVector = (TimestampColumnVector) previousVector;
      } else {
        longColVector.ensureSize(batchSize, false);
      }
      timestampColVector.changeCalendar(fileUsedProlepticGregorian, false);
      // Read present/isNull stream
      fromReader.nextVector(longColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(longColVector, timestampColVector, batchSize);
      timestampColVector.changeCalendar(useProlepticGregorian, true);
    }
  }

  public static class TimestampFromDoubleTreeReader extends ConvertTreeReader {
    private DoubleColumnVector doubleColVector;
    private TimestampColumnVector timestampColVector;
    private final boolean useUtc;
    private final TimeZone local;
    private final boolean useProlepticGregorian;
    private final boolean fileUsedProlepticGregorian;

    TimestampFromDoubleTreeReader(int columnId, TypeDescription fileType,
                                  TypeDescription readerType, Context context) throws IOException {
      super(columnId, fileType.getCategory() == Category.DOUBLE ?
          new DoubleTreeReader(columnId, context) :
          new FloatTreeReader(columnId, context), context);
      useUtc = readerType.getCategory() == Category.TIMESTAMP_INSTANT ||
          context.getUseUTCTimestamp();
      local = TimeZone.getDefault();
      useProlepticGregorian = context.useProlepticGregorian();
      fileUsedProlepticGregorian = context.fileUsedProlepticGregorian();
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      double seconds = doubleColVector.vector[elementNum];
      if (!useUtc) {
        seconds = SerializationUtils.convertFromUtc(local, seconds);
      }
      // overflow
      double doubleMillis = seconds * 1000;
      long millis = Math.round(doubleMillis);
      if (doubleMillis > Long.MAX_VALUE || doubleMillis < Long.MIN_VALUE ||
          ((millis >= 0) != (doubleMillis >= 0))) {
        timestampColVector.time[elementNum] = 0L;
        timestampColVector.nanos[elementNum] = 0;
        timestampColVector.isNull[elementNum] = true;
        timestampColVector.noNulls = false;
      } else {
        timestampColVector.time[elementNum] = millis;
        timestampColVector.nanos[elementNum] =
            (int) Math.floorMod(millis, 1000) * 1_000_000;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (doubleColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        doubleColVector = new DoubleColumnVector(batchSize);
        timestampColVector = (TimestampColumnVector) previousVector;
      } else {
        doubleColVector.ensureSize(batchSize, false);
      }
      timestampColVector.changeCalendar(fileUsedProlepticGregorian, false);
      // Read present/isNull stream
      fromReader.nextVector(doubleColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(doubleColVector, timestampColVector, batchSize);
      timestampColVector.changeCalendar(useProlepticGregorian, true);
    }
  }

  public static class TimestampFromDecimalTreeReader extends ConvertTreeReader {
    private final int precision;
    private final int scale;
    private DecimalColumnVector decimalColVector;
    private TimestampColumnVector timestampColVector;
    private final boolean useUtc;
    private final TimeZone local;
    private final boolean useProlepticGregorian;
    private final boolean fileUsedProlepticGregorian;
    private final HiveDecimalWritable value;

    TimestampFromDecimalTreeReader(int columnId, TypeDescription fileType,
                                   Context context,
                                   boolean isInstant) throws IOException {
      super(columnId, new DecimalTreeReader(columnId, fileType.getPrecision(),
          fileType.getScale(), context), context);
      this.precision = fileType.getPrecision();
      this.scale = fileType.getScale();
      useUtc = isInstant || context.getUseUTCTimestamp();
      local = TimeZone.getDefault();
      useProlepticGregorian = context.useProlepticGregorian();
      fileUsedProlepticGregorian = context.fileUsedProlepticGregorian();
      value = new HiveDecimalWritable();
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      Instant t = decimalToInstant(decimalColVector, elementNum, value);
      if (t == null) {
        timestampColVector.noNulls = false;
        timestampColVector.isNull[elementNum] = true;
      } else if (!useUtc) {
        long millis = t.toEpochMilli();
        timestampColVector.time[elementNum] =
            SerializationUtils.convertFromUtc(local, millis);
        timestampColVector.nanos[elementNum] = t.getNano();
      } else {
        timestampColVector.time[elementNum] = t.toEpochMilli();
        timestampColVector.nanos[elementNum] = t.getNano();
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (decimalColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        decimalColVector = new DecimalColumnVector(batchSize, precision, scale);
        timestampColVector = (TimestampColumnVector) previousVector;
      } else {
        decimalColVector.ensureSize(batchSize, false);
      }
      timestampColVector.changeCalendar(fileUsedProlepticGregorian, false);
      // Read present/isNull stream
      fromReader.nextVector(decimalColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(decimalColVector, timestampColVector, batchSize);
      timestampColVector.changeCalendar(useProlepticGregorian, true);
    }
  }

  public static class TimestampFromStringGroupTreeReader extends ConvertTreeReader {
    private BytesColumnVector bytesColVector;
    private TimestampColumnVector timestampColVector;
    private final DateTimeFormatter formatter;
    private final boolean useProlepticGregorian;

    TimestampFromStringGroupTreeReader(int columnId, TypeDescription fileType,
                                       Context context, boolean isInstant)
        throws IOException {
      super(columnId, getStringGroupTreeReader(columnId, fileType, context), context);
      useProlepticGregorian = context.useProlepticGregorian();
      Chronology chronology = useProlepticGregorian
          ? IsoChronology.INSTANCE
          : HybridChronology.INSTANCE;
      if (isInstant) {
        formatter = INSTANT_TIMESTAMP_FORMAT.withChronology(chronology);
      } else {
        formatter = TIMESTAMP_FORMAT
            .withZone(context.getUseUTCTimestamp() ?
                ZoneId.of("UTC") :
                ZoneId.systemDefault())
            .withChronology(chronology);
      }
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      String str = SerializationUtils.bytesVectorToString(bytesColVector,
          elementNum);
      try {
        Instant instant = Instant.from(formatter.parse(str));
        timestampColVector.time[elementNum] = instant.toEpochMilli();
        timestampColVector.nanos[elementNum] = instant.getNano();
      } catch (DateTimeParseException e) {
        timestampColVector.noNulls = false;
        timestampColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (bytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        bytesColVector = new BytesColumnVector(batchSize);
        timestampColVector = (TimestampColumnVector) previousVector;
      } else {
        bytesColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(bytesColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(bytesColVector, timestampColVector, batchSize);
      timestampColVector.changeCalendar(useProlepticGregorian, false);
    }
  }

  public static class TimestampFromDateTreeReader extends ConvertTreeReader {
    private DateColumnVector longColVector;
    private TimestampColumnVector timestampColVector;
    private final boolean useUtc;
    private final TimeZone local = TimeZone.getDefault();
    private final boolean useProlepticGregorian;

    TimestampFromDateTreeReader(int columnId, TypeDescription readerType,
                                Context context) throws IOException {
      super(columnId, new DateTreeReader(columnId, context), context);
      useUtc = readerType.getCategory() == Category.TIMESTAMP_INSTANT ||
          context.getUseUTCTimestamp();
      useProlepticGregorian = context.useProlepticGregorian();
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      long days = longColVector.vector[elementNum];
      long millis = days * 24 * 60 * 60 * 1000;
      timestampColVector.time[elementNum] = useUtc ?
          millis :
          SerializationUtils.convertFromUtc(local, millis);
      timestampColVector.nanos[elementNum] = 0;
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (longColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        longColVector = new DateColumnVector(batchSize);
        timestampColVector = (TimestampColumnVector) previousVector;
      } else {
        longColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(longColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(longColVector, timestampColVector, batchSize);
      timestampColVector.changeCalendar(useProlepticGregorian, false);
    }
  }

  public static class DateFromStringGroupTreeReader extends ConvertTreeReader {
    private BytesColumnVector bytesColVector;
    private LongColumnVector longColVector;
    private DateColumnVector dateColumnVector;
    private final boolean useProlepticGregorian;

    DateFromStringGroupTreeReader(int columnId, TypeDescription fileType, Context context)
        throws IOException {
      super(columnId, getStringGroupTreeReader(columnId, fileType, context), context);
      useProlepticGregorian = context.useProlepticGregorian();
    }

    @Override
    public void setConvertVectorElement(int elementNum) {
      String stringValue =
          SerializationUtils.bytesVectorToString(bytesColVector, elementNum);
      Integer dateValue = DateUtils.parseDate(stringValue, useProlepticGregorian);
      if (dateValue != null) {
        longColVector.vector[elementNum] = dateValue;
      } else {
        longColVector.noNulls = false;
        longColVector.isNull[elementNum] = true;
      }
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (bytesColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        bytesColVector = new BytesColumnVector(batchSize);
        longColVector = (LongColumnVector) previousVector;
        if (longColVector instanceof DateColumnVector) {
          dateColumnVector = (DateColumnVector) longColVector;
        } else {
          dateColumnVector = null;
          if (useProlepticGregorian) {
            throw new IllegalArgumentException("Can't use LongColumnVector with" +
                " proleptic Gregorian dates.");
          }
        }
      } else {
        bytesColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(bytesColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(bytesColVector, longColVector, batchSize);
      if (dateColumnVector != null) {
        dateColumnVector.changeCalendar(useProlepticGregorian, false);
      }
    }
  }

  public static class DateFromTimestampTreeReader extends ConvertTreeReader {
    private TimestampColumnVector timestampColVector;
    private LongColumnVector longColVector;
    private final ZoneId local;
    private final boolean useProlepticGregorian;

    DateFromTimestampTreeReader(int columnId, Context context,
                                boolean instantType) throws IOException {
      super(columnId, new TimestampTreeReader(columnId, context, instantType), context);
      boolean useUtc = instantType || context.getUseUTCTimestamp();
      local = useUtc ? ZoneId.of("UTC") : ZoneId.systemDefault();
      useProlepticGregorian = context.useProlepticGregorian();
    }

    @Override
    public void setConvertVectorElement(int elementNum) throws IOException {
      LocalDate day = LocalDate.from(
          Instant.ofEpochSecond(timestampColVector.time[elementNum] / 1000,
                  timestampColVector.nanos[elementNum])
              .atZone(local));
      longColVector.vector[elementNum] = day.toEpochDay();
    }

    @Override
    public void nextVector(ColumnVector previousVector,
                           boolean[] isNull,
                           final int batchSize,
                           FilterContext filterContext,
                           ReadPhase readPhase) throws IOException {
      if (timestampColVector == null) {
        // Allocate column vector for file; cast column vector for reader.
        timestampColVector = new TimestampColumnVector(batchSize);
        longColVector = (LongColumnVector) previousVector;
        if (useProlepticGregorian && !(longColVector instanceof DateColumnVector)) {
          throw new IllegalArgumentException("Can't use LongColumnVector with" +
              " proleptic Gregorian dates.");
        }
      } else {
        timestampColVector.ensureSize(batchSize, false);
      }
      // Read present/isNull stream
      fromReader.nextVector(timestampColVector, isNull, batchSize, filterContext, readPhase);

      convertVector(timestampColVector, longColVector, batchSize);
      if (longColVector instanceof DateColumnVector) {
        ((DateColumnVector) longColVector)
            .changeCalendar(useProlepticGregorian, false);
      }
    }
  }

  private static TypeReader createBooleanConvertTreeReader(int columnId,
                                                           TypeDescription fileType,
                                                           TypeDescription readerType,
                                                           Context context) throws IOException {

    // CONVERT from BOOLEAN to schema type.
    //
    switch (readerType.getCategory()) {

      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        if (fileType.getCategory() == readerType.getCategory()) {
          throw new IllegalArgumentException("No conversion of type " +
              readerType.getCategory() + " to self needed");
        }
        return new AnyIntegerFromAnyIntegerTreeReader(columnId, fileType, readerType,
            context);

      case FLOAT:
      case DOUBLE:
        return new DoubleFromAnyIntegerTreeReader(columnId, fileType, context);

      case DECIMAL:
        return new DecimalFromAnyIntegerTreeReader(columnId, fileType, context);

      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringGroupFromBooleanTreeReader(columnId, fileType, readerType,
            context);

      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return new TimestampFromAnyIntegerTreeReader(columnId, fileType, context,
            readerType.getCategory() == Category.TIMESTAMP_INSTANT);

      // Not currently supported conversion(s):
      case BINARY:
      case DATE:
      case STRUCT:
      case LIST:
      case MAP:
      case UNION:
      default:
        throw new IllegalArgumentException("Unsupported type " +
            readerType.getCategory());
    }
  }

  private static TypeReader createAnyIntegerConvertTreeReader(int columnId,
                                                              TypeDescription fileType,
                                                              TypeDescription readerType,
                                                              Context context) throws IOException {

    // CONVERT from (BYTE, SHORT, INT, LONG) to schema type.
    //
    switch (readerType.getCategory()) {

      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        if (fileType.getCategory() == readerType.getCategory()) {
          throw new IllegalArgumentException("No conversion of type " +
              readerType.getCategory() + " to self needed");
        }
        return new AnyIntegerFromAnyIntegerTreeReader(columnId, fileType, readerType,
            context);

      case FLOAT:
      case DOUBLE:
        return new DoubleFromAnyIntegerTreeReader(columnId, fileType,
            context);

      case DECIMAL:
        return new DecimalFromAnyIntegerTreeReader(columnId, fileType, context);

      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringGroupFromAnyIntegerTreeReader(columnId, fileType, readerType,
            context);

      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return new TimestampFromAnyIntegerTreeReader(columnId, fileType, context,
            readerType.getCategory() == Category.TIMESTAMP_INSTANT);

      // Not currently supported conversion(s):
      case BINARY:
      case DATE:

      case STRUCT:
      case LIST:
      case MAP:
      case UNION:
      default:
        throw new IllegalArgumentException("Unsupported type " +
            readerType.getCategory());
    }
  }

  private static TypeReader createDoubleConvertTreeReader(int columnId,
                                                          TypeDescription fileType,
                                                          TypeDescription readerType,
                                                          Context context) throws IOException {

    // CONVERT from DOUBLE to schema type.
    switch (readerType.getCategory()) {

      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new AnyIntegerFromDoubleTreeReader(columnId, fileType, readerType, context);

      case FLOAT:
        return new FloatFromDoubleTreeReader(columnId, context);

      case DOUBLE:
        return new FloatTreeReader(columnId, context);

      case DECIMAL:
        return new DecimalFromDoubleTreeReader(columnId, fileType, readerType, context);

      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringGroupFromDoubleTreeReader(columnId, fileType, readerType, context);

      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return new TimestampFromDoubleTreeReader(columnId, fileType, readerType, context);

      // Not currently supported conversion(s):
      case BINARY:
      case DATE:

      case STRUCT:
      case LIST:
      case MAP:
      case UNION:
      default:
        throw new IllegalArgumentException("Unsupported type " +
            readerType.getCategory());
    }
  }

  private static TypeReader createDecimalConvertTreeReader(int columnId,
                                                           TypeDescription fileType,
                                                           TypeDescription readerType,
                                                           Context context) throws IOException {

    // CONVERT from DECIMAL to schema type.
    switch (readerType.getCategory()) {

      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new AnyIntegerFromDecimalTreeReader(columnId, fileType, readerType, context);

      case FLOAT:
      case DOUBLE:
        return new DoubleFromDecimalTreeReader(columnId, fileType, context);

      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringGroupFromDecimalTreeReader(columnId, fileType, readerType, context);

      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return new TimestampFromDecimalTreeReader(columnId, fileType, context,
            readerType.getCategory() == Category.TIMESTAMP_INSTANT);

      case DECIMAL:
        return new DecimalFromDecimalTreeReader(columnId, fileType, readerType, context);

      // Not currently supported conversion(s):
      case BINARY:
      case DATE:

      case STRUCT:
      case LIST:
      case MAP:
      case UNION:
      default:
        throw new IllegalArgumentException("Unsupported type " +
            readerType.getCategory());
    }
  }

  private static TypeReader createStringConvertTreeReader(int columnId,
                                                          TypeDescription fileType,
                                                          TypeDescription readerType,
                                                          Context context) throws IOException {

    // CONVERT from STRING to schema type.
    switch (readerType.getCategory()) {

      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new AnyIntegerFromStringGroupTreeReader(columnId, fileType, readerType, context);

      case FLOAT:
      case DOUBLE:
        return new DoubleFromStringGroupTreeReader(columnId, fileType, context);

      case DECIMAL:
        return new DecimalFromStringGroupTreeReader(columnId, fileType, readerType, context);

      case CHAR:
      case VARCHAR:
      case STRING:
        return new StringGroupFromStringGroupTreeReader(columnId, fileType, readerType, context);

      case BINARY:
        return new BinaryTreeReader(columnId, context);

      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return new TimestampFromStringGroupTreeReader(columnId, fileType, context,
            readerType.getCategory() == Category.TIMESTAMP_INSTANT);

      case DATE:
        return new DateFromStringGroupTreeReader(columnId, fileType, context);

      // Not currently supported conversion(s):

      case STRUCT:
      case LIST:
      case MAP:
      case UNION:
      default:
        throw new IllegalArgumentException("Unsupported type " +
            readerType.getCategory());
    }
  }

  private static TypeReader createTimestampConvertTreeReader(int columnId,
                                                             TypeDescription fileType,
                                                             TypeDescription readerType,
                                                             Context context) throws IOException {
    boolean isInstant = fileType.getCategory() == Category.TIMESTAMP_INSTANT;
    // CONVERT from TIMESTAMP to schema type.
    switch (readerType.getCategory()) {

      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new AnyIntegerFromTimestampTreeReader(columnId, readerType,
            context, isInstant);

      case FLOAT:
      case DOUBLE:
        return new DoubleFromTimestampTreeReader(columnId, context, isInstant);

      case DECIMAL:
        return new DecimalFromTimestampTreeReader(columnId, context, isInstant);

      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringGroupFromTimestampTreeReader(columnId, readerType,
            context, isInstant);

      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return new TimestampTreeReader(columnId, context, isInstant);

      case DATE:
        return new DateFromTimestampTreeReader(columnId, context, isInstant);

      // Not currently supported conversion(s):
      case BINARY:

      case STRUCT:
      case LIST:
      case MAP:
      case UNION:
      default:
        throw new IllegalArgumentException("Unsupported type " +
            readerType.getCategory());
    }
  }

  private static TypeReader createDateConvertTreeReader(int columnId,
                                                        TypeDescription readerType,
                                                        Context context) throws IOException {

    // CONVERT from DATE to schema type.
    switch (readerType.getCategory()) {

      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringGroupFromDateTreeReader(columnId, readerType, context);

      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return new TimestampFromDateTreeReader(columnId, readerType, context);

      case DATE:
        throw new IllegalArgumentException("No conversion of type " +
            readerType.getCategory() + " to self needed");

        // Not currently supported conversion(s):
      case BOOLEAN:
      case BYTE:
      case FLOAT:
      case SHORT:
      case INT:
      case LONG:
      case DOUBLE:
      case BINARY:
      case DECIMAL:

      case STRUCT:
      case LIST:
      case MAP:
      case UNION:
      default:
        throw new IllegalArgumentException("Unsupported type " +
            readerType.getCategory());
    }
  }

  private static TypeReader createBinaryConvertTreeReader(int columnId,
                                                          TypeDescription readerType,
                                                          Context context) throws IOException {

    // CONVERT from BINARY to schema type.
    switch (readerType.getCategory()) {

      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringGroupFromBinaryTreeReader(columnId, readerType, context);

      case BINARY:
        throw new IllegalArgumentException("No conversion of type " +
            readerType.getCategory() + " to self needed");

        // Not currently supported conversion(s):
      case BOOLEAN:
      case BYTE:
      case FLOAT:
      case SHORT:
      case INT:
      case LONG:
      case DOUBLE:
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
      case DECIMAL:
      case STRUCT:
      case LIST:
      case MAP:
      case UNION:
      default:
        throw new IllegalArgumentException("Unsupported type " +
            readerType.getCategory());
    }
  }

  /**
   * (Rules from Hive's PrimitiveObjectInspectorUtils conversion)
   *
   * To BOOLEAN, BYTE, SHORT, INT, LONG:
   *   Convert from (BOOLEAN, BYTE, SHORT, INT, LONG) with down cast if necessary.
   *   Convert from (FLOAT, DOUBLE) using type cast to long and down cast if necessary.
   *   Convert from DECIMAL from longValue and down cast if necessary.
   *   Convert from STRING using LazyLong.parseLong and down cast if necessary.
   *   Convert from (CHAR, VARCHAR) from Integer.parseLong and down cast if necessary.
   *   Convert from TIMESTAMP using timestamp getSeconds and down cast if necessary.
   *
   *   AnyIntegerFromAnyIntegerTreeReader (written)
   *   AnyIntegerFromFloatTreeReader (written)
   *   AnyIntegerFromDoubleTreeReader (written)
   *   AnyIntegerFromDecimalTreeReader (written)
   *   AnyIntegerFromStringGroupTreeReader (written)
   *   AnyIntegerFromTimestampTreeReader (written)
   *
   * To FLOAT/DOUBLE:
   *   Convert from (BOOLEAN, BYTE, SHORT, INT, LONG) using cast
   *   Convert from FLOAT using cast
   *   Convert from DECIMAL using getDouble
   *   Convert from (STRING, CHAR, VARCHAR) using Double.parseDouble
   *   Convert from TIMESTAMP using timestamp getDouble
   *
   *   FloatFromAnyIntegerTreeReader (existing)
   *   FloatFromDoubleTreeReader (written)
   *   FloatFromDecimalTreeReader (written)
   *   FloatFromStringGroupTreeReader (written)
   *
   *   DoubleFromAnyIntegerTreeReader (existing)
   *   DoubleFromFloatTreeReader (existing)
   *   DoubleFromDecimalTreeReader (written)
   *   DoubleFromStringGroupTreeReader (written)
   *
   * To DECIMAL:
   *   Convert from (BOOLEAN, BYTE, SHORT, INT, LONG) using to HiveDecimal.create()
   *   Convert from (FLOAT, DOUBLE) using to HiveDecimal.create(string value)
   *   Convert from (STRING, CHAR, VARCHAR) using HiveDecimal.create(string value)
   *   Convert from TIMESTAMP using HiveDecimal.create(string value of timestamp getDouble)
   *
   *   DecimalFromAnyIntegerTreeReader (existing)
   *   DecimalFromFloatTreeReader (existing)
   *   DecimalFromDoubleTreeReader (existing)
   *   DecimalFromStringGroupTreeReader (written)
   *
   * To STRING, CHAR, VARCHAR:
   *   Convert from (BYTE, SHORT, INT, LONG) using to string conversion
   *   Convert from BOOLEAN using boolean (True/False) conversion
   *   Convert from (FLOAT, DOUBLE) using to string conversion
   *   Convert from DECIMAL using HiveDecimal.toString
   *   Convert from CHAR by stripping pads
   *   Convert from VARCHAR with value
   *   Convert from TIMESTAMP using Timestamp.toString
   *   Convert from DATE using Date.toString
   *   Convert from BINARY using Text.decode
   *
   *   StringGroupFromAnyIntegerTreeReader (written)
   *   StringGroupFromBooleanTreeReader (written)
   *   StringGroupFromFloatTreeReader (written)
   *   StringGroupFromDoubleTreeReader (written)
   *   StringGroupFromDecimalTreeReader (written)
   *
   *   String from Char/Varchar conversion
   *   Char from String/Varchar conversion
   *   Varchar from String/Char conversion
   *
   *   StringGroupFromTimestampTreeReader (written)
   *   StringGroupFromDateTreeReader (written)
   *   StringGroupFromBinaryTreeReader *****
   *
   * To TIMESTAMP:
   *   Convert from (BOOLEAN, BYTE, SHORT, INT, LONG) using TimestampWritable.longToTimestamp
   *   Convert from (FLOAT, DOUBLE) using TimestampWritable.doubleToTimestamp
   *   Convert from DECIMAL using TimestampWritable.decimalToTimestamp
   *   Convert from (STRING, CHAR, VARCHAR) using string conversion
   *   Or, from DATE
   *
   *   TimestampFromAnyIntegerTreeReader (written)
   *   TimestampFromFloatTreeReader (written)
   *   TimestampFromDoubleTreeReader (written)
   *   TimestampFromDecimalTreeReader (written)
   *   TimestampFromStringGroupTreeReader (written)
   *   TimestampFromDateTreeReader
   *
   *
   * To DATE:
   *   Convert from (STRING, CHAR, VARCHAR) using string conversion.
   *   Or, from TIMESTAMP.
   *
   *  DateFromStringGroupTreeReader (written)
   *  DateFromTimestampTreeReader (written)
   *
   * To BINARY:
   *   Convert from (STRING, CHAR, VARCHAR) using getBinaryFromText
   *
   *  BinaryFromStringGroupTreeReader (written)
   *
   * (Notes from StructConverter)
   *
   * To STRUCT:
   *   Input must be data type STRUCT
   *   minFields = Math.min(numSourceFields, numTargetFields)
   *   Convert those fields
   *   Extra targetFields to NULL
   *
   * (Notes from ListConverter)
   *
   * To LIST:
   *   Input must be data type LIST
   *   Convert elements
   *
   * (Notes from MapConverter)
   *
   * To MAP:
   *   Input must be data type MAP
   *   Convert keys and values
   *
   * (Notes from UnionConverter)
   *
   * To UNION:
   *   Input must be data type UNION
   *   Convert value for tag
   *
   * @param readerType
   * @return
   * @throws IOException
   */
  public static TypeReader createConvertTreeReader(TypeDescription readerType,
                                                   Context context) throws IOException {
    final SchemaEvolution evolution = context.getSchemaEvolution();

    TypeDescription fileType = evolution.getFileType(readerType.getId());
    int columnId = fileType.getId();

    switch (fileType.getCategory()) {

      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return createAnyIntegerConvertTreeReader(columnId, fileType, readerType, context);

      case BOOLEAN:
        return createBooleanConvertTreeReader(columnId, fileType, readerType, context);

      case FLOAT:
      case DOUBLE:
        return createDoubleConvertTreeReader(columnId, fileType, readerType, context);

      case DECIMAL:
        return createDecimalConvertTreeReader(columnId, fileType, readerType, context);

      case STRING:
      case CHAR:
      case VARCHAR:
        return createStringConvertTreeReader(columnId, fileType, readerType, context);

      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return createTimestampConvertTreeReader(columnId, fileType, readerType, context);

      case DATE:
        return createDateConvertTreeReader(columnId, readerType, context);

      case BINARY:
        return createBinaryConvertTreeReader(columnId, readerType, context);

      // UNDONE: Complex conversions...
      case STRUCT:
      case LIST:
      case MAP:
      case UNION:
      default:
        throw new IllegalArgumentException("Unsupported type " +
            fileType.getCategory());
    }
  }

  public static boolean canConvert(TypeDescription fileType, TypeDescription readerType) {

    Category readerTypeCategory = readerType.getCategory();

    // We don't convert from any to complex.
    switch (readerTypeCategory) {
      case STRUCT:
      case LIST:
      case MAP:
      case UNION:
        return false;

      default:
        // Fall through.
    }

    // Now look for the few cases we don't convert from
    switch (fileType.getCategory()) {

      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        switch (readerType.getCategory()) {
          // Not currently supported conversion(s):
          case BINARY:
          case DATE:
            return false;
          default:
            return true;
        }


      case STRING:
      case CHAR:
      case VARCHAR:
        switch (readerType.getCategory()) {
          // Not currently supported conversion(s):
          // (None)
          default:
            return true;
        }

      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        switch (readerType.getCategory()) {
          // Not currently supported conversion(s):
          case BINARY:
            return false;
          default:
            return true;
        }

      case DATE:
        switch (readerType.getCategory()) {
          // Not currently supported conversion(s):
          case BOOLEAN:
          case BYTE:
          case FLOAT:
          case SHORT:
          case INT:
          case LONG:
          case DOUBLE:
          case BINARY:
          case DECIMAL:
            return false;
          default:
            return true;
        }

      case BINARY:
        switch (readerType.getCategory()) {
          // Not currently supported conversion(s):
          case BOOLEAN:
          case BYTE:
          case FLOAT:
          case SHORT:
          case INT:
          case LONG:
          case DOUBLE:
          case TIMESTAMP:
          case TIMESTAMP_INSTANT:
          case DECIMAL:
            return false;
          default:
            return true;
        }

        // We don't convert from complex to any.
      case STRUCT:
      case LIST:
      case MAP:
      case UNION:
        return false;

      default:
        throw new IllegalArgumentException("Unsupported type " +
            fileType.getCategory());
    }
  }
}
