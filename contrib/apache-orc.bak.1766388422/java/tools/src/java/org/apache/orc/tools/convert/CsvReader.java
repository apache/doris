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
package org.apache.orc.tools.convert;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class CsvReader implements RecordReader {
  private long rowNumber = 0;
  private final Converter converter;
  private final int columns;
  private final CSVReader reader;
  private final String nullString;
  private final FSDataInputStream underlying;
  private final long totalSize;
  private final DateTimeFormatter dateTimeFormatter;

  /**
   * Create a CSV reader
   * @param reader the stream to read from
   * @param input the underlying file that is only used for getting the
   *              position within the file
   * @param size the number of bytes in the underlying stream
   * @param schema the schema to read into
   * @param separatorChar the character between fields
   * @param quoteChar the quote character
   * @param escapeChar the escape character
   * @param headerLines the number of header lines
   * @param nullString the string that is translated to null
   * @param timestampFormat the timestamp format string
   */
  public CsvReader(java.io.Reader reader,
                   FSDataInputStream input,
                   long size,
                   TypeDescription schema,
                   char separatorChar,
                   char quoteChar,
                   char escapeChar,
                   int headerLines,
                   String nullString,
                   String timestampFormat) {
    this.underlying = input;
    CSVParser parser = new CSVParserBuilder()
        .withSeparator(separatorChar)
        .withQuoteChar(quoteChar)
        .withEscapeChar(escapeChar)
        .build();
    this.reader = new CSVReaderBuilder(reader)
        .withSkipLines(headerLines)
        .withCSVParser(parser)
        .build();
    this.nullString = nullString;
    this.totalSize = size;
    IntWritable nextColumn = new IntWritable(0);
    this.converter = buildConverter(nextColumn, schema);
    this.columns = nextColumn.get();
    this.dateTimeFormatter = DateTimeFormatter.ofPattern(timestampFormat);
  }

  interface Converter {
    void convert(String[] values, VectorizedRowBatch batch, int row);
    void convert(String[] values, ColumnVector column, int row);
  }

  @Override
  public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
    batch.reset();
    final int BATCH_SIZE = batch.getMaxSize();
    String[] nextLine;
    // Read the CSV rows and place them into the column vectors.
    try {
      while ((nextLine = reader.readNext()) != null) {
        rowNumber++;
        if (nextLine.length != columns &&
            !(nextLine.length == columns + 1 && "".equals(nextLine[columns]))) {
          throw new IllegalArgumentException("Too many columns on line " +
              rowNumber + ". Expected " + columns + ", but got " +
              nextLine.length + ".");
        }
        converter.convert(nextLine, batch, batch.size++);
        if (batch.size == BATCH_SIZE) {
          break;
        }
      }
    } catch (CsvValidationException e) {
      throw new IOException(e);
    }
    return batch.size != 0;
  }

  @Override
  public long getRowNumber() throws IOException {
    return rowNumber;
  }

  @Override
  public float getProgress() throws IOException {
    long pos = underlying.getPos();
    return totalSize != 0 && pos < totalSize ? (float) pos / totalSize : 1;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public void seekToRow(long rowCount) throws IOException {
    throw new UnsupportedOperationException("Seeking not supported");
  }

  abstract class ConverterImpl implements Converter {
    final int offset;

    ConverterImpl(IntWritable offset) {
      this.offset = offset.get();
      offset.set(this.offset + 1);
    }

    @Override
    public void convert(String[] values, VectorizedRowBatch batch, int row) {
      convert(values, batch.cols[0], row);
    }
  }

  class BooleanConverter extends ConverterImpl {
    BooleanConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        if (values[offset].equalsIgnoreCase("true") ||
            values[offset].equalsIgnoreCase("t") ||
            values[offset].equals("1")) {
          ((LongColumnVector) column).vector[row] = 1;
        } else {
          ((LongColumnVector) column).vector[row] = 0;
        }
      }
    }
  }

  class LongConverter extends ConverterImpl {
    LongConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        ((LongColumnVector) column).vector[row] =
            Long.parseLong(values[offset]);
      }
    }
  }

  class DoubleConverter extends ConverterImpl {
    DoubleConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        ((DoubleColumnVector) column).vector[row] =
            Double.parseDouble(values[offset]);
      }
    }
  }

  class DecimalConverter extends ConverterImpl {
    DecimalConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        ((DecimalColumnVector) column).vector[row].set(
            new HiveDecimalWritable(values[offset]));
      }
    }
  }

  class BytesConverter extends ConverterImpl {
    BytesConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        byte[] value = values[offset].getBytes(StandardCharsets.UTF_8);
        ((BytesColumnVector) column).setRef(row, value, 0, value.length);
      }
    }
  }

  class DateColumnConverter extends ConverterImpl {
    DateColumnConverter(IntWritable offset) { super(offset); }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        DateColumnVector vector = (DateColumnVector) column;

        final LocalDate dt = LocalDate.parse(values[offset]);

        if (dt != null) {
          vector.vector[row] = dt.toEpochDay();
        } else {
          column.noNulls = false;
          column.isNull[row] = true;
        }
      }
    }
  }

  class TimestampConverter extends ConverterImpl {
    TimestampConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        TimestampColumnVector vector = (TimestampColumnVector) column;
        TemporalAccessor temporalAccessor =
            dateTimeFormatter.parseBest(values[offset],
                ZonedDateTime::from, OffsetDateTime::from, LocalDateTime::from);
        if (temporalAccessor instanceof ZonedDateTime) {
          ZonedDateTime zonedDateTime = ((ZonedDateTime) temporalAccessor);
          Timestamp timestamp = Timestamp.from(zonedDateTime.toInstant());
          vector.set(row, timestamp);
        } else if (temporalAccessor instanceof OffsetDateTime) {
          OffsetDateTime offsetDateTime = (OffsetDateTime) temporalAccessor;
          Timestamp timestamp = Timestamp.from(offsetDateTime.toInstant());
          vector.set(row, timestamp);
        } else if (temporalAccessor instanceof LocalDateTime) {
          Timestamp timestamp = Timestamp.valueOf((LocalDateTime) temporalAccessor);
          vector.set(row, timestamp);
        } else {
          column.noNulls = false;
          column.isNull[row] = true;
        }
      }
    }
  }

  class StructConverter implements Converter {
    final Converter[] children;


    StructConverter(IntWritable offset, TypeDescription schema) {
      children = new Converter[schema.getChildren().size()];
      int c = 0;
      for(TypeDescription child: schema.getChildren()) {
        children[c++] = buildConverter(offset, child);
      }
    }

    @Override
    public void convert(String[] values, VectorizedRowBatch batch, int row) {
      for(int c=0; c < children.length; ++c) {
        children[c].convert(values, batch.cols[c], row);
      }
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      StructColumnVector cv = (StructColumnVector) column;
      for(int c=0; c < children.length; ++c) {
        children[c].convert(values, cv.fields[c], row);
      }
    }
  }

  Converter buildConverter(IntWritable startOffset, TypeDescription schema) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return new BooleanConverter(startOffset);
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new LongConverter(startOffset);
      case FLOAT:
      case DOUBLE:
        return new DoubleConverter(startOffset);
      case DECIMAL:
        return new DecimalConverter(startOffset);
      case BINARY:
      case STRING:
      case CHAR:
      case VARCHAR:
        return new BytesConverter(startOffset);
      case DATE:
        return new DateColumnConverter(startOffset);
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return new TimestampConverter(startOffset);
      case STRUCT:
        return new StructConverter(startOffset, schema);
      default:
        throw new IllegalArgumentException("Unhandled type " + schema);
    }
  }
}
