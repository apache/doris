/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.tools;

import com.google.gson.stream.JsonWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Print the contents of an ORC file as JSON.
 */
public class PrintData {

  private static void printMap(JsonWriter writer,
                               MapColumnVector vector,
                               TypeDescription schema,
                               int row) throws IOException {
    writer.beginArray();
    TypeDescription keyType = schema.getChildren().get(0);
    TypeDescription valueType = schema.getChildren().get(1);
    int offset = (int) vector.offsets[row];
    for (int i = 0; i < vector.lengths[row]; ++i) {
      writer.beginObject();
      writer.name("_key");
      printValue(writer, vector.keys, keyType, offset + i);
      writer.name("_value");
      printValue(writer, vector.values, valueType, offset + i);
      writer.endObject();
    }
    writer.endArray();
  }

  private static void printList(JsonWriter writer,
                                ListColumnVector vector,
                                TypeDescription schema,
                                int row) throws IOException {
    writer.beginArray();
    int offset = (int) vector.offsets[row];
    TypeDescription childType = schema.getChildren().get(0);
    for (int i = 0; i < vector.lengths[row]; ++i) {
      printValue(writer, vector.child, childType, offset + i);
    }
    writer.endArray();
  }

  private static void printUnion(JsonWriter writer,
                                 UnionColumnVector vector,
                                 TypeDescription schema,
                                 int row) throws IOException {
    int tag = vector.tags[row];
    printValue(writer, vector.fields[tag], schema.getChildren().get(tag), row);
  }

  static void printStruct(JsonWriter writer,
                          StructColumnVector batch,
                          TypeDescription schema,
                          int row) throws IOException {
    writer.beginObject();
    List<String> fieldNames = schema.getFieldNames();
    List<TypeDescription> fieldTypes = schema.getChildren();
    for (int i = 0; i < fieldTypes.size(); ++i) {
      writer.name(fieldNames.get(i));
      printValue(writer, batch.fields[i], fieldTypes.get(i), row);
    }
    writer.endObject();
  }

  static void printBinary(JsonWriter writer, BytesColumnVector vector,
                          int row) throws IOException {
    writer.beginArray();
    int offset = vector.start[row];
    for(int i=0; i < vector.length[row]; ++i) {
      writer.value(0xff & (int) vector.vector[row][offset + i]);
    }
    writer.endArray();
  }
  static void printValue(JsonWriter writer, ColumnVector vector,
                         TypeDescription schema, int row) throws IOException {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      switch (schema.getCategory()) {
        case BOOLEAN:
          writer.value(((LongColumnVector) vector).vector[row] != 0);
          break;
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
          writer.value(((LongColumnVector) vector).vector[row]);
          break;
        case FLOAT:
        case DOUBLE:
          writer.value(((DoubleColumnVector) vector).vector[row]);
          break;
        case STRING:
        case CHAR:
        case VARCHAR:
          writer.value(((BytesColumnVector) vector).toString(row));
          break;
        case BINARY:
          printBinary(writer, (BytesColumnVector) vector, row);
          break;
        case DECIMAL:
          writer.value(((DecimalColumnVector) vector).vector[row].toString());
          break;
        case DATE:
          writer.value(new DateWritable(
              (int) ((LongColumnVector) vector).vector[row]).toString());
          break;
        case TIMESTAMP:
        case TIMESTAMP_INSTANT:
          writer.value(((TimestampColumnVector) vector)
              .asScratchTimestamp(row).toString());
          break;
        case LIST:
          printList(writer, (ListColumnVector) vector, schema, row);
          break;
        case MAP:
          printMap(writer, (MapColumnVector) vector, schema, row);
          break;
        case STRUCT:
          printStruct(writer, (StructColumnVector) vector, schema, row);
          break;
        case UNION:
          printUnion(writer, (UnionColumnVector) vector, schema, row);
          break;
        default:
          throw new IllegalArgumentException("Unknown type " + schema);
      }
    } else {
      writer.nullValue();
    }
  }

  static void printRow(JsonWriter writer,
                       VectorizedRowBatch batch,
                       TypeDescription schema,
                       int row) throws IOException {
    if (schema.getCategory() == TypeDescription.Category.STRUCT) {
      List<TypeDescription> fieldTypes = schema.getChildren();
      List<String> fieldNames = schema.getFieldNames();
      writer.beginObject();
      for (int c = 0; c < batch.cols.length; ++c) {
        writer.name(fieldNames.get(c));
        printValue(writer, batch.cols[c], fieldTypes.get(c), row);
      }
      writer.endObject();
    } else {
      printValue(writer, batch.cols[0], schema, row);
    }
  }

  static void printJsonData(PrintStream printStream,
          Reader reader, Optional<Integer> numberOfRows) throws IOException {
    OutputStreamWriter out = new OutputStreamWriter(printStream, StandardCharsets.UTF_8);
    RecordReader rows = reader.rows();
    try {
      TypeDescription schema = reader.getSchema();
      VectorizedRowBatch batch = schema.createRowBatch();
      Integer counter = 0;
      while (rows.nextBatch(batch)) {
        if (numberOfRows.isPresent() && counter >= numberOfRows.get()){
          break;
        }
        for (int r=0; r < batch.size; ++r) {
          JsonWriter writer = new JsonWriter(out);
          printRow(writer, batch, schema, r);
          out.write("\n");
          out.flush();
          if (printStream.checkError()) {
            throw new IOException("Error encountered when writing to stdout.");
          }
          if (numberOfRows.isPresent()) {
            counter++;
            if (counter >= numberOfRows.get()){
              break;
            }
          }
        }
      }
    } finally {
      rows.close();
    }
  }

  private static Options getOptions() {
    Option help = Option.builder("h").longOpt("help")
            .hasArg(false)
            .desc("Provide help")
            .build();
    Option linesOpt = Option.builder("n").longOpt("lines")
            .argName("LINES")
            .hasArg()
            .build();

    Options options = new Options()
            .addOption(help)
            .addOption(linesOpt);
    return options;
  }

  private static void printHelp(){
    Options opts = getOptions();

    PrintWriter pw = new PrintWriter(System.err);
    new HelpFormatter().printHelp(pw, HelpFormatter.DEFAULT_WIDTH,
            "java -jar orc-tools-*.jar data <orc file>*",
            null,
            opts,
            HelpFormatter.DEFAULT_LEFT_PAD,
            HelpFormatter.DEFAULT_DESC_PAD, null);
    pw.flush();
  }

  static CommandLine parseCommandLine(String[] args) throws ParseException {
    Options options = getOptions();
    return new DefaultParser().parse(options, args);
  }

  static void main(Configuration conf, String[] args
                   ) throws ParseException {
    CommandLine cli = parseCommandLine(args);
    if (cli.hasOption('h') || cli.getArgs().length == 0) {
      printHelp();
      System.exit(1);
    } else {
      Optional<Integer> lines = Optional.empty();
      if(cli.hasOption("n")){
        lines = Optional.of( Integer.parseInt(cli.getOptionValue("n")));
      }

      List<String> badFiles = new ArrayList<>();
      for (String file : cli.getArgs()) {
        try {
          Path path = new Path(file);
          Reader reader = FileDump.getReader(path, conf, badFiles);
          if (reader == null) {
            continue;
          }
          printJsonData(System.out, reader, lines);
          System.out.println(FileDump.SEPARATOR);
        } catch (Exception e) {
          System.err.println("Unable to dump data for file: " + file);
          e.printStackTrace();
        }
      }
    }
  }
}
