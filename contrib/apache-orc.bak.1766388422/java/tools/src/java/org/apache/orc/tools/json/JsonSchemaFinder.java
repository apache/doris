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
package org.apache.orc.tools.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonStreamParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescriptionPrettyPrint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * This class determines the equivalent Hive schema for a group of JSON
 * documents.
 * boolean
 */
public class JsonSchemaFinder {
  private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaFinder.class);

  private static final Pattern HEX_PATTERN =
      Pattern.compile("^([0-9a-fA-F][0-9a-fA-F])+$");
  private static final Pattern TIMESTAMP_PATTERN =
      Pattern.compile("^[\"]?([0-9]{4}[-/][0-9]{2}[-/][0-9]{2})[T ]" +
          "([0-9]{2}:[0-9]{2}:[0-9]{2})" +
          "(( [-+]?|[-+])([0-9]{2}(:[0-9]{2})?)|Z)?[\"]?$");
  private static final Pattern DECIMAL_PATTERN =
      Pattern.compile("^-?(?<int>[0-9]+)([.](?<fraction>[0-9]+))?$");
  private static final int INDENT = 2;
  private static final int MAX_DECIMAL_DIGITS = 38;

  static final BigInteger MIN_LONG = new BigInteger("-9223372036854775808");
  static final BigInteger MAX_LONG = new BigInteger("9223372036854775807");

  private HiveType mergedType = null;
  private long records = 0;

  static HiveType pickType(JsonElement json) {
    if (json.isJsonPrimitive()) {
      JsonPrimitive prim = (JsonPrimitive) json;
      if (prim.isBoolean()) {
        return new BooleanType();
      } else if (prim.isNumber()) {
        Matcher matcher = DECIMAL_PATTERN.matcher(prim.getAsString());
        if (matcher.matches()) {
          int intDigits = matcher.group("int").length();
          String fraction = matcher.group("fraction");
          int scale = fraction == null ? 0 : fraction.length();
          if (scale == 0) {
            if (intDigits < 19) {
              long value = prim.getAsLong();
              if (value >= -128 && value < 128) {
                return new NumericType(HiveType.Kind.BYTE, intDigits, scale);
              } else if (value >= -32768 && value < 32768) {
                return new NumericType(HiveType.Kind.SHORT, intDigits, scale);
              } else if (value >= -2147483648 && value < 2147483648L) {
                return new NumericType(HiveType.Kind.INT, intDigits, scale);
              } else {
                return new NumericType(HiveType.Kind.LONG, intDigits, scale);
              }
            } else if (intDigits == 19) {
              // at 19 digits, it may fit inside a long, but we need to check
              BigInteger val = prim.getAsBigInteger();
              if (val.compareTo(MIN_LONG) >= 0 && val.compareTo(MAX_LONG) <= 0) {
                return new NumericType(HiveType.Kind.LONG, intDigits, scale);
              }
            }
          }
          if (intDigits + scale <= MAX_DECIMAL_DIGITS) {
            return new NumericType(HiveType.Kind.DECIMAL, intDigits, scale);
          }
        }
        double value = prim.getAsDouble();
        if (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE) {
          return new NumericType(HiveType.Kind.FLOAT, 0, 0);
        } else {
          return new NumericType(HiveType.Kind.DOUBLE, 0, 0);
        }
      } else {
        String str = prim.getAsString();
        if (TIMESTAMP_PATTERN.matcher(str).matches()) {
          return new StringType(HiveType.Kind.TIMESTAMP);
        } else if (HEX_PATTERN.matcher(str).matches()) {
          return new StringType(HiveType.Kind.BINARY);
        } else {
          return new StringType(HiveType.Kind.STRING);
        }
      }
    } else if (json.isJsonNull()) {
      return new NullType();
    } else if (json.isJsonArray()) {
      ListType result = new ListType();
      result.elementType = new NullType();
      for(JsonElement child: ((JsonArray) json)) {
        HiveType sub = pickType(child);
        if (result.elementType.subsumes(sub)) {
          result.elementType.merge(sub);
        } else if (sub.subsumes(result.elementType)) {
          sub.merge(result.elementType);
          result.elementType = sub;
        } else {
          result.elementType = new UnionType(result.elementType, sub);
        }
      }
      return result;
    } else {
      JsonObject obj = (JsonObject) json;
      StructType result = new StructType();
      for(Map.Entry<String,JsonElement> field: obj.entrySet()) {
        String fieldName = field.getKey();
        HiveType type = pickType(field.getValue());
        result.fields.put(fieldName, type);
      }
      return result;
    }
  }

  static HiveType mergeType(HiveType previous, HiveType type) {
    if (previous == null) {
      return type;
    } else if (type == null) {
      return previous;
    }
    if (previous.subsumes(type)) {
      previous.merge(type);
    } else if (type.subsumes(previous)) {
      type.merge(previous);
      previous = type;
    } else {
      previous = new UnionType(previous, type);
    }
    return previous;
  }

  static void printType(PrintStream out, HiveType type, int margin) {
    if (type == null) {
      out.print("void");
    } else if (type.kind.isPrimitive) {
      out.print(type);
    } else {
      switch (type.kind) {
        case STRUCT:
          out.println("struct <");
          boolean first = true;
          for(Map.Entry<String, HiveType> field:
              ((StructType) type).fields.entrySet()) {
            if (!first) {
              out.println(",");
            } else {
              first = false;
            }
            for(int i=0; i < margin; i++) {
              out.print(' ');
            }
            out.print(field.getKey());
            out.print(": ");
            printType(out, field.getValue(), margin + INDENT);
          }
          out.print(">");
          break;
        case LIST:
          out.print("array <");
          printType(out, ((ListType) type).elementType, margin + INDENT);
          out.print(">");
          break;
        case UNION:
          out.print("uniontype <");
          first = true;
          for(HiveType child: ((UnionType) type).children) {
            if (!first) {
              out.print(',');
            } else {
              first = false;
            }
            printType(out, child, margin + INDENT);
          }
          out.print(">");
          break;
        default:
          throw new IllegalArgumentException("Unknown kind " + type.kind);
      }
    }
  }

  static void printAsTable(PrintStream out, StructType type) {
    out.println("create table tbl (");
    boolean first = true;
    for(Map.Entry<String, HiveType> field: type.fields.entrySet()) {
      if (!first) {
        out.println(",");
      } else {
        first = false;
      }
      for(int i=0; i < INDENT; ++i) {
        out.print(' ');
      }
      out.print(field.getKey());
      out.print(" ");
      printType(out, field.getValue(), 2 * INDENT);
    }
    out.println();
    out.println(")");
  }

  public void addFile(String filename) throws IOException {
    java.io.Reader reader;
    FileInputStream inputStream = new FileInputStream(filename);
    if (filename.endsWith(".gz")) {
      reader = new InputStreamReader(new GZIPInputStream(inputStream),
          StandardCharsets.UTF_8);
    } else {
      reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
    }
    addFile(reader, filename);
  }

  public void addFile(java.io.Reader reader, String filename) {
    JsonStreamParser parser = new JsonStreamParser(reader);
    try {
      while (parser.hasNext()) {
        mergedType = mergeType(mergedType, pickType(parser.next()));
        records += 1;
      }
    } catch (JsonParseException e) {
      printParseExceptionMsg(e, filename);
    }
  }

  private void printParseExceptionMsg(JsonParseException e, String filename) {
    System.err.printf(
        "A JsonParseException was thrown while processing the %dth record of file %s.%n",
        records + 1, filename);

    String pattern = "at line (\\d+) column (\\d+)";
    Pattern r = Pattern.compile(pattern);
    Matcher m = r.matcher(e.getMessage());
    int line;
    int column;
    if (m.find( )) {
      line = Integer.parseInt(m.group(1));
      column = Integer.parseInt(m.group(2));
      if (line == 1 && column == 1) {
        System.err.printf("File %s is empty.%n", filename);
        System.exit(1);
      }
    }
    System.err.printf("Please check the file.%n%n%s%n", ExceptionUtils.getStackTrace(e));
    System.exit(1);
  }

  HiveType makeHiveType(TypeDescription schema) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return new BooleanType();
      case BYTE:
        return new NumericType(HiveType.Kind.BYTE, 3, 0);
      case SHORT:
        return new NumericType(HiveType.Kind.SHORT, 5, 0);
      case INT:
        return new NumericType(HiveType.Kind.INT, 10, 0);
      case LONG:
        return new NumericType(HiveType.Kind.LONG, 19, 0);
      case FLOAT:
        return new NumericType(HiveType.Kind.FLOAT, 0, 0);
      case DOUBLE:
        return new NumericType(HiveType.Kind.DOUBLE, 0, 0);
      case DECIMAL: {
        int scale = schema.getScale();
        int intDigits = schema.getPrecision() - scale;
        return new NumericType(HiveType.Kind.DECIMAL, intDigits, scale);
      }
      case CHAR:
      case VARCHAR:
      case STRING:
        return new StringType(HiveType.Kind.STRING);
      case TIMESTAMP:
        return new StringType(HiveType.Kind.TIMESTAMP);
      case TIMESTAMP_INSTANT:
        return new StringType(HiveType.Kind.TIMESTAMP_INSTANT);
      case DATE:
        return new StringType(HiveType.Kind.DATE);
      case BINARY:
        return new StringType(HiveType.Kind.BINARY);
      case LIST:
        return new ListType(makeHiveType(schema.getChildren().get(0)));
      case STRUCT: {
        StructType result = new StructType();
        List<String> fields = schema.getFieldNames();
        List<TypeDescription> children = schema.getChildren();
        for(int i = 0; i < fields.size(); ++i) {
          result.addField(fields.get(i), makeHiveType(children.get(i)));
        }
        return result;
      }
      case UNION: {
        UnionType result = new UnionType();
        for(TypeDescription child: schema.getChildren()) {
          result.addType(makeHiveType(child));
        }
        return result;
      }
      case MAP:
        return new MapType(
            makeHiveType(schema.getChildren().get(0)),
            makeHiveType(schema.getChildren().get(1)));
      default:
        throw new IllegalArgumentException("Unhandled type " + schema);
    }
  }

  public void addSchema(TypeDescription schema) {
    mergedType = mergeType(mergedType, makeHiveType(schema));
  }

  public TypeDescription getSchema() {
    return mergedType.getSchema();
  }

  public static void main(Configuration conf,
                          String[] args) throws Exception {
    JsonSchemaFinder result = new JsonSchemaFinder();
    CommandLine cli = parseArguments(args);
    for (String filename: cli.getArgs()) {
      System.err.println("Reading file " + filename);
      result.addFile(filename);
    }
    System.err.println(result.records + " records read");
    System.err.println();
    if (cli.hasOption('f')) {
      result.mergedType.printFlat(System.out, "root");
    } else if (cli.hasOption('t')) {
      printAsTable(System.out, (StructType) result.mergedType);
    } else if (cli.hasOption('p')) {
      TypeDescriptionPrettyPrint.print(System.out, result.getSchema());
    } else {
      System.out.println(result.getSchema());
    }
  }

  static CommandLine parseArguments(String[] args) throws ParseException {
    Options options = new Options();

    options.addOption(Option.builder("h").longOpt("help")
        .desc("Provide help").build());
    options.addOption(Option.builder("f").longOpt("flat")
        .desc("Print types as flat list of types").build());
    options.addOption(Option.builder("t").longOpt("table")
        .desc("Print types as Hive table declaration").build());
    options.addOption(Option.builder("p").longOpt("pretty")
        .desc("Pretty print the schema").build());
    CommandLine cli = new DefaultParser().parse(options, args);
    if (cli.hasOption('h') || cli.getArgs().length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("json-schema", options);
      System.exit(1);
    }
    return cli;
  }

  public static void main(String[] args) throws Exception {
    main(new Configuration(), args);
  }
}
