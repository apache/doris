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

package org.apache.orc;

import java.io.PrintStream;
import java.util.List;

/**
 * A pretty printer for TypeDescription.
 */
public class TypeDescriptionPrettyPrint {

  static void pad(PrintStream output, int offset) {
    for(int i=0; i < offset; ++i) {
      output.print(' ');
    }
  }

  static void printFieldName(PrintStream output, String fieldName){
    if (TypeDescription.UNQUOTED_NAMES.matcher(fieldName).matches()) {
      output.print(fieldName);
    } else {
      output.print('`');
      output.print(fieldName.replaceAll("`", "``"));
      output.print('`');
    }
  }

  static void printStruct(PrintStream output,
                          int offset,
                          TypeDescription type) {
    output.print("<");
    List<TypeDescription> children = type.getChildren();
    List<String> fields = type.getFieldNames();
    for(int c = 0; c < children.size(); ++c) {
      if (c == 0) {
        output.println();
      } else {
        output.println(",");
      }
      pad(output, offset + 2);
      printFieldName(output, fields.get(c));
      output.print(':');
      printType(output, offset + 2, children.get(c));
    }
    output.print('>');
  }

  static void printComplex(PrintStream output,
                           int offset,
                           TypeDescription type) {
    output.print("<");
    List<TypeDescription> children = type.getChildren();
    for(int c = 0; c < children.size(); ++c) {
      if (c != 0) {
        output.print(",");
      }
      printType(output, offset + 2, children.get(c));
    }
    output.print('>');
  }

  static void printType(PrintStream output,
                        int offset,
                        TypeDescription type) {
    output.print(type.getCategory().getName());
    switch (type.getCategory()) {
      case BOOLEAN:
      case BINARY:
      case BYTE:
      case DATE:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
      case SHORT:
      case STRING:
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        break;

      case DECIMAL:
        output.print('(');
        output.print(type.getPrecision());
        output.print(',');
        output.print(type.getScale());
        output.print(')');
        break;

      case CHAR:
      case VARCHAR:
        output.print('(');
        output.print(type.getMaxLength());
        output.print(')');
        break;

      case STRUCT:
        printStruct(output, offset, type);
        break;

      case LIST:
      case MAP:
      case UNION:
        printComplex(output, offset, type);
        break;

      default:
        throw new IllegalArgumentException("Unhandled type " + type);
    }
  }

  public static void print(PrintStream output,
                           TypeDescription schema) {
    printType(output, 0, schema);
  }
}
