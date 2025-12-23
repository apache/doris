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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ParserUtils {

  static TypeDescription.Category parseCategory(ParserUtils.StringPosition source) {
    StringBuilder word = new StringBuilder();
    boolean hadSpace = true;
    while (source.position < source.length) {
      char ch = source.value.charAt(source.position);
      if (Character.isLetter(ch)) {
        word.append(Character.toLowerCase(ch));
        hadSpace = false;
      } else if (ch == ' ') {
        if (!hadSpace) {
          hadSpace = true;
          word.append(ch);
        }
      } else {
        break;
      }
      source.position += 1;
    }
    String catString = word.toString();
    // if there were trailing spaces, remove them.
    if (hadSpace) {
      catString = catString.trim();
    }
    if (!catString.isEmpty()) {
      for (TypeDescription.Category cat : TypeDescription.Category.values()) {
        if (cat.getName().equals(catString)) {
          return cat;
        }
      }
    }
    throw new IllegalArgumentException("Can't parse category at " + source);
  }

  static int parseInt(ParserUtils.StringPosition source) {
    int start = source.position;
    int result = 0;
    while (source.position < source.length) {
      char ch = source.value.charAt(source.position);
      if (!Character.isDigit(ch)) {
        break;
      }
      result = result * 10 + (ch - '0');
      source.position += 1;
    }
    if (source.position == start) {
      throw new IllegalArgumentException("Missing integer at " + source);
    }
    return result;
  }

  public static String parseName(ParserUtils.StringPosition source) {
    if (source.position == source.length) {
      throw new IllegalArgumentException("Missing name at " + source);
    }
    final int start = source.position;
    if (source.value.charAt(source.position) == '`') {
      source.position += 1;
      StringBuilder buffer = new StringBuilder();
      boolean closed = false;
      while (source.position < source.length) {
        char ch = source.value.charAt(source.position);
        source.position += 1;
        if (ch == '`') {
          if (source.position < source.length &&
                  source.value.charAt(source.position) == '`') {
            source.position += 1;
            buffer.append('`');
          } else {
            closed = true;
            break;
          }
        } else {
          buffer.append(ch);
        }
      }
      if (!closed) {
        source.position = start;
        throw new IllegalArgumentException("Unmatched quote at " + source);
      } else if (buffer.length() == 0) {
        throw new IllegalArgumentException("Empty quoted field name at " + source);
      }
      return buffer.toString();
    } else {
      while (source.position < source.length) {
        char ch = source.value.charAt(source.position);
        if (!Character.isLetterOrDigit(ch) && ch != '_') {
          break;
        }
        source.position += 1;
      }
      if (source.position == start) {
        throw new IllegalArgumentException("Missing name at " + source);
      }
      return source.value.substring(start, source.position);
    }
  }

  static void requireChar(ParserUtils.StringPosition source, char required) {
    if (source.position >= source.length ||
            source.value.charAt(source.position) != required) {
      throw new IllegalArgumentException("Missing required char '" +
              required + "' at " + source);
    }
    source.position += 1;
  }

  private static boolean consumeChar(ParserUtils.StringPosition source,
                                     char ch) {
    boolean result = source.position < source.length &&
            source.value.charAt(source.position) == ch;
    if (result) {
      source.position += 1;
    }
    return result;
  }

  private static void parseUnion(TypeDescription type,
                                 ParserUtils.StringPosition source) {
    requireChar(source, '<');
    do {
      type.addUnionChild(parseType(source));
    } while (consumeChar(source, ','));
    requireChar(source, '>');
  }

  private static void parseStruct(TypeDescription type,
                                  ParserUtils.StringPosition source) {
    requireChar(source, '<');
    boolean needComma = false;
    while (!consumeChar(source, '>')) {
      if (needComma) {
        requireChar(source, ',');
      } else {
        needComma = true;
      }
      String fieldName = parseName(source);
      requireChar(source, ':');
      type.addField(fieldName, parseType(source));
    }
  }

  public static TypeDescription parseType(ParserUtils.StringPosition source) {
    TypeDescription result = new TypeDescription(parseCategory(source));
    switch (result.getCategory()) {
      case BINARY:
      case BOOLEAN:
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
      case CHAR:
      case VARCHAR:
        requireChar(source, '(');
        result.withMaxLength(parseInt(source));
        requireChar(source, ')');
        break;
      case DECIMAL: {
        requireChar(source, '(');
        int precision = parseInt(source);
        requireChar(source, ',');
        result.withScale(parseInt(source));
        result.withPrecision(precision);
        requireChar(source, ')');
        break;
      }
      case LIST: {
        requireChar(source, '<');
        TypeDescription child = parseType(source);
        result.addChild(child);
        requireChar(source, '>');
        break;
      }
      case MAP: {
        requireChar(source, '<');
        TypeDescription keyType = parseType(source);
        result.addChild(keyType);
        requireChar(source, ',');
        TypeDescription valueType = parseType(source);
        result.addChild(valueType);
        requireChar(source, '>');
        break;
      }
      case UNION:
        parseUnion(result, source);
        break;
      case STRUCT:
        parseStruct(result, source);
        break;
      default:
        throw new IllegalArgumentException("Unknown type " +
            result.getCategory() + " at " + source);
    }
    return result;
  }

  /**
   * Split a compound name into parts separated by '.'.
   * @param source the string to parse into simple names
   * @return a list of simple names from the source
   */
  private static List<String> splitName(ParserUtils.StringPosition source) {
    List<String> result = new ArrayList<>();
    do {
      result.add(parseName(source));
    } while (consumeChar(source, '.'));
    return result;
  }


  private static final Pattern INTEGER_PATTERN = Pattern.compile("^[0-9]+$");

  public static TypeDescription findSubtype(TypeDescription schema,
                                            ParserUtils.StringPosition source) {
    return findSubtype(schema, source, true);
  }


  public interface TypeVisitor {
    /**
     * As we navigate to the column, call this on each level
     * @param type new level we are moving to
     * @param posn the position relative to the parent
     */
    void visit(TypeDescription type, int posn);
  }

  public static class TypeFinder implements TypeVisitor {
    public TypeDescription current;

    public TypeFinder(TypeDescription schema) {
      current = schema;
    }

    @Override
    public void visit(TypeDescription type, int posn) {
      current = type;
    }
  }

  public static TypeDescription findSubtype(TypeDescription schema,
                                            ParserUtils.StringPosition source,
                                            boolean isSchemaEvolutionCaseAware) {
    TypeFinder result = new TypeFinder(removeAcid(schema));
    findColumn(result.current, source, isSchemaEvolutionCaseAware, result);
    return result.current;
  }

  private static TypeDescription removeAcid(TypeDescription schema) {
    return SchemaEvolution.checkAcidSchema(schema)
        ? SchemaEvolution.getBaseRow(schema) : schema;
  }

  private static int findCaseInsensitive(List<String> list, String goal) {
    for (int i = 0; i < list.size(); i++) {
      if (list.get(i).equalsIgnoreCase(goal)) {
        return i;
      }
    }
    return -1;
  }

  public static void findSubtype(TypeDescription schema,
                                 int goal,
                                 TypeVisitor visitor) {
    TypeDescription current = schema;
    int id = schema.getId();
    if (goal < id || goal > schema.getMaximumId()) {
      throw new IllegalArgumentException("Unknown type id " + goal + " in " +
          current.toJson());
    }
    while (id != goal) {
      List<TypeDescription> children = current.getChildren();
      for(int i=0; i < children.size(); ++i) {
        TypeDescription child = children.get(i);
        if (goal <= child.getMaximumId()) {
          current = child;
          visitor.visit(current, i);
          break;
        }
      }
      id = current.getId();
    }
  }

  /**
   * Find a column in a schema by walking down the type tree to find the right column.
   * @param schema the schema to look in
   * @param source the name of the column
   * @param isSchemaEvolutionCaseAware should the string compare be case sensitive
   * @param visitor The visitor, which is called on each level
   */
  public static void findColumn(TypeDescription schema,
                                ParserUtils.StringPosition source,
                                boolean isSchemaEvolutionCaseAware,
                                TypeVisitor visitor) {
    findColumn(schema, ParserUtils.splitName(source), isSchemaEvolutionCaseAware, visitor);
  }

  /**
   * Find a column in a schema by walking down the type tree to find the right column.
   * @param schema the schema to look in
   * @param names the name of the column broken into a list of names per level
   * @param isSchemaEvolutionCaseAware should the string compare be case sensitive
   * @param visitor The visitor, which is called on each level
   */
  public static void findColumn(TypeDescription schema,
                                List<String> names,
                                boolean isSchemaEvolutionCaseAware,
                                TypeVisitor visitor) {
    if (names.size() == 1 && INTEGER_PATTERN.matcher(names.get(0)).matches()) {
      findSubtype(schema, Integer.parseInt(names.get(0)), visitor);
      return;
    }
    TypeDescription current = schema;
    int posn;
    while (names.size() > 0) {
      String first = names.remove(0);
      switch (current.getCategory()) {
        case STRUCT: {
          posn = isSchemaEvolutionCaseAware
              ? current.getFieldNames().indexOf(first)
              : findCaseInsensitive(current.getFieldNames(), first);
          break;
        }
        case LIST:
          if (first.equals("_elem")) {
            posn = 0;
          } else {
            posn = -1;
          }
          break;
        case MAP:
          if (first.equals("_key")) {
            posn = 0;
          } else if (first.equals("_value")) {
            posn = 1;
          } else {
            posn = -1;
          }
          break;
        case UNION: {
          try {
            posn = Integer.parseInt(first);
            if (posn < 0 || posn >= current.getChildren().size()) {
              throw new NumberFormatException("off end of union");
            }
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Field " + first +
                "not found in " + current, e);
          }
          break;
        }
        default:
          posn = -1;
      }
      if (posn < 0) {
        throw new IllegalArgumentException("Field " + first +
                                           " not found in " + current);
      }
      current = current.getChildren().get(posn);
      visitor.visit(current, posn);
    }
  }

  static class ColumnFinder implements TypeVisitor {
    // top and current are interpreted as a union, only one of them is expected to be set at any
    // given time.
    private ColumnVector[] top;
    private ColumnVector current = null;
    private final ColumnVector[] result;
    private int resultIdx = 0;

    ColumnFinder(TypeDescription schema, VectorizedRowBatch batch, int levels) {
      if (schema.getCategory() == TypeDescription.Category.STRUCT) {
        top = batch.cols;
        result = new ColumnVector[levels];
      } else {
        result = new ColumnVector[levels + 1];
        current = batch.cols[0];
        top = null;
        addResult(current);
      }
    }

    private void addResult(ColumnVector vector) {
      result[resultIdx] = vector;
      resultIdx += 1;
    }

    @Override
    public void visit(TypeDescription type, int posn) {
      if (current == null) {
        current = top[posn];
        top = null;
      } else {
        current = navigate(current, posn);
      }
      addResult(current);
    }

    private ColumnVector navigate(ColumnVector parent, int posn) {
      if (parent instanceof ListColumnVector) {
        return ((ListColumnVector) parent).child;
      } else if (parent instanceof StructColumnVector) {
        return ((StructColumnVector) parent).fields[posn];
      } else if (parent instanceof UnionColumnVector) {
        return ((UnionColumnVector) parent).fields[posn];
      } else if (parent instanceof MapColumnVector) {
        MapColumnVector m = (MapColumnVector) parent;
        return posn == 0 ? m.keys : m.values;
      }
      throw new IllegalArgumentException("Unknown complex column vector " + parent.getClass());
    }
  }

  public static ColumnVector[] findColumnVectors(TypeDescription schema,
                                                 StringPosition source,
                                                 boolean isCaseSensitive,
                                                 VectorizedRowBatch batch) {
    List<String> names = ParserUtils.splitName(source);
    ColumnFinder result = new ColumnFinder(schema, batch, names.size());
    findColumn(removeAcid(schema), names, isCaseSensitive, result);
    return result.result;
  }

  public static List<TypeDescription> findSubtypeList(TypeDescription schema,
                                                      StringPosition source) {
    List<TypeDescription> result = new ArrayList<>();
    if (source.hasCharactersLeft()) {
      do {
        result.add(findSubtype(schema, source));
      } while (consumeChar(source, ','));
    }
    return result;
  }

  public static class StringPosition {
    final String value;
    int position;
    final int length;

    public StringPosition(String value) {
      this.value = value == null ? "" : value;
      position = 0;
      length = this.value.length();
    }

    @Override
    public String toString() {
      return '\'' + value.substring(0, position) + '^' +
          value.substring(position) + '\'';
    }

    public String fromPosition(int start) {
      return value.substring(start, this.position);
    }

    public boolean hasCharactersLeft() {
      return position != length;
    }
  }

  /**
   * Annotate the given schema with the encryption information.
   *
   * Format of the string is a key-list.
   * <ul>
   *   <li>key-list = key (';' key-list)?</li>
   *   <li>key = key-name ':' field-list</li>
   *   <li>field-list = field-name ( ',' field-list )?</li>
   *   <li>field-name = number | field-part ('.' field-name)?</li>
   *   <li>field-part = quoted string | simple name</li>
   * </ul>
   *
   * @param source the string to parse
   * @param schema the top level schema
   * @throws IllegalArgumentException if there are conflicting keys for a field
   */
  public static void parseKeys(StringPosition source, TypeDescription schema) {
    if (source.hasCharactersLeft()) {
      do {
        String keyName = parseName(source);
        requireChar(source, ':');
        for (TypeDescription field : findSubtypeList(schema, source)) {
          String prev = field.getAttributeValue(TypeDescription.ENCRYPT_ATTRIBUTE);
          if (prev != null && !prev.equals(keyName)) {
            throw new IllegalArgumentException("Conflicting encryption keys " +
                keyName + " and " + prev);
          }
          field.setAttribute(TypeDescription.ENCRYPT_ATTRIBUTE, keyName);
        }
      } while (consumeChar(source, ';'));
    }
  }

  /**
   * Annotate the given schema with the masking information.
   *
   * Format of the string is a mask-list.
   * <ul>
   *   <li>mask-list = mask (';' mask-list)?</li>
   *   <li>mask = mask-name (',' parameter)* ':' field-list</li>
   *   <li>field-list = field-name ( ',' field-list )?</li>
   *   <li>field-name = number | field-part ('.' field-name)?</li>
   *   <li>field-part = quoted string | simple name</li>
   * </ul>
   *
   * @param source the string to parse
   * @param schema the top level schema
   * @throws IllegalArgumentException if there are conflicting masks for a field
   */
  public static void parseMasks(StringPosition source, TypeDescription schema) {
    if (source.hasCharactersLeft()) {
      do {
        // parse the mask and parameters, but only get the underlying string
        int start = source.position;
        parseName(source);
        while (consumeChar(source, ',')) {
          parseName(source);
        }
        String maskString = source.fromPosition(start);
        requireChar(source, ':');
        for (TypeDescription field : findSubtypeList(schema, source)) {
          String prev = field.getAttributeValue(TypeDescription.MASK_ATTRIBUTE);
          if (prev != null && !prev.equals(maskString)) {
            throw new IllegalArgumentException("Conflicting encryption masks " +
                maskString + " and " + prev);
          }
          field.setAttribute(TypeDescription.MASK_ATTRIBUTE, maskString);
        }
      } while (consumeChar(source, ';'));
    }
  }

  public static MaskDescriptionImpl buildMaskDescription(String value) {
    StringPosition source = new StringPosition(value);
    String maskName = parseName(source);
    List<String> params = new ArrayList<>();
    while (consumeChar(source, ',')) {
      params.add(parseName(source));
    }
    return new MaskDescriptionImpl(maskName, params.toArray(new String[0]));
  }
}
