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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.impl.ParserUtils;
import org.apache.orc.impl.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This is the description of the types in an ORC file.
 */
public class TypeDescription
    implements Comparable<TypeDescription>, Serializable, Cloneable {
  private static final int MAX_PRECISION = 38;
  private static final int MAX_SCALE = 38;
  private static final int DEFAULT_PRECISION = 38;
  private static final int DEFAULT_SCALE = 10;
  public static final int MAX_DECIMAL64_PRECISION = 18;
  public static final long MAX_DECIMAL64 = 999_999_999_999_999_999L;
  public static final long MIN_DECIMAL64 = -MAX_DECIMAL64;
  private static final int DEFAULT_LENGTH = 256;
  static final Pattern UNQUOTED_NAMES = Pattern.compile("^[a-zA-Z0-9_]+$");

  // type attributes
  public static final String ENCRYPT_ATTRIBUTE = "encrypt";
  public static final String MASK_ATTRIBUTE = "mask";

  @Override
  public int compareTo(TypeDescription other) {
    if (this == other) {
      return 0;
    } else if (other == null) {
      return -1;
    } else {
      int result = category.compareTo(other.category);
      if (result == 0) {
        switch (category) {
          case CHAR:
          case VARCHAR:
            return maxLength - other.maxLength;
          case DECIMAL:
            if (precision != other.precision) {
              return precision - other.precision;
            }
            return scale - other.scale;
          case UNION:
          case LIST:
          case MAP:
            if (children.size() != other.children.size()) {
              return children.size() - other.children.size();
            }
            for(int c=0; result == 0 && c < children.size(); ++c) {
              result = children.get(c).compareTo(other.children.get(c));
            }
            break;
          case STRUCT:
            if (children.size() != other.children.size()) {
              return children.size() - other.children.size();
            }
            for(int c=0; result == 0 && c < children.size(); ++c) {
              result = fieldNames.get(c).compareTo(other.fieldNames.get(c));
              if (result == 0) {
                result = children.get(c).compareTo(other.children.get(c));
              }
            }
            break;
          default:
            // PASS
        }
      }
      return result;
    }
  }

  public enum Category {
    BOOLEAN("boolean", true),
    BYTE("tinyint", true),
    SHORT("smallint", true),
    INT("int", true),
    LONG("bigint", true),
    FLOAT("float", true),
    DOUBLE("double", true),
    STRING("string", true),
    DATE("date", true),
    TIMESTAMP("timestamp", true),
    BINARY("binary", true),
    DECIMAL("decimal", true),
    VARCHAR("varchar", true),
    CHAR("char", true),
    LIST("array", false),
    MAP("map", false),
    STRUCT("struct", false),
    UNION("uniontype", false),
    TIMESTAMP_INSTANT("timestamp with local time zone", true);

    Category(String name, boolean isPrimitive) {
      this.name = name;
      this.isPrimitive = isPrimitive;
    }

    final boolean isPrimitive;
    final String name;

    public boolean isPrimitive() {
      return isPrimitive;
    }

    public String getName() {
      return name;
    }
  }

  public static TypeDescription createBoolean() {
    return new TypeDescription(Category.BOOLEAN);
  }

  public static TypeDescription createByte() {
    return new TypeDescription(Category.BYTE);
  }

  public static TypeDescription createShort() {
    return new TypeDescription(Category.SHORT);
  }

  public static TypeDescription createInt() {
    return new TypeDescription(Category.INT);
  }

  public static TypeDescription createLong() {
    return new TypeDescription(Category.LONG);
  }

  public static TypeDescription createFloat() {
    return new TypeDescription(Category.FLOAT);
  }

  public static TypeDescription createDouble() {
    return new TypeDescription(Category.DOUBLE);
  }

  public static TypeDescription createString() {
    return new TypeDescription(Category.STRING);
  }

  public static TypeDescription createDate() {
    return new TypeDescription(Category.DATE);
  }

  public static TypeDescription createTimestamp() {
    return new TypeDescription(Category.TIMESTAMP);
  }

  public static TypeDescription createTimestampInstant() {
    return new TypeDescription(Category.TIMESTAMP_INSTANT);
  }

  public static TypeDescription createBinary() {
    return new TypeDescription(Category.BINARY);
  }

  public static TypeDescription createDecimal() {
    return new TypeDescription(Category.DECIMAL);
  }

  /**
   * Parse TypeDescription from the Hive type names. This is the inverse
   * of TypeDescription.toString()
   * @param typeName the name of the type
   * @return a new TypeDescription or null if typeName was null
   * @throws IllegalArgumentException if the string is badly formed
   */
  public static TypeDescription fromString(String typeName) {
    if (typeName == null) {
      return null;
    }
    ParserUtils.StringPosition source = new ParserUtils.StringPosition(typeName);
    TypeDescription result = ParserUtils.parseType(source);
    if (source.hasCharactersLeft()) {
      throw new IllegalArgumentException("Extra characters at " + source);
    }
    return result;
  }

  /**
   * For decimal types, set the precision.
   * @param precision the new precision
   * @return this
   */
  public TypeDescription withPrecision(int precision) {
    if (category != Category.DECIMAL) {
      throw new IllegalArgumentException("precision is only allowed on decimal"+
         " and not " + category.name);
    } else if (precision < 1 || precision > MAX_PRECISION || scale > precision){
      throw new IllegalArgumentException("precision " + precision +
          " is out of range 1 .. " + scale);
    }
    this.precision = precision;
    return this;
  }

  /**
   * For decimal types, set the scale.
   * @param scale the new scale
   * @return this
   */
  public TypeDescription withScale(int scale) {
    if (category != Category.DECIMAL) {
      throw new IllegalArgumentException("scale is only allowed on decimal"+
          " and not " + category.name);
    } else if (scale < 0 || scale > MAX_SCALE || scale > precision) {
      throw new IllegalArgumentException("scale is out of range at " + scale);
    }
    this.scale = scale;
    return this;
  }

  /**
   * Set an attribute on this type.
   * @param key the attribute name
   * @param value the attribute value or null to clear the value
   * @return this for method chaining
   */
  public TypeDescription setAttribute(@NotNull String key,
                                      String value) {
    if (value == null) {
      attributes.remove(key);
    } else {
      attributes.put(key, value);
    }
    return this;
  }

  /**
   * Remove attribute on this type, if it is set.
   * @param key the attribute name
   * @return this for method chaining
   */
  public TypeDescription removeAttribute(@NotNull String key) {
    attributes.remove(key);
    return this;
  }

  public static TypeDescription createVarchar() {
    return new TypeDescription(Category.VARCHAR);
  }

  public static TypeDescription createChar() {
    return new TypeDescription(Category.CHAR);
  }

  /**
   * Set the maximum length for char and varchar types.
   * @param maxLength the maximum value
   * @return this
   */
  public TypeDescription withMaxLength(int maxLength) {
    if (category != Category.VARCHAR && category != Category.CHAR) {
      throw new IllegalArgumentException("maxLength is only allowed on char" +
                   " and varchar and not " + category.name);
    }
    this.maxLength = maxLength;
    return this;
  }

  public static TypeDescription createList(TypeDescription childType) {
    TypeDescription result = new TypeDescription(Category.LIST);
    result.children.add(childType);
    childType.parent = result;
    return result;
  }

  public static TypeDescription createMap(TypeDescription keyType,
                                          TypeDescription valueType) {
    TypeDescription result = new TypeDescription(Category.MAP);
    result.children.add(keyType);
    result.children.add(valueType);
    keyType.parent = result;
    valueType.parent = result;
    return result;
  }

  public static TypeDescription createUnion() {
    return new TypeDescription(Category.UNION);
  }

  public static TypeDescription createStruct() {
    return new TypeDescription(Category.STRUCT);
  }

  /**
   * Add a child to a union type.
   * @param child a new child type to add
   * @return the union type.
   */
  public TypeDescription addUnionChild(TypeDescription child) {
    if (category != Category.UNION) {
      throw new IllegalArgumentException("Can only add types to union type" +
          " and not " + category);
    }
    addChild(child);
    return this;
  }

  /**
   * Add a field to a struct type as it is built.
   * @param field the field name
   * @param fieldType the type of the field
   * @return the struct type
   */
  public TypeDescription addField(String field, TypeDescription fieldType) {
    if (category != Category.STRUCT) {
      throw new IllegalArgumentException("Can only add fields to struct type" +
          " and not " + category);
    }
    fieldNames.add(field);
    addChild(fieldType);
    return this;
  }

  /**
   * Get the id for this type.
   * The first call will cause all of the the ids in tree to be assigned, so
   * it should not be called before the type is completely built.
   * @return the sequential id
   */
  public int getId() {
    // if the id hasn't been assigned, assign all of the ids from the root
    if (id == -1) {
      TypeDescription root = this;
      while (root.parent != null) {
        root = root.parent;
      }
      root.assignIds(0);
    }
    return id;
  }

  @Override
  public TypeDescription clone() {
    TypeDescription result = new TypeDescription(category);
    result.maxLength = maxLength;
    result.precision = precision;
    result.scale = scale;
    if (fieldNames != null) {
      result.fieldNames.addAll(fieldNames);
    }
    if (children != null) {
      for(TypeDescription child: children) {
        TypeDescription clone = child.clone();
        clone.parent = result;
        result.children.add(clone);
      }
    }
    for (Map.Entry<String,String> pair: attributes.entrySet()) {
      result.attributes.put(pair.getKey(), pair.getValue());
    }
    return result;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + category.hashCode();
    if (children != null) {
      result = prime * result + children.hashCode();
    }
    result = prime * result + maxLength;
    result = prime * result + precision;
    result = prime * result + scale;
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return equals(other, true);
  }

  /**
   * Determines whether the two object are equal.
   * This function can either compare or ignore the type attributes as
   * desired.
   * @param other the reference object with which to compare.
   * @param checkAttributes should the type attributes be considered?
   * @return {@code true} if this object is the same as the other
   *         argument; {@code false} otherwise.
   */
  public boolean equals(Object other, boolean checkAttributes) {
    if (other == null || !(other instanceof TypeDescription)) {
      return false;
    }
    if (other == this) {
      return true;
    }
    TypeDescription castOther = (TypeDescription) other;
    if (category != castOther.category ||
        maxLength != castOther.maxLength ||
        scale != castOther.scale ||
        precision != castOther.precision) {
      return false;
    }
    if (checkAttributes) {
      // make sure the attributes are the same
      List<String> attributeNames = getAttributeNames();
      if (castOther.getAttributeNames().size() != attributeNames.size()) {
        return false;
      }
      for (String attribute : attributeNames) {
        if (!getAttributeValue(attribute).equals(castOther.getAttributeValue(attribute))) {
          return false;
        }
      }
    }
    // check the children
    if (children != null) {
      if (children.size() != castOther.children.size()) {
        return false;
      }
      for (int i = 0; i < children.size(); ++i) {
        if (!children.get(i).equals(castOther.children.get(i), checkAttributes)) {
          return false;
        }
      }
    }
    if (category == Category.STRUCT) {
      for(int i=0; i < fieldNames.size(); ++i) {
        if (!fieldNames.get(i).equals(castOther.fieldNames.get(i))) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Get the maximum id assigned to this type or its children.
   * The first call will cause all of the the ids in tree to be assigned, so
   * it should not be called before the type is completely built.
   * @return the maximum id assigned under this type
   */
  public int getMaximumId() {
    // if the id hasn't been assigned, assign all of the ids from the root
    if (maxId == -1) {
      TypeDescription root = this;
      while (root.parent != null) {
        root = root.parent;
      }
      root.assignIds(0);
    }
    return maxId;
  }

  /**
   * Specify the version of the VectorizedRowBatch that the user desires.
   */
  public enum RowBatchVersion {
    ORIGINAL,
    USE_DECIMAL64;
  }

  public VectorizedRowBatch createRowBatch(RowBatchVersion version, int size) {
    VectorizedRowBatch result;
    if (category == Category.STRUCT) {
      result = new VectorizedRowBatch(children.size(), size);
      for(int i=0; i < result.cols.length; ++i) {
        result.cols[i] = TypeUtils.createColumn(children.get(i), version, size);
      }
    } else {
      result = new VectorizedRowBatch(1, size);
      result.cols[0] = TypeUtils.createColumn(this, version, size);
    }
    result.reset();
    return result;
  }

  /**
   * Create a VectorizedRowBatch that uses Decimal64ColumnVector for
   * short (p &le; 18) decimals.
   * @return a new VectorizedRowBatch
   */
  public VectorizedRowBatch createRowBatchV2() {
    return createRowBatch(RowBatchVersion.USE_DECIMAL64,
        VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Create a VectorizedRowBatch with the original ColumnVector types
   * @param maxSize the maximum size of the batch
   * @return a new VectorizedRowBatch
   */
  public VectorizedRowBatch createRowBatch(int maxSize) {
    return createRowBatch(RowBatchVersion.ORIGINAL, maxSize);
  }

  /**
   * Create a VectorizedRowBatch with the original ColumnVector types
   * @return a new VectorizedRowBatch
   */
  public VectorizedRowBatch createRowBatch() {
    return createRowBatch(RowBatchVersion.ORIGINAL,
        VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Get the kind of this type.
   * @return get the category for this type.
   */
  public Category getCategory() {
    return category;
  }

  /**
   * Get the maximum length of the type. Only used for char and varchar types.
   * @return the maximum length of the string type
   */
  public int getMaxLength() {
    return maxLength;
  }

  /**
   * Get the precision of the decimal type.
   * @return the number of digits for the precision.
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * Get the scale of the decimal type.
   * @return the number of digits for the scale.
   */
  public int getScale() {
    return scale;
  }

  /**
   * For struct types, get the list of field names.
   * @return the list of field names.
   */
  public List<String> getFieldNames() {
    return Collections.unmodifiableList(fieldNames);
  }

  /**
   * Get the list of attribute names defined on this type.
   * @return a list of sorted attribute names
   */
  public List<String> getAttributeNames() {
    List<String> result = new ArrayList<>(attributes.keySet());
    Collections.sort(result);
    return result;
  }

  /**
   * Get the value of a given attribute.
   * @param attributeName the name of the attribute
   * @return the value of the attribute or null if it isn't set
   */
  public String getAttributeValue(String attributeName) {
    return attributes.get(attributeName);
  }

  /**
   * Get the parent of the current type
   * @return null if root else parent
   */
  public TypeDescription getParent() {
    return parent;
  }

  /**
   * Get the subtypes of this type.
   * @return the list of children types
   */
  public List<TypeDescription> getChildren() {
    return children == null ? null : Collections.unmodifiableList(children);
  }

  /**
   * Assign ids to all of the nodes under this one.
   * @param startId the lowest id to assign
   * @return the next available id
   */
  private int assignIds(int startId) {
    id = startId++;
    if (children != null) {
      for (TypeDescription child : children) {
        startId = child.assignIds(startId);
      }
    }
    maxId = startId - 1;
    return startId;
  }

  /**
   * Add a child to a type.
   * @param child the child to add
   */
  public void addChild(TypeDescription child) {
    switch (category) {
      case LIST:
        if (children.size() >= 1) {
          throw new IllegalArgumentException("Can't add more children to list");
        }
      case MAP:
        if (children.size() >= 2) {
          throw new IllegalArgumentException("Can't add more children to map");
        }
      case UNION:
      case STRUCT:
        children.add(child);
        child.parent = this;
        break;
      default:
        throw new IllegalArgumentException("Can't add children to " + category);
    }
  }

  public TypeDescription(Category category) {
    this.category = category;
    if (category.isPrimitive) {
      children = null;
    } else {
      children = new ArrayList<>();
    }
    if (category == Category.STRUCT) {
      fieldNames = new ArrayList<>();
    } else {
      fieldNames = null;
    }
  }

  private int id = -1;
  private int maxId = -1;
  private TypeDescription parent;
  private final Category category;
  private final List<TypeDescription> children;
  private final List<String> fieldNames;
  private final Map<String,String> attributes = new HashMap<>();
  private int maxLength = DEFAULT_LENGTH;
  private int precision = DEFAULT_PRECISION;
  private int scale = DEFAULT_SCALE;

  static void printFieldName(StringBuilder buffer, String name) {
    if (UNQUOTED_NAMES.matcher(name).matches()) {
      buffer.append(name);
    } else {
      buffer.append('`');
      buffer.append(name.replace("`", "``"));
      buffer.append('`');
    }
  }

  public void printToBuffer(StringBuilder buffer) {
    buffer.append(category.name);
    switch (category) {
      case DECIMAL:
        buffer.append('(');
        buffer.append(precision);
        buffer.append(',');
        buffer.append(scale);
        buffer.append(')');
        break;
      case CHAR:
      case VARCHAR:
        buffer.append('(');
        buffer.append(maxLength);
        buffer.append(')');
        break;
      case LIST:
      case MAP:
      case UNION:
        buffer.append('<');
        for(int i=0; i < children.size(); ++i) {
          if (i != 0) {
            buffer.append(',');
          }
          children.get(i).printToBuffer(buffer);
        }
        buffer.append('>');
        break;
      case STRUCT:
        buffer.append('<');
        for(int i=0; i < children.size(); ++i) {
          if (i != 0) {
            buffer.append(',');
          }
          printFieldName(buffer, fieldNames.get(i));
          buffer.append(':');
          children.get(i).printToBuffer(buffer);
        }
        buffer.append('>');
        break;
      default:
        break;
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    printToBuffer(buffer);
    return buffer.toString();
  }

  private void printJsonToBuffer(String prefix, StringBuilder buffer,
                                 int indent) {
    for(int i=0; i < indent; ++i) {
      buffer.append(' ');
    }
    buffer.append(prefix);
    buffer.append("{\"category\": \"");
    buffer.append(category.name);
    buffer.append("\", \"id\": ");
    buffer.append(getId());
    buffer.append(", \"max\": ");
    buffer.append(maxId);
    switch (category) {
      case DECIMAL:
        buffer.append(", \"precision\": ");
        buffer.append(precision);
        buffer.append(", \"scale\": ");
        buffer.append(scale);
        break;
      case CHAR:
      case VARCHAR:
        buffer.append(", \"length\": ");
        buffer.append(maxLength);
        break;
      case LIST:
      case MAP:
      case UNION:
        buffer.append(", \"children\": [");
        for(int i=0; i < children.size(); ++i) {
          buffer.append('\n');
          children.get(i).printJsonToBuffer("", buffer, indent + 2);
          if (i != children.size() - 1) {
            buffer.append(',');
          }
        }
        buffer.append("]");
        break;
      case STRUCT:
        buffer.append(", \"fields\": [");
        for(int i=0; i < children.size(); ++i) {
          buffer.append('\n');
          buffer.append('{');
          children.get(i).printJsonToBuffer("\"" + fieldNames.get(i) + "\": ",
              buffer, indent + 2);
          buffer.append('}');
          if (i != children.size() - 1) {
            buffer.append(',');
          }
        }
        buffer.append(']');
        break;
      default:
        break;
    }
    buffer.append('}');
  }

  public String toJson() {
    StringBuilder buffer = new StringBuilder();
    printJsonToBuffer("", buffer, 0);
    return buffer.toString();
  }

  /**
   * Locate a subtype by its id.
   * @param goal the column id to look for
   * @return the subtype
   */
  public TypeDescription findSubtype(int goal) {
    ParserUtils.TypeFinder result = new ParserUtils.TypeFinder(this);
    ParserUtils.findSubtype(this, goal, result);
    return result.current;
  }

  /**
   * Find a subtype of this schema by name.
   * If the name is a simple integer, it will be used as a column number.
   * Otherwise, this routine will recursively search for the name.
   * <ul>
   *   <li>Struct fields are selected by name.</li>
   *   <li>List children are selected by "_elem".</li>
   *   <li>Map children are selected by "_key" or "_value".</li>
   *   <li>Union children are selected by number starting at 0.</li>
   * </ul>
   * Names are separated by '.'.
   * @param columnName the name to search for
   * @return the subtype
   */
  public TypeDescription findSubtype(String columnName) {
    return findSubtype(columnName, true);
  }

  public TypeDescription findSubtype(String columnName,
      boolean isSchemaEvolutionCaseAware) {
    ParserUtils.StringPosition source = new ParserUtils.StringPosition(columnName);
    TypeDescription result = ParserUtils.findSubtype(this, source,
        isSchemaEvolutionCaseAware);
    if (source.hasCharactersLeft()) {
      throw new IllegalArgumentException("Remaining text in parsing field name "
          + source);
    }
    return result;
  }

  /**
   * Find a list of subtypes from a string, including the empty list.
   *
   * Each column name is separated by ','.
   * @param columnNameList the list of column names
   * @return the list of subtypes that correspond to the column names
   */
  public List<TypeDescription> findSubtypes(String columnNameList) {
    ParserUtils.StringPosition source = new ParserUtils.StringPosition(columnNameList);
    List<TypeDescription> result = ParserUtils.findSubtypeList(this, source);
    if (source.hasCharactersLeft()) {
      throw new IllegalArgumentException("Remaining text in parsing field name "
          + source);
    }
    return result;
  }

  /**
   * Annotate a schema with the encryption keys and masks.
   * @param encryption the encryption keys and the fields
   * @param masks the encryption masks and the fields
   */
  public void annotateEncryption(String encryption, String masks) {
    ParserUtils.StringPosition source = new ParserUtils.StringPosition(encryption);
    ParserUtils.parseKeys(source, this);
    if (source.hasCharactersLeft()) {
      throw new IllegalArgumentException("Remaining text in parsing encryption keys "
          + source);
    }
    source = new ParserUtils.StringPosition(masks);
    ParserUtils.parseMasks(source, this);
    if (source.hasCharactersLeft()) {
      throw new IllegalArgumentException("Remaining text in parsing encryption masks "
          + source);
    }
  }

  /**
   * Find the index of a given child object using == comparison.
   * @param child The child type
   * @return the index 0 to N-1 of the children.
   */
  private int getChildIndex(TypeDescription child) {
    for(int i=children.size() - 1; i >= 0; --i) {
      if (children.get(i) == child) {
        return i;
      }
    }
    throw new IllegalArgumentException("Child not found");
  }

  /**
   * For a complex type, get the partial name for this child. For structures,
   * it returns the corresponding field name. For lists and maps, it uses the
   * special names "_elem", "_key", and "_value". Unions use the integer index.
   * @param child The desired child, which must be the same object (==)
   * @return The name of the field for the given child.
   */
  private String getPartialName(TypeDescription child) {
    switch (category) {
      case LIST:
        return "_elem";
      case MAP:
        return getChildIndex(child) == 0 ? "_key" : "_value";
      case STRUCT:
        return fieldNames.get(getChildIndex(child));
      case UNION:
        return Integer.toString(getChildIndex(child));
      default:
        throw new IllegalArgumentException(
            "Can't get the field name of a primitive type");
    }
  }

  /**
   * Get the full field name for the given type. For
   * "struct&lt;a:struct&lt;list&lt;struct&lt;b:int,c:int&gt;&gt;&gt;&gt;" when
   * called on c, would return "a._elem.c".
   * @return A string that is the inverse of findSubtype
   */
  public String getFullFieldName() {
    List<String> parts = new ArrayList<>(getId());
    TypeDescription current = this;
    TypeDescription parent = current.getParent();
    // Handle the root as a special case so that it isn't an empty string.
    if (parent == null) {
      return Integer.toString(current.getId());
    }
    while (parent != null) {
      parts.add(parent.getPartialName(current));
      current = parent;
      parent = current.getParent();
    }
    // Put the string together backwards
    StringBuilder buffer = new StringBuilder();
    for (int part=parts.size() - 1; part >= 0; --part) {
      buffer.append(parts.get(part));
      if (part != 0) {
        buffer.append('.');
      }
    }
    return buffer.toString();
  }
}
