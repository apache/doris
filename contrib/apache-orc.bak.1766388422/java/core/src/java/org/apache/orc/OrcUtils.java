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

import org.apache.orc.impl.ParserUtils;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.SchemaEvolution;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.hadoop.util.StringUtils.COMMA_STR;

public class OrcUtils {

  /**
   * Returns selected columns as a boolean array with true value set for specified column names.
   * The result will contain number of elements equal to flattened number of columns.
   * For example:
   * selectedColumns - a,b,c
   * allColumns - a,b,c,d
   * If column c is a complex type, say list&lt;string&gt; and other types are
   * primitives then result will
   * be [false, true, true, true, true, true, false]
   * Index 0 is the root element of the struct which is set to false by default, index 1,2
   * corresponds to columns a and b. Index 3,4 correspond to column c which is list&lt;string&gt; and
   * index 5 correspond to column d. After flattening list&lt;string&gt; gets 2 columns.
   * <p>
   * Column names that aren't found are ignored.
   * @param selectedColumns - comma separated list of selected column names
   * @param schema       - object schema
   * @return - boolean array with true value set for the specified column names
   */
  public static boolean[] includeColumns(String selectedColumns,
                                         TypeDescription schema) {
    int numFlattenedCols = schema.getMaximumId();
    boolean[] results = new boolean[numFlattenedCols + 1];
    if ("*".equals(selectedColumns)) {
      Arrays.fill(results, true);
      return results;
    }
    TypeDescription baseSchema = SchemaEvolution.checkAcidSchema(schema) ?
        SchemaEvolution.getBaseRow(schema) : schema;

    if (selectedColumns != null &&
        baseSchema.getCategory() == TypeDescription.Category.STRUCT) {

      for (String columnName : selectedColumns.split(COMMA_STR)) {
        TypeDescription column = findColumn(baseSchema, columnName.trim());
        if (column != null) {
          for (int i = column.getId(); i <= column.getMaximumId(); ++i) {
            results[i] = true;
          }
        }
      }
    }
    return results;
  }

  private static TypeDescription findColumn(TypeDescription schema, String column) {
    TypeDescription result = schema;
    String[] columnMatcher = column.split("\\.");

    int index = 0;
    while (index < columnMatcher.length &&
        result.getCategory() == TypeDescription.Category.STRUCT) {

      String columnName = columnMatcher[index];
      int prevIndex = index;

      List<TypeDescription> fields = result.getChildren();
      List<String> fieldNames = result.getFieldNames();

      for (int i = 0; i < fields.size(); i++) {
        if (columnName.equalsIgnoreCase(fieldNames.get(i))) {
          result = fields.get(i);
          index++;

          break;
        }
      }
      if (prevIndex == index) {
        return null;
      }
    }
    return result;
  }

  public static List<OrcProto.Type> getOrcTypes(TypeDescription typeDescr) {
    List<OrcProto.Type> result = new ArrayList<>();
    appendOrcTypes(result, typeDescr);
    return result;
  }

  private static void appendOrcTypes(List<OrcProto.Type> result, TypeDescription typeDescr) {
    OrcProto.Type.Builder type = OrcProto.Type.newBuilder();
    List<TypeDescription> children = typeDescr.getChildren();
    // save the attributes
    for(String key: typeDescr.getAttributeNames()) {
      type.addAttributes(
          OrcProto.StringPair.newBuilder()
              .setKey(key).setValue(typeDescr.getAttributeValue(key))
              .build());
    }
    switch (typeDescr.getCategory()) {
      case BOOLEAN:
        type.setKind(OrcProto.Type.Kind.BOOLEAN);
        break;
      case BYTE:
        type.setKind(OrcProto.Type.Kind.BYTE);
        break;
      case SHORT:
        type.setKind(OrcProto.Type.Kind.SHORT);
        break;
      case INT:
        type.setKind(OrcProto.Type.Kind.INT);
        break;
      case LONG:
        type.setKind(OrcProto.Type.Kind.LONG);
        break;
      case FLOAT:
        type.setKind(OrcProto.Type.Kind.FLOAT);
        break;
      case DOUBLE:
        type.setKind(OrcProto.Type.Kind.DOUBLE);
        break;
      case STRING:
        type.setKind(OrcProto.Type.Kind.STRING);
        break;
      case CHAR:
        type.setKind(OrcProto.Type.Kind.CHAR);
        type.setMaximumLength(typeDescr.getMaxLength());
        break;
      case VARCHAR:
        type.setKind(OrcProto.Type.Kind.VARCHAR);
        type.setMaximumLength(typeDescr.getMaxLength());
        break;
      case BINARY:
        type.setKind(OrcProto.Type.Kind.BINARY);
        break;
      case TIMESTAMP:
        type.setKind(OrcProto.Type.Kind.TIMESTAMP);
        break;
      case TIMESTAMP_INSTANT:
        type.setKind(OrcProto.Type.Kind.TIMESTAMP_INSTANT);
        break;
      case DATE:
        type.setKind(OrcProto.Type.Kind.DATE);
        break;
      case DECIMAL:
        type.setKind(OrcProto.Type.Kind.DECIMAL);
        type.setPrecision(typeDescr.getPrecision());
        type.setScale(typeDescr.getScale());
        break;
      case LIST:
        type.setKind(OrcProto.Type.Kind.LIST);
        type.addSubtypes(children.get(0).getId());
        break;
      case MAP:
        type.setKind(OrcProto.Type.Kind.MAP);
        for(TypeDescription t: children) {
          type.addSubtypes(t.getId());
        }
        break;
      case STRUCT:
        type.setKind(OrcProto.Type.Kind.STRUCT);
        for(TypeDescription t: children) {
          type.addSubtypes(t.getId());
        }
        for(String field: typeDescr.getFieldNames()) {
          type.addFieldNames(field);
        }
        break;
      case UNION:
        type.setKind(OrcProto.Type.Kind.UNION);
        for(TypeDescription t: children) {
          type.addSubtypes(t.getId());
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown category: " +
            typeDescr.getCategory());
    }
    result.add(type.build());
    if (children != null) {
      for(TypeDescription child: children) {
        appendOrcTypes(result, child);
      }
    }
  }

  /**
   * Checks whether the list of protobuf types from the file are valid or not.
   * @param types the list of types from the protobuf
   * @param root the top of the tree to check
   * @return the next available id
   * @throws java.io.IOException if the tree is invalid
   */
  public static int isValidTypeTree(List<OrcProto.Type> types,
                                    int root) throws IOException  {
    if (root < 0 || root >= types.size()) {
      throw new IOException("Illegal type id " + root +
          ". The valid range is 0 to " + (types.size() - 1));
    }
    OrcProto.Type rootType = types.get(root);
    int current = root+1;
    List<Integer> children = rootType.getSubtypesList();
    if (!rootType.hasKind()) {
      throw new IOException("Type " + root + " has an unknown kind.");
    }
    // ensure that we have the right number of children
    switch(rootType.getKind()) {
      case LIST:
        if (children == null || children.size() != 1) {
          throw new IOException("Wrong number of type children in list " + root);
        }
        break;
      case MAP:
        if (children == null || children.size() != 2) {
          throw new IOException("Wrong number of type children in map " + root);
        }
        break;
      case UNION:
      case STRUCT:
        break;
      default:
        if (children != null && children.size() != 0) {
          throw new IOException("Type children under primitive type " + root);
        }
    }
    // ensure the children are also correct
    if (children != null) {
      for(int child: children) {
        if (child != current) {
          throw new IOException("Unexpected child type id " + child + " when " +
              current + " was expected.");
        }
        current = isValidTypeTree(types, current);
      }
    }
    return current;
  }
  /**
   * Translate the given rootColumn from the list of types to a TypeDescription.
   * @param types all of the types
   * @param rootColumn translate this type
   * @return a new TypeDescription that matches the given rootColumn
   */
  public static
        TypeDescription convertTypeFromProtobuf(List<OrcProto.Type> types,
                                                int rootColumn)
          throws FileFormatException {
    OrcProto.Type type = types.get(rootColumn);
    TypeDescription result;
    switch (type.getKind()) {
      case BOOLEAN:
        result = TypeDescription.createBoolean();
        break;
      case BYTE:
        result = TypeDescription.createByte();
        break;
      case SHORT:
        result = TypeDescription.createShort();
        break;
      case INT:
        result = TypeDescription.createInt();
        break;
      case LONG:
        result = TypeDescription.createLong();
        break;
      case FLOAT:
        result = TypeDescription.createFloat();
        break;
      case DOUBLE:
        result = TypeDescription.createDouble();
        break;
      case STRING:
        result = TypeDescription.createString();
        break;
      case CHAR:
      case VARCHAR:
        result = type.getKind() == OrcProto.Type.Kind.CHAR ?
            TypeDescription.createChar() : TypeDescription.createVarchar();
        if (type.hasMaximumLength()) {
          result.withMaxLength(type.getMaximumLength());
        }
        break;
      case BINARY:
        result = TypeDescription.createBinary();
        break;
      case TIMESTAMP:
        result = TypeDescription.createTimestamp();
        break;
      case TIMESTAMP_INSTANT:
        result = TypeDescription.createTimestampInstant();
        break;
      case DATE:
        result = TypeDescription.createDate();
        break;
      case DECIMAL:
        result = TypeDescription.createDecimal();
        if (type.hasScale()) {
          result.withScale(type.getScale());
        }
        if (type.hasPrecision()) {
          result.withPrecision(type.getPrecision());
        }
        break;
      case LIST:
        if (type.getSubtypesCount() != 1) {
          throw new FileFormatException("LIST type should contain exactly " +
                  "one subtype but has " + type.getSubtypesCount());
        }
        result = TypeDescription.createList(
            convertTypeFromProtobuf(types, type.getSubtypes(0)));
        break;
      case MAP:
        if (type.getSubtypesCount() != 2) {
          throw new FileFormatException("MAP type should contain exactly " +
                  "two subtypes but has " + type.getSubtypesCount());
        }
        result = TypeDescription.createMap(
            convertTypeFromProtobuf(types, type.getSubtypes(0)),
            convertTypeFromProtobuf(types, type.getSubtypes(1)));
        break;
      case STRUCT:
        result = TypeDescription.createStruct();
        for(int f=0; f < type.getSubtypesCount(); ++f) {
          String name = type.getFieldNames(f);
          name = name.startsWith("`") ? name : "`" + name + "`";
          String fieldName = ParserUtils.parseName(new ParserUtils.StringPosition(name));
          result.addField(fieldName, convertTypeFromProtobuf(types, type.getSubtypes(f)));
        }
        break;
      case UNION:
        if (type.getSubtypesCount() == 0) {
          throw new FileFormatException("UNION type should contain at least" +
                " one subtype but has none");
        }
        result = TypeDescription.createUnion();
        for(int f=0; f < type.getSubtypesCount(); ++f) {
          result.addUnionChild(
              convertTypeFromProtobuf(types, type.getSubtypes(f)));
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown ORC type " + type.getKind());
    }
    for(int i = 0; i < type.getAttributesCount(); ++i) {
      OrcProto.StringPair pair = type.getAttributes(i);
      result.setAttribute(pair.getKey(), pair.getValue());
    }
    return result;
  }

  public static List<StripeInformation> convertProtoStripesToStripes(
      List<OrcProto.StripeInformation> stripes) {
    List<StripeInformation> result = new ArrayList<>(stripes.size());
    long previousStripeId = 0;
    byte[][] previousKeys = null;
    long stripeId = 0;
    for (OrcProto.StripeInformation stripeProto: stripes) {
      ReaderImpl.StripeInformationImpl stripe =
          new ReaderImpl.StripeInformationImpl(stripeProto, stripeId++,
              previousStripeId, previousKeys);
      result.add(stripe);
      previousStripeId = stripe.getEncryptionStripeId();
      previousKeys = stripe.getEncryptedLocalKeys();
    }
    return result;
  }

  /**
   * Get the user-facing version string for the software that wrote the file.
   * @param writer the code for the writer from OrcProto.Footer
   * @param version the orcVersion from OrcProto.Footer
   * @return the version string
   */
  public static String getSoftwareVersion(int writer,
                                          String version) {
    String base;
    switch (writer) {
      case 0:
        base = "ORC Java";
        break;
      case 1:
        base = "ORC C++";
        break;
      case 2:
        base = "Presto";
        break;
      case 3:
        base = "Scritchley Go";
        break;
      case 4:
        base = "Trino";
        break;
      default:
        base = String.format("Unknown(%d)", writer);
        break;
    }
    if (version == null) {
      return base;
    } else {
      return base + " " + version;
    }
  }

  /**
   * Get the software version from Maven.
   * @return The version of the software.
   */
  public static String getOrcVersion() {
    Class<OrcFile> cls = OrcFile.class;
    // try to load from maven properties first
    try (InputStream is = cls.getResourceAsStream(
        "/META-INF/maven/org.apache.orc/orc-core/pom.properties")) {
      if (is != null) {
        Properties p = new Properties();
        p.load(is);
        String version = p.getProperty("version", null);
        if (version != null) {
          return version;
        }
      }
    } catch (IOException e) {
      // ignore
    }

    // fallback to using Java API
    Package aPackage = cls.getPackage();
    if (aPackage != null) {
      String version = aPackage.getImplementationVersion();
      if (version != null) {
        return version;
      }
    }
    return "unknown";
  }
}
