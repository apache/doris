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

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Infer and track the evolution between the schema as stored in the file and
 * the schema that has been requested by the reader.
 */
public class SchemaEvolution {
  // indexed by reader column id
  private final TypeDescription[] readerFileTypes;
  // indexed by reader column id
  private final boolean[] readerIncluded;
  // the offset to the first column id ignoring any ACID columns
  private final int readerColumnOffset;
  // indexed by file column id
  private final boolean[] fileIncluded;
  private final TypeDescription fileSchema;
  private final TypeDescription readerSchema;
  private boolean hasConversion;
  private boolean isOnlyImplicitConversion;
  private final boolean isAcid;
  final boolean isSchemaEvolutionCaseAware;
  /**
   * {@code true} if acid metadata columns should be decoded otherwise they will
   * be set to {@code null}.  {@link #acidEventFieldNames}.
   */
  private final boolean includeAcidColumns;

  // indexed by file column id
  private final boolean[] ppdSafeConversion;

  // columns are indexed, not named between Reader & File schema
  private final boolean positionalColumns;

  private static final Logger LOG =
      LoggerFactory.getLogger(SchemaEvolution.class);
  private static final Pattern missingMetadataPattern =
      Pattern.compile("_col\\d+");


  public static class IllegalEvolutionException extends RuntimeException {
    public IllegalEvolutionException(String msg) {
      super(msg);
    }
  }
  public SchemaEvolution(TypeDescription fileSchema,
                         TypeDescription readerSchema,
                         Reader.Options options) {
    boolean allowMissingMetadata = options.getTolerateMissingSchema();
    boolean[] includedCols = options.getInclude();
    this.isSchemaEvolutionCaseAware=options.getIsSchemaEvolutionCaseAware();
    this.readerIncluded = includedCols == null ? null :
      Arrays.copyOf(includedCols, includedCols.length);
    this.fileIncluded = new boolean[fileSchema.getMaximumId() + 1];
    this.hasConversion = false;
    this.isOnlyImplicitConversion = true;
    this.fileSchema = fileSchema;
    // Use file schema when reader schema not provided
    readerSchema =  readerSchema == null ? this.fileSchema : readerSchema;
    this.isAcid = checkAcidSchema(fileSchema);
    boolean readerSchemaIsAcid = checkAcidSchema(readerSchema);
    this.includeAcidColumns = options.getIncludeAcidColumns();
    this.readerColumnOffset = isAcid && !readerSchemaIsAcid ? acidEventFieldNames.size() : 0;
    // Create type conversion using reader schema
    if (isAcid && !readerSchemaIsAcid) {
      this.readerSchema = createEventSchema(readerSchema);
    } else {
      this.readerSchema = readerSchema;
    }
    if (readerIncluded != null &&
        readerIncluded.length + readerColumnOffset !=
          this.readerSchema.getMaximumId() + 1) {
      throw new IllegalArgumentException("Include vector the wrong length: "
          + this.readerSchema.toJson() + " with include length "
          + readerIncluded.length);
    }
    this.readerFileTypes =
        new TypeDescription[this.readerSchema.getMaximumId() + 1];
    int positionalLevels = 0;
    if (options.getForcePositionalEvolution()) {
      positionalLevels = isAcid ? 2 : options.getPositionalEvolutionLevel();
    } else if (!hasColumnNames(isAcid? getBaseRow(fileSchema) : fileSchema)) {
      if (!this.fileSchema.equals(this.readerSchema)) {
        if (!allowMissingMetadata) {
          throw new RuntimeException("Found that schema metadata is missing"
              + " from file. This is likely caused by"
              + " a writer earlier than HIVE-4243. Will"
              + " not try to reconcile schemas");
        } else {
          LOG.warn("Column names are missing from this file. This is"
              + " caused by a writer earlier than HIVE-4243. The reader will"
              + " reconcile schemas based on index. File type: " +
              this.fileSchema + ", reader type: " + this.readerSchema);
          positionalLevels = isAcid ? 2 : options.getPositionalEvolutionLevel();
        }
      }
    }
    buildConversion(fileSchema, this.readerSchema, positionalLevels);
    this.positionalColumns = options.getForcePositionalEvolution();
    this.ppdSafeConversion = populatePpdSafeConversion();
  }

  @Deprecated
  public SchemaEvolution(TypeDescription fileSchema, boolean[] readerIncluded) {
    this(fileSchema, null, readerIncluded);
  }

  @Deprecated
  public SchemaEvolution(TypeDescription fileSchema,
                         TypeDescription readerSchema,
                         boolean[] readerIncluded) {
    this(fileSchema, readerSchema,
        new Reader.Options(new Configuration())
            .include(readerIncluded));
  }

  // Return true iff all fields have names like _col[0-9]+
  private boolean hasColumnNames(TypeDescription fileSchema) {
    if (fileSchema.getCategory() != TypeDescription.Category.STRUCT) {
      return true;
    }
    for (String fieldName : fileSchema.getFieldNames()) {
      if (!missingMetadataPattern.matcher(fieldName).matches()) {
        return true;
      }
    }
    return false;
  }

  public boolean isSchemaEvolutionCaseAware() {
    return isSchemaEvolutionCaseAware;
  }

  public TypeDescription getReaderSchema() {
    return readerSchema;
  }

  /**
   * Returns the non-ACID (aka base) reader type description.
   *
   * @return the reader type ignoring the ACID rowid columns, if any
   */
  public TypeDescription getReaderBaseSchema() {
    return isAcid ? getBaseRow(readerSchema) : readerSchema;
  }

  /**
   * Does the file include ACID columns?
   * @return is this an ACID file?
   */
  boolean isAcid() {
    return isAcid;
  }

  /**
   * Is there Schema Evolution data type conversion?
   * @return
   */
  public boolean hasConversion() {
    return hasConversion;
  }

  /**
   * When there Schema Evolution data type conversion i.e. hasConversion() returns true,
   * is the conversion only the implicit kind?
   *
   * (see aaa).
   * @return
   */
  public boolean isOnlyImplicitConversion() {
    return isOnlyImplicitConversion;
  }

  public TypeDescription getFileSchema() {
    return fileSchema;
  }

  public TypeDescription getFileType(TypeDescription readerType) {
    return getFileType(readerType.getId());
  }

  /**
   * Get the file type by reader type id.
   * @param id reader column id
   * @return
   */
  public TypeDescription getFileType(int id) {
    return readerFileTypes[id];
  }

  /**
   * Get whether each column is included from the reader's point of view.
   * @return a boolean array indexed by reader column id
   */
  public boolean[] getReaderIncluded() {
    return readerIncluded;
  }

  /**
   * Get whether each column is included from the file's point of view.
   * @return a boolean array indexed by file column id
   */
  public boolean[] getFileIncluded() {
    return fileIncluded;
  }

  /**
   * Get whether the columns are handled via position or name
   */
  public boolean getPositionalColumns() {
    return this.positionalColumns;
  }

  /**
   * Determine if there is implicit conversion from a file to reader type.
   *
   * Implicit conversions are:
   *   Small to larger integer (e.g. INT to LONG)
   *   FLOAT to DOUBLE
   *   Some String Family conversions.
   *
   * NOTE: This check is independent of the PPD conversion checks.
   * @return
   */
  private boolean typesAreImplicitConversion(final TypeDescription fileType,
      final TypeDescription readerType) {
    switch (fileType.getCategory()) {
      case BYTE:
        if (readerType.getCategory().equals(TypeDescription.Category.SHORT) ||
            readerType.getCategory().equals(TypeDescription.Category.INT) ||
            readerType.getCategory().equals(TypeDescription.Category.LONG)) {
          return true;
        }
        break;
      case SHORT:
        if (readerType.getCategory().equals(TypeDescription.Category.INT) ||
            readerType.getCategory().equals(TypeDescription.Category.LONG)) {
          return true;
        }
        break;
      case INT:
        if (readerType.getCategory().equals(TypeDescription.Category.LONG)) {
          return true;
        }
        break;
      case FLOAT:
        if (readerType.getCategory().equals(TypeDescription.Category.DOUBLE)) {
          return true;
        }
        break;
      case CHAR:
      case VARCHAR:
        if (readerType.getCategory().equals(TypeDescription.Category.STRING)) {
          return true;
        }
        if (readerType.getCategory().equals(TypeDescription.Category.CHAR) ||
            readerType.getCategory().equals(TypeDescription.Category.VARCHAR)) {
          return (fileType.getMaxLength() <= readerType.getMaxLength());
        }
        break;
      default:
        break;
    }
    return false;
  }

  /**
   * Check if column is safe for ppd evaluation
   * @param fileColId file column id
   * @return true if the specified column is safe for ppd evaluation else false
   */
  public boolean isPPDSafeConversion(final int fileColId) {
    if (hasConversion()) {
      return !(fileColId < 0 || fileColId >= ppdSafeConversion.length) &&
          ppdSafeConversion[fileColId];
    }

    // when there is no schema evolution PPD is safe
    return true;
  }

  private boolean[] populatePpdSafeConversion() {
    if (fileSchema == null || readerSchema == null || readerFileTypes == null) {
      return null;
    }

    boolean[] result = new boolean[fileSchema.getMaximumId() + 1];
    boolean safePpd = validatePPDConversion(fileSchema, readerSchema);
    result[fileSchema.getId()] = safePpd;
    return populatePpdSafeConversionForChildren(result,
        readerSchema.getChildren());
  }

  /**
   * Recursion to check the conversion of nested field.
   *
   * @param ppdSafeConversion boolean array to specify which column are safe.
   * @param children reader schema children.
   *
   * @return boolean array to represent list of column safe or not.
   */
  private boolean[] populatePpdSafeConversionForChildren(
                        boolean[] ppdSafeConversion,
                        List<TypeDescription> children) {
    boolean safePpd;
    if (children != null) {
      for (TypeDescription child : children) {
        TypeDescription fileType = getFileType(child.getId());
        safePpd = validatePPDConversion(fileType, child);
        if (fileType != null) {
          ppdSafeConversion[fileType.getId()] = safePpd;
        }
        populatePpdSafeConversionForChildren(ppdSafeConversion,
            child.getChildren());
      }
    }
    return ppdSafeConversion;
  }

  private boolean validatePPDConversion(final TypeDescription fileType,
      final TypeDescription readerType) {
    if (fileType == null) {
      return false;
    }
    if (fileType.getCategory().isPrimitive()) {
      if (fileType.getCategory().equals(readerType.getCategory())) {
        // for decimals alone do equality check to not mess up with precision change
        return !(fileType.getCategory() == TypeDescription.Category.DECIMAL &&
            !fileType.equals(readerType));
      }

      // only integer and string evolutions are safe
      // byte -> short -> int -> long
      // string <-> char <-> varchar
      // NOTE: Float to double evolution is not safe as floats are stored as doubles in ORC's
      // internal index, but when doing predicate evaluation for queries like "select * from
      // orc_float where f = 74.72" the constant on the filter is converted from string -> double
      // so the precisions will be different and the comparison will fail.
      // Soon, we should convert all sargs that compare equality between floats or
      // doubles to range predicates.

      // Similarly string -> char and varchar -> char and vice versa is not possible, as ORC stores
      // char with padded spaces in its internal index.
      switch (fileType.getCategory()) {
        case BYTE:
          if (readerType.getCategory().equals(TypeDescription.Category.SHORT) ||
              readerType.getCategory().equals(TypeDescription.Category.INT) ||
              readerType.getCategory().equals(TypeDescription.Category.LONG)) {
            return true;
          }
          break;
        case SHORT:
          if (readerType.getCategory().equals(TypeDescription.Category.INT) ||
              readerType.getCategory().equals(TypeDescription.Category.LONG)) {
            return true;
          }
          break;
        case INT:
          if (readerType.getCategory().equals(TypeDescription.Category.LONG)) {
            return true;
          }
          break;
        case STRING:
          if (readerType.getCategory().equals(TypeDescription.Category.VARCHAR)) {
            return true;
          }
          break;
        case VARCHAR:
          if (readerType.getCategory().equals(TypeDescription.Category.STRING)) {
            return true;
          }
          break;
        default:
          break;
      }
    }
    return false;
  }

  /**
   * Should we read the given reader column?
   * @param readerId the id of column in the extended reader schema
   * @return true if the column should be read
   */
  public boolean includeReaderColumn(int readerId) {
    if(readerId == 0) {
      //always want top level struct - everything is its child
      return true;
    }
    if(isAcid) {
      if(readerId < readerColumnOffset) {
        return includeAcidColumns;
      }
      return readerIncluded == null ||
          readerIncluded[readerId - readerColumnOffset];
    }
    return readerIncluded == null || readerIncluded[readerId];
  }

  /**
   * Build the mapping from the file type to the reader type. For pre-HIVE-4243
   * ORC files, the top level structure is matched using position within the
   * row. Otherwise, structs fields are matched by name.
   * @param fileType the type in the file
   * @param readerType the type in the reader
   * @param positionalLevels the number of structure levels that must be
   *                         mapped by position rather than field name. Pre
   *                         HIVE-4243 files have either 1 or 2 levels matched
   *                         positionally depending on whether they are ACID.
   */
  void buildConversion(TypeDescription fileType,
                       TypeDescription readerType,
                       int positionalLevels) {
    // if the column isn't included, don't map it
    if (!includeReaderColumn(readerType.getId())) {
      return;
    }
    boolean isOk = true;
    // check the easy case first
    if (fileType.getCategory() == readerType.getCategory()) {
      switch (readerType.getCategory()) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DOUBLE:
        case FLOAT:
        case STRING:
        case TIMESTAMP:
        case TIMESTAMP_INSTANT:
        case BINARY:
        case DATE:
          // these are always a match
          break;
        case CHAR:
        case VARCHAR:
          // We do conversion when same CHAR/VARCHAR type but different
          // maxLength.
          if (fileType.getMaxLength() != readerType.getMaxLength()) {
            hasConversion = true;
            if (!typesAreImplicitConversion(fileType, readerType)) {
              isOnlyImplicitConversion = false;
            }
          }
          break;
        case DECIMAL:
          // We do conversion when same DECIMAL type but different
          // precision/scale.
          if (fileType.getPrecision() != readerType.getPrecision() ||
              fileType.getScale() != readerType.getScale()) {
            hasConversion = true;
            isOnlyImplicitConversion = false;
          }
          break;
        case UNION:
        case MAP:
        case LIST: {
          // these must be an exact match
          List<TypeDescription> fileChildren = fileType.getChildren();
          List<TypeDescription> readerChildren = readerType.getChildren();
          if (fileChildren.size() == readerChildren.size()) {
            for(int i=0; i < fileChildren.size(); ++i) {
              buildConversion(fileChildren.get(i),
                              readerChildren.get(i), positionalLevels - 1);
            }
          } else {
            isOk = false;
          }
          break;
        }
        case STRUCT: {
          List<TypeDescription> readerChildren = readerType.getChildren();
          List<TypeDescription> fileChildren = fileType.getChildren();
          if (fileChildren.size() != readerChildren.size()) {
            hasConversion = true;
            // UNDONE: Does LLAP detect fewer columns and NULL them out????
            isOnlyImplicitConversion = false;
          }

          if (positionalLevels <= 0) {
            List<String> readerFieldNames = readerType.getFieldNames();
            List<String> fileFieldNames = fileType.getFieldNames();

            final Map<String, TypeDescription> fileTypesIdx;
            if (isSchemaEvolutionCaseAware) {
              fileTypesIdx = new HashMap<>();
            } else {
              fileTypesIdx = new CaseInsensitiveMap<TypeDescription>();
            }
            for (int i = 0; i < fileFieldNames.size(); i++) {
              final String fileFieldName = fileFieldNames.get(i);
              fileTypesIdx.put(fileFieldName, fileChildren.get(i));
            }

            for (int i = 0; i < readerFieldNames.size(); i++) {
              final String readerFieldName = readerFieldNames.get(i);
              TypeDescription readerField = readerChildren.get(i);

              TypeDescription fileField = fileTypesIdx.get(readerFieldName);
              if (fileField == null) {
                continue;
              }

              buildConversion(fileField, readerField, 0);
            }
          } else {
            int jointSize = Math.min(fileChildren.size(),
                                     readerChildren.size());
            for (int i = 0; i < jointSize; ++i) {
              buildConversion(fileChildren.get(i), readerChildren.get(i),
                  positionalLevels - 1);
            }
          }
          break;
        }
        default:
          throw new IllegalArgumentException("Unknown type " + readerType);
      }
    } else {
      /*
       * Check for the few cases where will not convert....
       */

      isOk = ConvertTreeReaderFactory.canConvert(fileType, readerType);
      hasConversion = true;
      if (!typesAreImplicitConversion(fileType, readerType)) {
        isOnlyImplicitConversion = false;
      }
    }
    if (isOk) {
      readerFileTypes[readerType.getId()] = fileType;
      fileIncluded[fileType.getId()] = true;
    } else {
      throw new IllegalEvolutionException(
          String.format("ORC does not support type conversion from file" +
                        " type %s (%d) to reader type %s (%d)",
                        fileType, fileType.getId(),
                        readerType, readerType.getId()));
    }
  }

  public static boolean checkAcidSchema(TypeDescription type) {
    if (type.getCategory().equals(TypeDescription.Category.STRUCT)) {
      List<String> rootFields = type.getFieldNames();
      if (rootFields.size() != acidEventFieldNames.size()) {
        return false;
      }
      for (int i = 0; i < rootFields.size(); i++) {
        if (!acidEventFieldNames.get(i).equalsIgnoreCase(rootFields.get(i))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * @param typeDescr
   * @return ORC types for the ACID event based on the row's type description
   */
  public static TypeDescription createEventSchema(TypeDescription typeDescr) {
    TypeDescription result = TypeDescription.createStruct()
        .addField("operation", TypeDescription.createInt())
        .addField("originalTransaction", TypeDescription.createLong())
        .addField("bucket", TypeDescription.createInt())
        .addField("rowId", TypeDescription.createLong())
        .addField("currentTransaction", TypeDescription.createLong())
        .addField("row", typeDescr.clone());
    return result;
  }

  /**
   * Get the underlying base row from an ACID event struct.
   * @param typeDescription the ACID event schema.
   * @return the subtype for the real row
   */
  public static TypeDescription getBaseRow(TypeDescription typeDescription) {
    final int ACID_ROW_OFFSET = 5;
    return typeDescription.getChildren().get(ACID_ROW_OFFSET);
  }

  private static final List<String> acidEventFieldNames=
      new ArrayList<String>();

  static {
    acidEventFieldNames.add("operation");
    acidEventFieldNames.add("originalTransaction");
    acidEventFieldNames.add("bucket");
    acidEventFieldNames.add("rowId");
    acidEventFieldNames.add("currentTransaction");
    acidEventFieldNames.add("row");
  }

  private static class CaseInsensitiveMap<V> extends HashMap<String,V> {
    @Override
    public V put(String key, V value) {
      return super.put(key.toLowerCase(), value);
    }

    @Override
    public V get(Object key) {
      return this.get((String) key);
    }

    // not @Override as key to be of type Object
    public V get(String key) {
      return super.get(key.toLowerCase());
    }
  }
}
