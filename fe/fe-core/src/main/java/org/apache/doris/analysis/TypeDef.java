// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;

/**
 * Represents an anonymous type definition, e.g., used in DDL and CASTs.
 */
public class TypeDef implements ParseNode {
  private boolean isAnalyzed;
  private final Type parsedType;

  public TypeDef(Type parsedType) {
    this.parsedType = parsedType;
  }

  public static TypeDef create(PrimitiveType type) {
    return new TypeDef(ScalarType.createType(type));
  }
  public static TypeDef createDecimal(int precision, int scale) {
    return new TypeDef(ScalarType.createDecimalV2Type(precision, scale));
  }
  public static TypeDef createVarchar(int len) {
    return new TypeDef(ScalarType.createVarchar(len));
  }
  public static TypeDef createChar(int len) {
    return new TypeDef(ScalarType.createChar(len));
  }
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed) {
      return;
    }
    // Check the max nesting depth before calling the recursive analyze() to avoid
    // a stack overflow.
    if (parsedType.exceedsMaxNestingDepth()) {
      throw new AnalysisException(String.format(
          "Type exceeds the maximum nesting depth of %s:\n%s",
          Type.MAX_NESTING_DEPTH, parsedType.toSql()));
    }
    analyze(parsedType);
    isAnalyzed = true;
  }

  private void analyze(Type type) throws AnalysisException {
    if (!type.isSupported()) {
      throw new AnalysisException("Unsupported data type: " + type.toSql());
    }
    if (type.isScalarType()) {
      analyzeScalarType((ScalarType) type);
    }
  }

  private void analyzeScalarType(ScalarType scalarType)
      throws AnalysisException {
    PrimitiveType type = scalarType.getPrimitiveType();
    switch (type) {
      case CHAR:
      case VARCHAR: {
        String name;
        int maxLen;
        if (type == PrimitiveType.VARCHAR) {
          name = "Varchar";
          maxLen = ScalarType.MAX_VARCHAR_LENGTH;
        } else if (type == PrimitiveType.CHAR) {
          name = "Char";
          maxLen = ScalarType.MAX_CHAR_LENGTH;
        } else {
          Preconditions.checkState(false);
          return;
        }
        int len = scalarType.getLength();
        // len is decided by child, when it is -1.

        if (len <= 0) {
          throw new AnalysisException(name + " size must be > 0: " + len);
        }
        if (scalarType.getLength() > maxLen) {
          throw new AnalysisException(
              name + " size must be <= " + maxLen + ": " + len);
        }
        break;
      }
      case DECIMALV2: {
        int precision = scalarType.decimalPrecision();
        int scale = scalarType.decimalScale();
        // precision: [1, 27]
        if (precision < 1 || precision > 27) {
          throw new AnalysisException("Precision of decimal must between 1 and 27."
                  + " Precision was set to: " + precision + ".");
        }
        // scale: [0, 9]
        if (scale < 0 || scale > 9) {
          throw new AnalysisException("Scale of decimal must between 0 and 9."
                  + " Scale was set to: " + scale + ".");
        }
        // scale < precision
        if (scale >= precision) {
          throw new AnalysisException("Scale of decimal must be smaller than precision."
                  + " Scale is " + scale + " and precision is " + precision);
        }
        break;
      }
      case INVALID_TYPE:
        throw new AnalysisException("Invalid type.");
      default: break;
    }
  }

  public Type getType() {
    return parsedType;
  }

  @Override
  public String toString() {
    return parsedType.toSql();
  }

  @Override
  public String toSql() {
    return parsedType.toSql();
  }
}
