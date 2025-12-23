/**
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

#include "SchemaEvolution.hh"
#include "orc/Exceptions.hh"

namespace orc {

  SchemaEvolution::SchemaEvolution(const std::shared_ptr<Type>& _readType, const Type* fileType)
      : readType(_readType) {
    if (readType) {
      buildConversion(readType.get(), fileType);
    } else {
      for (uint64_t i = 0; i <= fileType->getMaximumColumnId(); ++i) {
        safePPDConversionMap.insert(i);
      }
    }
  }

  const Type* SchemaEvolution::getReadType(const Type& fileType) const {
    auto ret = readTypeMap.find(fileType.getColumnId());
    return ret == readTypeMap.cend() ? &fileType : ret->second;
  }

  inline void invalidConversion(const Type* readType, const Type* fileType) {
    throw SchemaEvolutionError("Cannot convert from " + fileType->toString() + " to " +
                               readType->toString());
  }

  struct EnumClassHash {
    template <typename T>
    std::size_t operator()(T t) const {
      return static_cast<std::size_t>(t);
    }
  };

  // map from file type to read type. it does not contain identity mapping.
  using TypeSet = std::unordered_set<TypeKind, EnumClassHash>;
  using ConvertMap = std::unordered_map<TypeKind, TypeSet, EnumClassHash>;

  inline bool supportConversion(const Type& readType, const Type& fileType) {
    static const ConvertMap& SUPPORTED_CONVERSIONS = *new ConvertMap{
        // support nothing now
    };
    auto iter = SUPPORTED_CONVERSIONS.find(fileType.getKind());
    if (iter == SUPPORTED_CONVERSIONS.cend()) {
      return false;
    }
    return iter->second.find(readType.getKind()) != iter->second.cend();
  }

  struct ConversionCheckResult {
    bool isValid;
    bool needConvert;
  };

  ConversionCheckResult checkConversion(const Type& readType, const Type& fileType) {
    ConversionCheckResult ret = {false, false};
    if (readType.getKind() == fileType.getKind()) {
      ret.isValid = true;
      if (fileType.getKind() == CHAR || fileType.getKind() == VARCHAR) {
        ret.needConvert = readType.getMaximumLength() < fileType.getMaximumLength();
      } else if (fileType.getKind() == DECIMAL) {
        ret.needConvert = readType.getPrecision() != fileType.getPrecision() ||
                          readType.getScale() != fileType.getScale();
      }
    } else {
      switch (fileType.getKind()) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case DECIMAL: {
          ret.isValid = ret.needConvert =
              (readType.getKind() != DATE && readType.getKind() != BINARY);
          break;
        }
        case STRING: {
          ret.isValid = ret.needConvert = true;
          break;
        }
        case CHAR:
        case VARCHAR: {
          ret.isValid = true;
          if (readType.getKind() == STRING) {
            ret.needConvert = false;
          } else if (readType.getKind() == CHAR || readType.getKind() == VARCHAR) {
            ret.needConvert = readType.getMaximumLength() < fileType.getMaximumLength();
          } else {
            ret.needConvert = true;
          }
          break;
        }
        case TIMESTAMP:
        case TIMESTAMP_INSTANT: {
          if (readType.getKind() == TIMESTAMP || readType.getKind() == TIMESTAMP_INSTANT) {
            ret = {true, false};
          } else {
            ret.isValid = ret.needConvert = (readType.getKind() != BINARY);
          }
          break;
        }
        case DATE: {
          ret.isValid = ret.needConvert =
              readType.getKind() == STRING || readType.getKind() == CHAR ||
              readType.getKind() == VARCHAR || readType.getKind() == TIMESTAMP ||
              readType.getKind() == TIMESTAMP_INSTANT;
          break;
        }
        case BINARY: {
          ret.isValid = ret.needConvert = readType.getKind() == STRING ||
                                          readType.getKind() == CHAR ||
                                          readType.getKind() == VARCHAR;
          break;
        }
        case STRUCT:
        case LIST:
        case MAP:
        case UNION: {
          ret.isValid = ret.needConvert = false;
          break;
        }
        default:
          break;
      }
    }
    return ret;
  }

  void SchemaEvolution::buildConversion(const Type* _readType, const Type* fileType) {
    if (fileType == nullptr) {
      throw SchemaEvolutionError("File does not have " + _readType->toString());
    }

    auto [valid, convert] = checkConversion(*_readType, *fileType);
    if (!valid) {
      invalidConversion(_readType, fileType);
    }
    readTypeMap.emplace(_readType->getColumnId(), convert ? _readType : fileType);

    // check whether PPD conversion is safe
    buildSafePPDConversionMap(_readType, fileType);

    for (uint64_t i = 0; i < _readType->getSubtypeCount(); ++i) {
      auto subType = _readType->getSubtype(i);
      if (subType) {
        // null subType means that this is a sub column of map/list type
        // and it does not exist in the file. simply skip it.
        buildConversion(subType, fileType->getTypeByColumnId(subType->getColumnId()));
      }
    }
  }

  bool SchemaEvolution::needConvert(const Type& fileType) const {
    auto _readType = getReadType(fileType);
    if (_readType == &fileType) {
      return false;
    }
    // it does not check valid here as verified by buildConversion()
    return checkConversion(*_readType, fileType).needConvert;
  }

  inline bool isPrimitive(const Type* type) {
    auto kind = type->getKind();
    return kind != STRUCT && kind != MAP && kind != LIST && kind != UNION;
  }

  void SchemaEvolution::buildSafePPDConversionMap(const Type* _readType, const Type* fileType) {
    if (_readType == nullptr || !isPrimitive(_readType) || fileType == nullptr ||
        !isPrimitive(fileType)) {
      return;
    }

    bool isSafe = false;
    if (_readType == fileType) {
      // short cut for same type
      isSafe = true;
    } else if (_readType->getKind() == DECIMAL && fileType->getKind() == DECIMAL) {
      // for decimals alone do equality check to not mess up with precision change
      if (fileType->getPrecision() == readType->getPrecision() &&
          fileType->getScale() == readType->getScale()) {
        isSafe = true;
      }
    } else {
      // only integer and string evolutions are safe
      // byte -> short -> int -> long
      // string <-> char <-> varchar
      // NOTE: Float to double evolution is not safe as floats are stored as
      // doubles in ORC's internal index, but when doing predicate evaluation
      // for queries like "select * from orc_float where f = 74.72" the constant
      // on the filter is converted from string -> double so the precisions will
      // be different and the comparison will fail.
      // Soon, we should convert all sargs that compare equality between floats
      // or doubles to range predicates.
      // Similarly string -> char and varchar -> char and vice versa is impossible
      // as ORC stores char with padded spaces in its internal index.
      switch (fileType->getKind()) {
        case BYTE: {
          if (readType->getKind() == SHORT || readType->getKind() == INT ||
              readType->getKind() == LONG) {
            isSafe = true;
          }
          break;
        }
        case SHORT: {
          if (readType->getKind() == INT || readType->getKind() == LONG) {
            isSafe = true;
          }
          break;
        }
        case INT: {
          if (readType->getKind() == LONG) {
            isSafe = true;
          }
          break;
        }
        case STRING: {
          if (readType->getKind() == VARCHAR) {
            isSafe = true;
          }
          break;
        }
        case VARCHAR: {
          if (readType->getKind() == STRING) {
            isSafe = true;
          }
          break;
        }
        case BOOLEAN:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BINARY:
        case TIMESTAMP:
        case LIST:
        case MAP:
        case STRUCT:
        case UNION:
        case DECIMAL:
        case DATE:
        case CHAR:
        case TIMESTAMP_INSTANT:
          break;
      }
    }

    if (isSafe) {
      safePPDConversionMap.insert(fileType->getColumnId());
    }
  }

  bool SchemaEvolution::isSafePPDConversion(uint64_t columnId) const {
    return safePPDConversionMap.find(columnId) != safePPDConversionMap.cend();
  }

}  // namespace orc
