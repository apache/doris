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

#include "orc/sargs/Literal.hh"

#include <cmath>
#include <functional>
#include <limits>
#include <sstream>

namespace orc {

  Literal::Literal(PredicateDataType type) {
    mType = type;
    mValue.DecimalVal = 0;
    mSize = 0;
    mIsNull = true;
    mPrecision = 0;
    mScale = 0;
    mHashCode = 0;
  }

  Literal::Literal(int64_t val) {
    mType = PredicateDataType::LONG;
    mValue.IntVal = val;
    mSize = sizeof(val);
    mIsNull = false;
    mPrecision = 0;
    mScale = 0;
    mHashCode = hashCode();
  }

  Literal::Literal(double val) {
    mType = PredicateDataType::FLOAT;
    mValue.DoubleVal = val;
    mSize = sizeof(val);
    mIsNull = false;
    mPrecision = 0;
    mScale = 0;
    mHashCode = hashCode();
  }

  Literal::Literal(bool val) {
    mType = PredicateDataType::BOOLEAN;
    mValue.BooleanVal = val;
    mSize = sizeof(val);
    mIsNull = false;
    mPrecision = 0;
    mScale = 0;
    mHashCode = hashCode();
  }

  Literal::Literal(PredicateDataType type, int64_t val) {
    if (type != PredicateDataType::DATE) {
      throw std::invalid_argument("only DATE is supported here!");
    }
    mType = type;
    mValue.IntVal = val;
    mSize = sizeof(val);
    mIsNull = false;
    mPrecision = 0;
    mScale = 0;
    mHashCode = hashCode();
  }

  Literal::Literal(const char* str, size_t size) {
    mType = PredicateDataType::STRING;
    mValue.Buffer = new char[size];
    memcpy(mValue.Buffer, str, size);
    mSize = size;
    mIsNull = false;
    mPrecision = 0;
    mScale = 0;
    mHashCode = hashCode();
  }

  Literal::Literal(Int128 val, int32_t precision, int32_t scale) {
    mType = PredicateDataType::DECIMAL;
    mValue.DecimalVal = val;
    mPrecision = precision;
    mScale = scale;
    mSize = sizeof(Int128);
    mIsNull = false;
    mHashCode = hashCode();
  }

  Literal::Literal(int64_t second, int32_t nanos) {
    mType = PredicateDataType::TIMESTAMP;
    mValue.TimeStampVal.second = second;
    mValue.TimeStampVal.nanos = nanos;
    mPrecision = 0;
    mScale = 0;
    mSize = sizeof(Timestamp);
    mIsNull = false;
    mHashCode = hashCode();
  }

  Literal::Literal(const Literal& r)
      : mType(r.mType), mSize(r.mSize), mIsNull(r.mIsNull), mHashCode(r.mHashCode) {
    if (mType == PredicateDataType::STRING) {
      mValue.Buffer = new char[r.mSize];
      memcpy(mValue.Buffer, r.mValue.Buffer, r.mSize);
      mPrecision = 0;
      mScale = 0;
    } else if (mType == PredicateDataType::DECIMAL) {
      mPrecision = r.mPrecision;
      mScale = r.mScale;
      mValue = r.mValue;
    } else if (mType == PredicateDataType::TIMESTAMP) {
      mValue.TimeStampVal = r.mValue.TimeStampVal;
    } else {
      mValue = r.mValue;
      mPrecision = 0;
      mScale = 0;
    }
  }

  Literal::~Literal() {
    if (mType == PredicateDataType::STRING && mValue.Buffer) {
      delete[] mValue.Buffer;
      mValue.Buffer = nullptr;
    }
  }

  Literal& Literal::operator=(const Literal& r) {
    if (this != &r) {
      if (mType == PredicateDataType::STRING && mValue.Buffer) {
        delete[] mValue.Buffer;
        mValue.Buffer = nullptr;
      }

      mType = r.mType;
      mSize = r.mSize;
      mIsNull = r.mIsNull;
      mPrecision = r.mPrecision;
      mScale = r.mScale;
      if (mType == PredicateDataType::STRING) {
        mValue.Buffer = new char[r.mSize];
        memcpy(mValue.Buffer, r.mValue.Buffer, r.mSize);
      } else if (mType == PredicateDataType::TIMESTAMP) {
        mValue.TimeStampVal = r.mValue.TimeStampVal;
      } else {
        mValue = r.mValue;
      }
      mHashCode = r.mHashCode;
    }
    return *this;
  }

  std::string Literal::toString() const {
    if (mIsNull) {
      return "null";
    }

    std::ostringstream sstream;
    switch (mType) {
      case PredicateDataType::LONG:
        sstream << mValue.IntVal;
        break;
      case PredicateDataType::DATE:
        sstream << mValue.DateVal;
        break;
      case PredicateDataType::TIMESTAMP:
        sstream << mValue.TimeStampVal.second << "." << mValue.TimeStampVal.nanos;
        break;
      case PredicateDataType::FLOAT:
        sstream << mValue.DoubleVal;
        break;
      case PredicateDataType::BOOLEAN:
        sstream << (mValue.BooleanVal ? "true" : "false");
        break;
      case PredicateDataType::STRING:
        sstream << std::string(mValue.Buffer, mSize);
        break;
      case PredicateDataType::DECIMAL:
        sstream << mValue.DecimalVal.toDecimalString(mScale);
        break;
    }
    return sstream.str();
  }

  size_t Literal::hashCode() const {
    if (mIsNull) {
      return 0;
    }

    switch (mType) {
      case PredicateDataType::LONG:
        return std::hash<int64_t>{}(mValue.IntVal);
      case PredicateDataType::DATE:
        return std::hash<int64_t>{}(mValue.DateVal);
      case PredicateDataType::TIMESTAMP:
        return std::hash<int64_t>{}(mValue.TimeStampVal.second) * 17 +
               std::hash<int32_t>{}(mValue.TimeStampVal.nanos);
      case PredicateDataType::FLOAT:
        return std::hash<double>{}(mValue.DoubleVal);
      case PredicateDataType::BOOLEAN:
        return std::hash<bool>{}(mValue.BooleanVal);
      case PredicateDataType::STRING:
        return std::hash<std::string>{}(std::string(mValue.Buffer, mSize));
      case PredicateDataType::DECIMAL:
        // current glibc does not support hash<int128_t>
        return std::hash<int64_t>{}(mValue.IntVal);
      default:
        return 0;
    }
  }

  bool Literal::operator==(const Literal& r) const {
    if (this == &r) {
      return true;
    }
    if (mHashCode != r.mHashCode || mType != r.mType || mIsNull != r.mIsNull) {
      return false;
    }

    if (mIsNull) {
      return true;
    }

    switch (mType) {
      case PredicateDataType::LONG:
        return mValue.IntVal == r.mValue.IntVal;
      case PredicateDataType::DATE:
        return mValue.DateVal == r.mValue.DateVal;
      case PredicateDataType::TIMESTAMP:
        return mValue.TimeStampVal == r.mValue.TimeStampVal;
      case PredicateDataType::FLOAT:
        return std::fabs(mValue.DoubleVal - r.mValue.DoubleVal) <
               std::numeric_limits<double>::epsilon();
      case PredicateDataType::BOOLEAN:
        return mValue.BooleanVal == r.mValue.BooleanVal;
      case PredicateDataType::STRING:
        return mSize == r.mSize && memcmp(mValue.Buffer, r.mValue.Buffer, mSize) == 0;
      case PredicateDataType::DECIMAL:
        return mValue.DecimalVal == r.mValue.DecimalVal;
      default:
        return true;
    }
  }

  bool Literal::operator!=(const Literal& r) const {
    return !(*this == r);
  }

  inline void validate(const bool& isNull, const PredicateDataType& type,
                       const PredicateDataType& expected) {
    if (isNull) {
      throw std::logic_error("cannot get value when it is null!");
    }
    if (type != expected) {
      throw std::logic_error("predicate type mismatch");
    }
  }

  int64_t Literal::getLong() const {
    validate(mIsNull, mType, PredicateDataType::LONG);
    return mValue.IntVal;
  }

  int64_t Literal::getDate() const {
    validate(mIsNull, mType, PredicateDataType::DATE);
    return mValue.DateVal;
  }

  Literal::Timestamp Literal::getTimestamp() const {
    validate(mIsNull, mType, PredicateDataType::TIMESTAMP);
    return mValue.TimeStampVal;
  }

  double Literal::getFloat() const {
    validate(mIsNull, mType, PredicateDataType::FLOAT);
    return mValue.DoubleVal;
  }

  std::string Literal::getString() const {
    validate(mIsNull, mType, PredicateDataType::STRING);
    return std::string(mValue.Buffer, mSize);
  }

  bool Literal::getBool() const {
    validate(mIsNull, mType, PredicateDataType::BOOLEAN);
    return mValue.BooleanVal;
  }

  Decimal Literal::getDecimal() const {
    validate(mIsNull, mType, PredicateDataType::DECIMAL);
    return Decimal(mValue.DecimalVal, mScale);
  }

}  // namespace orc
