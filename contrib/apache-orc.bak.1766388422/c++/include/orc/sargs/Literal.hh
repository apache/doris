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

#ifndef ORC_LITERAL_HH
#define ORC_LITERAL_HH

#include "orc/Int128.hh"
#include "orc/Vector.hh"

namespace orc {

  /**
   * Possible data types for predicates
   */
  enum class PredicateDataType { LONG = 0, FLOAT, STRING, DATE, DECIMAL, TIMESTAMP, BOOLEAN };

  /**
   * Represents a literal value in a predicate
   */
  class Literal {
   public:
    struct Timestamp {
      Timestamp() = default;
      Timestamp(const Timestamp&) = default;
      Timestamp(Timestamp&&) = default;
      ~Timestamp() = default;
      Timestamp(int64_t second_, int32_t nanos_) : second(second_), nanos(nanos_) {
        // PASS
      }
      Timestamp& operator=(const Timestamp&) = default;
      Timestamp& operator=(Timestamp&&) = default;
      bool operator==(const Timestamp& r) const {
        return second == r.second && nanos == r.nanos;
      }
      bool operator<(const Timestamp& r) const {
        return second < r.second || (second == r.second && nanos < r.nanos);
      }
      bool operator<=(const Timestamp& r) const {
        return second < r.second || (second == r.second && nanos <= r.nanos);
      }
      bool operator!=(const Timestamp& r) const {
        return !(*this == r);
      }
      bool operator>(const Timestamp& r) const {
        return r < *this;
      }
      bool operator>=(const Timestamp& r) const {
        return r <= *this;
      }
      int64_t getMillis() const {
        return second * 1000 + nanos / 1000000;
      }
      int64_t second;
      int32_t nanos;
    };

    Literal(const Literal& r);
    ~Literal();
    Literal& operator=(const Literal& r);
    bool operator==(const Literal& r) const;
    bool operator!=(const Literal& r) const;

    /**
     * Create a literal of null value for a specific type
     */
    Literal(PredicateDataType type);

    /**
     * Create a literal of LONG type
     */
    Literal(int64_t val);

    /**
     * Create a literal of FLOAT type
     */
    Literal(double val);

    /**
     * Create a literal of BOOLEAN type
     */
    Literal(bool val);

    /**
     * Create a literal of DATE type
     */
    Literal(PredicateDataType type, int64_t val);

    /**
     * Create a literal of TIMESTAMP type
     */
    Literal(int64_t second, int32_t nanos);

    /**
     * Create a literal of STRING type
     */
    Literal(const char* str, size_t size);

    /**
     * Create a literal of DECIMAL type
     */
    Literal(Int128 val, int32_t precision, int32_t scale);

    /**
     * Getters of a specific data type for not-null literals
     */
    int64_t getLong() const;
    int64_t getDate() const;
    Timestamp getTimestamp() const;
    double getFloat() const;
    std::string getString() const;
    bool getBool() const;
    Decimal getDecimal() const;

    /**
     * Check if a literal is null
     */
    bool isNull() const {
      return mIsNull;
    }

    PredicateDataType getType() const {
      return mType;
    }
    std::string toString() const;
    size_t getHashCode() const {
      return mHashCode;
    }

   private:
    size_t hashCode() const;

    union LiteralVal {
      int64_t IntVal;
      double DoubleVal;
      int64_t DateVal;
      char* Buffer;
      Timestamp TimeStampVal;
      Int128 DecimalVal;
      bool BooleanVal;

      // explicitly define default constructor
      LiteralVal() : DecimalVal(0) {}
    };

   private:
    LiteralVal mValue;        // data value for this literal if not null
    PredicateDataType mType;  // data type of the literal
    size_t mSize;             // size of mValue if it is Buffer
    int32_t mPrecision;       // precision of decimal type
    int32_t mScale;           // scale of decimal type
    bool mIsNull;             // whether this literal is null
    size_t mHashCode;         // precomputed hash code for the literal
  };

}  // namespace orc

#endif  // ORC_LITERAL_HH
