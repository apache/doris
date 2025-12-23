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

#ifndef ORC_PREDICATELEAF_HH
#define ORC_PREDICATELEAF_HH

#include "orc/Common.hh"
#include "orc/sargs/Literal.hh"
#include "orc/sargs/TruthValue.hh"
#include "wrap/orc-proto-wrapper.hh"

#include <string>
#include <vector>

namespace orc {

  static constexpr uint64_t INVALID_COLUMN_ID = std::numeric_limits<uint64_t>::max();

  class BloomFilter;

  /**
   * The primitive predicates that form a SearchArgument.
   */
  class PredicateLeaf {
   public:
    /**
     * The possible operators for predicates. To get the opposites, construct
     * an expression with a not operator.
     */
    enum class Operator {
      EQUALS = 0,
      NULL_SAFE_EQUALS,
      LESS_THAN,
      LESS_THAN_EQUALS,
      IN,
      BETWEEN,
      IS_NULL
    };

    // The possible types for sargs.
    enum class Type {
      LONG = 0,  // all of the integer types
      FLOAT,     // float and double
      STRING,    // string, char, varchar
      DATE,
      DECIMAL,
      TIMESTAMP,
      BOOLEAN
    };

    PredicateLeaf() = default;

    PredicateLeaf(Operator op, PredicateDataType type, const std::string& colName, Literal literal);

    PredicateLeaf(Operator op, PredicateDataType type, uint64_t columnId, Literal literal);

    PredicateLeaf(Operator op, PredicateDataType type, const std::string& colName,
                  const std::initializer_list<Literal>& literalList);

    PredicateLeaf(Operator op, PredicateDataType type, uint64_t columnId,
                  const std::initializer_list<Literal>& literalList);

    PredicateLeaf(Operator op, PredicateDataType type, const std::string& colName,
                  const std::vector<Literal>& literalList);

    PredicateLeaf(Operator op, PredicateDataType type, uint64_t columnId,
                  const std::vector<Literal>& literalList);

    /**
     * Get the operator for the leaf.
     */
    Operator getOperator() const;

    /**
     * Get the type of the column and literal by the file format.
     */
    PredicateDataType getType() const;

    /**
     * Get whether the predicate is created using column name.
     */
    bool hasColumnName() const;

    /**
     * Get the simple column name.
     */
    const std::string& getColumnName() const;

    /**
     * Get the column id.
     */
    uint64_t getColumnId() const;

    /**
     * Get the literal half of the predicate leaf.
     */
    Literal getLiteral() const;

    /**
     * For operators with multiple literals (IN and BETWEEN), get the literals.
     */
    const std::vector<Literal>& getLiteralList() const;

    /**
     * Evaluate current PredicateLeaf based on ColumnStatistics and BloomFilter
     */
    TruthValue evaluate(const WriterVersion writerVersion, const proto::ColumnStatistics& colStats,
                        const BloomFilter* bloomFilter) const;

    std::string toString() const;

    bool operator==(const PredicateLeaf& r) const;

    size_t getHashCode() const {
      return mHashCode;
    }

   private:
    size_t hashCode() const;

    void validate() const;
    void validateColumn() const;

    std::string columnDebugString() const;

    TruthValue evaluatePredicateMinMax(const proto::ColumnStatistics& colStats) const;

    TruthValue evaluatePredicateBloomFiter(const BloomFilter* bloomFilter, bool hasNull) const;

   private:
    Operator mOperator;
    PredicateDataType mType;
    std::string mColumnName;
    bool mHasColumnName;
    uint64_t mColumnId;
    std::vector<Literal> mLiterals;
    size_t mHashCode;
  };

  struct PredicateLeafHash {
    size_t operator()(const PredicateLeaf& leaf) const {
      return leaf.getHashCode();
    }
  };

  struct PredicateLeafComparator {
    bool operator()(const PredicateLeaf& lhs, const PredicateLeaf& rhs) const {
      return lhs == rhs;
    }
  };

}  // namespace orc

#endif  // ORC_PREDICATELEAF_HH
