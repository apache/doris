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

#ifndef ORC_SEARCHARGUMENT_HH
#define ORC_SEARCHARGUMENT_HH

#include "orc/sargs/Literal.hh"
#include "orc/sargs/TruthValue.hh"

namespace orc {

  /**
   * Primary interface for a search argument, which are the subset of predicates
   * that can be pushed down to the RowReader. Each SearchArgument consists
   * of a series of search clauses that must each be true for the row to be
   * accepted by the filter.
   *
   * This requires that the filter be normalized into conjunctive normal form
   * (<a href="http://en.wikipedia.org/wiki/Conjunctive_normal_form">CNF</a>).
   */
  class SearchArgument {
   public:
    virtual ~SearchArgument();

    /**
     * Evaluate the entire predicate based on the values for the leaf predicates.
     * @param leaves the value of each leaf predicate
     * @return the value of hte entire predicate
     */
    virtual TruthValue evaluate(const std::vector<TruthValue>& leaves) const = 0;

    virtual std::string toString() const = 0;
  };

  /**
   * A builder object to create a SearchArgument from expressions. The user
   * must call startOr, startAnd, or startNot before adding any leaves.
   */
  class SearchArgumentBuilder {
   public:
    virtual ~SearchArgumentBuilder();

    /**
     * Start building an or operation and push it on the stack.
     * @return this
     */
    virtual SearchArgumentBuilder& startOr() = 0;

    /**
     * Start building an and operation and push it on the stack.
     * @return this
     */
    virtual SearchArgumentBuilder& startAnd() = 0;

    /**
     * Start building a not operation and push it on the stack.
     * @return this
     */
    virtual SearchArgumentBuilder& startNot() = 0;

    /**
     * Finish the current operation and pop it off of the stack. Each start
     * call must have a matching end.
     * @return this
     */
    virtual SearchArgumentBuilder& end() = 0;

    /**
     * Add a less than leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    virtual SearchArgumentBuilder& lessThan(const std::string& column, PredicateDataType type,
                                            Literal literal) = 0;

    /**
     * Add a less than leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    virtual SearchArgumentBuilder& lessThan(uint64_t columnId, PredicateDataType type,
                                            Literal literal) = 0;

    /**
     * Add a less than equals leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    virtual SearchArgumentBuilder& lessThanEquals(const std::string& column, PredicateDataType type,
                                                  Literal literal) = 0;

    /**
     * Add a less than equals leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    virtual SearchArgumentBuilder& lessThanEquals(uint64_t columnId, PredicateDataType type,
                                                  Literal literal) = 0;

    /**
     * Add an equals leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    virtual SearchArgumentBuilder& equals(const std::string& column, PredicateDataType type,
                                          Literal literal) = 0;

    /**
     * Add an equals leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    virtual SearchArgumentBuilder& equals(uint64_t columnId, PredicateDataType type,
                                          Literal literal) = 0;

    /**
     * Add a null safe equals leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    virtual SearchArgumentBuilder& nullSafeEquals(const std::string& column, PredicateDataType type,
                                                  Literal literal) = 0;

    /**
     * Add a null safe equals leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    virtual SearchArgumentBuilder& nullSafeEquals(uint64_t columnId, PredicateDataType type,
                                                  Literal literal) = 0;

    /**
     * Add an in leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param literals the literals
     * @return this
     */
    virtual SearchArgumentBuilder& in(const std::string& column, PredicateDataType type,
                                      const std::initializer_list<Literal>& literals) = 0;

    /**
     * Add an in leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param literals the literals
     * @return this
     */
    virtual SearchArgumentBuilder& in(uint64_t columnId, PredicateDataType type,
                                      const std::initializer_list<Literal>& literals) = 0;

    /**
     * Add an in leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param literals the literals
     * @return this
     */
    virtual SearchArgumentBuilder& in(const std::string& column, PredicateDataType type,
                                      const std::vector<Literal>& literals) = 0;

    /**
     * Add an in leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param literals the literals
     * @return this
     */
    virtual SearchArgumentBuilder& in(uint64_t columnId, PredicateDataType type,
                                      const std::vector<Literal>& literals) = 0;

    /**
     * Add an is null leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @return this
     */
    virtual SearchArgumentBuilder& isNull(const std::string& column, PredicateDataType type) = 0;

    /**
     * Add an is null leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @return this
     */
    virtual SearchArgumentBuilder& isNull(uint64_t columnId, PredicateDataType type) = 0;

    /**
     * Add a between leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param lower the literal
     * @param upper the literal
     * @return this
     */
    virtual SearchArgumentBuilder& between(const std::string& column, PredicateDataType type,
                                           Literal lower, Literal upper) = 0;

    /**
     * Add a between leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param lower the literal
     * @param upper the literal
     * @return this
     */
    virtual SearchArgumentBuilder& between(uint64_t columnId, PredicateDataType type, Literal lower,
                                           Literal upper) = 0;

    /**
     * Add a truth value to the expression.
     * @param truth truth value
     * @return this
     */
    virtual SearchArgumentBuilder& literal(TruthValue truth) = 0;

    /**
     * Build and return the SearchArgument that has been defined. All of the
     * starts must have been ended before this call.
     * @return the new SearchArgument
     */
    virtual std::unique_ptr<SearchArgument> build() = 0;
  };

  /**
   * Factory to create SearchArgumentBuilder which builds SearchArgument
   */
  class SearchArgumentFactory {
   public:
    static std::unique_ptr<SearchArgumentBuilder> newBuilder();
  };

}  // namespace orc

#endif  // ORC_SEARCHARGUMENT_HH
