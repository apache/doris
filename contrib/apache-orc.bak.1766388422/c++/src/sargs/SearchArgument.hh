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

#ifndef ORC_SRC_SEARCHARGUMENT_HH
#define ORC_SRC_SEARCHARGUMENT_HH

#include "ExpressionTree.hh"
#include "orc/sargs/SearchArgument.hh"
#include "sargs/PredicateLeaf.hh"
#include "wrap/orc-proto-wrapper.hh"

#include <deque>
#include <stdexcept>
#include <unordered_map>

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
  class SearchArgumentImpl : public SearchArgument {
   public:
    SearchArgumentImpl(TreeNode root, const std::vector<PredicateLeaf>& leaves);

    /**
     * Get the leaf predicates that are required to evaluate the predicate. The
     * list will have the duplicates removed.
     * @return the list of leaf predicates
     */
    const std::vector<PredicateLeaf>& getLeaves() const;

    /**
     * Get the expression tree. This should only needed for file formats that
     * need to translate the expression to an internal form.
     */
    const ExpressionTree* getExpression() const;

    /**
     * Evaluate the entire predicate based on the values for the leaf predicates.
     * @param leaves the value of each leaf predicate
     * @return the value of the entire predicate
     */
    TruthValue evaluate(const std::vector<TruthValue>& leaves) const override;

    std::string toString() const override;

   private:
    std::shared_ptr<ExpressionTree> mExpressionTree;
    std::vector<PredicateLeaf> mLeaves;
  };

  /**
   * A builder object to create a SearchArgument from expressions. The user
   * must call startOr, startAnd, or startNot before adding any leaves.
   */
  class SearchArgumentBuilderImpl : public SearchArgumentBuilder {
   public:
    SearchArgumentBuilderImpl();

    /**
     * Start building an or operation and push it on the stack.
     * @return this
     */
    SearchArgumentBuilder& startOr() override;

    /**
     * Start building an and operation and push it on the stack.
     * @return this
     */
    SearchArgumentBuilder& startAnd() override;

    /**
     * Start building a not operation and push it on the stack.
     * @return this
     */
    SearchArgumentBuilder& startNot() override;

    /**
     * Finish the current operation and pop it off of the stack. Each start
     * call must have a matching end.
     * @return this
     */
    SearchArgumentBuilder& end() override;

    /**
     * Add a less than leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    SearchArgumentBuilder& lessThan(const std::string& column, PredicateDataType type,
                                    Literal literal) override;

    /**
     * Add a less than leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    SearchArgumentBuilder& lessThan(uint64_t columnId, PredicateDataType type,
                                    Literal literal) override;

    /**
     * Add a less than equals leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    SearchArgumentBuilder& lessThanEquals(const std::string& column, PredicateDataType type,
                                          Literal literal) override;

    /**
     * Add a less than equals leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    SearchArgumentBuilder& lessThanEquals(uint64_t columnId, PredicateDataType type,
                                          Literal literal) override;

    /**
     * Add an equals leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    SearchArgumentBuilder& equals(const std::string& column, PredicateDataType type,
                                  Literal literal) override;

    /**
     * Add an equals leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    SearchArgumentBuilder& equals(uint64_t columnId, PredicateDataType type,
                                  Literal literal) override;

    /**
     * Add a null safe equals leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    SearchArgumentBuilder& nullSafeEquals(const std::string& column, PredicateDataType type,
                                          Literal literal) override;

    /**
     * Add a null safe equals leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param literal the literal
     * @return this
     */
    SearchArgumentBuilder& nullSafeEquals(uint64_t columnId, PredicateDataType type,
                                          Literal literal) override;

    /**
     * Add an in leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param literals the literals
     * @return this
     */
    SearchArgumentBuilder& in(const std::string& column, PredicateDataType type,
                              const std::initializer_list<Literal>& literals) override;

    /**
     * Add an in leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param literals the literals
     * @return this
     */
    SearchArgumentBuilder& in(uint64_t columnId, PredicateDataType type,
                              const std::initializer_list<Literal>& literals) override;

    /**
     * Add an in leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param literals the literals
     * @return this
     */
    SearchArgumentBuilder& in(const std::string& column, PredicateDataType type,
                              const std::vector<Literal>& literals) override;

    /**
     * Add an in leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param literals the literals
     * @return this
     */
    SearchArgumentBuilder& in(uint64_t columnId, PredicateDataType type,
                              const std::vector<Literal>& literals) override;

    /**
     * Add an is null leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @return this
     */
    SearchArgumentBuilder& isNull(const std::string& column, PredicateDataType type) override;

    /**
     * Add an is null leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @return this
     */
    SearchArgumentBuilder& isNull(uint64_t columnId, PredicateDataType type) override;

    /**
     * Add a between leaf to the current item on the stack.
     * @param column the field name of the column
     * @param type the type of the expression
     * @param lower the literal
     * @param upper the literal
     * @return this
     */
    SearchArgumentBuilder& between(const std::string& column, PredicateDataType type, Literal lower,
                                   Literal upper) override;

    /**
     * Add a between leaf to the current item on the stack.
     * @param columnId the column id of the column
     * @param type the type of the expression
     * @param lower the literal
     * @param upper the literal
     * @return this
     */
    SearchArgumentBuilder& between(uint64_t columnId, PredicateDataType type, Literal lower,
                                   Literal upper) override;

    /**
     * Add a truth value to the expression.
     * @param truth truth value
     * @return this
     */
    SearchArgumentBuilder& literal(TruthValue truth) override;

    /**
     * Build and return the SearchArgument that has been defined. All of the
     * starts must have been ended before this call.
     * @return the new SearchArgument
     */
    std::unique_ptr<SearchArgument> build() override;

   private:
    SearchArgumentBuilder& start(ExpressionTree::Operator op);
    size_t addLeaf(PredicateLeaf leaf);

    static bool isInvalidColumn(const std::string& column);
    static bool isInvalidColumn(uint64_t columnId);

    template <typename T>
    SearchArgumentBuilder& compareOperator(PredicateLeaf::Operator op, T column,
                                           PredicateDataType type, Literal literal);

    template <typename T, typename CONTAINER>
    SearchArgumentBuilder& addChildForIn(T column, PredicateDataType type,
                                         const CONTAINER& literals);

    template <typename T>
    SearchArgumentBuilder& addChildForIsNull(T column, PredicateDataType type);

    template <typename T>
    SearchArgumentBuilder& addChildForBetween(T column, PredicateDataType type, Literal lower,
                                              Literal upper);

   public:
    static TreeNode pushDownNot(TreeNode root);
    static TreeNode foldMaybe(TreeNode expr);
    static TreeNode flatten(TreeNode root);
    static TreeNode convertToCNF(TreeNode root);

   private:
    std::deque<TreeNode> mCurrTree;
    std::unordered_map<PredicateLeaf, size_t, PredicateLeafHash, PredicateLeafComparator> mLeaves;
    std::shared_ptr<ExpressionTree> mRoot;
  };

}  // namespace orc

#endif  // ORC_SRC_SEARCHARGUMENT_HH
