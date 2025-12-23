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

#include "sargs/SearchArgument.hh"

#include <algorithm>
#include <functional>
#include <sstream>
#include <unordered_set>

namespace orc {

  SearchArgument::~SearchArgument() {
    // PASS
  }

  const std::vector<PredicateLeaf>& SearchArgumentImpl::getLeaves() const {
    return mLeaves;
  }

  const ExpressionTree* SearchArgumentImpl::getExpression() const {
    return mExpressionTree.get();
  }

  TruthValue SearchArgumentImpl::evaluate(const std::vector<TruthValue>& leaves) const {
    return mExpressionTree == nullptr ? TruthValue::YES : mExpressionTree->evaluate(leaves);
  }

  std::string SearchArgumentImpl::toString() const {
    std::ostringstream sstream;
    for (size_t i = 0; i != mLeaves.size(); ++i) {
      sstream << "leaf-" << i << " = " << mLeaves.at(i).toString() << ", ";
    }
    sstream << "expr = " << mExpressionTree->toString();
    return sstream.str();
  }

  SearchArgumentBuilder::~SearchArgumentBuilder() {
    // PASS
  }

  SearchArgumentBuilderImpl::SearchArgumentBuilderImpl() {
    mRoot.reset(new ExpressionTree(ExpressionTree::Operator::AND));
    mCurrTree.push_back(mRoot);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::start(ExpressionTree::Operator op) {
    TreeNode node = std::make_shared<ExpressionTree>(op);
    mCurrTree.front()->addChild(node);
    mCurrTree.push_front(node);
    return *this;
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::startOr() {
    return start(ExpressionTree::Operator::OR);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::startAnd() {
    return start(ExpressionTree::Operator::AND);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::startNot() {
    return start(ExpressionTree::Operator::NOT);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::end() {
    TreeNode& current = mCurrTree.front();
    if (current->getChildren().empty()) {
      throw std::invalid_argument("Cannot create expression " + mRoot->toString() +
                                  " with no children.");
    }
    if (current->getOperator() == ExpressionTree::Operator::NOT &&
        current->getChildren().size() != 1) {
      throw std::invalid_argument("Can't create NOT expression " + current->toString() +
                                  " with more than 1 child.");
    }
    mCurrTree.pop_front();
    return *this;
  }

  size_t SearchArgumentBuilderImpl::addLeaf(PredicateLeaf leaf) {
    size_t id = mLeaves.size();
    const auto& result = mLeaves.insert(std::make_pair(leaf, id));
    return result.first->second;
  }

  bool SearchArgumentBuilderImpl::isInvalidColumn(const std::string& column) {
    return column.empty();
  }

  bool SearchArgumentBuilderImpl::isInvalidColumn(uint64_t columnId) {
    return columnId == INVALID_COLUMN_ID;
  }

  template <typename T>
  SearchArgumentBuilder& SearchArgumentBuilderImpl::compareOperator(PredicateLeaf::Operator op,
                                                                    T column,
                                                                    PredicateDataType type,
                                                                    Literal literal) {
    TreeNode parent = mCurrTree.front();
    if (isInvalidColumn(column)) {
      parent->addChild(std::make_shared<ExpressionTree>(TruthValue::YES_NO_NULL));
    } else {
      PredicateLeaf leaf(op, type, column, literal);
      parent->addChild(std::make_shared<ExpressionTree>(addLeaf(leaf)));
    }
    return *this;
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::lessThan(const std::string& column,
                                                             PredicateDataType type,
                                                             Literal literal) {
    return compareOperator(PredicateLeaf::Operator::LESS_THAN, column, type, literal);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::lessThan(uint64_t columnId,
                                                             PredicateDataType type,
                                                             Literal literal) {
    return compareOperator(PredicateLeaf::Operator::LESS_THAN, columnId, type, literal);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::lessThanEquals(const std::string& column,
                                                                   PredicateDataType type,
                                                                   Literal literal) {
    return compareOperator(PredicateLeaf::Operator::LESS_THAN_EQUALS, column, type, literal);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::lessThanEquals(uint64_t columnId,
                                                                   PredicateDataType type,
                                                                   Literal literal) {
    return compareOperator(PredicateLeaf::Operator::LESS_THAN_EQUALS, columnId, type, literal);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::equals(const std::string& column,
                                                           PredicateDataType type,
                                                           Literal literal) {
    if (literal.isNull()) {
      return isNull(column, type);
    } else {
      return compareOperator(PredicateLeaf::Operator::EQUALS, column, type, literal);
    }
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::equals(uint64_t columnId,
                                                           PredicateDataType type,
                                                           Literal literal) {
    if (literal.isNull()) {
      return isNull(columnId, type);
    } else {
      return compareOperator(PredicateLeaf::Operator::EQUALS, columnId, type, literal);
    }
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::nullSafeEquals(const std::string& column,
                                                                   PredicateDataType type,
                                                                   Literal literal) {
    return compareOperator(PredicateLeaf::Operator::NULL_SAFE_EQUALS, column, type, literal);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::nullSafeEquals(uint64_t columnId,
                                                                   PredicateDataType type,
                                                                   Literal literal) {
    return compareOperator(PredicateLeaf::Operator::NULL_SAFE_EQUALS, columnId, type, literal);
  }

  template <typename T, typename CONTAINER>
  SearchArgumentBuilder& SearchArgumentBuilderImpl::addChildForIn(T column, PredicateDataType type,
                                                                  const CONTAINER& literals) {
    TreeNode& parent = mCurrTree.front();
    if (isInvalidColumn(column)) {
      parent->addChild(std::make_shared<ExpressionTree>((TruthValue::YES_NO_NULL)));
    } else {
      if (literals.size() == 0) {
        throw std::invalid_argument("Can't create in expression with no arguments");
      }
      PredicateLeaf leaf(PredicateLeaf::Operator::IN, type, column, literals);
      parent->addChild(std::make_shared<ExpressionTree>(addLeaf(leaf)));
    }
    return *this;
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::in(
      const std::string& column, PredicateDataType type,
      const std::initializer_list<Literal>& literals) {
    return addChildForIn(column, type, literals);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::in(
      uint64_t columnId, PredicateDataType type, const std::initializer_list<Literal>& literals) {
    return addChildForIn(columnId, type, literals);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::in(const std::string& column,
                                                       PredicateDataType type,
                                                       const std::vector<Literal>& literals) {
    return addChildForIn(column, type, literals);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::in(uint64_t columnId, PredicateDataType type,
                                                       const std::vector<Literal>& literals) {
    return addChildForIn(columnId, type, literals);
  }

  template <typename T>
  SearchArgumentBuilder& SearchArgumentBuilderImpl::addChildForIsNull(T column,
                                                                      PredicateDataType type) {
    TreeNode& parent = mCurrTree.front();
    if (isInvalidColumn(column)) {
      parent->addChild(std::make_shared<ExpressionTree>(TruthValue::YES_NO_NULL));
    } else {
      PredicateLeaf leaf(PredicateLeaf::Operator::IS_NULL, type, column, {});
      parent->addChild(std::make_shared<ExpressionTree>(addLeaf(leaf)));
    }
    return *this;
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::isNull(const std::string& column,
                                                           PredicateDataType type) {
    return addChildForIsNull(column, type);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::isNull(uint64_t columnId,
                                                           PredicateDataType type) {
    return addChildForIsNull(columnId, type);
  }

  template <typename T>
  SearchArgumentBuilder& SearchArgumentBuilderImpl::addChildForBetween(T column,
                                                                       PredicateDataType type,
                                                                       Literal lower,
                                                                       Literal upper) {
    TreeNode& parent = mCurrTree.front();
    if (isInvalidColumn(column)) {
      parent->addChild(std::make_shared<ExpressionTree>(TruthValue::YES_NO_NULL));
    } else {
      PredicateLeaf leaf(PredicateLeaf::Operator::BETWEEN, type, column, {lower, upper});
      parent->addChild(std::make_shared<ExpressionTree>(addLeaf(leaf)));
    }
    return *this;
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::between(const std::string& column,
                                                            PredicateDataType type, Literal lower,
                                                            Literal upper) {
    return addChildForBetween(column, type, lower, upper);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::between(uint64_t columnId,
                                                            PredicateDataType type, Literal lower,
                                                            Literal upper) {
    return addChildForBetween(columnId, type, lower, upper);
  }

  SearchArgumentBuilder& SearchArgumentBuilderImpl::literal(TruthValue truth) {
    TreeNode& parent = mCurrTree.front();
    parent->addChild(std::make_shared<ExpressionTree>(truth));
    return *this;
  }

  /**
   * Recursively explore the tree to find the leaves that are still reachable
   * after optimizations.
   * @param tree the node to check next
   * @param next the next available leaf id
   * @param leafReorder buffer for leaf reorder
   * @return the next available leaf id
   */
  static size_t compactLeaves(const TreeNode& tree, size_t next, size_t leafReorder[]) {
    if (tree->getOperator() == ExpressionTree::Operator::LEAF) {
      size_t oldLeaf = tree->getLeaf();
      if (leafReorder[oldLeaf] == UNUSED_LEAF) {
        leafReorder[oldLeaf] = next++;
      }
    } else {
      for (const TreeNode& child : tree->getChildren()) {
        next = compactLeaves(child, next, leafReorder);
      }
    }
    return next;
  }

  /**
   * Rewrite expression tree to update the leaves.
   * @param root the root of the tree to fix
   * @param leafReorder a map from old leaf ids to new leaf ids
   * @return the fixed root
   */
  static TreeNode rewriteLeaves(TreeNode root, size_t leafReorder[]) {
    // The leaves could be shared in the tree. Use Set to remove the duplicates.
    std::unordered_set<TreeNode> leaves;
    std::deque<TreeNode> nodes;
    nodes.push_back(root);

    // Perform BFS
    while (!nodes.empty()) {
      TreeNode& node = nodes.front();
      nodes.pop_front();

      if (node->getOperator() == ExpressionTree::Operator::LEAF) {
        leaves.insert(node);
      } else {
        for (auto& child : node->getChildren()) {
          nodes.push_back(child);
        }
      }
    }

    // Update the leaf in place
    for (auto& leaf : leaves) {
      leaf->setLeaf(leafReorder[leaf->getLeaf()]);
    }

    return root;
  }

  /**
   * Push the negations all the way to just before the leaves. Also remove
   * double negatives.
   *
   * @param root the expression to normalize
   * @return the normalized expression, which may share some or all of the
   * nodes of the original expression.
   */
  TreeNode SearchArgumentBuilderImpl::pushDownNot(TreeNode root) {
    if (root->getOperator() == ExpressionTree::Operator::NOT) {
      TreeNode child = root->getChild(0);
      switch (child->getOperator()) {
        case ExpressionTree::Operator::NOT: {
          return pushDownNot(child->getChild(0));
        }
        case ExpressionTree::Operator::CONSTANT: {
          return std::make_shared<ExpressionTree>(!child->getConstant());
        }
        case ExpressionTree::Operator::AND: {
          TreeNode result(new ExpressionTree(ExpressionTree::Operator::OR));
          for (auto& kid : child->getChildren()) {
            result->addChild(pushDownNot(
                std::make_shared<ExpressionTree>(ExpressionTree::Operator::NOT, NodeList{kid})));
          }
          return result;
        }
        case ExpressionTree::Operator::OR: {
          TreeNode result(new ExpressionTree(ExpressionTree::Operator::AND));
          for (auto& kid : child->getChildren()) {
            result->addChild(pushDownNot(
                std::make_shared<ExpressionTree>(ExpressionTree::Operator::NOT, NodeList{kid})));
          }
          return result;
        }
        // for leaf, we don't do anything
        case ExpressionTree::Operator::LEAF:
        default:
          break;
      }
    } else {
      // iterate through children and push down not for each one
      for (size_t i = 0; i != root->getChildren().size(); ++i) {
        root->getChildren()[i] = pushDownNot(root->getChild(i));
      }
    }
    return root;
  }

  /**
   * Remove MAYBE values from the expression. If they are in an AND operator,
   * they are dropped. If they are in an OR operator, they kill their parent.
   * This assumes that pushDownNot has already been called.
   *
   * @param expr The expression to clean up
   * @return The cleaned up expression
   */
  TreeNode SearchArgumentBuilderImpl::foldMaybe(TreeNode expr) {
    if (expr) {
      for (size_t i = 0; i != expr->getChildren().size(); ++i) {
        TreeNode child = foldMaybe(expr->getChild(i));
        if (child->getOperator() == ExpressionTree::Operator::CONSTANT &&
            child->getConstant() == TruthValue::YES_NO_NULL) {
          switch (expr->getOperator()) {
            case ExpressionTree::Operator::AND:
              expr->getChildren()[i] = nullptr;
              break;
            case ExpressionTree::Operator::OR:
              // a maybe will kill the or condition
              return child;
            case ExpressionTree::Operator::NOT:
            case ExpressionTree::Operator::LEAF:
            case ExpressionTree::Operator::CONSTANT:
            default:
              throw std::invalid_argument("Got a maybe as child of " + expr->toString());
          }
        } else {
          expr->getChildren()[i] = child;
        }
      }

      auto& children = expr->getChildren();
      if (!children.empty()) {
        // eliminate removed maybe nodes from expr
        std::vector<TreeNode> nodes;
        std::for_each(children.begin(), children.end(), [&](const TreeNode& node) {
          if (node) nodes.emplace_back(node);
        });
        std::swap(children, nodes);
        if (children.empty()) {
          return std::make_shared<ExpressionTree>(TruthValue::YES_NO_NULL);
        }
      }
    }
    return expr;
  }

  /**
   * Converts multi-level ands and ors into single level ones.
   *
   * @param root the expression to flatten
   * @return the flattened expression, which will always be root with
   *   potentially modified children.
   */
  TreeNode SearchArgumentBuilderImpl::flatten(TreeNode root) {
    if (root) {
      std::vector<TreeNode> nodes;
      for (size_t i = 0; i != root->getChildren().size(); ++i) {
        TreeNode child = flatten(root->getChild(i));
        // do we need to flatten?
        if (child->getOperator() == root->getOperator() &&
            child->getOperator() != ExpressionTree::Operator::NOT) {
          for (auto& grandkid : child->getChildren()) {
            nodes.emplace_back(grandkid);
          }
        } else {
          nodes.emplace_back(child);
        }
      }
      std::swap(root->getChildren(), nodes);

      // if we have a single AND or OR, just return the child
      if ((root->getOperator() == ExpressionTree::Operator::OR ||
           root->getOperator() == ExpressionTree::Operator::AND) &&
          root->getChildren().size() == 1) {
        return root->getChild(0);
      }
    }
    return root;
  }

  /**
   * Generate all combinations of items on the andList. For each item on the
   * andList, it generates all combinations of one child from each and
   * expression. Thus, (and a b) (and c d) will be expanded to: (or a c)
   * (or a d) (or b c) (or b d). If there are items on the nonAndList, they
   * are added to each or expression.
   * @param result a list to put the results onto
   * @param andList a list of and expressions
   * @param nonAndList a list of non-and expressions
   */
  static void generateAllCombinations(std::vector<TreeNode>& result,
                                      const std::vector<TreeNode>& andList,
                                      const std::vector<TreeNode>& nonAndList) {
    std::vector<TreeNode>& kids = andList.front()->getChildren();
    if (result.empty()) {
      for (TreeNode& kid : kids) {
        TreeNode root(new ExpressionTree(ExpressionTree::Operator::OR));
        result.emplace_back(root);
        for (const TreeNode& node : nonAndList) {
          root->addChild(std::make_shared<ExpressionTree>(*node));
        }
        root->addChild(kid);
      }
    } else {
      std::vector<TreeNode> work(result.begin(), result.end());
      result.clear();
      for (TreeNode& kid : kids) {
        for (TreeNode node : work) {
          TreeNode copy = std::make_shared<ExpressionTree>(*node);
          copy->addChild(kid);
          result.emplace_back(copy);
        }
      }
    }
    if (andList.size() > 1) {
      generateAllCombinations(result, std::vector<TreeNode>(andList.cbegin() + 1, andList.cend()),
                              nonAndList);
    }
  }

  static const size_t CNF_COMBINATIONS_THRESHOLD = 256;
  static bool checkCombinationsThreshold(const std::vector<TreeNode>& andList) {
    size_t numComb = 1;
    for (const TreeNode& tree : andList) {
      numComb *= tree->getChildren().size();
      if (numComb > CNF_COMBINATIONS_THRESHOLD) {
        return false;
      }
    }
    return true;
  }

  /**
   * Convert an expression so that the top level operator is AND with OR
   * operators under it. This routine assumes that all of the NOT operators
   * have been pushed to the leaves via pushdDownNot.
   * @param root the expression
   * @return the normalized expression
   */
  TreeNode SearchArgumentBuilderImpl::convertToCNF(TreeNode root) {
    if (root) {
      // convert all of the children to CNF
      size_t size = root->getChildren().size();
      for (size_t i = 0; i != size; ++i) {
        root->getChildren()[i] = convertToCNF(root->getChild(i));
      }
      if (root->getOperator() == ExpressionTree::Operator::OR) {
        // a list of leaves that weren't under AND expressions
        std::vector<TreeNode> nonAndList;
        // a list of AND expressions that we need to distribute
        std::vector<TreeNode> andList;
        for (TreeNode& child : root->getChildren()) {
          if (child->getOperator() == ExpressionTree::Operator::AND) {
            andList.emplace_back(child);
          } else if (child->getOperator() == ExpressionTree::Operator::OR) {
            // pull apart the kids of the OR expression
            for (TreeNode& grandkid : child->getChildren()) {
              nonAndList.emplace_back(grandkid);
            }
          } else {
            nonAndList.emplace_back(child);
          }
        }
        if (!andList.empty()) {
          if (checkCombinationsThreshold(andList)) {
            root = std::make_shared<ExpressionTree>(ExpressionTree::Operator::AND);
            generateAllCombinations(root->getChildren(), andList, nonAndList);
          } else {
            root = std::make_shared<ExpressionTree>(TruthValue::YES_NO_NULL);
          }
        }
      }
    }
    return root;
  }

  SearchArgumentImpl::SearchArgumentImpl(TreeNode root, const std::vector<PredicateLeaf>& leaves)
      : mExpressionTree(root), mLeaves(leaves) {
    // PASS
  }

  std::unique_ptr<SearchArgument> SearchArgumentBuilderImpl::build() {
    if (mCurrTree.size() != 1) {
      throw std::invalid_argument("Failed to end " + std::to_string(mCurrTree.size()) +
                                  " operations.");
    }
    mRoot = pushDownNot(mRoot);
    mRoot = foldMaybe(mRoot);
    mRoot = flatten(mRoot);
    mRoot = convertToCNF(mRoot);
    mRoot = flatten(mRoot);
    std::vector<size_t> leafReorder(mLeaves.size(), UNUSED_LEAF);
    size_t newLeafCount = compactLeaves(mRoot, 0, leafReorder.data());
    mRoot = rewriteLeaves(mRoot, leafReorder.data());

    std::vector<PredicateLeaf> leafList(newLeafCount, PredicateLeaf());

    // build the new list
    for (auto& leaf : mLeaves) {
      size_t newLoc = leafReorder[leaf.second];
      if (newLoc != UNUSED_LEAF) {
        leafList[newLoc] = leaf.first;
      }
    }
    return std::make_unique<SearchArgumentImpl>(mRoot, leafList);
  }

  std::unique_ptr<SearchArgumentBuilder> SearchArgumentFactory::newBuilder() {
    return std::make_unique<SearchArgumentBuilderImpl>();
  }

}  // namespace orc
