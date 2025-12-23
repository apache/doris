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

#include "ExpressionTree.hh"

#include <cassert>
#include <sstream>

namespace orc {

  ExpressionTree::ExpressionTree(Operator op)
      : mOperator(op), mLeaf(UNUSED_LEAF), mConstant(TruthValue::YES_NO_NULL) {}

  ExpressionTree::ExpressionTree(Operator op, std::initializer_list<TreeNode> children)
      : mOperator(op),
        mChildren(children.begin(), children.end()),
        mLeaf(UNUSED_LEAF),
        mConstant(TruthValue::YES_NO_NULL) {
    // PASS
  }

  ExpressionTree::ExpressionTree(size_t leaf)
      : mOperator(Operator::LEAF), mChildren(), mLeaf(leaf), mConstant(TruthValue::YES_NO_NULL) {
    // PASS
  }

  ExpressionTree::ExpressionTree(TruthValue constant)
      : mOperator(Operator::CONSTANT), mChildren(), mLeaf(UNUSED_LEAF), mConstant(constant) {
    // PASS
  }

  ExpressionTree::ExpressionTree(const ExpressionTree& other)
      : mOperator(other.mOperator), mLeaf(other.mLeaf), mConstant(other.mConstant) {
    for (TreeNode child : other.mChildren) {
      mChildren.emplace_back(std::make_shared<ExpressionTree>(*child));
    }
  }

  ExpressionTree::Operator ExpressionTree::getOperator() const {
    return mOperator;
  }

  const std::vector<TreeNode>& ExpressionTree::getChildren() const {
    return mChildren;
  }

  std::vector<TreeNode>& ExpressionTree::getChildren() {
    return const_cast<std::vector<TreeNode>&>(
        const_cast<const ExpressionTree*>(this)->getChildren());
  }

  const TreeNode ExpressionTree::getChild(size_t i) const {
    return mChildren.at(i);
  }

  TreeNode ExpressionTree::getChild(size_t i) {
    return std::const_pointer_cast<ExpressionTree>(
        const_cast<const ExpressionTree*>(this)->getChild(i));
  }

  TruthValue ExpressionTree::getConstant() const {
    assert(mOperator == Operator::CONSTANT);
    return mConstant;
  }

  size_t ExpressionTree::getLeaf() const {
    assert(mOperator == Operator::LEAF);
    return mLeaf;
  }

  void ExpressionTree::setLeaf(size_t leaf) {
    assert(mOperator == Operator::LEAF);
    mLeaf = leaf;
  }

  void ExpressionTree::addChild(TreeNode child) {
    mChildren.push_back(child);
  }

  TruthValue ExpressionTree::evaluate(const std::vector<TruthValue>& leaves) const {
    TruthValue result;
    switch (mOperator) {
      case Operator::OR: {
        result = mChildren.at(0)->evaluate(leaves);
        for (size_t i = 1; i < mChildren.size() && !isNeeded(result); ++i) {
          result = mChildren.at(i)->evaluate(leaves) || result;
        }
        return result;
      }
      case Operator::AND: {
        result = mChildren.at(0)->evaluate(leaves);
        for (size_t i = 1; i < mChildren.size() && isNeeded(result); ++i) {
          result = mChildren.at(i)->evaluate(leaves) && result;
        }
        return result;
      }
      case Operator::NOT:
        return !mChildren.at(0)->evaluate(leaves);
      case Operator::LEAF:
        return leaves[mLeaf];
      case Operator::CONSTANT:
        return mConstant;
      default:
        throw std::invalid_argument("Unknown operator!");
    }
  }

  std::string to_string(TruthValue truthValue) {
    switch (truthValue) {
      case TruthValue::YES:
        return "YES";
      case TruthValue::NO:
        return "NO";
      case TruthValue::IS_NULL:
        return "IS_NULL";
      case TruthValue::YES_NULL:
        return "YES_NULL";
      case TruthValue::NO_NULL:
        return "NO_NULL";
      case TruthValue::YES_NO:
        return "YES_NO";
      case TruthValue::YES_NO_NULL:
        return "YES_NO_NULL";
      default:
        throw std::invalid_argument("unknown TruthValue!");
    }
  }

  std::string ExpressionTree::toString() const {
    std::ostringstream sstream;
    switch (mOperator) {
      case Operator::OR:
        sstream << "(or";
        for (const auto& child : mChildren) {
          sstream << ' ' << child->toString();
        }
        sstream << ')';
        break;
      case Operator::AND:
        sstream << "(and";
        for (const auto& child : mChildren) {
          sstream << ' ' << child->toString();
        }
        sstream << ')';
        break;
      case Operator::NOT:
        sstream << "(not " << mChildren.at(0)->toString() << ')';
        break;
      case Operator::LEAF:
        sstream << "leaf-" << mLeaf;
        break;
      case Operator::CONSTANT:
        sstream << to_string(mConstant);
        break;
      default:
        throw std::invalid_argument("unknown operator!");
    }
    return sstream.str();
  }

}  // namespace orc
