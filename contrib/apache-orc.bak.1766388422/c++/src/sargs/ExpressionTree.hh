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

#ifndef ORC_EXPRESSIONTREE_HH
#define ORC_EXPRESSIONTREE_HH

#include "orc/sargs/TruthValue.hh"

#include <limits>
#include <memory>
#include <string>
#include <vector>

static const size_t UNUSED_LEAF = std::numeric_limits<size_t>::max();

namespace orc {

  class ExpressionTree;
  typedef std::shared_ptr<ExpressionTree> TreeNode;
  typedef std::initializer_list<TreeNode> NodeList;

  /**
   * The inner representation of the SearchArgument. Most users should not
   * need this interface, it is only for file formats that need to translate
   * the SearchArgument into an internal form.
   */
  class ExpressionTree {
   public:
    enum class Operator { OR, AND, NOT, LEAF, CONSTANT };

    ExpressionTree(Operator op);
    ExpressionTree(Operator op, std::initializer_list<TreeNode> children);
    ExpressionTree(size_t leaf);
    ExpressionTree(TruthValue constant);

    ExpressionTree(const ExpressionTree& other);
    ExpressionTree& operator=(const ExpressionTree&) = delete;

    Operator getOperator() const;

    const std::vector<TreeNode>& getChildren() const;

    std::vector<TreeNode>& getChildren();

    const TreeNode getChild(size_t i) const;

    TreeNode getChild(size_t i);

    TruthValue getConstant() const;

    size_t getLeaf() const;

    void setLeaf(size_t leaf);

    void addChild(TreeNode child);

    std::string toString() const;

    TruthValue evaluate(const std::vector<TruthValue>& leaves) const;

   private:
    Operator mOperator;
    std::vector<TreeNode> mChildren;
    size_t mLeaf;
    TruthValue mConstant;
  };

}  // namespace orc

#endif  // ORC_EXPRESSIONTREE_HH
