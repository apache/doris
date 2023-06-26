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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/Serializations/SubcolumnsTree.h
// and modified by Doris

#pragma once
#include "vec/columns/column.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/data_types/data_type.h"
#include "vec/json/path_in_data.h"
namespace doris::vectorized {
/// Tree that represents paths in document
/// with additional data in nodes.

template <typename NodeData>
class SubcolumnsTree {
public:
    struct Node {
        enum Kind { TUPLE, NESTED, SCALAR };

        explicit Node(Kind kind_) : kind(kind_) {}
        Node(Kind kind_, const NodeData& data_) : kind(kind_), data(data_) {}
        Node(Kind kind_, const NodeData& data_, const PathInData& path_)
                : kind(kind_), data(data_), path(path_) {}
        Node(Kind kind_, NodeData&& data_) : kind(kind_), data(std::move(data_)) {}
        Node(Kind kind_, NodeData&& data_, const PathInData& path_)
                : kind(kind_), data(std::move(data_)), path(path_) {}

        Kind kind = TUPLE;
        const Node* parent = nullptr;

        Arena strings_pool;
        HashMapWithStackMemory<StringRef, std::shared_ptr<Node>, StringRefHash, 4> children;

        NodeData data;
        PathInData path;

        bool is_nested() const { return kind == NESTED; }
        bool is_scalar() const { return kind == SCALAR; }
        bool is_scalar_without_children() const { return kind == SCALAR && children.empty(); }

        // Only modify data and kind
        void modify(std::shared_ptr<Node>&& other) {
            data = std::move(other->data);
            kind = other->kind;
            path = other->path;
        }

        void add_child(std::string_view key, std::shared_ptr<Node> next_node) {
            next_node->parent = this;
            StringRef key_ref {strings_pool.insert(key.data(), key.length()), key.length()};
            children[key_ref] = std::move(next_node);
        }
    };

    using NodeKind = typename Node::Kind;
    using NodePtr = std::shared_ptr<Node>;

    /// Add a leaf without any data in other nodes.
    bool add(const PathInData& path, const NodeData& leaf_data) {
        return add(path, [&](NodeKind kind, bool exists) -> NodePtr {
            if (exists) {
                return nullptr;
            }

            if (kind == Node::SCALAR) {
                return std::make_shared<Node>(kind, leaf_data, path);
            }

            return std::make_shared<Node>(kind);
        });
    }

    bool add(const PathInData& path, NodeData&& leaf_data) {
        return add(path, [&](NodeKind kind, bool exists) -> NodePtr {
            if (exists) {
                return nullptr;
            }

            if (kind == Node::SCALAR) {
                return std::make_shared<Node>(kind, std::move(leaf_data), path);
            }

            return std::make_shared<Node>(kind);
        });
    }

    /// Callback for creation of node. Receives kind of node and
    /// flag, which is true if node already exists.
    using NodeCreator = std::function<NodePtr(NodeKind, bool)>;

    // create root as SCALAR node
    void create_root(const NodeData& leaf_data) {
        root = std::make_shared<Node>(Node::SCALAR, leaf_data);
        leaves.push_back(root);
    }

    bool add(const PathInData& path, const NodeCreator& node_creator) {
        const auto& parts = path.get_parts();

        if (parts.empty()) {
            return false;
        }

        if (!root) {
            root = std::make_shared<Node>(Node::TUPLE);
        }

        Node* current_node = root.get();
        for (size_t i = 0; i < parts.size() - 1; ++i) {
            auto it = current_node->children.find(
                    StringRef {parts[i].key.data(), parts[i].key.size()});
            if (it != current_node->children.end()) {
                current_node = it->get_second().get();
                node_creator(current_node->kind, true);

                if (current_node->is_nested() != parts[i].is_nested) {
                    return false;
                }
            } else {
                auto next_kind = parts[i].is_nested ? Node::NESTED : Node::TUPLE;
                auto next_node = node_creator(next_kind, false);
                current_node->add_child(String(parts[i].key), next_node);
                current_node = next_node.get();
            }
        }

        auto it = current_node->children.find(
                StringRef {parts.back().key.data(), parts.back().key.size()});
        if (it != current_node->children.end()) {
            // Modify this node to Node::SCALAR
            auto new_node = node_creator(Node::SCALAR, false);
            it->get_second()->modify(std::move(new_node));
            leaves.push_back(it->get_second());
            return true;
        }

        auto next_node = node_creator(Node::SCALAR, false);
        current_node->add_child(String(parts.back().key), next_node);
        leaves.push_back(std::move(next_node));

        return true;
    }

    /// Find node that matches the path the best.
    const Node* find_best_match(const PathInData& path) const { return find_impl(path, false); }

    /// Find node that matches the path exactly.
    const Node* find_exact(const PathInData& path) const { return find_impl(path, true); }

    /// Find leaf by path.
    const Node* find_leaf(const PathInData& path) const {
        const auto* candidate = find_exact(path);
        if (!candidate || !candidate->is_scalar()) {
            return nullptr;
        }
        return candidate;
    }

    using NodePredicate = std::function<bool(const Node&)>;

    /// Finds leaf that satisfies the predicate.
    const Node* find_leaf(const NodePredicate& predicate) {
        return find_leaf(root.get(), predicate);
    }

    static const Node* find_leaf(const Node* node, const NodePredicate& predicate) {
        if (!node) {
            return nullptr;
        }

        if (node->is_scalar()) {
            return predicate(*node) ? node : nullptr;
        }

        for (auto it = node->children.begin(); it != node->children.end(); ++it) {
            auto child = it->get_second();
            if (const auto* leaf = find_leaf(child.get(), predicate)) {
                return leaf;
            }
        }
        return nullptr;
    }

    /// Find first parent node that satisfies the predicate.
    static const Node* find_parent(const Node* node, const NodePredicate& predicate) {
        while (node && !predicate(*node)) {
            node = node->parent;
        }
        return node;
    }

    bool empty() const { return root == nullptr; }
    size_t size() const { return leaves.size(); }

    using Nodes = std::vector<NodePtr>;

    const Nodes& get_leaves() const { return leaves; }
    const Node* get_root() const { return root.get(); }
    Node* get_mutable_root() { return root.get(); }

    static void get_leaves_of_node(const Node* node, std::vector<const Node*>& nodes,
                                   vectorized::PathsInData& paths) {
        if (node->is_scalar()) {
            nodes.push_back(node);
            paths.push_back(node->path);
        }
        for (auto it = node->children.begin(); it != node->children.end(); ++it) {
            auto child = it->get_second();
            get_leaves_of_node(child.get(), nodes, paths);
        }
    }

    using iterator = typename Nodes::iterator;
    using const_iterator = typename Nodes::const_iterator;

    iterator begin() { return leaves.begin(); }
    iterator end() { return leaves.end(); }

    const_iterator begin() const { return leaves.begin(); }
    const_iterator end() const { return leaves.end(); }

private:
    const Node* find_impl(const PathInData& path, bool find_exact) const {
        if (!root) {
            return nullptr;
        }

        const auto& parts = path.get_parts();
        const Node* current_node = root.get();

        for (const auto& part : parts) {
            auto it = current_node->children.find(StringRef {part.key.data(), part.key.size()});
            if (it == current_node->children.end()) {
                return find_exact ? nullptr : current_node;
            }

            current_node = it->get_second().get();
        }

        return current_node;
    }

    NodePtr root;
    Nodes leaves;
};

} // namespace doris::vectorized
