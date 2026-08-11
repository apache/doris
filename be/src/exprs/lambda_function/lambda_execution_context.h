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

#pragma once

#include <glog/logging.h>

#include <set>
#include <string>
#include <utility>
#include <vector>

namespace doris {

class LambdaExecutionContext {
public:
    struct Binding {
        std::string name;
        int column_position = -1;
    };

    struct Frame {
        bool bind_by_name = true;
        bool parent_bindings_visible = true;
        std::vector<Binding> argument_bindings;
    };

    struct ResolveResult {
        bool searched_named_scope = false;
        bool found = false;
        int column_position = -1;
    };

    class FrameGuard {
    public:
        FrameGuard(LambdaExecutionContext& context, Frame frame) : _context(&context) {
            _context->push_frame(std::move(frame));
        }

        FrameGuard(FrameGuard&& other) = delete;
        FrameGuard& operator=(FrameGuard&& other) = delete;
        FrameGuard(const FrameGuard&) = delete;
        FrameGuard& operator=(const FrameGuard&) = delete;

        ~FrameGuard() { release(); }

    private:
        void release() {
            if (_context != nullptr) {
                _context->pop_frame();
                _context = nullptr;
            }
        }

        LambdaExecutionContext* _context;
    };

    void push_frame(Frame frame) { _frames.push_back(std::move(frame)); }

    void pop_frame() {
        DCHECK(!_frames.empty());
        _frames.pop_back();
    }

    ResolveResult resolve_column_position(const std::string& name) const {
        ResolveResult result;
        for (auto frame_it = _frames.rbegin(); frame_it != _frames.rend(); ++frame_it) {
            const auto& frame = *frame_it;
            result.searched_named_scope |= frame.bind_by_name;
            for (auto binding_it = frame.argument_bindings.rbegin();
                 binding_it != frame.argument_bindings.rend(); ++binding_it) {
                const auto& argument_binding = *binding_it;
                if (argument_binding.name == name) {
                    result.found = true;
                    result.column_position = argument_binding.column_position;
                    return result;
                }
            }
            if (!frame.parent_bindings_visible) {
                break;
            }
        }
        return result;
    }

    void collect_visible_binding_column_positions(std::set<int>& column_positions) const {
        for (auto frame_it = _frames.rbegin(); frame_it != _frames.rend(); ++frame_it) {
            for (const auto& binding : frame_it->argument_bindings) {
                if (binding.column_position >= 0) {
                    column_positions.insert(binding.column_position);
                }
            }
            if (!frame_it->parent_bindings_visible) {
                break;
            }
        }
    }

private:
    std::vector<Frame> _frames;
};

} // namespace doris
