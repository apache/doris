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

#include "IKArbitrator.h"

namespace doris::segment_v2 {

void IKArbitrator::process(AnalyzeContext& context, bool use_smart) {
    auto org_lexemes = context.getOrgLexemes();
    auto org_lexeme = org_lexemes->pollFirst();
    LexemePath* cross_path = new LexemePath(arena_);

    auto process_path = [&](LexemePath* path) {
        if (path->size() == 1 || !use_smart) {
            // crossPath has no ambiguity or does not handle ambiguity
            // Directly output the current crossPath
            context.addLexemePath(path);
        } else {
            // Ambiguity handling for the current crossPath
            // Output the ambiguity resolution result: judgeResult
            LexemePath* result = judge(path->getHead(), path->getPathLength());
            context.addLexemePath(result);
            delete path;
        }
    };

    while (org_lexeme) {
        if (!cross_path->addCrossLexeme(*org_lexeme)) {
            // Find the next crossPath that does not intersect with crossPath.
            process_path(cross_path);
            // Add orgLexeme to the new crossPath
            cross_path = new LexemePath(arena_);
            cross_path->addCrossLexeme(*org_lexeme);
        }
        org_lexeme = org_lexemes->pollFirst();
    }

    // Process the final path
    process_path(cross_path);
}

// Performs ambiguity resolution on a given lexeme path.
LexemePath* IKArbitrator::judge(Cell* lexeme_cell, size_t full_text_length) {
    // Candidate result path
    LexemePath* path_option = new LexemePath(arena_);

    // Traverse crossPath once and return the stack of conflicting Lexemes
    std::stack<Cell*, std::vector<Cell*>> lexemeStack;
    forwardPath(lexeme_cell, path_option, lexemeStack);
    LexemePath* best_path = new LexemePath(*path_option, arena_);

    // Process ambiguous words if they exist
    while (!lexemeStack.empty()) {
        auto c = lexemeStack.top();
        lexemeStack.pop();
        backPath(c->getLexeme(), path_option);
        forwardPath(c, path_option);
        if (*path_option < *best_path) {
            delete best_path;
            best_path = new LexemePath(*path_option, arena_);
        }
    }
    delete path_option;
    // Return the optimal solution
    return best_path;
}

void IKArbitrator::forwardPath(Cell* lexeme_cell, LexemePath* path_option) {
    auto current_cell = lexeme_cell;
    while (current_cell) {
        path_option->addNotCrossLexeme(current_cell->getLexeme());
        current_cell = current_cell->getNext();
    }
}

void IKArbitrator::forwardPath(Cell* lexeme_cell, LexemePath* path_option,
                               std::stack<Cell*, std::vector<Cell*>>& conflictStack) {
    auto current_cell = lexeme_cell;
    while (current_cell) {
        if (!path_option->addNotCrossLexeme(current_cell->getLexeme())) {
            conflictStack.push(current_cell);
        }
        current_cell = current_cell->getNext();
    }
}

void IKArbitrator::backPath(const Lexeme& lexeme, LexemePath* option) {
    while (option->checkCross(lexeme)) {
        option->removeTail();
    }
}
} // namespace doris::segment_v2
