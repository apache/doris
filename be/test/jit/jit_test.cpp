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

#include <gtest/gtest.h>
#include <thread>

#include "common/logging.h"

#include "jit/jit.h"

#include <llvm/IR/IRBuilder.h>

namespace doris {

static void LifetimeTest() {
    Status status;
    for (int i = 0; i < 10; ++i) {
        JIT::CompiledModule compiled_module;
        status = JIT::get_instance().compile_module([&](llvm::Module& module) {
            auto& context = module.getContext();
            llvm::IRBuilder<> b(context);

            auto* func_type = llvm::FunctionType::get(llvm::Type::getVoidTy(context), {llvm::Type::getInt32Ty(context), llvm::Type::getInt32Ty(context)}, false);
            auto* function = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, "test", module);
            auto* entry = llvm::BasicBlock::Create(context, "entry", function);

            b.SetInsertPoint(entry);
            b.CreateRetVoid();

            return Status::OK();
        }, compiled_module);

        ASSERT_TRUE(status.ok());

        JIT::get_instance().delete_compiled_module(compiled_module);
    }
}

class JITTest : public testing::Test {

};

TEST_F(JITTest, life_time) {
    LifetimeTest();
}

TEST_F(JITTest, multi_thread_life_time) {
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        std::thread t(&LifetimeTest);
        threads.emplace_back(std::move(t));
    }

    for (auto& t : threads) {
        t.join();
    }
}

}