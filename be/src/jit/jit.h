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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/JIT/CHJIT.h
// and modified by Doris

#ifdef DORIS_ENABLE_JIT
#pragma once

#include <unordered_map>
#include <atomic>

#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Target/TargetMachine.h>

#include "common/status.h"

namespace doris {

class JITModuleMemoryManager;
class JITSymbolResolver;
class JITCompiler;

/** Custom jit implementation
  * Main use cases:
  * 1. Compiled functions in module.
  * 2. Release memory for compiled functions.
  *
  * In LLVM library there are 2 main JIT stacks MCJIT and ORCv2.
  *
  * Main reasons for custom implementation vs MCJIT
  * MCJIT keeps llvm::Module and compiled object code before linking process after module was compiled.
  * llvm::Module can be removed, but compiled object code cannot be removed. Memory for compiled code
  * will be release only during MCJIT instance destruction. It is too expensive to create MCJIT
  * instance for each compiled module.
  * Also important reason is that if some error occurred during compilation steps, MCJIT can just terminate
  * our program.
  *
  * Main reasons for custom implementation vs ORCv2.
  * ORC is on request compiler, we do not need support for asynchronous compilation.
  * It was possible to remove compiled code with ORCv1 but it was deprecated.
  * In ORCv2 this probably can be done only with custom layer and materialization unit.
  * But it is inconvenient, discard is only called for materialization units by JITDylib that are not yet materialized.
  *
  * JIT interface is thread safe, that means all functions can be called from multiple threads and state of JIT instance
  * will not be broken.
  * It is client responsibility to be sure and do not use compiled code after it was released.
  */
class JIT {
public:
    JIT();

    ~JIT();

    static JIT& get_instance();

    struct CompiledModule {
        /// Size of compiled module code in bytes
        size_t size;

        /// Module identifier. Should not be changed by client
        uint64_t identifier;

        /// Vector of compiled functions. Should not be changed by client.
        /// It is client responsibility to cast result function to right signature.
        /// After call to delete_compiled_module compiled functions from module become invalid.
        std::unordered_map<std::string, void *> function_name_to_symbol;

    };

    /** Compile module. In compile function client responsibility is to fill module with necessary
      * IR code, then it will be compiled by JIT instance.
      * Return compiled module.
      */
    Status compile_module(std::function<Status (llvm::Module &)> compile_function, CompiledModule& result);

    /** Delete compiled module. Pointers to functions from module become invalid after this call.
      * It is client responsibility to be sure that there are no pointers to compiled module code.
      */
    void delete_compiled_module(const CompiledModule & module_info);

    /** Register external symbol for JIT instance to use, during linking.
      * It can be function, or global constant.
      * It is client responsibility to be sure that address of symbol is valid during JIT instance lifetime.
      */
    void register_external_symbol(const std::string & symbol_name, void * address);

    /** Total compiled code size for module that are currently valid.
      */
    inline size_t get_compiled_code_size() const { return _compiled_code_size.load(std::memory_order_relaxed); }

private:

    std::unique_ptr<llvm::Module> _create_module_for_compilation();

    Status _compile_module(std::unique_ptr<llvm::Module> module, CompiledModule& result);

    std::string _get_mangled_name(const std::string & name_to_mangle) const;

    void _run_optimization_passes_on_module(llvm::Module & module) const;

    static std::unique_ptr<llvm::TargetMachine> _get_target_machine();

    llvm::LLVMContext _context;
    std::unique_ptr<llvm::TargetMachine> _machine;
    llvm::DataLayout _layout;
    std::unique_ptr<JITCompiler> _compiler;
    std::unique_ptr<JITSymbolResolver> _symbol_resolver;

    std::unordered_map<uint64_t, std::unique_ptr<JITModuleMemoryManager>> _module_identifier_to_memory_manager;
    uint64_t _current_module_key = 0;
    std::atomic<size_t> _compiled_code_size = 0;
    mutable std::mutex _jit_lock;

};

} // namespace doris

#endif
