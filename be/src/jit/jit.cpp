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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/JIT/CHJIT.cpp
// and modified by Doris

#ifdef DORIS_ENABLE_JIT

#include "jit.h"

#include <sys/mman.h>
#include <unistd.h>

#include <boost/noncopyable.hpp>

#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Mangler.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/ExecutionEngine/JITEventListener.h>
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/MC/TargetRegistry.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Support/SmallVectorMemoryBuffer.h>
#include <llvm/Support/raw_ostream.h>

#include "common/logging.h"

namespace doris {

/** Simple module to object file compiler.
  * Result object cannot be used as machine code directly, it should be passed to linker.
  */
class JITCompiler {
public:

    explicit JITCompiler(llvm::TargetMachine &target_machine_)
    : target_machine(target_machine_) {
    }

    Status compile(llvm::Module& module, std::unique_ptr<llvm::MemoryBuffer>& result) {
        auto materialize_error = module.materializeAll();
        if (materialize_error) {
            std::string error_message;
            handleAllErrors(std::move(materialize_error),
                [&](const llvm::ErrorInfoBase & error_info) { error_message = error_info.message(); });
            return Status::RuntimeError(fmt::format("Compile failed: {}", error_message));
        }

        llvm::SmallVector<char, 4096> object_buffer;

        llvm::raw_svector_ostream object_stream(object_buffer);
        llvm::legacy::PassManager pass_manager;

        llvm::MCContext * machine_code_context = nullptr;

        if (target_machine.addPassesToEmitMC(pass_manager, machine_code_context, object_stream))
            return Status::RuntimeError("MachineCode is not supported for the platform");

        pass_manager.run(module);

        result = std::make_unique<llvm::SmallVectorMemoryBuffer>(
            std::move(object_buffer), "<in memory object compiled from " + module.getModuleIdentifier() + ">");

        return Status::OK();
    }

    ~JITCompiler() = default;

private:
    llvm::TargetMachine & target_machine;
};

/** Arena that allocate all memory with system page_size.
  * All allocated pages can be protected with protection_flags using protect method.
  * During destruction all allocated pages protection_flags will be reset.
  */

class PageArena : private boost::noncopyable {
public:
    PageArena() : page_size(sysconf(_SC_PAGESIZE)) {}

    char * allocate(size_t size, size_t alignment) {
        /** First check if in some allocated page blocks there are enough free memory to make allocation.
          * If there is no such block create it and then allocate from it.
          */

        for (size_t i = 0; i < page_blocks.size(); ++i) {
            char * result = _try_allocate_from_page_block_with_index(size, alignment, i);
            if (result)
                return result;
        }

        allocate_next_page_block(size);
        size_t allocated_page_index = page_blocks.size() - 1;
        char * result = _try_allocate_from_page_block_with_index(size, alignment, allocated_page_index);
        assert(result);

        return result;
    }

    inline size_t getAllocatedSize() const { return allocated_size; }

    inline size_t get_page_size() const { return page_size; }

    ~PageArena() {
        protect(PROT_READ | PROT_WRITE);

        for (auto & page_block : page_blocks)
            free(page_block.base());
    }

    void protect(int protection_flags) {
        /** The code is partially based on the LLVM codebase
              * The LLVM Project is under the Apache License v2.0 with LLVM Exceptions.
              */

#    if defined(__NetBSD__) && defined(PROT_MPROTECT)
        protection_flags |= PROT_MPROTECT(PROT_READ | PROT_WRITE | PROT_EXEC);
#    endif

        bool invalidate_cache = (protection_flags & PROT_EXEC);

        for (const auto & block : page_blocks) {
#    if defined(__arm__) || defined(__aarch64__)
            /// Certain ARM implementations treat icache clear instruction as a memory read,
            /// and CPU segfaults on trying to clear cache on !PROT_READ page.
            /// Therefore we need to temporarily add PROT_READ for the sake of flushing the instruction caches.
            if (invalidate_cache && !(protection_flags & PROT_READ)) {
                int res = mprotect(block.base(), block.block_size(), protection_flags | PROT_READ);
                if (res != 0)
                    throwFromErrno("Cannot mprotect memory region", ErrorCodes::CANNOT_MPROTECT);

                llvm::sys::Memory::InvalidateInstructionCache(block.base(), block.block_size());
                invalidate_cache = false;
            }
#    endif
            int res = mprotect(block.base(), block.block_size(), protection_flags);
            DCHECK(res == 0);

            if (invalidate_cache)
                llvm::sys::Memory::InvalidateInstructionCache(block.base(), block.block_size());
        }
    }

private:
    struct PageBlock {
    public:
        PageBlock(void * pages_base_, size_t pages_size_, size_t page_size_)
            : _pages_base(pages_base_), _pages_size(pages_size_), _page_size(page_size_) {
        }

        inline void * base() const { return _pages_base; }
        inline size_t pages_size() const { return _pages_size; }
        inline size_t page_size() const { return _page_size; }
        inline size_t block_size() const { return _pages_size * _page_size; }

    private:
        void * _pages_base;
        size_t _pages_size;
        size_t _page_size;
    };

    std::vector<PageBlock> page_blocks;

    std::vector<size_t> page_blocks_allocated_size;

    size_t page_size = 0;

    size_t allocated_size = 0;

    char * _try_allocate_from_page_block_with_index(size_t size, size_t alignment, size_t page_block_index) {
        assert(page_block_index < page_blocks.size());
        auto & pages_block = page_blocks[page_block_index];

        size_t block_size = pages_block.block_size();
        size_t & block_allocated_size = page_blocks_allocated_size[page_block_index];
        size_t block_free_size = block_size - block_allocated_size;

        uint8_t * pages_start = static_cast<uint8_t *>(pages_block.base());
        void * pages_offset = pages_start + block_allocated_size;

        auto * result = std::align(alignment, size, pages_offset, block_free_size);

        if (result) {
            block_allocated_size = reinterpret_cast<uint8_t *>(result) - pages_start;
            block_allocated_size += size;

            return static_cast<char *>(result);
        }
        else {
            return nullptr;
        }
    }

    void allocate_next_page_block(size_t size) {
        size_t pages_to_allocate_size = ((size / page_size) + 1) * 2;
        size_t allocate_size = page_size * pages_to_allocate_size;

        void * buf = nullptr;
        int res = posix_memalign(&buf, page_size, allocate_size);
        DCHECK(res == 0);

        page_blocks.emplace_back(buf, pages_to_allocate_size, page_size);
        page_blocks_allocated_size.emplace_back(0);

        allocated_size += allocate_size;
    }
};

/** MemoryManager for module.
  * Keep total allocated size during RuntimeDyld linker execution.
  */
class JITModuleMemoryManager : public llvm::RTDyldMemoryManager {
public:

    uint8_t * allocateCodeSection(uintptr_t size, unsigned alignment, unsigned, llvm::StringRef) override {
        return reinterpret_cast<uint8_t *>(ex_page_arena.allocate(size, alignment));
    }

    uint8_t * allocateDataSection(uintptr_t size, unsigned alignment, unsigned, llvm::StringRef, bool is_read_only) override {
        if (is_read_only)
            return reinterpret_cast<uint8_t *>(ro_page_arena.allocate(size, alignment));
        else
            return reinterpret_cast<uint8_t *>(rw_page_arena.allocate(size, alignment));
    }

    bool finalizeMemory(std::string *) override {
        ro_page_arena.protect(PROT_READ);
        ex_page_arena.protect(PROT_READ | PROT_EXEC);
        return true;
    }

    inline size_t allocated_size() const {
        size_t data_size = rw_page_arena.getAllocatedSize() + ro_page_arena.getAllocatedSize();
        size_t code_size = ex_page_arena.getAllocatedSize();

        return data_size + code_size;
    }

private:
    PageArena rw_page_arena;
    PageArena ro_page_arena;
    PageArena ex_page_arena;
};

class JITSymbolResolver : public llvm::LegacyJITSymbolResolver {
public:
    llvm::JITSymbol findSymbolInLogicalDylib(const std::string &) override { return nullptr; }

    llvm::JITSymbol findSymbol(const std::string & Name) override {
        auto address_it = symbol_name_to_symbol_address.find(Name);
        if (address_it == symbol_name_to_symbol_address.cend())
            return llvm::JITSymbol(nullptr);

        uint64_t symbol_address = reinterpret_cast<uint64_t>(address_it->second);
        auto jit_symbol = llvm::JITSymbol(symbol_address, llvm::JITSymbolFlags::None);

        return jit_symbol;
    }

    void registerSymbol(const std::string & symbol_name, void * symbol) { symbol_name_to_symbol_address[symbol_name] = symbol; }

    ~JITSymbolResolver() override = default;

private:
    std::unordered_map<std::string, void *> symbol_name_to_symbol_address;
};

JIT::JIT()
    : machine(_get_target_machine())
    , layout(machine->createDataLayout())
    , compiler(std::make_unique<JITCompiler>(*machine))
    , symbol_resolver(std::make_unique<JITSymbolResolver>()) {
    /// Define common symbols that can be generated during compilation
    /// Necessary for valid linker symbol resolution
    symbol_resolver->registerSymbol("memset", reinterpret_cast<void *>(&memset));
    symbol_resolver->registerSymbol("memcpy", reinterpret_cast<void *>(&memcpy));
    symbol_resolver->registerSymbol("memcmp", reinterpret_cast<void *>(&memcmp));
}

JIT& JIT::get_instance() {
    static JIT instance;
    return instance;
}

JIT::~JIT() = default;

Status JIT::compile_module(std::function<Status (llvm::Module &)> compile_function, CompiledModule& result) {
    std::lock_guard<std::mutex> lock(jit_lock);

    auto module = _create_module_for_compilation();
    auto status = compile_function(*module);
    if (!status.ok())
        return status;

    status = _compile_module(std::move(module), result);
    if (status.ok())
        ++current_module_key;

    return status;
}

std::unique_ptr<llvm::Module> JIT::_create_module_for_compilation() {
    std::unique_ptr<llvm::Module> module = std::make_unique<llvm::Module>("jit" + std::to_string(current_module_key), context);
    module->setDataLayout(layout);
    module->setTargetTriple(machine->getTargetTriple().getTriple());

    return module;
}

Status JIT::_compile_module(std::unique_ptr<llvm::Module> module, CompiledModule& result) {
    _run_optimization_passes_on_module(*module);

    std::unique_ptr<llvm::MemoryBuffer> buffer;
    auto status = compiler->compile(*module, buffer);
    if (!status.ok())
        return status;

    llvm::Expected<std::unique_ptr<llvm::object::ObjectFile>> object = llvm::object::ObjectFile::createObjectFile(*buffer);

    if (!object) {
        auto e = object.takeError();
        std::string error_message;
        handleAllErrors(object.takeError(), [&](const llvm::ErrorInfoBase & error_info) { error_message = error_info.message(); });
        return Status::RuntimeError(fmt::format("Here something wrong while createObjectFile: {}", error_message));
    }

    std::unique_ptr<JITModuleMemoryManager> module_memory_manager = std::make_unique<JITModuleMemoryManager>();
    llvm::RuntimeDyld dynamic_linker = {*module_memory_manager, *symbol_resolver};

    std::unique_ptr<llvm::RuntimeDyld::LoadedObjectInfo> linked_object = dynamic_linker.loadObject(*object.get());

    dynamic_linker.resolveRelocations();
    module_memory_manager->finalizeMemory(nullptr);

    for (const auto & function : *module) {
        if (function.isDeclaration())
            continue;

        auto function_name = std::string(function.getName());

        auto mangled_name = get_mangled_name(function_name);
        auto jit_symbol = dynamic_linker.getSymbol(mangled_name);

        if (!jit_symbol)
            return Status::RuntimeError(fmt::format("cannot find symbol: {}", function.getName()));;

        auto * jit_symbol_address = reinterpret_cast<void *>(jit_symbol.getAddress());
        result.function_name_to_symbol.emplace(std::move(function_name), jit_symbol_address);
    }

    result.size = module_memory_manager->allocated_size();
    result.identifier = current_module_key;

    module_identifier_to_memory_manager[current_module_key] = std::move(module_memory_manager);

    compiled_code_size.fetch_add(result.size, std::memory_order_relaxed);

    return Status::OK();
}

void JIT::delete_compiled_module(const JIT::CompiledModule & module) {
    std::lock_guard<std::mutex> lock(jit_lock);

    auto module_it = module_identifier_to_memory_manager.find(module.identifier);
    if (module_it != module_identifier_to_memory_manager.end())
        module_identifier_to_memory_manager.erase(module_it);

    compiled_code_size.fetch_sub(module.size, std::memory_order_relaxed);
}

void JIT::register_external_symbol(const std::string & symbol_name, void * address) {
    std::lock_guard<std::mutex> lock(jit_lock);
    symbol_resolver->registerSymbol(symbol_name, address);
}

std::string JIT::get_mangled_name(const std::string & name_to_mangle) const {
    std::string mangled_name;
    llvm::raw_string_ostream mangled_name_stream(mangled_name);
    llvm::Mangler::getNameWithPrefix(mangled_name_stream, name_to_mangle, layout);
    mangled_name_stream.flush();

    return mangled_name;
}

void JIT::_run_optimization_passes_on_module(llvm::Module& module) const {
    llvm::PassManagerBuilder pass_manager_builder;
    llvm::legacy::PassManager mpm;
    llvm::legacy::FunctionPassManager fpm(&module);
    pass_manager_builder.OptLevel = 3;
    pass_manager_builder.SLPVectorize = false;
    pass_manager_builder.LoopVectorize = true;
    pass_manager_builder.RerollLoops = false;
    pass_manager_builder.VerifyInput = true;
    pass_manager_builder.VerifyOutput = true;
    machine->adjustPassManager(pass_manager_builder);

    fpm.add(llvm::createTargetTransformInfoWrapperPass(machine->getTargetIRAnalysis()));
    mpm.add(llvm::createTargetTransformInfoWrapperPass(machine->getTargetIRAnalysis()));

    pass_manager_builder.populateFunctionPassManager(fpm);
    pass_manager_builder.populateModulePassManager(mpm);

    fpm.doInitialization();
    for (auto & function : module)
        fpm.run(function);
    fpm.doFinalization();

    mpm.run(module);
}

std::unique_ptr<llvm::TargetMachine> JIT::_get_target_machine() {
    static std::once_flag llvm_target_initialized;
    std::call_once(llvm_target_initialized, []() {
        llvm::InitializeNativeTarget();
        llvm::InitializeNativeTargetAsmPrinter();
        llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
    });

    std::string error;
    auto cpu = llvm::sys::getHostCPUName();
    auto triple = llvm::sys::getProcessTriple();
    const auto * target = llvm::TargetRegistry::lookupTarget(triple, error);
    if (!target)
        LOG(ERROR) << "Cannot find target";

    llvm::SubtargetFeatures features;
    llvm::StringMap<bool> feature_map;
    if (llvm::sys::getHostCPUFeatures(feature_map))
        for (auto & f : feature_map)
            features.AddFeature(f.first(), f.second);

    llvm::TargetOptions options;

    bool jit = true;
    auto * target_machine = target->createTargetMachine(triple,
        cpu,
        features.getString(),
        options,
        llvm::None,
        llvm::None,
        llvm::CodeGenOpt::Aggressive,
        jit);

    if (!target_machine)
        LOG(ERROR) << "Cannot create target machine";

    return std::unique_ptr<llvm::TargetMachine>(target_machine);
}

} // namespace doris

#endif
