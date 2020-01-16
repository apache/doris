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

#include "codegen/llvm_codegen.h"

#include <fstream>
#include <mutex>
#include <iostream>
#include <sstream>
#include <boost/thread/mutex.hpp>

#include <llvm/IR/DataLayout.h>
#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/InstructionSimplify.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JIT.h>
#include <llvm/Bitcode/ReaderWriter.h>
#include <llvm/PassManager.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/NoFolder.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Support/system_error.h>
#include <llvm/Support/InstIterator.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils/Cloning.h>

#include "common/logging.h"
#include "codegen/subexpr_elimination.h"
#include "codegen/doris_ir_data.h"
#include "doris_ir/doris_ir_names.h"
#include "util/cpu_info.h"
#include "util/path_builder.h"

using llvm::Value;
using llvm::Function;
using llvm::Module;
using llvm::PassManager;
using llvm::PassManagerBuilder;
using llvm::DataLayout;
using llvm::FunctionPassManager;

namespace doris {

static std::mutex s_llvm_initialization_lock;
static bool s_llvm_initialized = false;

void LlvmCodeGen::initialize_llvm(bool load_backend) {
    std::unique_lock<std::mutex> initialization_lock(s_llvm_initialization_lock);
    if (s_llvm_initialized) {
        return;
    }

    // This allocates a global llvm struct and enables multithreading.
    // There is no real good time to clean this up but we only make it once.
    bool result = llvm::llvm_start_multithreaded();
    DCHECK(result);
    // This can *only* be called once per process and is used to setup
    // dynamically linking jitted code.
    llvm::InitializeNativeTarget();
    s_llvm_initialized = true;

    if (load_backend) {
        std::string path;
        // For test env, we have to load libfesupport.so to provide sym for LLVM.
        PathBuilder::get_full_build_path("service/libfesupport.so", &path);
        bool failed = llvm::sys::DynamicLibrary::LoadLibraryPermanently(path.c_str());
        DCHECK_EQ(failed, 0);
    }
}

LlvmCodeGen::LlvmCodeGen(ObjectPool* pool, const std::string& name) :
        _name(name),
        _profile(pool, "CodeGen"),
        _optimizations_enabled(false),
        _is_corrupt(false),
        _is_compiled(false),
        _context(new llvm::LLVMContext()),
        _module(NULL),
        _execution_engine(NULL),
        _scratch_buffer_offset(0),
        _debug_trace_fn(NULL) {
    DCHECK(s_llvm_initialized) << "Must call LlvmCodeGen::initialize_llvm first.";

    _load_module_timer = ADD_TIMER(&_profile, "LoadTime");
    _prepare_module_timer = ADD_TIMER(&_profile, "PrepareTime");
    _module_file_size = ADD_COUNTER(&_profile, "ModuleFileSize", TUnit::BYTES);
    _codegen_timer = ADD_TIMER(&_profile, "CodegenTime");
    _optimization_timer = ADD_TIMER(&_profile, "OptimizationTime");
    _compile_timer = ADD_TIMER(&_profile, "CompileTime");

    _loaded_functions.resize(IRFunction::FN_END);
}

Status LlvmCodeGen::load_from_file(
        ObjectPool* pool,
        const std::string& file,
        boost::scoped_ptr<LlvmCodeGen>* codegen) {
    codegen->reset(new LlvmCodeGen(pool, ""));
    SCOPED_TIMER((*codegen)->_profile.total_time_counter());
    SCOPED_TIMER((*codegen)->_load_module_timer);
    llvm::OwningPtr<llvm::MemoryBuffer> file_buffer;
    llvm::error_code err = llvm::MemoryBuffer::getFile(file, file_buffer);

    if (err.value() != 0) {
        std::stringstream ss;
        ss << "Could not load module " << file << ": " << err.message();
        return Status::InternalError(ss.str());
    }

    COUNTER_UPDATE((*codegen)->_module_file_size, file_buffer->getBufferSize());
    std::string error;
    llvm::Module* loaded_module = NULL;
    // llvm::ParseBitcodeFile(file_buffer.get(),
    //                        (*codegen)->context(), &error);

    if (loaded_module == NULL) {
        std::stringstream ss;
        ss << "Could not parse module " << file << ": " << error;
        return Status::InternalError(ss.str());
    }

    (*codegen)->_module = loaded_module;

    return (*codegen)->init();
}

Status LlvmCodeGen::load_from_memory(
        ObjectPool* pool, llvm::MemoryBuffer* module_ir,
        const std::string& module_name, const std::string& id, 
        boost::scoped_ptr<LlvmCodeGen>* codegen) {
    codegen->reset(new LlvmCodeGen(pool, id));
    SCOPED_TIMER((*codegen)->_profile.total_time_counter());

    llvm::Module* loaded_module = NULL;
    RETURN_IF_ERROR(load_module_from_memory(
            codegen->get(), module_ir, module_name, &loaded_module));
    (*codegen)->_module = loaded_module;

    return (*codegen)->init();
}

Status LlvmCodeGen::load_module_from_memory(
        LlvmCodeGen* codegen, llvm::MemoryBuffer* module_ir,
        const std::string& module_name, llvm::Module** module) {
    SCOPED_TIMER(codegen->_prepare_module_timer);
    std::string error;
    *module = llvm::ParseBitcodeFile(module_ir, codegen->context(), &error);
    if (*module == NULL) {
        std::stringstream ss;
        ss << "Could not parse module " << module_name << ": " << error;
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status LlvmCodeGen::load_doris_ir(
        ObjectPool* pool, 
        const std::string& id,
        boost::scoped_ptr<LlvmCodeGen>* codegen_ret) {
    // Select the appropriate IR version.  We cannot use LLVM IR with sse instructions on
    // a machine without sse support (loading the module will fail regardless of whether
    // those instructions are run or not).
    llvm::StringRef module_ir;
    std::string module_name;
    if (CpuInfo::is_supported(CpuInfo::SSE4_2)) {
        module_ir = llvm::StringRef(reinterpret_cast<const char*>(doris_sse_llvm_ir),
                              doris_sse_llvm_ir_len);
        module_name = "Doris IR with SSE support";
    } else {
        module_ir = llvm::StringRef(reinterpret_cast<const char*>(doris_no_sse_llvm_ir),
                              doris_no_sse_llvm_ir_len);
        module_name = "Doris IR with no SSE support";
    }
    boost::scoped_ptr<llvm::MemoryBuffer> module_ir_buf(
        llvm::MemoryBuffer::getMemBuffer(module_ir, "", false));
    RETURN_IF_ERROR(load_from_memory(pool, module_ir_buf.get(), module_name, id,
                                   codegen_ret));
    LlvmCodeGen* codegen = codegen_ret->get();

    // Parse module for cross compiled functions and types
    SCOPED_TIMER(codegen->_profile.total_time_counter());
    SCOPED_TIMER(codegen->_load_module_timer);

    // Get type for StringValue
    codegen->_string_val_type = codegen->get_type(StringValue::s_llvm_class_name);
    codegen->_decimal_val_type = codegen->get_type(DecimalValue::_s_llvm_class_name);
    // Get type for DateTimeValue
    codegen->_datetime_val_type = codegen->get_type(DateTimeValue::_s_llvm_class_name);

    // Verify size is correct
    const llvm::DataLayout* data_layout = codegen->execution_engine()->getDataLayout();
    const llvm::StructLayout* layout =
        data_layout->getStructLayout(static_cast<llvm::StructType*>(codegen->_string_val_type));

    if (layout->getSizeInBytes() != sizeof(StringValue)) {
        DCHECK_EQ(layout->getSizeInBytes(), sizeof(StringValue));
        return Status::InternalError("Could not create llvm struct type for StringVal");
    }

    // Parse functions from module
    std::vector<llvm::Function*> functions;
    codegen->get_functions(&functions);
    int parsed_functions = 0;

    for (int i = 0; i < functions.size(); ++i) {
        std::string fn_name = functions[i]->getName();

        for (int j = IRFunction::FN_START; j < IRFunction::FN_END; ++j) {
            // Substring match to match precompiled functions.  The compiled function names
            // will be mangled.
            // TODO: reconsider this.  Substring match is probably not strict enough but
            // undoing the mangling is no fun either.
            if (fn_name.find(FN_MAPPINGS[j].fn_name) != std::string::npos) {
                if (codegen->_loaded_functions[FN_MAPPINGS[j].fn] != NULL) {
                    return Status::InternalError("Duplicate definition found for function: " + fn_name);
                }

                codegen->_loaded_functions[FN_MAPPINGS[j].fn] = functions[i];
                ++parsed_functions;
            }
        }
    }

    if (parsed_functions != IRFunction::FN_END) {
        std::stringstream ss;
        ss << "Unable to find these precompiled functions: ";
        bool first = true;

        for (int i = IRFunction::FN_START; i != IRFunction::FN_END; ++i) {
            if (codegen->_loaded_functions[i] == NULL) {
                if (!first) {
                    ss << ", ";
                }

                ss << FN_MAPPINGS[i].fn_name;
                first = false;
            }
        }

        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status LlvmCodeGen::init() {
    if (_module == NULL) {
        _module = new llvm::Module(_name, context());
    }

    llvm::CodeGenOpt::Level opt_level = llvm::CodeGenOpt::Aggressive;
#ifndef NDEBUG
    // For debug builds, don't generate JIT compiled optimized assembly.
    // This takes a non-neglible amount of time (~.5 ms per function) and
    // blows up the fe tests (which take ~10-20 ms each).
    opt_level = llvm::CodeGenOpt::None;
#endif
    llvm::EngineBuilder builder = llvm::EngineBuilder(_module).setOptLevel(opt_level);
    // TODO Uncomment the below line as soon as we upgrade to LLVM 3.5 to enable SSE, if
    // available. In LLVM 3.3 this is done automatically and cannot be enabled because
    // for some reason SSE4 intrinsics selection will not work.
    // builder.setMCPU(llvm::sys::getHostCPUName());
    builder.setErrorStr(&_error_string);
    _execution_engine.reset(builder.create());
    if (_execution_engine == NULL) {
        // _execution_engine will take ownership of the module if it is created
        delete _module;
        std::stringstream ss;
        ss << "Could not create ExecutionEngine: " << _error_string;
        return Status::InternalError(ss.str());
    }
    _void_type = llvm::Type::getVoidTy(context());
    _ptr_type = llvm::PointerType::get(get_type(TYPE_TINYINT), 0);
    _true_value = llvm::ConstantInt::get(context(), llvm::APInt(1, true, true));
    _false_value = llvm::ConstantInt::get(context(), llvm::APInt(1, false, true));

    RETURN_IF_ERROR(load_intrinsics());

    return Status::OK();
}

LlvmCodeGen::~LlvmCodeGen() {
    for (auto& it : _jitted_functions) {
        _execution_engine->freeMachineCodeForFunction(it.first);
    }
}

void LlvmCodeGen::enable_optimizations(bool enable) {
    _optimizations_enabled = enable;
}

std::string LlvmCodeGen::get_ir(bool full_module) const {
    std::string str;
    llvm::raw_string_ostream stream(str);
    if (full_module) {
        _module->print(stream, NULL);
    } else {
        for (int i = 0; i < _codegend_functions.size(); ++i) {
            _codegend_functions[i]->print(stream, NULL);
        }
    }
    return str;
}

llvm::PointerType* LlvmCodeGen::get_ptr_type(llvm::Type* type) {
    return llvm::PointerType::get(type, 0);
}

llvm::Type* LlvmCodeGen::get_type(const PrimitiveType& type) {
    switch (type) {
    case TYPE_NULL:
        return llvm::Type::getInt1Ty(context());
    case TYPE_BOOLEAN:
        return llvm::Type::getInt1Ty(context());
    case TYPE_TINYINT:
        return llvm::Type::getInt8Ty(context());
    case TYPE_SMALLINT:
        return llvm::Type::getInt16Ty(context());
    case TYPE_INT:
        return llvm::Type::getInt32Ty(context());
    case TYPE_BIGINT:
        return llvm::Type::getInt64Ty(context());
    case TYPE_LARGEINT:
         return llvm::Type::getIntNTy(context(), 128);
    case TYPE_FLOAT:
        return llvm::Type::getFloatTy(context());
    case TYPE_DOUBLE:
        return llvm::Type::getDoubleTy(context());
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
        return _string_val_type;
    case TYPE_DECIMAL:
        return _decimal_val_type;
    case TYPE_DATE:
    case TYPE_DATETIME:
        return _datetime_val_type;
    default:
        DCHECK(false) << "Invalid type.";
        return NULL;
    }
}

llvm::Type* LlvmCodeGen::get_type(const TypeDescriptor& type) {
    return get_type(type.type);
}

llvm::PointerType* LlvmCodeGen::get_ptr_type(const TypeDescriptor& type) {
    return llvm::PointerType::get(get_type(type.type), 0);
}

llvm::PointerType* LlvmCodeGen::get_ptr_type(const PrimitiveType& type) {
    return llvm::PointerType::get(get_type(type), 0);
}


llvm::Type* LlvmCodeGen::get_type(const std::string& name) {
    return _module->getTypeByName(name);
}

llvm::PointerType* LlvmCodeGen::get_ptr_type(const std::string& name) {
    llvm::Type* type = get_type(name);
    DCHECK(type != NULL) << name;
    return llvm::PointerType::get(type, 0);
}

// Llvm doesn't let you create a PointerValue from a c-side ptr.  Instead
// cast it to an int and then to 'type'.
llvm::Value* LlvmCodeGen::cast_ptr_to_llvm_ptr(llvm::Type* type, void* ptr) {
    llvm::Constant* const_int = llvm::ConstantInt::get(
            llvm::Type::getInt64Ty(context()), (int64_t)ptr);
    return llvm::ConstantExpr::getIntToPtr(const_int, type);
}

llvm::Value* LlvmCodeGen::get_int_constant(PrimitiveType type, int64_t val) {
    switch (type) {
    case TYPE_NULL:
        return llvm::ConstantInt::get(context(), llvm::APInt(8, val));
    case TYPE_TINYINT:
        return llvm::ConstantInt::get(context(), llvm::APInt(8, val));
    case TYPE_SMALLINT:
        return llvm::ConstantInt::get(context(), llvm::APInt(16, val));
    case TYPE_INT:
        return llvm::ConstantInt::get(context(), llvm::APInt(32, val));
    case TYPE_BIGINT:
        return llvm::ConstantInt::get(context(), llvm::APInt(64, val));
    case TYPE_LARGEINT:
        return llvm::ConstantInt::get(context(), llvm::APInt(128, val));
    default:
        DCHECK(false);
        return NULL;
    }
}

llvm::AllocaInst* LlvmCodeGen::create_entry_block_alloca(
        llvm::Function* f,
        const NamedVariable& var) {
    llvm::IRBuilder<> tmp(&f->getEntryBlock(), f->getEntryBlock().begin());
    return tmp.CreateAlloca(var.type, 0, var.name.c_str());
}

llvm::AllocaInst* LlvmCodeGen::create_entry_block_alloca(
        const LlvmBuilder& builder, llvm::Type* type, const char* name) {
    return create_entry_block_alloca(
        builder.GetInsertBlock()->getParent(), NamedVariable(name, type));
}

void LlvmCodeGen::create_if_else_blocks(
        llvm::Function* fn, const std::string& if_name,
        const std::string& else_name, llvm::BasicBlock** if_block, llvm::BasicBlock** else_block,
        llvm::BasicBlock* insert_before) {
    *if_block = llvm::BasicBlock::Create(context(), if_name, fn, insert_before);
    *else_block = llvm::BasicBlock::Create(context(), else_name, fn, insert_before);
}

llvm::Function* LlvmCodeGen::get_lib_c_function(FnPrototype* prototype) {
    if (_external_functions.find(prototype->name()) != _external_functions.end()) {
        return _external_functions[prototype->name()];
    }

    llvm::Function* func = prototype->generate_prototype();
    _external_functions[prototype->name()] = func;
    return func;
}

llvm::Function* LlvmCodeGen::get_function(IRFunction::Type function) {
    DCHECK(_loaded_functions[function] != NULL);
    return _loaded_functions[function];
}

// There is an llvm bug (#10957) that causes the first step of the verifier to always
// abort the process if it runs into an issue and ignores ReturnStatusAction.  This
// would cause Doris to go down if one query has a problem.
// To work around this, we will copy that step here and not abort on error.
// TODO: doesn't seem there is much traction in getting this fixed but we'll see
bool LlvmCodeGen::verify_function(llvm::Function* fn) {
    if (_is_corrupt) {
        return false;
    }

    // Check that there are no calls to Expr::GetConstant(). These should all have been
    // inlined via Expr::InlineConstants().
    for (llvm::inst_iterator iter = inst_begin(fn); iter != inst_end(fn); ++iter) {
        llvm::Instruction* instr = &*iter;
        if (!llvm::isa<llvm::CallInst>(instr)) {
            continue;
        }
        llvm::CallInst* call_instr = reinterpret_cast<llvm::CallInst*>(instr);
        llvm::Function* called_fn = call_instr->getCalledFunction();
        // look for call to Expr::GetConstant()
        if (called_fn != NULL && called_fn->getName().find(
                    Expr::_s_get_constant_symbol_prefix) != std::string::npos) {
            LOG(ERROR) << "Found call to Expr::GetConstant(): " << print(call_instr);
            _is_corrupt = true;
            break;
        }
    }

    // There is an llvm bug (#10957) that causes the first step of the verifier to always
    // abort the process if it runs into an issue and ignores ReturnStatusAction.  This
    // would cause impalad to go down if one query has a problem.  To work around this, we
    // will copy that step here and not abort on error. Adapted from the pre-verifier
    // function pass.
    // TODO: doesn't seem there is much traction in getting this fixed but we'll see
    for (llvm::Function::iterator i = fn->begin(), e = fn->end(); i != e; ++i) {
        if (i->empty() || !i->back().isTerminator()) {
            LOG(ERROR) << "Basic block must end with terminator: \n" << print(&(*i));
            _is_corrupt = true;
            break;
        }
    }

    if (!_is_corrupt) {
        _is_corrupt = llvm::verifyFunction(*fn, llvm::PrintMessageAction);
    }

    if (_is_corrupt) {
        std::string fn_name = fn->getName(); // llvm has some fancy operator overloading
        LOG(ERROR) << "Function corrupt: " << fn_name;
        return false;
    }
    return true;
}

LlvmCodeGen::FnPrototype::FnPrototype(
    LlvmCodeGen* gen, const std::string& name, llvm::Type* ret_type) :
    _codegen(gen), _name(name), _ret_type(ret_type) {
    DCHECK(!_codegen->_is_compiled) << "Not valid to add additional functions";
}

llvm::Function* LlvmCodeGen::FnPrototype::generate_prototype(
        LlvmBuilder* builder, llvm::Value** params) {
    std::vector<llvm::Type*> arguments;
    for (int i = 0; i < _args.size(); ++i) {
        arguments.push_back(_args[i].type);
    }
    llvm::FunctionType* prototype = llvm::FunctionType::get(_ret_type, arguments, false);
    llvm::Function* fn = llvm::Function::Create(
        prototype, llvm::Function::ExternalLinkage, _name, _codegen->_module);
    DCHECK(fn != NULL);

    // Name the arguments
    int idx = 0;
    for (llvm::Function::arg_iterator iter = fn->arg_begin();
            iter != fn->arg_end(); ++iter, ++idx) {
        iter->setName(_args[idx].name);
        if (params != NULL) {
            params[idx] = iter;
        }
    }

    if (builder != NULL) {
        llvm::BasicBlock* entry_block = llvm::BasicBlock::Create(_codegen->context(), "entry", fn);
        builder->SetInsertPoint(entry_block);
    }

    _codegen->_codegend_functions.push_back(fn);
    return fn;
}

llvm::Function* LlvmCodeGen::replace_call_sites(
        llvm::Function* caller, bool update_in_place,
        llvm::Function* new_fn, const std::string& replacee_name, int* replaced) {
    DCHECK(caller->getParent() == _module);

    if (!update_in_place) {
        // Clone the function and add it to the module
        llvm::ValueToValueMapTy dummy_vmap;
        llvm::Function* new_caller = llvm::CloneFunction(caller, dummy_vmap, false);
        new_caller->copyAttributesFrom(caller);
        _module->getFunctionList().push_back(new_caller);
        caller = new_caller;
    } else if (_jitted_functions.find(caller) != _jitted_functions.end()) {
        // This function is already dynamically linked, unlink it.
        _execution_engine->freeMachineCodeForFunction(caller);
        _jitted_functions.erase(caller);
    }

    *replaced = 0;
    // loop over all blocks
    llvm::Function::iterator block_iter = caller->begin();

    while (block_iter != caller->end()) {
        llvm::BasicBlock* block = block_iter++;
        // loop over instructions in the block
        llvm::BasicBlock::iterator instr_iter = block->begin();

        while (instr_iter != block->end()) {
            llvm::Instruction* instr = instr_iter++;

            // look for call instructions
            if (llvm::CallInst::classof(instr)) {
                llvm::CallInst* call_instr = reinterpret_cast<llvm::CallInst*>(instr);
                llvm::Function* old_fn = call_instr->getCalledFunction();

                // look for call instruction that matches the name
                if (old_fn->getName().find(replacee_name) != std::string::npos) {
                    // Replace the called function
                    call_instr->setCalledFunction(new_fn);
                    ++*replaced;
                }
            }
        }
    }

    return caller;
}

Function* LlvmCodeGen::clone_function(Function* fn) {
    llvm::ValueToValueMapTy dummy_vmap;
    // CloneFunction() automatically gives the new function a unique name
    Function* fn_clone = llvm::CloneFunction(fn, dummy_vmap, false);
    fn_clone->copyAttributesFrom(fn);
    _module->getFunctionList().push_back(fn_clone);
    return fn_clone;
}

// TODO: revisit this.  Inlining all call sites might not be the right call.  We
// probably need to make this more complicated and somewhat cost based or write
// our own optimization passes.
int LlvmCodeGen::inline_call_sites(llvm::Function* fn, bool skip_registered_fns) {
    int functions_inlined = 0;
    // Collect all call sites
    std::vector<llvm::CallInst*> call_sites;

    // loop over all blocks
    llvm::Function::iterator block_iter = fn->begin();

    while (block_iter != fn->end()) {
        llvm::BasicBlock* block = block_iter++;
        // loop over instructions in the block
        llvm::BasicBlock::iterator instr_iter = block->begin();

        while (instr_iter != block->end()) {
            llvm::Instruction* instr = instr_iter++;

            // look for call instructions
            if (llvm::CallInst::classof(instr)) {
                llvm::CallInst* call_instr = reinterpret_cast<llvm::CallInst*>(instr);
                llvm::Function* called_fn = call_instr->getCalledFunction();

                if (skip_registered_fns) {
                    if (_registered_exprs.find(called_fn) != _registered_exprs.end()) {
                        continue;
                    }
                }

                call_sites.push_back(call_instr);
            }
        }
    }

    // Inline all call sites.  InlineFunction can still fail (function is recursive, etc)
    // but that always leaves the original function in a consistent state
    for (int i = 0; i < call_sites.size(); ++i) {
        llvm::InlineFunctionInfo info;

        if (llvm::InlineFunction(call_sites[i], info)) {
            ++functions_inlined;
        }
    }

    return functions_inlined;
}

llvm::Function* LlvmCodeGen::optimize_function_with_exprs(llvm::Function* fn) {
    int num_inlined = 0;
    do {
        // This assumes that all redundant exprs have been registered.
        num_inlined = inline_call_sites(fn, false);
    } while (num_inlined > 0);
    // TODO(zc): fix
    // SubExprElimination subexpr_elim(this);
    // subexpr_elim.run(fn);
    return finalize_function(fn);
}

llvm::Function* LlvmCodeGen::finalize_function(llvm::Function* function) {
    if (!verify_function(function)) {
        return NULL;
    }

    return function;
}

Status LlvmCodeGen::finalize_module() {
    DCHECK(!_is_compiled);
    _is_compiled = true;

    // TODO(zc)
#if 0
    if (FLAGS_unopt_module_dir.size() != 0) {
        string path = FLAGS_unopt_module_dir + "/" + id_ + "_unopt.ll";
        fstream f(path.c_str(), fstream::out | fstream::trunc);
        if (f.fail()) {
            LOG(ERROR) << "Could not save IR to: " << path;
        } else {
            f << GetIR(true);
            f.close();
        }
    }
#endif

    if (_is_corrupt) {
        return Status::InternalError("Module is corrupt.");
    }
    SCOPED_TIMER(_profile.total_time_counter());

    // Don't waste time optimizing module if there are no functions to JIT. This can happen
    // if the codegen object is created but no functions are successfully codegen'd.
    if (_optimizations_enabled // TODO(zc): && !FLAGS_disable_optimization_passes 
            && !_fns_to_jit_compile.empty()) {
        optimize_module();
    }

    SCOPED_TIMER(_compile_timer);
    // JIT compile all codegen'd functions
    for (int i = 0; i < _fns_to_jit_compile.size(); ++i) {
        *_fns_to_jit_compile[i].second = jit_function(_fns_to_jit_compile[i].first);
    }
#if 0
    if (FLAGS_opt_module_dir.size() != 0) {
        string path = FLAGS_opt_module_dir + "/" + id_ + "_opt.ll";
        fstream f(path.c_str(), fstream::out | fstream::trunc);
        if (f.fail()) {
            LOG(ERROR) << "Could not save IR to: " << path;
        } else {
            f << GetIR(true);
            f.close();
        }
    }
#endif

    return Status::OK();
}

void LlvmCodeGen::optimize_module() {
    SCOPED_TIMER(_optimization_timer);

    // This pass manager will construct optimizations passes that are "typical" for
    // c/c++ programs.  We're relying on llvm to pick the best passes for us.
    // TODO: we can likely muck with this to get better compile speeds or write
    // our own passes.  Our subexpression elimination optimization can be rolled into
    // a pass.
    PassManagerBuilder pass_builder;
    // 2 maps to -O2
    // TODO: should we switch to 3? (3 may not produce different IR than 2 while taking
    // longer, but we should check)
    pass_builder.OptLevel = 2;
    // Don't optimize for code size (this corresponds to -O2/-O3)
    pass_builder.SizeLevel = 0;
    pass_builder.Inliner = llvm::createFunctionInliningPass() ;

    // Specifying the data layout is necessary for some optimizations (e.g. removing many
    // of the loads/stores produced by structs).
    const std::string& data_layout_str = _module->getDataLayout();
    DCHECK(!data_layout_str.empty());

    // Before running any other optimization passes, run the internalize pass, giving it
    // the names of all functions registered by AddFunctionToJit(), followed by the
    // global dead code elimination pass. This causes all functions not registered to be
    // JIT'd to be marked as internal, and any internal functions that are not used are
    // deleted by DCE pass. This greatly decreases compile time by removing unused code.
    std::vector<const char*> exported_fn_names;
    for (int i = 0; i < _fns_to_jit_compile.size(); ++i) {
        exported_fn_names.push_back(_fns_to_jit_compile[i].first->getName().data());
    }
    boost::scoped_ptr<PassManager> module_pass_manager(new PassManager());
    module_pass_manager->add(new DataLayout(data_layout_str));
    module_pass_manager->add(llvm::createInternalizePass(exported_fn_names));
    module_pass_manager->add(llvm::createGlobalDCEPass());
    module_pass_manager->run(*_module);

    // Create and run function pass manager
    boost::scoped_ptr<FunctionPassManager> fn_pass_manager(new FunctionPassManager(_module));
    fn_pass_manager->add(new DataLayout(data_layout_str));
    pass_builder.populateFunctionPassManager(*fn_pass_manager);
    fn_pass_manager->doInitialization();
    for (Module::iterator it = _module->begin(), end = _module->end(); it != end; ++it) {
        if (!it->isDeclaration()) fn_pass_manager->run(*it);
    }
    fn_pass_manager->doFinalization();

    // Create and run module pass manager
    module_pass_manager.reset(new PassManager());
    module_pass_manager->add(new DataLayout(data_layout_str));
    pass_builder.populateModulePassManager(*module_pass_manager);
    module_pass_manager->run(*_module);

    // if (FLAGS_print_llvm_ir_instruction_count) {
    //     for (int i = 0; i < _fns_to_jit_compile.size(); ++i) {
    //         InstructionCounter counter;
    //         counter.visit(*_fns_to_jit_compile[i].first);
    //         VLOG(1) << _fns_to_jit_compile[i].first->getName().str();
    //         VLOG(1) << counter.PrintCounters();
    //     }
    // }
}

void LlvmCodeGen::add_function_to_jit(llvm::Function* fn, void** fn_ptr) {
#if 0
    llvm::Type* decimal_val_type = get_type(CodegenAnyVal::LLVM_DECIMALVAL_NAME);
    if (fn->getReturnType() == decimal_val_type) {
        // Per the x86 calling convention ABI, DecimalVals should be returned via an extra
        // first DecimalVal* argument. We generate non-compliant functions that return the
        // DecimalVal directly, which we can call from generated code, but not from compiled
        // native code.  To avoid accidentally calling a non-compliant function from native
        // code, call 'function' from an ABI-compliant wrapper.
        stringstream name;
        name << fn->getName().str() << "ABIWrapper";
        LlvmCodeGen::FnPrototype prototype(this, name.str(), void_type_);
        // Add return argument
        prototype.AddArgument(NamedVariable("result", decimal_val_type->getPointerTo()));
        // Add regular arguments
        for (Function::arg_iterator arg = fn->arg_begin(); arg != fn->arg_end(); ++arg) {
            prototype.AddArgument(NamedVariable(arg->getName(), arg->getType()));
        }
        LlvmBuilder builder(context());
        Value* args[fn->arg_size() + 1];
        Function* fn_wrapper = prototype.GeneratePrototype(&builder, &args[0]);
        fn_wrapper->addFnAttr(llvm::Attribute::AlwaysInline);
        // Mark first argument as sret (not sure if this is necessary but it can't hurt)
        fn_wrapper->addAttribute(1, Attribute::StructRet);
        // Call 'fn' and store the result in the result argument
        Value* result =
            builder.CreateCall(fn, ArrayRef<Value*>(&args[1], fn->arg_size()), "result");
        builder.CreateStore(result, args[0]);
        builder.CreateRetVoid();
        fn = FinalizeFunction(fn_wrapper);
        DCHECK(fn != NULL);
    }
#endif
    _fns_to_jit_compile.push_back(std::make_pair(fn, fn_ptr));
}


void* LlvmCodeGen::jit_function(llvm::Function* function, int* scratch_size) {
    if (_is_corrupt) {
        return NULL;
    }

    if (scratch_size == NULL) {
        DCHECK_EQ(_scratch_buffer_offset, 0);
    } else {
        *scratch_size = _scratch_buffer_offset;
    }

    // TODO: log a warning if the jitted function is too big (larger than I cache)
    void* jitted_function = _execution_engine->getPointerToFunction(function);
    boost::lock_guard<boost::mutex> l(_jitted_functions_lock);

    if (jitted_function != NULL) {
        _jitted_functions[function] = true;
    }

    return jitted_function;
}

int LlvmCodeGen::get_scratch_buffer(int byte_size) {
    // TODO: this is not yet implemented/tested
    DCHECK(false);
    int result = _scratch_buffer_offset;
    // TODO: alignment?
    result += byte_size;
    return result;
}

// Wrapper around printf to make it easier to call from IR
extern "C" void debug_trace(const char* str) {
    printf("LLVM Trace: %s\n", str);
}

void LlvmCodeGen::codegen_debug_trace(LlvmBuilder* builder, const char* str) {
    LOG(ERROR) << "Remove IR codegen debug traces before checking in.";

    // Lazily link in debug function to the module
    if (_debug_trace_fn == NULL) {
        std::vector<llvm::Type*> args;
        args.push_back(_ptr_type);
        llvm::FunctionType* fn_type = llvm::FunctionType::get(_void_type, args, false);
        _debug_trace_fn = llvm::Function::Create(fn_type, llvm::GlobalValue::ExternalLinkage,
                                           "debug_trace", _module);

        DCHECK(_debug_trace_fn != NULL);
        // debug_trace shouldn't already exist (llvm mangles function names if there
        // are duplicates)
        DCHECK(_debug_trace_fn->getName() ==  "debug_trace");

        _debug_trace_fn->setCallingConv(llvm::CallingConv::C);

        // Add a mapping to the execution engine so it can link the debug_trace function
        _execution_engine->addGlobalMapping(_debug_trace_fn,
                                            reinterpret_cast<void*>(&debug_trace));
    }

    // Make a copy of str into memory owned by this object.  This is no guarantee that str is
    // still around when the debug printf is executed.
    _debug_strings.push_back(str);
    str = _debug_strings[_debug_strings.size() - 1].c_str();

    // Call the function by turning 'str' into a constant ptr value
    llvm::Value* str_ptr = cast_ptr_to_llvm_ptr(_ptr_type, const_cast<char*>(str));
    std::vector<llvm::Value*> calling_args;
    calling_args.push_back(str_ptr);
    builder->CreateCall(_debug_trace_fn, calling_args);
}

void LlvmCodeGen::get_functions(std::vector<llvm::Function*>* functions) {
    llvm::Module::iterator fn_iter = _module->begin();

    while (fn_iter != _module->end()) {
        llvm::Function* fn = fn_iter++;

        if (!fn->empty()) {
            functions->push_back(fn);
        }
    }
}

// TODO: cache this function (e.g. all min(int, int) are identical).
// we probably want some more global IR function cache, or, implement this
// in c and precompile it with clang.
// define i32 @Min(i32 %v1, i32 %v2) {
// entry:
//   %0 = icmp slt i32 %v1, %v2
//   br i1 %0, label %ret_v1, label %ret_v2
//
// ret_v1:                                           ; preds = %entry
//   ret i32 %v1
//
// ret_v2:                                           ; preds = %entry
//   ret i32 %v2
// }
llvm::Function* LlvmCodeGen::codegen_min_max(const TypeDescriptor& type, bool min) {
    LlvmCodeGen::FnPrototype prototype(this, min ? "Min" : "Max", get_type(type));
    prototype.add_argument(LlvmCodeGen::NamedVariable("v1", get_type(type)));
    prototype.add_argument(LlvmCodeGen::NamedVariable("v2", get_type(type)));

    llvm::Value* params[2];
    LlvmBuilder builder(context());
    llvm::Function* fn = prototype.generate_prototype(&builder, &params[0]);

    llvm::Value* compare = NULL;

    switch (type.type) {
    case TYPE_NULL:
        compare = false_value();
        break;

    case TYPE_BOOLEAN:
        if (min) {
            // For min, return x && y
            compare = builder.CreateAnd(params[0], params[1]);
        } else {
            // For max, return x || y
            compare = builder.CreateOr(params[0], params[1]);
        }

        break;

    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        if (min) {
            compare = builder.CreateICmpSLT(params[0], params[1]);
        } else {
            compare = builder.CreateICmpSGT(params[0], params[1]);
        }

        break;

    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        if (min) {
            compare = builder.CreateFCmpULT(params[0], params[1]);
        } else {
            compare = builder.CreateFCmpUGT(params[0], params[1]);
        }

        break;

    default:
        DCHECK(false);
    }

    if (type.type == TYPE_BOOLEAN) {
        builder.CreateRet(compare);
    } else {
        llvm::BasicBlock* ret_v1 = NULL;
        llvm::BasicBlock* ret_v2 = NULL;
        create_if_else_blocks(fn, "ret_v1", "ret_v2", &ret_v1, &ret_v2);

        builder.CreateCondBr(compare, ret_v1, ret_v2);
        builder.SetInsertPoint(ret_v1);
        builder.CreateRet(params[0]);
        builder.SetInsertPoint(ret_v2);
        builder.CreateRet(params[1]);
    }

    if (!verify_function(fn)) {
        return NULL;
    }

    return fn;
}

// Intrinsics are loaded one by one.  Some are overloaded (e.g. memcpy) and the types must
// be specified.
// TODO: is there a better way to do this?
Status LlvmCodeGen::load_intrinsics() {
    // Load memcpy
    {
        llvm::Type* types[] = { ptr_type(), ptr_type(), get_type(TYPE_INT) };
        llvm::Function* fn = llvm::Intrinsic::getDeclaration(
                module(), llvm::Intrinsic::memcpy, types);

        if (fn == NULL) {
            return Status::InternalError("Could not find memcpy intrinsic.");
        }

        _llvm_intrinsics[llvm::Intrinsic::memcpy] = fn;
    }

    // TODO: where is the best place to put this?
    struct {
        llvm::Intrinsic::ID id;
        const char* error;
    } non_overloaded_intrinsics[] = {
        { llvm::Intrinsic::x86_sse42_crc32_32_8, "sse4.2 crc32_u8" },
        { llvm::Intrinsic::x86_sse42_crc32_32_16, "sse4.2 crc32_u16" },
        { llvm::Intrinsic::x86_sse42_crc32_32_32, "sse4.2 crc32_u32" },
        { llvm::Intrinsic::x86_sse42_crc32_64_64, "sse4.2 crc32_u64" },
    };
    const int num_intrinsics =
        sizeof(non_overloaded_intrinsics) / sizeof(non_overloaded_intrinsics[0]);

    for (int i = 0; i < num_intrinsics; ++i) {
        llvm::Intrinsic::ID id = non_overloaded_intrinsics[i].id;
        llvm::Function* fn = llvm::Intrinsic::getDeclaration(module(), id);

        if (fn == NULL) {
            std::stringstream ss;
            ss << "Could not find " << non_overloaded_intrinsics[i].error << " intrinsic";
            return Status::InternalError(ss.str());
        }

        _llvm_intrinsics[id] = fn;
    }

    return Status::OK();
}

void LlvmCodeGen::codegen_memcpy(LlvmBuilder* builder, llvm::Value* dst, llvm::Value* src, int size) {
    // Cast src/dst to int8_t*.  If they already are, this will get optimized away
    DCHECK(llvm::PointerType::classof(dst->getType()));
    DCHECK(llvm::PointerType::classof(src->getType()));
    dst = builder->CreateBitCast(dst, ptr_type());
    src = builder->CreateBitCast(src, ptr_type());

    // Get intrinsic function.
    llvm::Function* memcpy_fn = _llvm_intrinsics[llvm::Intrinsic::memcpy];
    DCHECK(memcpy_fn != NULL);

    // The fourth argument is the alignment.  For non-zero values, the caller
    // must guarantee that the src and dst values are aligned to that byte boundary.
    // TODO: We should try to take advantage of this since our tuples are well aligned.
    llvm::Value* args[] = {
        dst, src, get_int_constant(TYPE_INT, size),
        get_int_constant(TYPE_INT, 0),
        false_value()                       // is_volatile.
    };
    builder->CreateCall(memcpy_fn, args);
}

Value* LlvmCodeGen::codegen_array_at(
        LlvmBuilder* builder, Value* array, int idx, const char* name) {
    DCHECK(array->getType()->isPointerTy() || array->getType()->isArrayTy())
        << print(array->getType());
    Value* ptr = builder->CreateConstGEP1_32(array, idx);
    return builder->CreateLoad(ptr, name);
}

void LlvmCodeGen::codegen_assign(LlvmBuilder* builder,
                                llvm::Value* dst, llvm::Value* src, PrimitiveType type) {
    switch (type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR: 
    case TYPE_HLL:  {
        codegen_memcpy(builder, dst, src, sizeof(StringValue));
        break;
    }

    case TYPE_DATETIME:
        DCHECK(false) << "Timestamp NYI"; // TODO
        break;

    default:
        builder->CreateStore(src, dst);
        break;
    }
}

void LlvmCodeGen::clear_hash_fns() {
    _hash_fns.clear();
}

// Codegen to compute hash for a particular byte size.  Loops are unrolled in this
// process.  For the case where num_bytes == 11, we'd do this by calling
//   1. crc64 (for first 8 bytes)
//   2. crc16 (for bytes 9, 10)
//   3. crc8 (for byte 11)
// The resulting IR looks like:
// define i32 @CrcHash11(i8* %data, i32 %len, i32 %seed) {
// entry:
//   %0 = zext i32 %seed to i64
//   %1 = bitcast i8* %data to i64*
//   %2 = getelementptr i64* %1, i32 0
//   %3 = load i64* %2
//   %4 = call i64 @llvm.x86.sse42.crc32.64.64(i64 %0, i64 %3)
//   %5 = trunc i64 %4 to i32
//   %6 = getelementptr i8* %data, i32 8
//   %7 = bitcast i8* %6 to i16*
//   %8 = load i16* %7
//   %9 = call i32 @llvm.x86.sse42.crc32.32.16(i32 %5, i16 %8)
//   %10 = getelementptr i8* %6, i32 2
//   %11 = load i8* %10
//   %12 = call i32 @llvm.x86.sse42.crc32.32.8(i32 %9, i8 %11)
//   ret i32 %12
// }
llvm::Function* LlvmCodeGen::get_hash_function(int num_bytes) {
    if (CpuInfo::is_supported(CpuInfo::SSE4_2)) {
        if (num_bytes == -1) {
            // -1 indicates variable length, just return the generic loop based
            // hash fn.
            return get_function(IRFunction::HASH_CRC);
            return NULL;
        }

        std::map<int, llvm::Function*>::iterator cached_fn = _hash_fns.find(num_bytes);
        if (cached_fn != _hash_fns.end()) {
            return cached_fn->second;
        }

        // Generate a function to hash these bytes
        std::stringstream ss;
        ss << "CrcHash" << num_bytes;
        FnPrototype prototype(this, ss.str(), get_type(TYPE_INT));
        prototype.add_argument(LlvmCodeGen::NamedVariable("data", ptr_type()));
        prototype.add_argument(LlvmCodeGen::NamedVariable("len", get_type(TYPE_INT)));
        prototype.add_argument(LlvmCodeGen::NamedVariable("seed", get_type(TYPE_INT)));

        llvm::Value* args[3];
        LlvmBuilder builder(context());
        llvm::Function* fn = prototype.generate_prototype(&builder, &args[0]);
        llvm::Value* data = args[0];
        llvm::Value* result = args[2];

        llvm::Function* crc8_fn = _llvm_intrinsics[llvm::Intrinsic::x86_sse42_crc32_32_8];
        llvm::Function* crc16_fn = _llvm_intrinsics[llvm::Intrinsic::x86_sse42_crc32_32_16];
        llvm::Function* crc32_fn = _llvm_intrinsics[llvm::Intrinsic::x86_sse42_crc32_32_32];
        llvm::Function* crc64_fn = _llvm_intrinsics[llvm::Intrinsic::x86_sse42_crc32_64_64];

        // Generate the crc instructions starting with the highest number of bytes
        if (num_bytes >= 8) {
            llvm::Value* result_64 = builder.CreateZExt(result, get_type(TYPE_BIGINT));
            llvm::Value* ptr = builder.CreateBitCast(data, get_ptr_type(TYPE_BIGINT));
            int i = 0;

            while (num_bytes >= 8) {
                llvm::Value* index[] = { get_int_constant(TYPE_INT, i++) };
                llvm::Value* d = builder.CreateLoad(builder.CreateGEP(ptr, index));
                result_64 = builder.CreateCall2(crc64_fn, result_64, d);
                num_bytes -= 8;
            }

            result = builder.CreateTrunc(result_64, get_type(TYPE_INT));
            llvm::Value* index[] = { get_int_constant(TYPE_INT, i * 8) };
            // Update data to past the 8-byte chunks
            data = builder.CreateGEP(data, index);
        }

        if (num_bytes >= 4) {
            DCHECK_LT(num_bytes, 8);
            llvm::Value* ptr = builder.CreateBitCast(data, get_ptr_type(TYPE_INT));
            llvm::Value* d = builder.CreateLoad(ptr);
            result = builder.CreateCall2(crc32_fn, result, d);
            llvm::Value* index[] = { get_int_constant(TYPE_INT, 4) };
            data = builder.CreateGEP(data, index);
            num_bytes -= 4;
        }

        if (num_bytes >= 2) {
            DCHECK_LT(num_bytes, 4);
            llvm::Value* ptr = builder.CreateBitCast(data, get_ptr_type(TYPE_SMALLINT));
            llvm::Value* d = builder.CreateLoad(ptr);
            result = builder.CreateCall2(crc16_fn, result, d);
            llvm::Value* index[] = { get_int_constant(TYPE_INT, 2) };
            data = builder.CreateGEP(data, index);
            num_bytes -= 2;
        }

        if (num_bytes > 0) {
            DCHECK_EQ(num_bytes, 1);
            llvm::Value* d = builder.CreateLoad(data);
            result = builder.CreateCall2(crc8_fn, result, d);
            --num_bytes;
        }
        DCHECK_EQ(num_bytes, 0);
        Value* shift_16 = get_int_constant(TYPE_INT, 16);
        Value* upper_bits = builder.CreateShl(result, shift_16);
        Value* lower_bits = builder.CreateLShr(result, shift_16);
        result = builder.CreateOr(upper_bits, lower_bits);
        builder.CreateRet(result);

        fn = finalize_function(fn);
        if (fn != NULL) {
            _hash_fns[num_bytes] = fn;
        }
        return fn;
    } else {
        // Don't bother with optimizations without crc hash instruction
        return get_function(IRFunction::HASH_FNV);
        return NULL;
    }
}

llvm::Value* LlvmCodeGen::get_ptr_to(LlvmBuilder* builder, llvm::Value* v, const char* name) {
    llvm::Value* ptr = create_entry_block_alloca(*builder, v->getType(), name);
    builder->CreateStore(v, ptr);
    return ptr;
}

llvm::Instruction::CastOps LlvmCodeGen::get_cast_op(
        const TypeDescriptor& from_type, const TypeDescriptor& to_type) {
    switch (from_type.type) {
    case TYPE_BOOLEAN: {
        switch (to_type.type) {
        case TYPE_BOOLEAN:
            return llvm::Instruction::Trunc;
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            return llvm::Instruction::ZExt;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            return llvm::Instruction::SIToFP;
        default:
            return llvm::Instruction::CastOpsEnd;
        }
    }
    case TYPE_TINYINT: {
        switch (to_type.type) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            return llvm::Instruction::Trunc;
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            return llvm::Instruction::SExt;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            return llvm::Instruction::SIToFP;
        default:
            return llvm::Instruction::CastOpsEnd;
        }
    }
    case TYPE_SMALLINT: {
        switch (to_type.type) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
            return llvm::Instruction::Trunc;
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            return llvm::Instruction::SExt;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            return llvm::Instruction::SIToFP;
        default:
            return llvm::Instruction::CastOpsEnd;
        }
    }
    case TYPE_INT: {
        switch (to_type.type) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
            return llvm::Instruction::Trunc;
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            return llvm::Instruction::SExt;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            return llvm::Instruction::SIToFP;
        default:
            return llvm::Instruction::CastOpsEnd;
        }
    }
    case TYPE_BIGINT: {
        switch (to_type.type) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
            return llvm::Instruction::Trunc;
        case TYPE_LARGEINT:
            return llvm::Instruction::SExt;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            return llvm::Instruction::SIToFP;
        default:
            return llvm::Instruction::CastOpsEnd;
        }
    }
    case TYPE_LARGEINT: {
        switch (to_type.type) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            return llvm::Instruction::Trunc;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            return llvm::Instruction::SIToFP;
        default:
            return llvm::Instruction::CastOpsEnd;
        }
    }
    case TYPE_FLOAT: {
        switch (to_type.type) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            return llvm::Instruction::FPToSI;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            return llvm::Instruction::FPExt;
        default:
            return llvm::Instruction::CastOpsEnd;
        }
    }
    case TYPE_DOUBLE: {
        switch (to_type.type) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            return llvm::Instruction::FPToSI;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            return llvm::Instruction::FPTrunc;
        default:
            return llvm::Instruction::CastOpsEnd;
        }
    }
    default:
        return llvm::Instruction::CastOpsEnd;
    }
    return llvm::Instruction::CastOpsEnd;
}

}
