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

#ifndef DORIS_BE_SRC_QUERY_CODEGEN_LLVM_CODEGEN_H
#define DORIS_BE_SRC_QUERY_CODEGEN_LLVM_CODEGEN_H

#include <map>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Analysis/Verifier.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Support/MemoryBuffer.h>

#include "common/status.h"
#include "runtime/primitive_type.h"
#include "exprs/expr.h"
#include "util/runtime_profile.h"
#include "doris_ir/doris_ir_functions.h"

// Forward declare all llvm classes to avoid namespace pollution.
namespace llvm {
class AllocaInst;
class BasicBlock;
class ConstantFolder;
class ExecutionEngine;
class Function;
// class FunctionPassManager;
class LLVMContext;
class Module;
class NoFolder;
// class PassManager;
class PointerType;
class StructType;
class TargetData;
class Type;
class Value;

template<bool B, typename T, typename I>
class IRBuilder;

template<bool preserveName>
class IRBuilderDefaultInserter;
}

namespace doris {

class SubExprElimination;

// LLVM code generator.  This is the top level object to generate jitted code.
//
// LLVM provides a c++ IR builder interface so IR does not need to be written
// manually.  The interface is very low level so each line of IR that needs to
// be output maps 1:1 with calls to the interface.
// The llvm documentation is not fantastic and a lot of this was figured out
// by experimenting.  Thankfully, their API is pretty well designed so it's
// possible to get by without great documentation.  The llvm tutorial is very
// helpful, http://llvm.org/docs/tutorial/LangImpl1.html.  In this tutorial, they
// go over how to JIT an AST for a toy language they create.
// It is also helpful to use their online app that lets you compile c/c++ to IR.
// http://llvm.org/demo/index.cgi.
//
// This class provides two interfaces, one for testing and one for the query
// engine.  The interface for the query engine will load the cross-compiled
// IR module (output during the build) and extract all of functions that will
// be called directly.  The test interface can be used to load any precompiled
// module or none at all (but this class will not validate the module).
//
// This class is mostly not threadsafe.  During the Prepare() phase of the fragment
// execution, nodes should codegen functions.
// Afterward, optimize_module() should be called at which point all codegened functions
// are optimized.
// Subsequently, nodes can get at the jit compiled function pointer (typically during the
// Open() call).  Getting the jit compiled function (jit_function()) is the only thread
// safe function.
//
// Currently, each query will create and initialize one of these
// objects.  This requires loading and parsing the cross compiled modules.
// TODO: we should be able to do this once per process and let llvm compile
// functions from across modules.
//
// LLVM has a nontrivial memory management scheme and objects will take
// ownership of others.  The document is pretty good about being explicit with this
// but it is not very intuitive.
// TODO: look into diagnostic output and debuggability
// TODO: confirm that the multi-threaded usage is correct
class LlvmCodeGen {
public:
    // This function must be called once per process before any llvm API calls are
    // made.  LLVM needs to allocate data structures for multi-threading support and
    // to enable dynamic linking of jitted code.
    // if 'load_backend', load the backend static object for llvm.  This is needed
    // when libbackend.so is loaded from java.  llvm will be default only look in
    // the current object and not be able to find the backend symbols
    // TODO: this can probably be removed after Doris refactor where the java
    // side is not loading the be explicitly anymore.
    static void initialize_llvm(bool load_backend = false);

    // Loads and parses the precompiled doris IR module
    // codegen will contain the created object on success.
    static Status load_doris_ir(
        ObjectPool*, const std::string& id, boost::scoped_ptr<LlvmCodeGen>* codegen);

    // Removes all jit compiled dynamically linked functions from the process.
    ~LlvmCodeGen();

    RuntimeProfile* runtime_profile() {
        return &_profile;
    }
    RuntimeProfile::Counter* codegen_timer() {
        return _codegen_timer;
    }

    // Turns on/off optimization passes
    void enable_optimizations(bool enable);

    // For debugging. Returns the IR that was generated.  If full_module, the
    // entire module is dumped, including what was loaded from precompiled IR.
    // If false, only output IR for functions which were generated.
    std::string get_ir(bool full_module) const;

    // Typedef builder in case we want to change the template arguments later
    typedef llvm::IRBuilder<> LlvmBuilder;

    // Utility struct that wraps a variable name and llvm type.
    struct NamedVariable {
        std::string name;
        llvm::Type* type;

        NamedVariable(const std::string& name = "", llvm::Type* type = NULL) {
            this->name = name;
            this->type = type;
        }
    };

    // Abstraction over function prototypes.  Contains helpers to build prototypes and
    // generate IR for the types.
    class FnPrototype {
    public:
        // Create a function prototype object, specifying the name of the function and
        // the return type.
        FnPrototype(LlvmCodeGen*, const std::string& name, llvm::Type* ret_type);

        // Returns name of function
        const std::string& name() const {
            return _name;
        }

        // Add argument
        void add_argument(const NamedVariable& var) {
            _args.push_back(var);
        }

        void add_argument(const std::string& name, llvm::Type* type) {
            _args.push_back(NamedVariable(name, type));
        }

        // Generate LLVM function prototype.
        // If a non-null builder is passed, this function will also create the entry block
        // and set the builder's insert point to there.
        // If params is non-null, this function will also return the arguments
        // values (params[0] is the first arg, etc).
        // In that case, params should be preallocated to be number of arguments
        llvm::Function* generate_prototype(LlvmBuilder* builder = NULL,
                                          llvm::Value** params = NULL);

    private:
        friend class LlvmCodeGen;

        LlvmCodeGen* _codegen;
        std::string _name;
        llvm::Type* _ret_type;
        std::vector<NamedVariable> _args;
    };

    /// Codegens IR to load array[idx] and returns the loaded value. 'array' should be a
    /// C-style array (e.g. i32*) or an IR array (e.g. [10 x i32]). This function does not
    /// do bounds checking.
    llvm::Value* codegen_array_at(
        LlvmBuilder*, llvm::Value* array, int idx, const char* name);

    /// Return a pointer type to 'type'
    llvm::PointerType* get_ptr_type(llvm::Type* type);

    // Returns llvm type for the primitive type
    llvm::Type* get_type(const PrimitiveType& type);

    // Returns llvm type for the primitive type
    llvm::Type* get_type(const TypeDescriptor& type);

    // Return a pointer type to 'type' (e.g. int16_t*)
    llvm::PointerType* get_ptr_type(const TypeDescriptor& type);
    llvm::PointerType* get_ptr_type(const PrimitiveType& type);

    // Returns the type with 'name'.  This is used to pull types from clang
    // compiled IR.  The types we generate at runtime are unnamed.
    // The name is generated by the clang compiler in this form:
    // <class/struct>.<namespace>::<class name>.  For example:
    // "class.doris::AggregationNode"
    llvm::Type* get_type(const std::string& name);

    /// Returns the pointer type of the type returned by GetType(name)
    llvm::PointerType* get_ptr_type(const std::string& name);

    /// Alloca's an instance of the appropriate pointer type and sets it to point at 'v'
    llvm::Value* get_ptr_to(LlvmBuilder* builder, llvm::Value* v, const char* name);

    /// Alloca's an instance of the appropriate pointer type and sets it to point at 'v'
    llvm::Value* get_ptr_to(LlvmBuilder* builder, llvm::Value* v) {
        return get_ptr_to(builder, v, "");
    }

    // Returns reference to llvm context object.  Each LlvmCodeGen has its own
    // context to allow multiple threads to be calling into llvm at the same time.
    llvm::LLVMContext& context() {
        return *_context.get();
    }

    // Returns execution engine interface
    llvm::ExecutionEngine* execution_engine() {
        return _execution_engine.get();
    }

    // Returns the underlying llvm module
    llvm::Module* module() {
        return _module;
    }

    // Register a expr function with unique id.  It can be subsequently retrieved via
    // get_registered_expr_fn with that id.
    void register_expr_fn(int64_t id, llvm::Function* function) {
        DCHECK(_registered_exprs_map.find(id) == _registered_exprs_map.end());
        _registered_exprs_map[id] = function;
        _registered_exprs.insert(function);
    }

    // Returns a registered expr function for id or NULL if it does not exist.
    llvm::Function* get_registered_expr_fn(int64_t id) {
        std::map<int64_t, llvm::Function*>::iterator it = _registered_exprs_map.find(id);

        if (it == _registered_exprs_map.end()) {
            return NULL;
        }

        return it->second;
    }

    /// Optimize and compile the module. This should be called after all functions to JIT
    /// have been added to the module via AddFunctionToJit(). If optimizations_enabled_ is
    /// false, the module will not be optimized before compilation.
    Status finalize_module();

    // Optimize the entire module.  LLVM is more built for running its optimization
    // passes over the entire module (all the functions) rather than individual
    // functions.
    void optimize_module();

    // Replaces all instructions that call 'target_name' with a call instruction
    // to the new_fn.  Returns the modified function.
    // - target_name is the unmangled function name that should be replaced.
    //   The name is assumed to be unmangled so all call sites that contain the
    //   replace_name substring will be replaced. target_name is case-sensitive
    //   TODO: be more strict than substring? work out the mangling rules?
    // - If update_in_place is true, the caller function will be modified in place.
    //   Otherwise, the caller function will be cloned and the original function
    //   is unmodified.  If update_in_place is false and the function is already
    //   been dynamically linked, the existing function will be unlinked. Note that
    //   this is very unthread-safe, if there are threads in the function to be unlinked,
    //   bad things will happen.
    // - 'num_replaced' returns the number of call sites updated
    //
    // Most of our use cases will likely not be in place.  We will have one 'template'
    // version of the function loaded for each type of Node (e.g. AggregationNode).
    // Each instance of the node will clone the function, replacing the inner loop
    // body with the codegened version.  The codegened bodies differ from instance
    // to instance since they are specific to the node's tuple desc.
    llvm::Function* replace_call_sites(llvm::Function* caller, bool update_in_place,
                                     llvm::Function* new_fn, const std::string& target_name, int* num_replaced);

    /// Returns a copy of fn. The copy is added to the module.
    llvm::Function* clone_function(llvm::Function* fn);

    // Verify and optimize function.  This should be called at the end for each
    // codegen'd function.  If the function does not verify, it will return NULL,
    // otherwise, it will optimize, mark the function for inlining and return the
    // function object.
    llvm::Function* finalize_function(llvm::Function* function);

    // Inline all function calls for 'fn'.  'fn' is modified in place.  Returns
    // the number of functions inlined.  This is *not* called recursively
    // (i.e. second level function calls are not inlined).  This can be called
    // again to inline those until this returns 0.
    int inline_call_sites(llvm::Function* fn, bool skip_registered_fns);

    // Optimizes the function in place.  This uses a combination of llvm optimization
    // passes as well as some custom heuristics.  This should be called for all
    // functions which call Exprs.  The exprs will be inlined as much as possible,
    // and will do basic sub expression elimination.
    // This should be called before optimize_module for functions that want to remove
    // redundant exprs.  This should be called at the highest level possible to
    // maximize the number of redundant exprs that can be found.
    // TODO: we need to spend more time to output better IR.  Asking llvm to
    // remove redundant codeblocks on its own is too difficult for it.
    // TODO: this should implement the llvm FunctionPass interface and integrated
    // with the llvm optimization passes.
    llvm::Function* optimize_function_with_exprs(llvm::Function* fn);

    /// Adds the function to be automatically jit compiled after the module is optimized.
    /// That is, after FinalizeModule(), this will do *result_fn_ptr = JitFunction(fn);
    //
    /// This is useful since it is not valid to call JitFunction() before every part of the
    /// query has finished adding their IR and it's convenient to not have to rewalk the
    /// objects. This provides the same behavior as walking each of those objects and calling
    /// JitFunction().
    //
    /// In addition, any functions not registered with AddFunctionToJit() are marked as
    /// internal in FinalizeModule() and may be removed as part of optimization.
    //
    /// This will also wrap functions returning DecimalVals in an ABI-compliant wrapper (see
    /// the comment in the .cc file for details). This is so we don't accidentally try to
    /// call non-compliant code from native code.
    void add_function_to_jit(llvm::Function* fn, void** fn_ptr);

    // Jit compile the function.  This will run optimization passes and verify
    // the function.  The result is a function pointer that is dynamically linked
    // into the process.
    // Returns NULL if the function is invalid.
    // scratch_size will be set to the buffer size required to call the function
    // scratch_size is the total size from all LlvmCodeGen::get_scratch_buffer
    // calls (with some additional bytes for alignment)
    // This function is thread safe.
    void* jit_function(llvm::Function* function, int* scratch_size = NULL);

    // Verfies the function if the verfier is enabled.  Returns false if function
    // is invalid.
    bool verify_function(llvm::Function* function);

    // This will generate a printf call instruction to output 'message' at the
    // builder's insert point.  Only for debugging.
    void codegen_debug_trace(LlvmBuilder* builder, const char* message);

    /// Returns the string representation of a llvm::Value* or llvm::Type*
    template <typename T> 
    static std::string print(T* value_or_type) {
        std::string str;
        llvm::raw_string_ostream stream(str);
        value_or_type->print(stream);
        return str;
    }

    // Returns the libc function, adding it to the module if it has not already been.
    llvm::Function* get_lib_c_function(FnPrototype* prototype);

    // Returns the cross compiled function. IRFunction::Type is an enum which is
    // defined in 'doris-ir/doris-ir-functions.h'
    llvm::Function* get_function(IRFunction::Type);

    // Returns the hash function with signature:
    //   int32_t Hash(int8_t* data, int len, int32_t seed);
    // If num_bytes is non-zero, the returned function will be codegen'd to only
    // work for that number of bytes.  It is invalid to call that function with a
    // different 'len'.
    llvm::Function* get_hash_function(int num_bytes = -1);

    // Allocate stack storage for local variables.  This is similar to traditional c, where
    // all the variables must be declared at the top of the function.  This helper can be
    // called from anywhere and will add a stack allocation for 'var' at the beginning of
    // the function.  This would be used, for example, if a function needed a temporary
    // struct allocated.  The allocated variable is scoped to the function.
    // This is not related to get_scratch_buffer which is used for structs that are returned
    // to the caller.
    llvm::AllocaInst* create_entry_block_alloca(llvm::Function* f, const NamedVariable& var);
    llvm::AllocaInst* create_entry_block_alloca(
        const LlvmBuilder& builder, llvm::Type* type, const char* name);

    // Utility to create two blocks in 'fn' for if/else codegen.  if_block and else_block
    // are return parameters.  insert_before is optional and if set, the two blocks
    // will be inserted before that block otherwise, it will be inserted at the end
    // of 'fn'.  Being able to place blocks is useful for debugging so the IR has a
    // better looking control flow.
    void create_if_else_blocks(llvm::Function* fn, const std::string& if_name,
                            const std::string& else_name,
                            llvm::BasicBlock** if_block, llvm::BasicBlock** else_block,
                            llvm::BasicBlock* insert_before = NULL);

    // Returns offset into scratch buffer: offset points to area of size 'byte_size'
    // Called by expr generation to request scratch buffer.  This is used for struct
    // types (i.e. StringValue) where data cannot be returned by registers.
    // For example, to jit the expr "strlen(str_col)", we need a temporary StringValue
    // struct from the inner SlotRef expr node.  The SlotRef node would call
    // get_scratch_buffer(sizeof(StringValue)) and output the intermediate struct at
    // scratch_buffer (passed in as argument to compute function) + offset.
    int get_scratch_buffer(int byte_size);

    // Create a llvm pointer value from 'ptr'.  This is used to pass pointers between
    // c-code and code-generated IR.  The resulting value will be of 'type'.
    llvm::Value* cast_ptr_to_llvm_ptr(llvm::Type* type, void* ptr);

    // Returns the constant 'val' of 'type'
    llvm::Value* get_int_constant(PrimitiveType type, int64_t val);

    // Returns true/false constants (bool type)
    llvm::Value* true_value() {
        return _true_value;
    }
    llvm::Value* false_value() {
        return _false_value;
    }
    llvm::Value* null_ptr_value() {
        return llvm::ConstantPointerNull::get(ptr_type());
    }

    // Simple wrappers to reduce code verbosity
    llvm::Type* boolean_type() {
        return get_type(TYPE_BOOLEAN);
    }
    llvm::Type* tinyint_type() {
        return get_type(TYPE_TINYINT);
    }
    llvm::Type* smallint_type() {
        return get_type(TYPE_SMALLINT);
    }
    llvm::Type* int_type() {
        return get_type(TYPE_INT);
    }
    llvm::Type* bigint_type() {
        return get_type(TYPE_BIGINT);
    }
    llvm::Type* largeint_type() {
        return get_type(TYPE_LARGEINT);
    }
    llvm::Type* float_type() {
        return get_type(TYPE_FLOAT);
    }
    llvm::Type* double_type() {
        return get_type(TYPE_DOUBLE);
    }
    llvm::Type* string_val_type() const {
        return _string_val_type;
    }
    llvm::Type* datetime_val_type() const {
        return _datetime_val_type;
    }
    llvm::Type* decimal_val_type() const {
        return _decimal_val_type;
    }
    llvm::PointerType* ptr_type() {
        return _ptr_type;
    }
    llvm::Type* void_type() {
        return _void_type;
    }

    llvm::Type* i128_type() { 
        return llvm::Type::getIntNTy(context(), 128); 
    }

    // Fills 'functions' with all the functions that are defined in the module.
    // Note: this does not include functions that are just declared
    void get_functions(std::vector<llvm::Function*>* functions);

    // Generates function to return min/max(v1, v2)
    llvm::Function* codegen_min_max(const TypeDescriptor& type, bool min);

    // Codegen to call llvm memcpy intrinsic at the current builder location
    // dst & src must be pointer types.  size is the number of bytes to copy.
    void codegen_memcpy(LlvmBuilder*, llvm::Value* dst, llvm::Value* src, int size);

    // Codegen for do *dst = src.  For native types, this is just a store, for structs
    // we need to assign the fields one by one
    void codegen_assign(LlvmBuilder*, llvm::Value* dst, llvm::Value* src, PrimitiveType);

    llvm::Instruction::CastOps get_cast_op(
            const TypeDescriptor& from_type, const TypeDescriptor& to_type);

private:
    friend class LlvmCodeGenTest;
    friend class SubExprElimination;

    // Top level codegen object.  'module_name' is only used for debugging when
    // outputting the IR.  module's loaded from disk will be named as the file
    // path.
    LlvmCodeGen(ObjectPool* pool, const std::string& module_name);

    // Initializes the jitter and execution engine.
    Status init();

    // Load a pre-compiled IR module from 'file'.  This creates a top level
    // codegen object.  This is used by tests to load custom modules.
    // codegen will contain the created object on success.
    static Status load_from_file(ObjectPool*, const std::string& file,
                               boost::scoped_ptr<LlvmCodeGen>* codegen);

    /// Load a pre-compiled IR module from module_ir.  This creates a top level codegen
    /// object.  codegen will contain the created object on success.
    static Status load_from_memory(ObjectPool* pool, llvm::MemoryBuffer* module_ir,
                                   const std::string& module_name, const std::string& id, 
                                   boost::scoped_ptr<LlvmCodeGen>* codegen);

    /// Loads an LLVM module. 'module_ir' should be a reference to a memory buffer containing
    /// LLVM bitcode. module_name is the name of the module to use when reporting errors.
    /// The caller is responsible for cleaning up module.
    static Status load_module_from_memory(LlvmCodeGen* codegen, llvm::MemoryBuffer* module_ir,
                                          const std::string& module_name, llvm::Module** module);

    // Load the intrinsics doris needs.  This is a one time initialization.
    // Values are stored in '_llvm_intrinsics'
    Status load_intrinsics();

    // Clears generated hash fns.  This is only used for testing.
    void clear_hash_fns();

    // Name of the JIT module.  Useful for debugging.
    std::string _name;

    // Codegen counters
    RuntimeProfile _profile;
    RuntimeProfile::Counter* _load_module_timer;
    RuntimeProfile::Counter* _prepare_module_timer;
    RuntimeProfile::Counter* _module_file_size;
    RuntimeProfile::Counter* _codegen_timer;
    RuntimeProfile::Counter* _optimization_timer;
    RuntimeProfile::Counter* _compile_timer;

    // whether or not optimizations are enabled
    bool _optimizations_enabled;

    // If true, the module is corrupt and we cannot codegen this query.
    // TODO: we could consider just removing the offending function and attempting to
    // codegen the rest of the query.  This requires more testing though to make sure
    // that the error is recoverable.
    bool _is_corrupt;

    // If true, the module has been compiled.  It is not valid to add additional
    // functions after this point.
    bool _is_compiled;

    // Error string that llvm will write to
    std::string _error_string;

    // Top level llvm object.  Objects from different contexts do not share anything.
    // We can have multiple instances of the LlvmCodeGen object in different threads
    boost::scoped_ptr<llvm::LLVMContext> _context;

    // Top level codegen object.  Contains everything to jit one 'unit' of code.
    // Owned by the _execution_engine.
    llvm::Module* _module;

    // Execution/Jitting engine.
    boost::scoped_ptr<llvm::ExecutionEngine> _execution_engine;

    // current offset into scratch buffer
    int _scratch_buffer_offset;

    // Keeps track of all the functions that have been jit compiled and linked into
    // the process. Special care needs to be taken if we need to modify these functions.
    // bool is unused.
    std::map<llvm::Function*, bool> _jitted_functions;

    // Lock protecting _jitted_functions
    boost::mutex _jitted_functions_lock;

    // Keeps track of the external functions that have been included in this module
    // e.g libc functions or non-jitted doris functions.
    // TODO: this should probably be FnPrototype->Functions mapping
    std::map<std::string, llvm::Function*> _external_functions;

    // Functions parsed from pre-compiled module.  Indexed by DorisIR::Function enum
    std::vector<llvm::Function*> _loaded_functions;

    // Stores functions codegen'd by doris.  This does not contain cross compiled
    // functions, only function that were generated at runtime.  Does not overlap
    // with _loaded_functions.
    std::vector<llvm::Function*> _codegend_functions;

    // A mapping of unique id to registered expr functions
    std::map<int64_t, llvm::Function*> _registered_exprs_map;

    // A set of all the functions in '_registered_exprs_map' for quick lookup.
    std::set<llvm::Function*> _registered_exprs;

    // A cache of loaded llvm intrinsics
    std::map<llvm::Intrinsic::ID, llvm::Function*> _llvm_intrinsics;

    // This is a cache of generated hash functions by byte size.  It is common
    // for the caller to know the number of bytes to hash (e.g. tuple width) and
    // we can codegen a loop unrolled hash function.
    std::map<int, llvm::Function*> _hash_fns;

    /// The locations of modules that have been linked. Used to avoid linking the same module
    /// twice, which causes symbol collision errors.
    std::set<std::string> _linked_modules;

    /// The vector of functions to automatically JIT compile after FinalizeModule().
    std::vector<std::pair<llvm::Function*, void**> > _fns_to_jit_compile;

    // Debug utility that will insert a printf-like function into the generated
    // IR.  Useful for debugging the IR.  This is lazily created.
    llvm::Function* _debug_trace_fn;

    // Debug strings that will be outputted by jitted code.  This is a copy of all
    // strings passed to codegen_debug_trace.
    std::vector<std::string> _debug_strings;

    // llvm representation of a few common types.  Owned by context.
    llvm::PointerType* _ptr_type;     // int8_t*
    llvm::Type* _void_type;           // void
    llvm::Type* _string_val_type;     // StringVal
    llvm::Type* _decimal_val_type;    // StringVal
    llvm::Type* _datetime_val_type;   // DateTimeValue

    // llvm constants to help with code gen verbosity
    llvm::Value* _true_value;
    llvm::Value* _false_value;
};

}

#endif

