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

#include "codegen/subexpr_elimination.h"

#include <fstream>
#include <iostream>
#include <sstream>

#include <boost/thread/mutex.hpp>
#include <llvm/Analysis/Dominators.h>
#include <llvm/Analysis/Passes.h>
#include <llvm/Analysis/InstructionSimplify.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/InstIterator.h>
#include <llvm/Support/NoFolder.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Support/system_error.h>
#include "llvm/Transforms/IPO.h"
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils/SSAUpdater.h>

#include "common/logging.h"
#include "codegen/subexpr_elimination.h"
#include "doris_ir/doris_ir_names.h"
#include "util/cpu_info.h"
#include "util/path_builder.h"

using llvm::CallInst;
using llvm::BitCastInst;
using llvm::Instruction;
using llvm::LoadInst;
using llvm::StoreInst;
using llvm::Function;
using llvm::Value;
using llvm::DominatorTree;
namespace doris {

SubExprElimination::SubExprElimination(LlvmCodeGen* codegen) : _codegen(codegen) {
}

// Before running the standard llvm optimization passes, first remove redundant calls
// to slotref expression.  SlotRefs are more heavyweight due to the null handling that
// is required and after they are inlined, llvm is unable to eliminate the redundant
// inlined code blocks.
// For example:
//   select colA + colA would generate an inner loop with 2 calls to the colA slot ref,
// rather than doing subexpression elimination.  To handle this, we will:
//   1. inline all call sites in the original function except calls to SlotRefs
//   2. for all call sites to SlotRefs except the first to that SlotRef, replace the
//      results from the secondary calls with the result from the first and remove
//      the call instruction.
//   3. Inline calls to the SlotRefs (there should only be one for each slot ref).
//
// In the above example, the input function would look something like:
// int ArithmeticAdd(TupleRow* row, bool* is_null) {
//   bool lhs_is_null, rhs_is_null;
//   int lhs_value = SlotRef(row, &lhs_is_null);
//   if (lhs_is_null) { *is_null = true; return 0; }
//   int rhs_value = SlotRef(row, &rhs_is_null);
//   if (rhs_is_null) { *is_null = true; return 0; }
//   *is_null = false; return lhs_value + rhs_value;
// }
// During step 2, we'd substitute the second call to SlotRef with the results from
// the first call.
// int ArithmeticAdd(TupleRow* row, bool* is_null) {
//   bool lhs_is_null, rhs_is_null;
//   int lhs_value = SlotRef(row, &lhs_is_null);
//   if (lhs_is_null) { *is_null = true; return 0; }
//   int rhs_value = lhs_value;
//   rhs_is_null = lhs_is_null;
//   if (rhs_is_null) { *is_null = true; return 0; }
//   *is_null = false; return lhs_value + rhs_value;
// }
// And then rely on llvm to finish the removing the redundant code, resulting in:
// int ArithmeticAdd(TupleRow* row, bool* is_null) {
//   bool lhs_is_null, rhs_is_null;
//   int lhs_value = SlotRef(row, &lhs_is_null);
//   if (lhs_is_null) { *is_null = true; return 0; }
//   *is_null = false; return lhs_value + lhs_value;
// }
// Details on how to do this:
// http://llvm.org/docs/ProgrammersManual.html#replacing-an-instruction-with-another-value

// Step 2 requires more manipulation to ensure the resulting IR is still valid IR.
// The call to the expr returns two things, both of which need to be replaced.
// The value of the function as the return argument and whether or not the result was
// null as a function output argument.
//    1. The return value is trivial since with SSA, it is easy to identity all uses of
//       We simply replace the subsequent call instructions with the value.
//    2. For the is_null result ptr, we replace the call to the expr with a store
//       instruction of the cached value.
//       i.e:
//           val1 = Call(is_null_ptr);
//           is_null1 = *is_null_ptr
//           ...
//           val2 = Call(is_null_ptr);
//           is_null2 = *is_null_ptr
//       Becomes:
//           val1 = Call(is_null_ptr);
//           is_null1 = *is_null_ptr
//           ...
//           val2 = val1;
//           *is_null_ptr = is_null1;
//           is_null2 = *is_null_ptr
//       We do this because the is_null ptr is not SSA form, making manipulating it
//       complex. The above approach exactly preserves the Call function, including
//       all writes to ptrs. We then rely on the llvm load/store removal pass which
//       will remove the redundant loads (which is tricky since you have to track
//       other instructions that wrote to the ptr, etc).
// When doing the eliminations, we need to consider the call graph to make sure
// the instruction we are replacing with dominates the instruction we are replacing;
// that is, we need to guarantee the instruction we are replacing with always executes
// before the replacee instruction in all code paths.
// TODO: remove all this with expr refactoring. Everything will be SSA form then.
struct CachedExprResult {
    // First function call result. Subsequent calls will be replaced with this value
    CallInst* result;
    // First is null result. Subsequent calls will be replaced with this value.
    Instruction* is_null_value;
};

bool SubExprElimination::run(Function* fn) {
    // Step 1:
    int num_inlined = 0;
    do {
        // This assumes that all redundant exprs have been registered.
        num_inlined = _codegen->inline_call_sites(fn, true);
    } while (num_inlined > 0);

    // Mapping of (expr eval function, its 'row' arg) to cached result.  We want to remove
    // redundant calls to the same function with the same argument.
    std::map<std::pair<Function*, Value*>, CachedExprResult> cached_slot_ref_results;

    // Step 2:
    DominatorTree dom_tree;
    dom_tree.runOnFunction(*fn);

    llvm::inst_iterator fn_end = llvm::inst_end(fn);
    llvm::inst_iterator instr_iter = llvm::inst_begin(fn);
    // Loop over every instruction in the function.
    while (instr_iter != fn_end) {
        Instruction* instr = &*instr_iter;
        ++instr_iter;
        // Look for call instructions
        if (!CallInst::classof(instr)) {
            continue;
        }

        CallInst* call_instr = reinterpret_cast<CallInst*>(instr);
        Function* called_fn = call_instr->getCalledFunction();
        if (_codegen->_registered_exprs.find(called_fn) == 
                _codegen->_registered_exprs.end()) {
            continue;
        }

        // Found a registered expr function.  We generate the IR in a very specific way
        // when calling the expr.  The call instruction is always followed by loading the
        // resulting is_null result.  We need to update both.
        // TODO: we need to update this to do more analysis since we are relying on a very
        // specific code structure to do this.

        // Arguments are (row, scratch_buffer, is_null);
        DCHECK_EQ(call_instr->getNumArgOperands(), 3);
        Value* row_arg = call_instr->getArgOperand(0);

        DCHECK(BitCastInst::classof(row_arg));
        BitCastInst* row_cast = reinterpret_cast<BitCastInst*>(row_arg);
        // Get at the underlying row arg.  We need to differentiate between
        // call Fn(row1) and call Fn(row2). (identical fns but different input).
        row_arg = row_cast->getOperand(0);

        instr = &*instr_iter;
        ++instr_iter;

        if (!LoadInst::classof(instr)) {
            continue;
        }
        LoadInst* is_null_value = reinterpret_cast<LoadInst*>(instr);
        Value* loaded_ptr = is_null_value->getPointerOperand();

        // Subexpr elimination requires the IR to be a very specific form.
        //   call SlotRef(row, NULL, is_null_ptr)
        //   load is_null_ptr
        // Since we generate this IR currently, we can enforce this logic in our exprs
        // TODO: this should be removed/generalized with expr refactoring
        DCHECK_EQ(loaded_ptr, call_instr->getArgOperand(2));

        std::pair<Function*, Value*> call_desc = std::make_pair(called_fn, row_arg);
        if (cached_slot_ref_results.find(call_desc) == cached_slot_ref_results.end()) {
            CachedExprResult cache_entry;
            cache_entry.result = call_instr;
            cache_entry.is_null_value = is_null_value;
            cached_slot_ref_results[call_desc] = cache_entry;
        } else {
            // Reuse the result.
            CachedExprResult& cache_entry = cached_slot_ref_results[call_desc];
            if (dom_tree.dominates(cache_entry.result, call_instr)) {
                new StoreInst(cache_entry.is_null_value, loaded_ptr, call_instr);
                call_instr->replaceAllUsesWith(cache_entry.result);
                call_instr->eraseFromParent();
            }
        }
    }

    // Step 3:
    _codegen->inline_call_sites(fn, false);
    return true;
}

}
