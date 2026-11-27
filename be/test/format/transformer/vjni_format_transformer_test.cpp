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

#include "format/transformer/vjni_format_transformer.h"

#include <gtest/gtest.h>
#include <jni.h>

#include <cstdlib>

#include "common/status.h"
#include "exprs/vexpr_fwd.h"
#include "util/defer_op.h"
#include "util/jni-util.h"
#include "util/jni_plugin_registry.h"

namespace doris {
namespace {
struct CloseProbe {
    int calls = 0;
    bool throw_on_close = false;
    bool pending = false;
    _jthrowable error;
};

CloseProbe& probe(JNIEnv* env) {
    return *static_cast<CloseProbe*>(env->functions->reserved0);
}
} // namespace

class VJniFormatTransformerTest : public testing::Test {
protected:
    static void check_close(bool fail_java, bool fail_attach) {
        CloseProbe state;
        state.throw_on_close = fail_java;
        JNINativeInterface_ functions {};
        functions.reserved0 = &state;
        functions.CallVoidMethodA = [](JNIEnv* env, jobject, jmethodID, const jvalue*) {
            auto& state = probe(env);
            ++state.calls;
            state.pending = state.throw_on_close;
        };
        functions.ExceptionCheck = [](JNIEnv* env) -> jboolean { return probe(env).pending; };
        functions.ExceptionOccurred = [](JNIEnv* env) -> jthrowable {
            return probe(env).pending ? &probe(env).error : nullptr;
        };
        functions.ExceptionDescribe = [](JNIEnv*) {};
        functions.ExceptionClear = [](JNIEnv* env) { probe(env).pending = false; };
        functions.DeleteLocalRef = [](JNIEnv*, jobject) {};
        functions.DeleteGlobalRef = [](JNIEnv*, jobject) {};
        JNIEnv env {&functions};
        auto* previous_env = Jni::Env::tls_env_;
        Jni::Env::tls_env_ = &env;
        DEFER(Jni::Env::tls_env_ = previous_env);

        VExprContextSPtrs output_exprs;
        VJniFormatTransformer transformer(nullptr, output_exprs,
                                          Jni::PluginRef {.plugin = "unused", .factory = "unused"},
                                          {});
        Jni::WriterApi api;
        api.close._id = reinterpret_cast<jmethodID>(1);
        _jobject writer;
        transformer._jni_writer_obj._obj = &writer;
        transformer._writer_api = &api;
        expect_partial_open_skips_java(transformer, state);
        transformer._opened = true;
        if (fail_attach) {
            expect_attach_failure(transformer, state);
        }
        expect_close_once(transformer, state, fail_java);
    }

    static void expect_partial_open_skips_java(VJniFormatTransformer& transformer,
                                               const CloseProbe& state) {
        // No completed open: no Env request and no Java finalization.
        EXPECT_TRUE(transformer
                            ._close_with_env([](JNIEnv**) {
                                ADD_FAILURE() << "partial open must not finalize";
                                return Status::InternalError("unexpected attach");
                            })
                            .ok());
        EXPECT_EQ(0, state.calls);
    }

    static void expect_attach_failure(VJniFormatTransformer& transformer, const CloseProbe& state) {
        auto failure = transformer._close_with_env(
                [](JNIEnv**) { return Status::JniError("injected attach failure"); });
        EXPECT_FALSE(failure.ok());
        EXPECT_EQ("injected attach failure", failure.msg());
        EXPECT_EQ(0, state.calls);
        EXPECT_FALSE(transformer._close_result.has_value());
    }

    static void expect_close_once(VJniFormatTransformer& transformer, const CloseProbe& state,
                                  bool fail_java) {
        auto first = transformer.close();
        EXPECT_EQ(!fail_java, first.ok()) << first;
        EXPECT_EQ(1, state.calls);
        EXPECT_FALSE(state.pending);
        auto second = transformer.close();
        EXPECT_EQ(first.code(), second.code());
        EXPECT_EQ(first.msg(), second.msg());
        EXPECT_EQ(1, state.calls);
        EXPECT_TRUE(transformer._opened); // Statistics remain available after close.
    }
};

TEST_F(VJniFormatTransformerTest, SuccessfulCloseRunsOnce) {
    check_close(false, false);
}

TEST_F(VJniFormatTransformerTest, AttachFailureCanRetryBeforeEnteringJava) {
    check_close(false, true);
}

TEST_F(VJniFormatTransformerTest, JavaCloseFailureIsRetainedWithoutRepeatingSideEffects) {
    // Isolate Env's lazy SPI diagnostics from other tests that may initialize a real JVM.
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_EXIT(
            {
                Jni::Env::jni_util_cl_ = nullptr;
                check_close(true, false);
                std::_Exit(HasFailure() ? 1 : 0);
            },
            testing::ExitedWithCode(0), "");
}

} // namespace doris
