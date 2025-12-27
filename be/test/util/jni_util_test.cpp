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

#include <iomanip>
#include <iostream>

#include "util/jni-util.h"

namespace doris {

struct JavaMemInfo {
    jlong used;
    jlong free;
    jlong total;
    jlong max;
};

class JniUtilTest : public testing::Test {
public:
    JniUtilTest() {
        _test = false;

        // can not run as UT now, because ASAN will report memory leak.
        // https://blog.gypsyengineer.com/en/security/running-java-with-addresssanitizer.html
        // <<How to use AddressSanitizer with Java>>
        // should export ASAN_OPTIONS=detect_leaks=0;
        if (!_test) {
            return;
        }

        std::cout << "init = " << init();
    }
    ~JniUtilTest() override = default;

    Status init() {
        auto st = doris::Jni::Util::Init();

        if (!st.ok()) {
            exit(1);
        }
        JNIEnv* env;

        // jvm mem
        RETURN_IF_ERROR(Jni::Env::Get(&env));
        RETURN_IF_ERROR(Jni::Util::find_class(env, "java/lang/Runtime", &runtime_cls));

        RETURN_IF_ERROR(runtime_cls.get_static_method(env, "getRuntime", "()Ljava/lang/Runtime;",
                                                      &get_runtime));
        RETURN_IF_ERROR(runtime_cls.get_method(env, "totalMemory", "()J", &total_mem));
        RETURN_IF_ERROR(runtime_cls.get_method(env, "freeMemory", "()J", &free_mem));
        RETURN_IF_ERROR(runtime_cls.get_method(env, "maxMemory", "()J", &max_mem));

        // gc
        RETURN_IF_ERROR(Jni::Util::find_class(env, "java/lang/System", &system_cls));
        RETURN_IF_ERROR(system_cls.get_static_method(env, "gc", "()V", &gc_method));

        RETURN_IF_ERROR(trigger_gc(env));

        return Status::OK();
    }

    Status get_mem_info(JNIEnv* env, JavaMemInfo* info) {
        Jni::LocalObject runtime_obj;
        RETURN_IF_ERROR(runtime_cls.call_static_object_method(env, get_runtime).call(&runtime_obj));

        jlong total, free, max, used;
        RETURN_IF_ERROR(runtime_obj.call_long_method(env, total_mem).call(&total));
        RETURN_IF_ERROR(runtime_obj.call_long_method(env, free_mem).call(&free));
        RETURN_IF_ERROR(runtime_obj.call_long_method(env, max_mem).call(&max));
        used = total - free;

        info->used = used;
        info->free = free;
        info->total = total;
        info->max = max;

        return Status::OK();
    }

    void print_mem_info(const std::string& title, const JavaMemInfo& info) {
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "==== " << title << " ====\n";
        std::cout << "Used:  " << (info.used / 1024.0 / 1024.0) << " MB\n";
        std::cout << "Free:  " << (info.free / 1024.0 / 1024.0) << " MB\n";
        std::cout << "Total: " << (info.total / 1024.0 / 1024.0) << " MB\n";
        std::cout << "Max:   " << (info.max / 1024.0 / 1024.0) << " MB\n";
    }

    Status trigger_gc(JNIEnv* env) {
        RETURN_IF_ERROR(system_cls.call_static_void_method(env, gc_method).call());
        return Status::OK();
    }

    bool check_mem_diff(const JavaMemInfo& before, const JavaMemInfo& after,
                        double allowed_diff_mb) {
        auto to_mb = [](jlong bytes) { return bytes / 1024.0 / 1024.0; };

        double used_before = to_mb(before.used);
        double used_after = to_mb(after.used);

        double diff = used_after - used_before;

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "==== Memory Diff Check ====\n";
        std::cout << "Used:  " << used_before << " -> " << used_after << " (Δ=" << diff << " MB)\n";

        if (diff > allowed_diff_mb) {
            std::cout << "Memory change " << diff << " MB exceeds allowed " << allowed_diff_mb
                      << " MB\n";
            return false;
        }

        std::cout << "Memory change within limit (" << diff << " MB ≤ " << allowed_diff_mb
                  << " MB)\n";
        return true;
    }

private:
    Jni::GlobalClass runtime_cls;
    Jni::GlobalClass system_cls;

    Jni::MethodId get_runtime;
    Jni::MethodId total_mem;
    Jni::MethodId free_mem;
    Jni::MethodId max_mem;

    Jni::MethodId gc_method;

    bool _test;
};

TEST_F(JniUtilTest, MemoryStableAfterStringAllocations) {
    if (!_test) {
        std::cout << "Skip MemoryStableAfterStringAllocations test\n";
        return;
    }
    JNIEnv* env = nullptr;
    auto st = Jni::Env::Get(&env);
    EXPECT_TRUE(st.ok()) << st;

    JavaMemInfo info_before;
    st = get_mem_info(env, &info_before);
    ASSERT_TRUE(st.ok()) << st;

    print_mem_info("MemoryStableAfterStringAllocations_before", info_before);
    for (int i = 0; i < 10000; i++) {
        Jni::LocalClass string_cls;
        st = Jni::Util::find_class(env, "java/lang/String", &string_cls);
        ASSERT_TRUE(st.ok()) << st;

        Jni::MethodId length_method;
        st = string_cls.get_method(env, "length", "()I", &length_method);
        ASSERT_TRUE(st.ok()) << st;

        Jni::MethodId empty_method;
        st = string_cls.get_method(env, "isEmpty", "()Z", &empty_method);
        ASSERT_TRUE(st.ok()) << st;

        Jni::LocalString jstr_obj;
        st = Jni::LocalString::new_string(env, "hello", &jstr_obj);
        ASSERT_TRUE(st.ok()) << st;

        Jni::LocalStringBufferGuard str_buf;
        st = jstr_obj.get_string_chars(env, &str_buf);
        ASSERT_TRUE(st.ok()) << st;

        ASSERT_EQ(strlen(str_buf.get()), 5);

        ASSERT_EQ(strcmp(str_buf.get(), "hello"), 0);

        jint len;
        st = jstr_obj.call_int_method(env, length_method).call(&len);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(len, 5);

        st = jstr_obj.call_int_method(env, length_method).call();
        ASSERT_TRUE(st.ok()) << st;

        jboolean empty;
        st = jstr_obj.call_boolean_method(env, empty_method).call(&empty);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(empty, false);
    }

    JavaMemInfo info_after;
    st = get_mem_info(env, &info_after);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("MemoryStableAfterStringAllocations_after", info_after);

    st = trigger_gc(env);
    ASSERT_TRUE(st.ok()) << st;

    JavaMemInfo info_after_gc;
    st = get_mem_info(env, &info_after_gc);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("MemoryStableAfterStringAllocations_after_gc", info_after_gc);

    ASSERT_TRUE(check_mem_diff(info_before, info_after_gc, 5.0));
}

TEST_F(JniUtilTest, ByteBuffer) {
    if (!_test) {
        std::cout << "Skip ByteBuffer test\n";
        return;
    }
    JNIEnv* env = nullptr;
    auto st = Jni::Env::Get(&env);
    EXPECT_TRUE(st.ok()) << st;

    JavaMemInfo info_before;
    st = get_mem_info(env, &info_before);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("ByteBuffer_before", info_before);

    for (int i = 0; i < 300; i++) { // allocate 300M
        Jni::LocalClass local_class;
        st = Jni::Util::find_class(env, "java/nio/ByteBuffer", &local_class);
        ASSERT_TRUE(st.ok()) << st;

        Jni::MethodId allocate_method;
        st = local_class.get_static_method(env, "allocate", "(I)Ljava/nio/ByteBuffer;",
                                           &allocate_method);
        ASSERT_TRUE(st.ok()) << st;

        Jni::LocalObject bytebuffer_obj;
        st = local_class.call_static_object_method(env, allocate_method)
                     .with_arg(1024 * 1024)
                     .call(&bytebuffer_obj); // 1M

        ASSERT_TRUE(st.ok()) << st;
    }

    JavaMemInfo info_after;
    st = get_mem_info(env, &info_after);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("ByteBuffer_after", info_after);

    st = trigger_gc(env);
    ASSERT_TRUE(st.ok()) << st;

    JavaMemInfo info_after_gc;
    st = get_mem_info(env, &info_after_gc);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("ByteBuffer_after_gc", info_after_gc);

    ASSERT_TRUE(check_mem_diff(info_before, info_after_gc, 5.0));
}

TEST_F(JniUtilTest, StringMethodTest) {
    if (!_test) {
        std::cout << "Skip StringMethodTest test\n";
        return;
    }
    JNIEnv* env;
    Status st = Jni::Env::Get(&env);
    ASSERT_TRUE(st.ok()) << st;

    JavaMemInfo info_before;
    st = get_mem_info(env, &info_before);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("String_before", info_before);

    Jni::LocalClass string_cls;
    st = Jni::Util::find_class(env, "java/lang/String", &string_cls);
    ASSERT_TRUE(st.ok()) << st;

    // get methodId
    Jni::MethodId equals_method;
    st = string_cls.get_method(env, "equals", "(Ljava/lang/Object;)Z", &equals_method);
    ASSERT_TRUE(st.ok()) << st;

    Jni::MethodId equals_ignore_case_method;
    st = string_cls.get_method(env, "equalsIgnoreCase", "(Ljava/lang/String;)Z",
                               &equals_ignore_case_method);
    ASSERT_TRUE(st.ok()) << st;

    Jni::MethodId to_upper_method;
    st = string_cls.get_method(env, "toUpperCase", "()Ljava/lang/String;", &to_upper_method);
    ASSERT_TRUE(st.ok()) << st;

    Jni::MethodId to_lower_method;
    st = string_cls.get_method(env, "toLowerCase", "()Ljava/lang/String;", &to_lower_method);
    ASSERT_TRUE(st.ok()) << st;

    Jni::MethodId substring_method;
    st = string_cls.get_method(env, "substring", "(II)Ljava/lang/String;", &substring_method);
    ASSERT_TRUE(st.ok()) << st;

    Jni::MethodId contains_method;
    st = string_cls.get_method(env, "contains", "(Ljava/lang/CharSequence;)Z", &contains_method);
    ASSERT_TRUE(st.ok()) << st;

    Jni::MethodId starts_with_method;
    st = string_cls.get_method(env, "startsWith", "(Ljava/lang/String;)Z", &starts_with_method);
    ASSERT_TRUE(st.ok()) << st;

    Jni::MethodId ends_with_method;
    st = string_cls.get_method(env, "endsWith", "(Ljava/lang/String;)Z", &ends_with_method);
    ASSERT_TRUE(st.ok()) << st;

    // 3. create some local string object.
    Jni::LocalString str_hello, str_hello2, str_hello_upper, str_world;
    st = Jni::LocalString::new_string(env, "hello", &str_hello);
    ASSERT_TRUE(st.ok()) << st;

    st = Jni::LocalString::new_string(env, "HELLO", &str_hello_upper);
    ASSERT_TRUE(st.ok()) << st;

    st = Jni::LocalString::new_string(env, "hello", &str_hello2);
    ASSERT_TRUE(st.ok()) << st;

    st = Jni::LocalString::new_string(env, "world", &str_world);
    ASSERT_TRUE(st.ok()) << st;

    for (int i = 0; i < 10000; i++) {
        // 4. equals() and equalsIgnoreCase()
        {
            jboolean eq;
            st = str_hello.call_boolean_method(env, equals_method).with_arg(str_hello2).call(&eq);
            ASSERT_TRUE(st.ok()) << st;
            ASSERT_EQ(eq, JNI_TRUE); // "hello".equals("hello")

            jboolean eq_ic;
            st = str_hello.call_boolean_method(env, equals_ignore_case_method)
                         .with_arg(str_hello_upper)
                         .call(&eq_ic);
            ASSERT_TRUE(st.ok()) << st;
            ASSERT_EQ(eq_ic, JNI_TRUE); // "hello".equalsIgnoreCase("HELLO")

            jboolean neq;
            st = str_hello.call_boolean_method(env, equals_method).with_arg(str_world).call(&neq);
            ASSERT_TRUE(st.ok()) << st;
            ASSERT_EQ(neq, JNI_FALSE);
        }

        // 5. toUpperCase() / toLowerCase()
        {
            Jni::LocalString upper_obj;
            st = str_hello.call_object_method(env, to_upper_method).call(&upper_obj);
            ASSERT_TRUE(st.ok()) << st;

            Jni::LocalStringBufferGuard buf;
            st = upper_obj.get_string_chars(env, &buf);
            ASSERT_TRUE(st.ok()) << st;
            ASSERT_STREQ(buf.get(), "HELLO");

            Jni::LocalString lower_obj;
            st = str_hello_upper.call_object_method(env, to_lower_method).call(&lower_obj);
            ASSERT_TRUE(st.ok()) << st;

            Jni::LocalStringBufferGuard buf2;
            st = lower_obj.get_string_chars(env, &buf2);
            ASSERT_TRUE(st.ok()) << st;
            ASSERT_STREQ(buf2.get(), "hello");
        }

        // 6. substring() test
        {
            Jni::LocalString sub_obj;
            st = str_hello.call_object_method(env, substring_method)
                         .with_arg(1)
                         .with_arg(4)
                         .call(&sub_obj); // "ell"
            ASSERT_TRUE(st.ok()) << st;

            Jni::LocalStringBufferGuard buf;
            st = sub_obj.get_string_chars(env, &buf);
            ASSERT_TRUE(st.ok()) << st;
            ASSERT_STREQ(buf.get(), "ell");
        }

        // 7. contains() / startsWith() / endsWith()   hello
        {
            Jni::LocalString str_el;
            st = Jni::LocalString::new_string(env, "el", &str_el);
            ASSERT_TRUE(st.ok()) << st;

            jboolean contains;
            st = str_hello.call_boolean_method(env, contains_method)
                         .with_arg(str_el)
                         .call(&contains);
            ASSERT_TRUE(st.ok()) << st;
            ASSERT_EQ(contains, true);

            Jni::LocalString str_he;
            st = Jni::LocalString::new_string(env, "he", &str_he);
            ASSERT_TRUE(st.ok()) << st;

            jboolean starts;
            st = str_hello.call_boolean_method(env, starts_with_method)
                         .with_arg(str_he)
                         .call(&starts);
            ASSERT_TRUE(st.ok()) << st;
            ASSERT_EQ(starts, JNI_TRUE);

            Jni::LocalString str_ll;
            st = Jni::LocalString::new_string(env, "ll", &str_ll);
            ASSERT_TRUE(st.ok()) << st;

            jboolean ends;
            st = str_hello.call_boolean_method(env, ends_with_method).with_arg(str_ll).call(&ends);
            ASSERT_TRUE(st.ok()) << st;
            ASSERT_EQ(ends, false);
        }
    }

    JavaMemInfo info_after;
    st = get_mem_info(env, &info_after);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("String_after", info_after);

    st = trigger_gc(env);
    ASSERT_TRUE(st.ok()) << st;

    JavaMemInfo info_after_gc;
    st = get_mem_info(env, &info_after_gc);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("String_after_gc", info_after_gc);

    ASSERT_TRUE(check_mem_diff(info_before, info_after_gc, 5.0));
}

TEST_F(JniUtilTest, TestJavaDateArray) {
    if (!_test) {
        std::cout << "Skip TestJavaDateArray test\n";
        return;
    }
    JNIEnv* env;
    Status st = Jni::Env::Get(&env);
    ASSERT_TRUE(st.ok()) << st;

    JavaMemInfo info_before;
    st = get_mem_info(env, &info_before);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("DateArray_before", info_before);
    {
        size_t test_size = 3;

        // Load java.util.Date class
        Jni::LocalClass date_cls;
        st = Jni::Util::find_class(env, "java/util/Date", &date_cls);
        ASSERT_TRUE(st.ok()) << st;

        // Load java.lang.reflect.Array class
        Jni::LocalClass reflect_array_cls;
        st = Jni::Util::find_class(env, "java/lang/reflect/Array", &reflect_array_cls);
        ASSERT_TRUE(st.ok()) << st;

        // Get static method Array.newInstance(Class, int)
        Jni::MethodId new_instance_method;
        st = reflect_array_cls.get_static_method(
                env, "newInstance", "(Ljava/lang/Class;I)Ljava/lang/Object;", &new_instance_method);
        ASSERT_TRUE(st.ok()) << st;

        Jni::MethodId set_method;
        st = reflect_array_cls.get_static_method(
                env, "set", "(Ljava/lang/Object;ILjava/lang/Object;)V", &set_method);
        ASSERT_TRUE(st.ok()) << st;

        // Create a Date array: new Date[3]
        Jni::LocalArray date_array;
        st = reflect_array_cls.call_static_object_method(env, new_instance_method)
                     .with_arg(date_cls)
                     .with_arg((jint)test_size)
                     .call(&date_array);
        ASSERT_TRUE(st.ok()) << st;

        // Get Date.<init>() constructor
        Jni::MethodId date_ctor;
        st = date_cls.get_method(env, "<init>", "()V", &date_ctor);
        ASSERT_TRUE(st.ok()) << st;

        // Fill each element with new Date()
        for (int i = 0; i < test_size; i++) {
            Jni::LocalObject date_obj;
            st = date_cls.new_object(env, date_ctor)
                         .with_arg((jlong)1700000000000LL)
                         .call(&date_obj);
            ASSERT_TRUE(st.ok()) << st;

            st = reflect_array_cls.call_static_void_method(env, set_method)
                         .with_arg(date_array)
                         .with_arg(i)
                         .with_arg(date_obj)
                         .call();
            ASSERT_TRUE(st.ok()) << st;
        }

        // Verify array length
        jsize len;
        st = date_array.get_length(env, &len);
        ASSERT_TRUE(st.ok()) << st;

        ASSERT_EQ(len, test_size);

        // Get elements and print their toString()
        Jni::MethodId toString_method;
        st = date_cls.get_method(env, "toString", "()Ljava/lang/String;", &toString_method);
        ASSERT_TRUE(st.ok()) << st;

        for (int i = 0; i < test_size; i++) {
            Jni::LocalObject date_obj;
            st = date_array.get_object_array_element(env, i, &date_obj);
            ASSERT_TRUE(st.ok()) << st;

            Jni::LocalString date_str;
            st = date_obj.call_object_method(env, toString_method).call(&date_str);
            ASSERT_TRUE(st.ok()) << st;

            Jni::LocalStringBufferGuard str_buf;
            st = date_str.get_string_chars(env, &str_buf);
            ASSERT_TRUE(st.ok()) << st;

            std::cout << "Date[" << i << "] = " << str_buf.get() << std::endl;
        }
    }

    JavaMemInfo info_after;
    st = get_mem_info(env, &info_after);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("DateArray_after", info_after);

    st = trigger_gc(env);
    ASSERT_TRUE(st.ok()) << st;

    JavaMemInfo info_after_gc;
    st = get_mem_info(env, &info_after_gc);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("DateArray_gc", info_after_gc);

    ASSERT_TRUE(check_mem_diff(info_before, info_after_gc, 5.0));
}

TEST_F(JniUtilTest, TestConvertMap) {
    if (!_test) {
        std::cout << "Skip TestConvertMap test\n";
        return;
    }
    JNIEnv* env;
    Status st = Jni::Env::Get(&env);
    ASSERT_TRUE(st.ok()) << st;

    JavaMemInfo info_before;
    st = get_mem_info(env, &info_before);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("ConvertMap_before", info_before);

    {
        std::map<std::string, std::string> cpp_map;
        for (int i = 0; i < 10000; i++) {
            cpp_map["key_" + std::to_string(i)] = "value_" + std::to_string(i);
        }

        // Step 2: Convert to Java HashMap
        Jni::LocalObject java_map;
        st = Jni::Util::convert_to_java_map(env, cpp_map, &java_map);
        ASSERT_TRUE(st.ok()) << st;

        // Step 3: Get java/util/HashMap class
        Jni::LocalClass map_class;
        st = Jni::Util::find_class(env, "java/util/HashMap", &map_class);
        ASSERT_TRUE(st.ok()) << st;

        // Step 4: Prepare commonly used methods
        Jni::MethodId size_method, get_method, contains_key_method, contains_value_method,
                is_empty_method;
        st = map_class.get_method(env, "size", "()I", &size_method);
        ASSERT_TRUE(st.ok()) << st;

        st = map_class.get_method(env, "get", "(Ljava/lang/Object;)Ljava/lang/Object;",
                                  &get_method);
        ASSERT_TRUE(st.ok()) << st;

        st = map_class.get_method(env, "containsKey", "(Ljava/lang/Object;)Z",
                                  &contains_key_method);
        ASSERT_TRUE(st.ok()) << st;

        st = map_class.get_method(env, "containsValue", "(Ljava/lang/Object;)Z",
                                  &contains_value_method);
        ASSERT_TRUE(st.ok()) << st;

        st = map_class.get_method(env, "isEmpty", "()Z", &is_empty_method);
        ASSERT_TRUE(st.ok()) << st;

        jint size;
        st = java_map.call_int_method(env, size_method).call(&size);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(size, (jint)cpp_map.size());

        jboolean empty;
        st = java_map.call_boolean_method(env, is_empty_method).call(&empty);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(empty, JNI_FALSE);

        // Step 6: Create test key and check containsKey / containsValue
        Jni::LocalString test_key;
        Jni::LocalString test_value;
        st = Jni::LocalString::new_string(env, "key_1234", &test_key);
        ASSERT_TRUE(st.ok()) << st;

        st = Jni::LocalString::new_string(env, "value_1234", &test_value);
        ASSERT_TRUE(st.ok()) << st;

        jboolean has_key;
        st = java_map.call_boolean_method(env, contains_key_method)
                     .with_arg(test_key)
                     .call(&has_key);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(has_key, JNI_TRUE);

        jboolean has_value;
        st = java_map.call_boolean_method(env, contains_value_method)
                     .with_arg(test_value)
                     .call(&has_value);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(has_value, JNI_TRUE);

        Jni::LocalString result_obj;
        st = java_map.call_object_method(env, get_method).with_arg(test_key).call(&result_obj);
        ASSERT_TRUE(st.ok()) << st;

        Jni::LocalStringBufferGuard buf;
        st = result_obj.get_string_chars(env, &buf);
        ASSERT_TRUE(st.ok()) << st;

        ASSERT_STREQ(buf.get(), "value_1234");

        Jni::LocalString fake_key;
        st = Jni::String<Jni::Local>::new_string(env, "key_not_exist", &fake_key);
        ASSERT_TRUE(st.ok()) << st;

        jboolean has_fake;
        st = java_map.call_boolean_method(env, contains_key_method)
                     .with_arg(fake_key)
                     .call(&has_fake);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(has_fake, JNI_FALSE);

        std::map<std::string, std::string> result_map;
        st = Jni::Util::convert_to_cpp_map(env, java_map, &result_map);

        for (auto const& [a, b] : cpp_map) {
            ASSERT_TRUE(result_map.contains(a));
            ASSERT_EQ(result_map[a], b);
        }
    }

    JavaMemInfo info_after;
    st = get_mem_info(env, &info_after);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("ConvertMap_after", info_after);

    st = trigger_gc(env);
    ASSERT_TRUE(st.ok()) << st;

    JavaMemInfo info_after_gc;
    st = get_mem_info(env, &info_after_gc);
    ASSERT_TRUE(st.ok()) << st;
    print_mem_info("ConvertMap_after_gc", info_after_gc);

    ASSERT_TRUE(check_mem_diff(info_before, info_after_gc, 5.0));

    ASSERT_TRUE(Jni::Env::Get(&env).ok());
}

} // namespace doris
