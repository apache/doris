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

/* ==================== USAGE EXAMPLE ====================
 *
 * Suppose we have a class with a private member:
 *
 * class MyClass {
 * private:
 *     int secret_value = 42;
 *     std::string name = "test";
 *     static int count;
 * };
 *
 * MyClass::count = 10086;
 *
 * In a.cpp, we can access the private members like this:
 *
 * // First, declare access to the private members
 * ACCESS_PRIVATE_FIELD(MyClass_secret, MyClass, int, secret_value)
 * ACCESS_PRIVATE_FIELD(MyClass_name, MyClass, std::string, name)
 * ACCESS_PRIVATE_FIELD(MyClass_count, MyClass, int, count)
 *
 * // Later in code, use the hijacked pointers:
 * void example() {
 *     MyClass obj;
 *
 *     // Get pointer to member
 *     auto secret_ptr = GET_PRIVATE_FIELD(MyClass_secret);
 *     auto name_ptr = GET_PRIVATE_FIELD(MyClass_name);
 *     auto count_ptr = GET_PRIVATE_STATIC_FIELD(MyClass_count);
 *
 *     // Access private members
 *     int value = obj.*secret_ptr;  // value = 42
 *     std::string& name = obj.*name_ptr;  // name = "test"
 *     int& count = *count_ptr; // 10086
 *
 *     // Modify private members
 *     obj.*secret_ptr = 100;
 *     obj.*name_ptr = "modified";
 *     *count = 10010;
 * }
 *
 * Note: This technique bypasses access control and should only
 * be used for debugging, testing, or legacy code workarounds.
 * ====================================================== */

// A global container for storing hijacked member pointers
template <typename Tag>
struct AccessStorage {
    using Type = typename Tag::MemberType;
    static Type ptr;
};

// Initialize static members
template <typename Tag>
typename Tag::MemberType AccessStorage<Tag>::ptr;

// Hijacker template
template <typename Tag, typename Tag::MemberType M>
struct AccessRobber {
    struct Initer {
        Initer() { AccessStorage<Tag>::ptr = M; }
    };
    static Initer initer;
};

template <typename Tag, typename Tag::MemberType M>
typename AccessRobber<Tag, M>::Initer AccessRobber<Tag, M>::initer;

/**
 * Universal macro: Injects hijacking logic
 * @param TagName  Custom tag name (must be unique)
 * @param Class    Target class name
 * @param Type     Member variable type
 * @param Member   Member variable name
 */
#define ACCESS_PRIVATE_FIELD(TagName, Class, Type, Member) \
    struct TagName {                                       \
        using MemberType = Type Class::*;                  \
    };                                                     \
    template struct AccessRobber<TagName, &Class::Member>;

// Similar to ACCESS_PRIVATE_FIELD but for private static field
#define ACCESS_PRIVATE_STATIC_FIELD(TagName, Class, Type, Member) \
    struct TagName {                                              \
        using MemberType = Type*;                                 \
    };                                                            \
    template struct AccessRobber<TagName, &Class::Member>;

// Convenience macro for retrieving hijacked pointers
#define GET_PRIVATE_FIELD(TagName) AccessStorage<TagName>::ptr

// Similar to GET_PRIVATE_FIELD but for private static field
#define GET_PRIVATE_STATIC_FIELD(TagName) AccessStorage<TagName>::ptr
