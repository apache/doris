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

#include "util/jni_plugin_registry.h"

#include <filesystem>
#include <mutex>

#include "common/config.h"

namespace doris::Jni {

namespace {
const char* const REGISTRY_CLASS = "org/apache/doris/jni/bootstrap/PluginRegistry";
const char* const SCANNER_CLASS = "org/apache/doris/jni/spi/JniScanner";
const char* const WRITER_CLASS = "org/apache/doris/jni/spi/JniWriter";
} // namespace

PluginRegistry::Registry PluginRegistry::_registry;
ScannerApi PluginRegistry::_scanner_api;
WriterApi PluginRegistry::_writer_api;
std::atomic<bool> PluginRegistry::_registry_ready = false;

Status PluginRegistry::_init_registry() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Env::Get(&env));

    RETURN_IF_ERROR(Util::find_class(env, REGISTRY_CLASS, &_registry.cls));
    RETURN_IF_ERROR(_registry.cls.get_static_method(
            env, "createInstance",
            "(Ljava/lang/String;Ljava/lang/String;ILjava/util/Map;)Ljava/lang/Object;",
            &_registry.create_instance));
    RETURN_IF_ERROR(_registry.cls.get_static_method(
            env, "createUdfExecutor", "(Ljava/lang/String;Ljava/lang/String;[B)Ljava/lang/Object;",
            &_registry.create_udf_executor));
    RETURN_IF_ERROR(_registry.cls.get_static_method(env, "cleanUdfCache", "(JLjava/lang/String;)V",
                                                    &_registry.clean_udf_cache));
    RETURN_IF_ERROR(_registry.cls.get_static_method(env, "pluginStatusJson", "()Ljava/lang/String;",
                                                    &_registry.plugin_status_json));
    RETURN_IF_ERROR(_registry.cls.get_static_method(env, "warmup", "()V", &_registry.warmup));

    _registry_ready = true;
    return Status::OK();
}

Status PluginRegistry::_ensure_registry(const Registry** registry) {
    static std::once_flag registry_once;
    static Status registry_status;
    std::call_once(registry_once, []() { registry_status = _init_registry(); });
    RETURN_IF_ERROR(registry_status);
    *registry = &_registry;
    return Status::OK();
}

// The method table below is PROTOCOL.md section 1. Every entry is final on the SPI base
// class, so what is resolved here is what runs, whatever the plugin does.
Status PluginRegistry::_init_scanner_api() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Env::Get(&env));

    GlobalClass& cls = _scanner_api.cls;
    RETURN_IF_ERROR(Util::find_class(env, SCANNER_CLASS, &cls));
    RETURN_IF_ERROR(cls.get_method(env, "open", "()V", &_scanner_api.open));
    RETURN_IF_ERROR(
            cls.get_method(env, "getNextBatchMeta", "()J", &_scanner_api.get_next_batch_meta));
    RETURN_IF_ERROR(cls.get_method(env, "getStatistics", "()Ljava/util/Map;",
                                   &_scanner_api.get_statistics));
    RETURN_IF_ERROR(
            cls.get_method(env, "getAppendDataTime", "()J", &_scanner_api.get_append_data_time));
    RETURN_IF_ERROR(cls.get_method(env, "getCreateVectorTableTime", "()J",
                                   &_scanner_api.get_create_vector_table_time));
    RETURN_IF_ERROR(cls.get_method(env, "setBatchSize", "(I)V", &_scanner_api.set_batch_size));
    RETURN_IF_ERROR(cls.get_method(env, "releaseColumn", "(I)V", &_scanner_api.release_column));
    RETURN_IF_ERROR(cls.get_method(env, "releaseTable", "()V", &_scanner_api.release_table));
    RETURN_IF_ERROR(cls.get_method(env, "close", "()V", &_scanner_api.close));
    return Status::OK();
}

Status PluginRegistry::_init_writer_api() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Env::Get(&env));

    GlobalClass& cls = _writer_api.cls;
    RETURN_IF_ERROR(Util::find_class(env, WRITER_CLASS, &cls));
    RETURN_IF_ERROR(cls.get_method(env, "open", "()V", &_writer_api.open));
    RETURN_IF_ERROR(cls.get_method(env, "write", "(Ljava/util/Map;)V", &_writer_api.write));
    RETURN_IF_ERROR(
            cls.get_method(env, "getStatistics", "()Ljava/util/Map;", &_writer_api.get_statistics));
    RETURN_IF_ERROR(cls.get_method(env, "close", "()V", &_writer_api.close));
    return Status::OK();
}

Status PluginRegistry::_create_instance(JNIEnv* env, const PluginRef& ref, int batch_size,
                                        const std::map<std::string, std::string>& params,
                                        GlobalObject* instance) {
    const Registry* registry = nullptr;
    RETURN_IF_ERROR(_ensure_registry(&registry));

    LocalString plugin;
    RETURN_IF_ERROR(LocalString::new_string(env, std::string(ref.plugin).c_str(), &plugin));
    LocalString factory;
    RETURN_IF_ERROR(LocalString::new_string(env, std::string(ref.factory).c_str(), &factory));
    LocalObject params_map;
    RETURN_IF_ERROR(Util::convert_to_java_map(env, params, &params_map));

    return registry->cls.call_static_object_method(env, registry->create_instance)
            .with_arg(plugin)
            .with_arg(factory)
            .with_arg(static_cast<jint>(batch_size))
            .with_arg(params_map)
            .call(instance);
}

// Scanner and writer factories share one namespace per plugin, and createInstance() looks a name
// up in both lists - returning a scanner first when a name somehow sits in both. For every
// compile-time PluginRef that is a non-issue, but writer_class is user text: a TVF sink can ask
// for "jdbc:connection-tester", get a JniScanner back, and then have JniWriter's method ids
// called on it. Those methods are final, so HotSpot dispatches them non-virtually against a
// foreign receiver - undefined behaviour where a clean error is required.
Status PluginRegistry::_check_kind(JNIEnv* env, const PluginRef& ref, const GlobalObject& instance,
                                   const GlobalClass& expected, const char* expected_kind) {
    if (expected.is_instance(env, instance)) {
        return Status::OK();
    }
    return Status::InvalidArgument("Java plugin '{}' factory '{}' does not produce {}", ref.plugin,
                                   ref.factory, expected_kind);
}

Status PluginRegistry::create_scanner(JNIEnv* env, const PluginRef& ref, int batch_size,
                                      const std::map<std::string, std::string>& params,
                                      GlobalObject* scanner, const ScannerApi** api) {
    static std::once_flag scanner_api_once;
    static Status scanner_api_status;
    std::call_once(scanner_api_once, []() { scanner_api_status = _init_scanner_api(); });
    RETURN_IF_ERROR(scanner_api_status);

    RETURN_IF_ERROR(_create_instance(env, ref, batch_size, params, scanner));
    RETURN_IF_ERROR(_check_kind(env, ref, *scanner, _scanner_api.cls, "a scanner"));
    *api = &_scanner_api;
    return Status::OK();
}

Status PluginRegistry::create_writer(JNIEnv* env, const PluginRef& ref, int batch_size,
                                     const std::map<std::string, std::string>& params,
                                     GlobalObject* writer, const WriterApi** api) {
    static std::once_flag writer_api_once;
    static Status writer_api_status;
    std::call_once(writer_api_once, []() { writer_api_status = _init_writer_api(); });
    RETURN_IF_ERROR(writer_api_status);

    RETURN_IF_ERROR(_create_instance(env, ref, batch_size, params, writer));
    RETURN_IF_ERROR(_check_kind(env, ref, *writer, _writer_api.cls, "a writer"));
    *api = &_writer_api;
    return Status::OK();
}

Status PluginRegistry::create_udf_executor(JNIEnv* env, const PluginRef& ref,
                                           const LocalArray& thrift_params, GlobalObject* executor,
                                           GlobalClass* executor_class) {
    const Registry* registry = nullptr;
    RETURN_IF_ERROR(_ensure_registry(&registry));

    LocalString plugin;
    RETURN_IF_ERROR(LocalString::new_string(env, std::string(ref.plugin).c_str(), &plugin));
    LocalString factory;
    RETURN_IF_ERROR(LocalString::new_string(env, std::string(ref.factory).c_str(), &factory));

    RETURN_IF_ERROR(registry->cls.call_static_object_method(env, registry->create_udf_executor)
                            .with_arg(plugin)
                            .with_arg(factory)
                            .with_arg(thrift_params)
                            .call(executor));
    return Util::get_object_class(env, *executor, executor_class);
}

Status PluginRegistry::clean_udf_cache(int64_t function_id, const std::string& function_signature) {
    if (!_registry_ready) {
        return Status::OK();
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Env::Get(&env));
    const Registry* registry = nullptr;
    RETURN_IF_ERROR(_ensure_registry(&registry));

    LocalString signature;
    RETURN_IF_ERROR(LocalString::new_string(env, function_signature.c_str(), &signature));
    return registry->cls.call_static_void_method(env, registry->clean_udf_cache)
            .with_arg(static_cast<jlong>(function_id))
            .with_arg(signature)
            .call();
}

Status PluginRegistry::plugin_status_json(std::string* status) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Env::Get(&env));
    const Registry* registry = nullptr;
    RETURN_IF_ERROR(_ensure_registry(&registry));

    LocalString json;
    RETURN_IF_ERROR(
            registry->cls.call_static_object_method(env, registry->plugin_status_json).call(&json));
    LocalStringBufferGuard chars;
    RETURN_IF_ERROR(json.get_string_chars(env, &chars));
    *status = chars.get();
    return Status::OK();
}

bool PluginRegistry::any_plugin_deployed() {
    std::error_code ec;
    std::filesystem::directory_iterator entries(config::jni_plugin_dir, ec);
    if (ec) {
        // No directory at all is the ordinary state of a BE that reads no Java table format,
        // not an error to report.
        return false;
    }
    // Advanced through the error_code overload rather than with range-for: range-for uses
    // operator++, which throws, and one of the two callers is the /api/jni_plugin_status
    // handler - a libevent C callback with no catch anywhere above it, so a plugin directory
    // removed or made unreadable while that endpoint is being polled would reach
    // std::terminate. An entry we cannot read is simply not counted.
    const std::filesystem::directory_iterator end;
    for (; entries != end; entries.increment(ec)) {
        if (ec) {
            LOG(WARNING) << "failed to walk the Java plugin directory " << config::jni_plugin_dir
                         << ": " << ec.message();
            return false;
        }
        // A plugin is a directory; a stray file next to them is not one.
        if (entries->is_directory(ec) && !ec) {
            return true;
        }
    }
    return false;
}

Status PluginRegistry::warmup() {
    if (!any_plugin_deployed()) {
        // Nothing to warm, and reaching Java to find that out would create the JVM this whole
        // arrangement exists to avoid. This is what makes warmup safe to turn on: on a BE with
        // no plugin deployed it costs one directory read.
        return Status::OK();
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Env::Get(&env));
    const Registry* registry = nullptr;
    RETURN_IF_ERROR(_ensure_registry(&registry));

    return registry->cls.call_static_void_method(env, registry->warmup).call();
}

} // namespace doris::Jni
