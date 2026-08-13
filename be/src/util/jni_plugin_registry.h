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

#pragma once

#include <jni.h>

#include <map>
#include <string>
#include <string_view>

#include "common/status.h"
#include "util/jni-util.h"

namespace doris::Jni {

// How BE names one Java factory: the plugin's directory under lib/java/plugins/, and the
// factory's own name within that plugin. It replaces the class name BE used to reflect on -
// a concrete class is private to its plugin's classloader, so naming one from C++ stopped
// being possible the moment plugins were isolated.
//
// Both halves are compile-time constants; nothing builds a PluginRef out of a temporary.
struct PluginRef {
    std::string_view plugin;
    std::string_view factory;
};

// Every plugin and factory BE addresses, in one place because the plugin half is also a
// deployment directory name: it appears in lib/java/plugins/, in build.sh, in the CI check
// that scans those directories, and in the "is not deployed" a user sees. It is named here
// once so renaming a plugin is one edit rather than a hunt through the readers - the same
// connector is reached from both the v1 and the v2 scan paths, and two independent string
// literals for one directory is how they would drift apart.
//
// The factory half must be unique within its plugin across all kinds of factory, because the
// pair below is everything BE sends: PluginRegistry receives a plugin name and a factory name
// and nothing that says which kind was wanted, so a name meaning two things would resolve by
// whichever list happens to be searched first.
//
// So a factory is named for what it does within its plugin, not for the connector - the plugin
// half already says which connector this is. A connector's ordinary read side is "reader" and
// its write side "writer"; anything else says what it is, as "connection-tester" does.
//
// The entry still repeating its plugin's name predates that rule and is renamed to "reader" by
// the change that turns it into a plugin, together with the getName() of the factory that change
// writes. Renaming it earlier would name a factory nothing implements.
namespace plugin {

inline constexpr PluginRef PAIMON_SCANNER {"paimon", "reader"};
inline constexpr PluginRef HUDI_SCANNER {"hudi", "reader"};
// Only the Iceberg system tables come through Java; Iceberg data files are read natively - so
// this one keeps its own name when it is migrated. It is not the connector's reader.
inline constexpr PluginRef ICEBERG_SYS_TABLE_SCANNER {"iceberg", "sys-table"};
inline constexpr PluginRef MAX_COMPUTE_SCANNER {"max-compute", "reader"};
inline constexpr PluginRef MAX_COMPUTE_WRITER {"max-compute", "writer"};
inline constexpr PluginRef JDBC_SCANNER {"jdbc", "reader"};
inline constexpr PluginRef JDBC_WRITER {"jdbc", "writer"};
// Testing a JDBC connection is a scan of zero rows, so it is a scanner factory of its own
// rather than a second entry point.
inline constexpr PluginRef JDBC_CONNECTION_TESTER {"jdbc", "connection-tester"};
inline constexpr PluginRef TRINO_CONNECTOR_SCANNER {"trino-connector", "trino-connector"};

} // namespace plugin

// The methods BE calls on a scanner, resolved on the SPI's JniScanner rather than on the
// concrete class. Two reasons, and the first is what forces the design: the concrete class
// is loaded by its plugin's classloader, so its jclass cannot be shared between plugins,
// while the abstract base is loaded by the shared parent and is one class for the whole
// process. The second is that every method here is final on the base class, so a plugin
// cannot shadow one and change what BE calls.
//
// The names and descriptors are fixed by PROTOCOL.md section 1. Changing one means changing
// both sides in the same commit.
struct ScannerApi {
    MethodId open;
    MethodId get_next_batch_meta;
    MethodId get_table_schema;
    MethodId get_statistics;
    MethodId get_append_data_time;
    MethodId get_create_vector_table_time;
    MethodId set_batch_size;
    MethodId release_column;
    MethodId release_table;
    MethodId close;
};

// The writer half of the same contract, resolved on the SPI's JniWriter.
struct WriterApi {
    MethodId open;
    MethodId write;
    MethodId get_statistics;
    MethodId close;
};

// The single Java entry point BE calls into, wrapping
// org.apache.doris.jni.bootstrap.PluginRegistry.
//
// There is deliberately no fallback anywhere below. If the registry cannot answer - the
// plugin is not deployed, its jars are broken, the factory name is wrong - that is the
// result, and the Java message says which of those it was. Nothing here retries against
// another classloader or degrades to a built-in reader: doing so would report a plugin's
// deployment problem as a query producing no rows.
class PluginRegistry {
public:
    // Creates a scanner and hands back the shared method ids for it. They come together
    // because an instance is useless without them and resolving them anywhere but on the
    // SPI base class is the mistake this API exists to prevent.
    static Status create_scanner(JNIEnv* env, const PluginRef& ref, int batch_size,
                                 const std::map<std::string, std::string>& params,
                                 GlobalObject* scanner, const ScannerApi** api);

    static Status create_writer(JNIEnv* env, const PluginRef& ref, int batch_size,
                                const std::map<std::string, std::string>& params,
                                GlobalObject* writer, const WriterApi** api);

    // Creates the executor behind one Java function. The parameters stay a serialized thrift
    // struct all the way into the plugin, which owns the only copy of thrift that can read
    // them; the executor comes back as a plain object because the three function kinds do
    // not share a method shape. See UdfExecutorFactory.
    static Status create_udf_executor(JNIEnv* env, const PluginRef& ref, const jbyte* thrift_params,
                                      jint length, GlobalObject* executor);

    // Forwarded to every loaded plugin on DROP FUNCTION, so whichever one compiled that
    // function can drop what it cached for it.
    //
    // Returns without touching Java when no plugin has ever been loaded: dropping a
    // function must not be the thing that starts a JVM on a BE that runs no Java at all.
    static Status clean_udf_cache(const std::string& function_signature);

    // State of every plugin loaded so far, as JSON.
    static Status plugin_status_json(std::string* status);

    // Loads every deployed plugin now rather than on first use, so a broken deployment is
    // in the log before a user query finds it. Never fails for a single plugin's sake.
    //
    // Does nothing, and creates no JVM, when no plugin is deployed.
    static Status warmup();

    // Whether the plugin directory holds at least one plugin. Public because it is the
    // reason warmup can be on by default, and something has to be able to check it.
    static bool any_plugin_deployed();

private:
    // The bootstrap class and its static method ids, resolved once.
    struct Registry {
        GlobalClass cls;
        MethodId create_instance;
        MethodId create_udf_executor;
        MethodId clean_udf_cache;
        MethodId plugin_status_json;
        MethodId warmup;
    };

    static Status _ensure_registry(const Registry** registry);
    static Status _init_registry();
    static Status _init_scanner_api();
    static Status _init_writer_api();

    // createInstance() backs both scanners and writers: what a plugin hands back is decided
    // by which factory list the name was looked up in, not by a separate Java entry point.
    static Status _create_instance(JNIEnv* env, const PluginRef& ref, int batch_size,
                                   const std::map<std::string, std::string>& params,
                                   GlobalObject* instance);

    static Registry _registry;
    static ScannerApi _scanner_api;
    static WriterApi _writer_api;

    // Whether _init_registry() has ever succeeded, so that dropping a function can tell "no
    // plugin has ever been loaded" from "the registry is unreachable".
    static std::atomic<bool> _registry_ready;
};

} // namespace doris::Jni
