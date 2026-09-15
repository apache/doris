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

#include <map>
#include <string>

#include "format/transformer/vfile_format_transformer.h"
#include "util/jni-util.h"
#include "util/jni_plugin_registry.h"

namespace doris {

/**
 * VJniFormatTransformer is a VFileFormatTransformer implementation that delegates
 * write operations to a Java-side JniWriter via JNI. It sits alongside
 * VCSVTransformer/VParquetTransformer/VOrcTransformer as a peer implementation.
 *
 * The Java writer must extend org.apache.doris.jni.spi.JniWriter and is built by the named
 * plugin factory, from the same (batchSize, params) pair a scanner gets.
 *
 * The writer is named in two different ways, so there are two constructors. Most call sites know
 * their plugin at compile time and pass one of the Jni::plugin constants. The TVF sink does not:
 * its plugin and factory come from a write statement's properties, and a PluginRef is a pair of
 * views, so pointing one at a request-scoped string would outlive its storage. Both forms
 * therefore end up in the owned strings below, and the PluginRef is rebuilt from them at use.
 */
class VJniFormatTransformer final : public VFileFormatTransformer {
public:
    // For a plugin named at compile time; the ref's halves are copied, not borrowed.
    VJniFormatTransformer(RuntimeState* state, const VExprContextSPtrs& output_vexpr_ctxs,
                          Jni::PluginRef plugin_ref,
                          std::map<std::string, std::string> writer_params);

    // For a plugin named at run time, e.g. by a TVF sink's writer_class property.
    VJniFormatTransformer(RuntimeState* state, const VExprContextSPtrs& output_vexpr_ctxs,
                          std::string plugin, std::string factory,
                          std::map<std::string, std::string> writer_params);

    ~VJniFormatTransformer() override = default;

    Status open() override;
    Status write(const Block& block) override;
    Status close() override;
    int64_t written_len() override;

    // Retrieve statistics from Java-side writer (calls JniWriter.getStatistics())
    std::map<std::string, std::string> get_statistics();

private:
    Status _init_jni_writer(JNIEnv* env, int batch_size);

    std::string _plugin;
    std::string _factory;
    std::map<std::string, std::string> _writer_params;

    // JNI handles (same pattern as JniConnector)
    Jni::GlobalObject _jni_writer_obj;
    // Resolved on the SPI base class and shared process-wide, so this is a borrowed pointer
    // into storage that outlives any transformer.
    const Jni::WriterApi* _writer_api = nullptr;

    // Schema cache (computed on first write, reused afterwards)
    bool _schema_cached = false;
    std::string _cached_required_fields;
    std::string _cached_columns_types;

    bool _opened = false;
    bool _closed = false;
};

} // namespace doris
