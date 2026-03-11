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

// Suppress the GEOS warning about using the C++ API instead of the C API.
// We intentionally use the C++ API for convenience; the warning is treated
// as an error by -Werror, so we must suppress it explicitly.
#define USE_UNSTABLE_GEOS_CPP_API

#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include <geos/operation/valid/MakeValid.h>
#include <geos/simplify/TopologyPreservingSimplifier.h>

#include <memory>
#include <string>

namespace doris {

// Thread-local GEOS context wrapper. GEOS GeometryFactory and WKTReader/Writer
// are not thread-safe, so each thread gets its own instance.
class GEOSWrapper {
public:
    static GEOSWrapper& instance() {
        thread_local GEOSWrapper inst;
        return inst;
    }

    // Parse WKT string into a GEOS Geometry
    std::unique_ptr<geos::geom::Geometry> from_wkt(const std::string& wkt) {
        try {
            return _reader.read(wkt);
        } catch (const std::exception&) {
            return nullptr;
        }
    }

    // Convert GEOS Geometry to WKT string
    std::string to_wkt(const geos::geom::Geometry* geom) {
        try {
            return _writer.write(geom);
        } catch (const std::exception&) {
            return "";
        }
    }

    const geos::geom::GeometryFactory& factory() { return *_factory; }

private:
    GEOSWrapper()
            : _factory(geos::geom::GeometryFactory::create()),
              _reader(_factory.get()),
              _writer() {
        _writer.setTrim(true);
    }

    geos::geom::GeometryFactory::Ptr _factory;
    geos::io::WKTReader _reader;
    geos::io::WKTWriter _writer;
};

} // namespace doris
