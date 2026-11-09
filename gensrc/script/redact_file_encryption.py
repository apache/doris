#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Protect file-key diagnostics by transforming generated Thrift code at build time.

The FE Thrift module and regression framework invoke this in Maven process-sources,
after Java generation; gensrc/thrift/Makefile invokes it after C++ generation.
Queries execute the transformed code, without invoking this Python script.

TFileEncryptionInfo intentionally carries an unwrapped per-file key and AAD from
FE to BE for reader setup. Default generated strings and JSON can expose those
bytes in diagnostics, so redact them while preserving the actual RPC payload.

Thrift 0.24 ignores sensitive annotations. Keep this transform narrow and fail
on generator drift rather than silently compiling an unredacted representation.
Only binary/compact protocols (and Java tuple encoding) carry the real payload.
"""

import argparse
import pathlib
import re

MARKER = "DORIS_FILE_ENCRYPTION_REDACTED"


def replace_body(source, signature, body):
    matches = list(re.finditer(signature + r"\s*\{", source))
    if len(matches) != 1:
        raise ValueError("Unexpected generated secret-carrier method shape")
    start = matches[0].end()
    depth = 1
    end = start
    while depth and end < len(source):
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    if depth:
        raise ValueError("Unterminated generated secret-carrier method")
    return source[:start] + "\n" + body + "\n" + source[end - 1:]


def redact_java(source):
    source = replace_body(source, r"public java\.lang\.String toString\(\)",
                          '    return "TFileEncryptionInfo(<redacted>)";')
    signature = r"public void write\(org\.apache\.thrift\.protocol\.TProtocol oprot\) throws org\.apache\.thrift\.TException"
    guard = """    // Diagnostic protocols must never serialize query-scoped secrets.
    if (!(oprot instanceof org.apache.thrift.protocol.TBinaryProtocol
        || oprot instanceof org.apache.thrift.protocol.TCompactProtocol)) {
      oprot.writeStructBegin(new org.apache.thrift.protocol.TStruct("TFileEncryptionInfo"));
      oprot.writeFieldBegin(new org.apache.thrift.protocol.TField(
          "redacted", org.apache.thrift.protocol.TType.STRING, (short) 1000));
      oprot.writeString("<redacted>");
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
      return;
    }
    scheme(oprot).write(oprot, this);"""
    source = replace_body(source, signature, guard)
    # Gson omits transient fields. Thrift reads/writes fields explicitly, so wire bytes are unchanged.
    source = re.sub(r"(?m)^(  (?:public|private|protected)) (?!static\b|transient\b)"
                            r"([^\n(){};]*;)", r"\1 transient \2", source)
    for field in ("version", "key", "aad_prefix"):
        if not re.search(r"(?m)^  public transient [^\n;]*\b" + field + r";", source):
            raise ValueError("Unexpected generated secret-carrier fields")
    return source + "\n// " + MARKER + "\n"


def redact_cpp(source):
    includes = """#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
"""
    if source.startswith(includes):
        source = source[len(includes):]
    source = re.sub(r"\n  // DORIS_FILE_ENCRYPTION_WIRE_GUARD_BEGIN.*?"
                    r"  // DORIS_FILE_ENCRYPTION_WIRE_GUARD_END\n", "", source, flags=re.S)
    source = includes + source
    source = replace_body(source, r"void TFileEncryptionInfo::printTo\(std::ostream& out\) const",
                          '  out << "TFileEncryptionInfo(<redacted>)";')
    pattern = r"(uint32_t TFileEncryptionInfo::write\(::apache::thrift::protocol::TProtocol\* oprot\) const \{)"
    guard = """
  // DORIS_FILE_ENCRYPTION_WIRE_GUARD_BEGIN
  // ThriftDebugString uses write(), not printTo(). Only wire protocols may see keys.
  using namespace ::apache::thrift::protocol;
  using ::apache::thrift::transport::TMemoryBuffer;
  if (dynamic_cast<TBinaryProtocol*>(oprot) == nullptr
      && dynamic_cast<TCompactProtocol*>(oprot) == nullptr
      && dynamic_cast<TBinaryProtocolT<TMemoryBuffer>*>(oprot) == nullptr
      && dynamic_cast<TCompactProtocolT<TMemoryBuffer>*>(oprot) == nullptr) {
    uint32_t xfer = oprot->writeStructBegin("TFileEncryptionInfo");
    xfer += oprot->writeFieldBegin("redacted", T_STRING, 1000);
    xfer += oprot->writeString("<redacted>");
    xfer += oprot->writeFieldEnd();
    xfer += oprot->writeFieldStop();
    xfer += oprot->writeStructEnd();
    return xfer;
  }
  // DORIS_FILE_ENCRYPTION_WIRE_GUARD_END
"""
    matches = list(re.finditer(pattern, source))
    if len(matches) != 1:
        raise ValueError("Unexpected generated secret-carrier writer shape")
    point = matches[0].end()
    return source[:point] + guard + source[point:] + "\n// " + MARKER + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java", type=pathlib.Path)
    parser.add_argument("--cpp", type=pathlib.Path)
    args = parser.parse_args()
    if not args.java and not args.cpp:
        parser.error("Expected a generated Java or C++ file")
    for path, transform in [(args.java, redact_java), (args.cpp, redact_cpp)]:
        if path is None:
            continue
        source = path.read_text(encoding="utf-8")
        source = source.replace("\n// " + MARKER + "\n", "")
        transformed = transform(source)
        if transformed != path.read_text(encoding="utf-8"):
            path.write_text(transformed, encoding="utf-8")


if __name__ == "__main__":
    main()
