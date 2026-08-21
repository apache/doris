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

"""Compiler/linker timing wrapper for `build.sh --compile-bench`.

In compile-bench mode this script is installed as
CMAKE_<LANG>_COMPILER_LAUNCHER / CMAKE_<LANG>_LINKER_LAUNCHER, i.e. in the
slot where ccache would normally sit (compile-bench never uses ccache, so the
slot is free). Every underlying compiler/linker invocation is timed and one
JSON line is appended to $DORIS_COMPILE_BENCH_DIR/compile_log.jsonl:

    {"ts": ..., "kind": "compile|pch|link|other", "src": ..., "out": ...,
     "cwd": ..., "compiler": ..., "wall_s": ..., "user_s": ..., "sys_s": ...,
     "maxrss_mb": ..., "rc": ...}

Non-interference guarantees:
- child stdout/stderr are fully inherited, diagnostics are unchanged
- the child's exit code is propagated verbatim (signals become 128+N)
- if DORIS_COMPILE_BENCH_DIR is not set, exec() straight through: zero overhead
- a bookkeeping failure never fails the build
"""

import fcntl
import json
import os
import sys
import time

SRC_EXTS = (".cpp", ".cc", ".cxx", ".c", ".mm", ".m", ".S", ".s")
HDR_EXTS = (".h", ".hpp", ".hxx", ".hh")
# Flags whose presence means "not a real compile/link" (probes, dep scans...).
NON_BUILD_FLAGS = ("-E", "-M", "-MM", "--version", "-dumpversion", "-dumpmachine")


def classify(args):
    """Return (kind, src, out) extracted from a compiler command line.

    Only the `-o path` split form is handled: that is the only form CMake and
    ninja generate. Flags that merely start with "-o" (-objc..., -only...)
    must not be misparsed, so the fused "-opath" form is deliberately ignored.
    """
    out = None
    src = None
    has_c = False
    x_header = False
    prev = None
    for arg in args[1:]:
        if prev == "-o":
            out = arg
        elif prev == "-x" and arg in ("c++-header", "c-header"):
            x_header = True
        elif not arg.startswith("-"):
            if arg.endswith(SRC_EXTS):
                src = arg
            elif x_header and src is None and arg.endswith(HDR_EXTS):
                src = arg
        if arg == "-c":
            has_c = True
        prev = arg

    if x_header or (out is not None and out.endswith((".pch", ".gch"))):
        kind = "pch"
    elif any(a in NON_BUILD_FLAGS for a in args):
        kind = "other"
    elif has_c:
        kind = "compile"
    elif out is not None:
        kind = "link"
    else:
        kind = "other"
    return kind, src, out


def main():
    argv = sys.argv[1:]
    if not argv:
        sys.stderr.write("cc-timing-wrapper.py: missing compiler command\n")
        return 2

    log_dir = os.environ.get("DORIS_COMPILE_BENCH_DIR")
    if not log_dir:
        # Not in a bench run (or env was stripped): become the compiler.
        os.execvp(argv[0], argv)

    start_ts = time.time()
    t0 = time.monotonic()
    pid = os.fork()
    if pid == 0:
        try:
            os.execvp(argv[0], argv)
        except OSError as exc:
            sys.stderr.write(
                "cc-timing-wrapper.py: failed to exec {}: {}\n".format(argv[0], exc)
            )
        os._exit(127)
    _, status, rusage = os.wait4(pid, 0)
    wall_s = time.monotonic() - t0

    if os.WIFSIGNALED(status):
        rc = 128 + os.WTERMSIG(status)
    else:
        rc = os.WEXITSTATUS(status)

    kind, src, out = classify(argv)
    # ru_maxrss is bytes on macOS, KiB on Linux.
    if sys.platform == "darwin":
        maxrss_mb = rusage.ru_maxrss / (1024.0 * 1024.0)
    else:
        maxrss_mb = rusage.ru_maxrss / 1024.0

    record = {
        "ts": round(start_ts, 3),
        "kind": kind,
        "src": src,
        "out": out,
        "cwd": os.getcwd(),
        "compiler": os.path.basename(argv[0]),
        "wall_s": round(wall_s, 3),
        "user_s": round(rusage.ru_utime, 3),
        "sys_s": round(rusage.ru_stime, 3),
        "maxrss_mb": round(maxrss_mb, 1),
        "rc": rc,
    }
    try:
        with open(os.path.join(log_dir, "compile_log.jsonl"), "a") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            fh.write(json.dumps(record, sort_keys=True) + "\n")
    except OSError:
        pass
    return rc


if __name__ == "__main__":
    sys.exit(main())
