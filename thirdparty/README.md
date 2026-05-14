# Doris Thirdparty — Build Modes & Maintenance Guide

Doris BE supports **two thirdparty build modes**:

| Mode | When | Speed | Sanitizer support |
|------|------|-------|-------------------|
| **prebuilt** (`USE_CONTRIB_SOURCE=OFF`) | Release / Debug only | Fast clean rebuild (thirdparty already built) | ❌ Not allowed |
| **source** (`USE_CONTRIB_SOURCE=ON`) | Any build type | Slower clean rebuild | ✅ Required for ASAN/TSAN/MSAN/LSAN/UBSAN |

The default is **source mode** for any build type. `--use-prebuilt` and
`--use-source` flags on `build.sh` switch explicitly. Sanitizer build types
force source mode regardless of the flag.

## Quick start

After `git clone`, `thirdparty/src/` and `thirdparty/installed/` are both empty
(both are .gitignore'd). `build.sh` populates them on demand depending on mode.

```sh
# Source mode (default). build.sh auto-runs download-thirdparty.sh if
# thirdparty/src/ is empty, then cmake adds it as a subdirectory and builds
# everything from source.
./build.sh --be

# Prebuilt mode. build.sh auto-downloads + extracts the official prebuilt
# tarball if thirdparty/installed/ is empty. ~700 MB compressed, ~2 GB extracted.
./build.sh --be --use-prebuilt

# Pin a specific release branch (default is master):
DORIS_PREBUILT_VERSION=3.1 ./build.sh --be --use-prebuilt

# Or build prebuilt from source once (slow, 1-2 hours) — uncommon path:
bash thirdparty/build-thirdparty.sh
./build.sh --be --use-prebuilt

# Sanitizer modes always use source. --use-prebuilt is a hard error here.
./build.sh --be --msan  # equivalent to --be --msan --use-source
```

### What "auto-download" means in source mode

When you run `./build.sh --be` (or any non-`--use-prebuilt` invocation),
build.sh inspects `thirdparty/src/`:

- If it has fewer than 10 entries (fresh-clone or freshly cleaned),
  `bash thirdparty/download-thirdparty.sh` runs first to fetch every archive
  listed in `thirdparty/vars.sh`, unpack, verify MD5, and apply all patches
  in `thirdparty/patches/`. This takes ~5–15 minutes depending on network.
- If it already has the source tree, build.sh skips the download.

A separate `thirdparty/CMakeLists.txt` guard also fails fast with the same
message if cmake is invoked directly with an empty source tree.

## Why two modes

Source mode (introduced in tp-build branch, replacing autoconf with pure CMake)
gives:

- **ABI consistency**: thirdparty + BE compiled with the same toolchain and
  flags. Same `-stdlib=libc++`, same C++ ABI version.
- **Sanitizer flag propagation**: `add_subdirectory(thirdparty)` makes the BE
  CMake's `-fsanitize=memory` (etc.) reach every thirdparty .o, so the LLVM
  sanitizer IR pass instruments them. Prebuilt `.a` files cannot be
  retroactively instrumented — that's why sanitizer builds require source mode.
- **Less coupling to system tools**: no autoconf, no `./configure`,
  CMake-everywhere.

Prebuilt mode preserves the legacy doris/master flow:

- **Faster `--clean` rebuild**: thirdparty is already built and installed; BE
  just links against the `.a` files.
- **Stable**: matches what doris/master historically did. Useful when bisecting
  build issues unrelated to thirdparty.

## How the two modes are wired

```
build.sh --be [--use-prebuilt | --use-source]
        |
        v
cmake -DUSE_CONTRIB_SOURCE=ON|OFF
        |
        v
be/CMakeLists.txt:
  if (sanitizer build type)
    force USE_CONTRIB_SOURCE=ON
  if (USE_CONTRIB_SOURCE)
    add_subdirectory(thirdparty/)              ← source mode entry
  else()
    include(be/cmake/thirdparty.cmake)         ← prebuilt mode entry
```

| File | Purpose | Active in mode |
|------|---------|----------------|
| `thirdparty/CMakeLists.txt` | source: declares 95+ targets via `add_contrib(<lib>)` → `thirdparty/cmake/<lib>.cmake` | source |
| `thirdparty/cmake/<lib>.cmake` | source: per-lib `add_library(<lib> STATIC <sources>)` + `target_*` | source |
| `be/cmake/thirdparty.cmake` | prebuilt: per-lib `add_thirdparty(<lib>)` registers `STATIC IMPORTED` pointing at `${THIRDPARTY_DIR}/lib*/lib<lib>.a` | prebuilt |
| `thirdparty/build-thirdparty.sh` | prebuilt: builds every lib from source and installs to `${THIRDPARTY_DIR}` | prebuilt prep |

`be/cmake/thirdparty.cmake` also contains an idempotency guard
(`if(TARGET <lib>) return()`) that means including it during source mode is
safe; it just no-ops on already-defined targets.

## Target name reconcile

Source mode and prebuilt mode use slightly different target names for some
libraries (Arrow's CMake adds a `_static` suffix; hyperscan's prebuilt is named
after its lib `hyperscan` while source uses the `hs.a`-based name `hs`; etc.).
`be/cmake/thirdparty.cmake` declares ALIAS targets at the bottom so code paths
written against either set of names work in both modes:

| Source-mode name | Prebuilt-mode name | Reconcile |
|------------------|---------------------|-----------|
| `arrow_static` / `arrow_flight_static` / `arrow_flight_sql_static` / `arrow_dataset_static` / `arrow_acero_static` / `parquet_static` | `arrow` / `arrow_flight` / `arrow_flight_sql` / `arrow_dataset` / `arrow_acero` / `parquet` | ALIAS in prebuilt |
| `hs` | `hyperscan` | ALIAS in prebuilt |
| `s2n` | `aws-s2n` | ALIAS in prebuilt |
| `libprotobuf` | `protobuf` | ALIAS in prebuilt (BE main CMakeLists.txt:458 references `libprotobuf` directly) |

When introducing a new dual-named target, add the ALIAS in
`be/cmake/thirdparty.cmake` after the `add_thirdparty()` block.

## Adding a new thirdparty library

1. Add download URL/MD5/source-name to `thirdparty/vars.sh`.
2. Add `build_<lib>()` function and call to `thirdparty/build-thirdparty.sh`
   (prebuilt mode).
3. Create `thirdparty/cmake/<lib>.cmake` with `add_library` + sources +
   includes (source mode).
4. Add `add_contrib(<lib>)` to `thirdparty/CMakeLists.txt` and append the target
   name to `COMMON_THIRDPARTY` if BE should link it.
5. Add `add_thirdparty(<lib> [LIB64|LIBNAME ...])` to
   `be/cmake/thirdparty.cmake` (prebuilt mode entry).
6. If source/prebuilt names differ, add an ALIAS at the bottom of
   `be/cmake/thirdparty.cmake`.

## Upgrading a library

1. Update `thirdparty/vars.sh`: URL, MD5, `*_SOURCE` directory name.
2. Update `thirdparty/cmake/<lib>.cmake` if the new version's source layout
   changed (file list, configuration macros, include paths).
3. Re-baseline patches in `thirdparty/patches/`: try `patch --dry-run`, rebase
   if hunks no longer apply cleanly. Patch filenames typically encode the
   version, so a renamed file may also be needed.
4. Clean rebuild and check both modes pass.

## Patch management

Patches live in `thirdparty/patches/<source-dir-name>.patch` and are applied
by `download-thirdparty.sh` at archive-extract time. The `patched_mark` file
in the source dir prevents re-applying.

To author a new patch:

1. Modify files in `thirdparty/src/<source-dir>/` directly.
2. `diff -u --label a/<rel> --label b/<rel> <orig> <changed>` for each file
   into one combined `.patch` file.
3. Save under `thirdparty/patches/<source-dir>.patch` (or
   `<source-dir>-<reason>.patch` for multiple).
4. Add `patch -p1 < ...` block to `download-thirdparty.sh`, guarded by
   `${PATCHED_MARK}`.

## Sub-CMake forwarding (gotcha)

Libraries built via `add_custom_command(... ${CMAKE_COMMAND} ...)` or
`ExternalProject_Add` invoke an **independent** sub-CMake that **does not
inherit** the parent's `CMAKE_C_FLAGS` / `CMAKE_CXX_FLAGS`. Parent sanitizer
flags (and any custom flags) are silently dropped.

The fix: forward parent flags explicitly into the sub-CMake invocation:

```cmake
add_custom_command(
    ...
    COMMAND ${CMAKE_COMMAND}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        "-DCMAKE_C_FLAGS=-fPIC ${OTHER_FLAGS} ${CMAKE_C_FLAGS}"
        "-DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}"
        ...
```

Without this, MSAN/ASAN flags vanish and the resulting `.a` is uninstrumented
even under a sanitizer build. We hit this with `hadoop_hdfs.cmake` —
[`thirdparty/cmake/hadoop_hdfs.cmake`](cmake/hadoop_hdfs.cmake) is the
canonical example of doing it right.

## Shim Find modules

Some thirdparty libraries (Arrow, gRPC, RocksDB, etc.) call
`find_package(<dep>)` from inside their CMake. In source mode, those
dependencies are `add_library` targets we already built; the shim modules in
`thirdparty/cmake/shims/` (Findlz4.cmake, Findre2Alt.cmake, Findsnappy.cmake,
Findzlib.cmake, Findzstd.cmake) make `find_package` find them without going
through the system.

When a thirdparty's CMake adds a new `find_package(<X>)` call, check if a shim
exists; if not, add one in `thirdparty/cmake/shims/Find<X>.cmake` redirecting
to the source-built target.

## Vendored dependencies (paimon-cpp pattern)

paimon-cpp ships its own copies of `roaring_bitmap`, `xxhash`, etc. Where the
versions are compatible with Doris's main thirdparty, `paimon_cpp.cmake`
declares ALIAS targets (e.g., `fmt_paimon` → `fmt`). Where versions diverge, a
separate static target is built (e.g., `roaring_bitmap` is paimon's bundled
single-file amalgamation, distinct from `CRoaring`).

When adding a new vendored target, document in the per-lib `.cmake` why an
ALIAS to a Doris main target isn't possible (API differences, version mismatch).

## CI matrix

Minimum CI verification:

| Build type | Mode | Why |
|------------|------|-----|
| Release | prebuilt | Verifies prebuilt mode doesn't regress |
| Release | source | Default path for new contributors |
| MSAN | source | Sanitizer flag propagation works end-to-end |

Adding any sanitizer (ASAN/TSAN/UBSAN) automatically uses source mode — no new
matrix dimension needed.
