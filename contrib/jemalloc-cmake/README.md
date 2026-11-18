# jemalloc

BE, Cloud, and the native allocator-hook tests build jemalloc 5.3.1 with
CMake ExternalProject. The version and SHA256 are pinned in CMakeLists.txt.
No jemalloc headers or archives from thirdparty/installed are used.

The ExternalProject runs upstream configure and GNU make out of source. It
preserves Doris's `je` public-symbol prefix, static PIC library, heap profiling,
Linux GNU libunwind backend, and 64KiB allocator pages on ARM64. The local patch
omits jemalloc_mangle.h from the generated public header for Doris's malloc hooks.
macOS retains its previous profiler backend. ASAN/tcmalloc builds skip jemalloc.

Sources are cached in `.dependency-downloads` at the checkout root. Set the CMake
cache variable `DORIS_DEPENDENCY_DOWNLOAD_DIR` to share verified archives or to
prepopulate `jemalloc-5.3.1.tar.bz2` for offline builds. Each build directory has
its own sources, objects, installation, and completion stamps; incremental builds
do not rebuild jemalloc. Compiler launchers (including ccache) are passed to
configure. `build.sh` and `run-be-ut.sh` pass their job count to
`JEMALLOC_BUILD_JOBS`.

To upgrade, update the version and SHA256, check the patch against the new source,
and run `bash run-be-ut.sh --jemalloc-hook --run -j 8`, followed by a BE build with
jemalloc enabled and heap-profile collection. A patch change selects a fresh
source tree. Configure options and compiler changes invalidate the configure
step; an unchanged invocation reuses the completed build.
