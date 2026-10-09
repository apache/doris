# Third-Party Dependency Lookup Guide

This file applies to all changes under `thirdparty/`, including patches applied to vendored
projects.

## Keep dependency lookup inside Doris third-party

When locating a header, library, or CMake package provided by the Doris third-party build, search
only inside the Doris third-party install directory. Do not allow CMake to fall back to system
directories, user-installed packages, environment-provided prefixes, or another checkout.

The third-party build sets `CMAKE_INSTALL_PREFIX` to `${TP_INSTALL_DIR}`. Prefer that existing
value instead of introducing another variable for the same directory.

For `find_path` and `find_library`:

- Provide explicit `PATHS` below `${CMAKE_INSTALL_PREFIX}`.
- Always specify `NO_DEFAULT_PATH`.
- Use `NO_CACHE` and a Doris-specific result variable when supported, so a stale CMake cache cannot
  select a path outside the Doris third-party directory.
- Use `REQUIRED` unless absence is an explicitly supported configuration.

Example:

```cmake
find_path(DORIS_FOO_INCLUDE_DIR
          NAMES foo/foo.h
          PATHS "${CMAKE_INSTALL_PREFIX}/include"
          NO_DEFAULT_PATH
          NO_CACHE
          REQUIRED)
find_library(DORIS_FOO_LIBRARY
             NAMES foo
             PATHS "${CMAKE_INSTALL_PREFIX}/lib" "${CMAKE_INSTALL_PREFIX}/lib64"
             NO_DEFAULT_PATH
             NO_CACHE
             REQUIRED)
```

Apply the same rule to `find_package`: provide only package paths rooted under
`${CMAKE_INSTALL_PREFIX}`, use `NO_DEFAULT_PATH`, and ensure a cached `<Package>_DIR` cannot point
outside that directory.

Do not rely on an unconstrained `find_path`, `find_library`, `find_package`, `CMAKE_PREFIX_PATH`,
the host `PATH`, or platform default search paths for a Doris-managed dependency. A dependency
missing from the Doris third-party directory must fail configuration instead of silently linking a
different installation.

System toolchain components and dependencies intentionally supplied by the operating system are
outside this rule, but that intent must be explicit in the surrounding build configuration.

## Preserve environment sanitization

`build-thirdparty.sh` clears ambient CMake code-injection, vcpkg, and Conda variables immediately
after loading `env.sh`. Keep this sanitization before any third-party download or build command,
and add newly supported package-manager or CMake injection variables when they could redirect
dependency resolution outside the Doris third-party directory.

Never `source` or use `.` to execute a script from an extracted third-party source tree. Invoke
upstream scripts as executables or through `bash`/`sh` so they run in a child process. Keep each
`build_<package>` function invocation inside its package subshell in the main build loop. This
boundary prevents exports, shell options, traps, functions, and working-directory changes made by
one package from leaking into later package builds, including if an upstream script is accidentally
sourced in the future.

Only repository-owned initialization files such as `env.sh` and `thirdparty/vars.sh` may be sourced,
and they must be sourced before package builds begin.
