# Developer manual

## Source code layout

```
OpenBLAS/  
├── benchmark                  Benchmark codes for BLAS
├── cmake                      CMakefiles
├── ctest                      Test codes for CBLAS interfaces
├── driver                     Implemented in C
│   ├── level2
│   ├── level3
│   ├── mapper
│   └── others                 Memory management, threading, etc
├── exports                    Generate shared library
├── interface                  Implement BLAS and CBLAS interfaces (calling driver or kernel)
│   ├── lapack
│   └── netlib
├── kernel                     Optimized assembly kernels for CPU architectures
│   ├── alpha                  Original GotoBLAS kernels for DEC Alpha
│   ├── arm                    ARMV5,V6,V7 kernels (including generic C codes used by other architectures)
│   ├── arm64                  ARMV8
│   ├── generic                General kernel codes written in plain C, parts used by many architectures.
│   ├── ia64                   Original GotoBLAS kernels for Intel Itanium
│   ├── mips
│   ├── mips64
│   ├── power
|   ├── riscv64
|   ├── simd                   Common code for Universal Intrinsics, used by some x86_64 and arm64 kernels
│   ├── sparc
│   ├── x86
│   ├── x86_64
│   └── zarch   
├── lapack                      Optimized LAPACK codes (replacing those in regular LAPACK)
│   ├── getf2
│   ├── getrf
│   ├── getrs
│   ├── laswp
│   ├── lauu2
│   ├── lauum
│   ├── potf2
│   ├── potrf
│   ├── trti2
│   ├── trtri
│   └── trtrs
├── lapack-netlib               LAPACK codes from netlib reference implementation
├── reference                   BLAS Fortran reference implementation (unused)
├── relapack                    Elmar Peise's recursive LAPACK (implemented on top of regular LAPACK)
├── test                        Test codes for BLAS
└── utest                       Regression test

```

A call tree for `dgemm` looks as follows:
```
interface/gemm.c
        │
driver/level3/level3.c
        │
gemm assembly kernels at kernel/
```

To find the kernel currently used for a particular supported CPU, please check the corresponding `kernel/$(ARCH)/KERNEL.$(CPU)` file.

Here is an example for `kernel/x86_64/KERNEL.HASWELL`:
```
...
DTRMMKERNEL    =  dtrmm_kernel_4x8_haswell.c
DGEMMKERNEL    =  dgemm_kernel_4x8_haswell.S
...
```
According to the above `KERNEL.HASWELL`, OpenBLAS Haswell dgemm kernel file is `dgemm_kernel_4x8_haswell.S`.


## Optimizing GEMM for a given hardware

!!! abstract "Read the Goto paper to understand the algorithm"

    Goto, Kazushige; van de Geijn, Robert A. (2008).
    ["Anatomy of High-Performance Matrix Multiplication"](http://delivery.acm.org/10.1145/1360000/1356053/a12-goto.pdf?ip=155.68.162.54&id=1356053&acc=ACTIVE%20SERVICE&key=A79D83B43E50B5B8%2EF070BBE7E45C3F17%2E4D4702B0C3E38B35%2E4D4702B0C3E38B35&__acm__=1517932837_edfe766f1e295d9a7830812371e1d173).
    ACM Transactions on Mathematical Software 34 (3): Article 12

    (The above link is available only to ACM members, but this and many related
    papers is also available on [the pages of van de Geijn's FLAME project](http://www.cs.utexas.edu/~flame/web/FLAMEPublications.html))

The `driver/level3/level3.c` is the implementation of Goto's algorithm.
Meanwhile, you can look at `kernel/generic/gemmkernel_2x2.c`, which is a naive
`2x2` register blocking `gemm` kernel in C. Then:

* Write optimized assembly kernels. Consider instruction pipeline, available registers, memory/cache access.
* Tune cache block sizes (`Mc`, `Kc`, and `Nc`)

Note that not all of the CPU-specific parameters in `param.h` are actively used in algorithms.
`DNUMOPT` only appears as a scale factor in profiling output of the level3 `syrk` interface code,
while its counterpart `SNUMOPT` (aliased as `NUMOPT` in `common.h`) is not used anywhere at all. 

`SYMV_P` is only used in the generic kernels for the `symv` and `chemv`/`zhemv` functions -
at least some of those are usually overridden by CPU-specific implementations, so if you start
by cloning the existing implementation for a related CPU you need to check its `KERNEL` file
to see if tuning `SYMV_P` would have any effect at all.

`GEMV_UNROLL` is only used by some older x86-64 kernels, so not all sections in `param.h` define it.
Similarly, not all of the CPU parameters like L2 or L3 cache sizes are necessarily used in current
kernels for a given model - by all indications the CPU identification code was imported from some
other project originally.


## Running OpenBLAS tests

We use tests for Netlib BLAS, CBLAS, and LAPACK. In addition, we use
OpenBLAS-specific regression tests. They can be run with Make:

* `make -C test` for BLAS tests
* `make -C ctest` for CBLAS tests
* `make -C utest` for OpenBLAS regression tests
*  `make lapack-test` for LAPACK tests

We also use the [BLAS-Tester](https://github.com/xianyi/BLAS-Tester) tests for regression testing.
It is basically the ATLAS test suite adapted for building with OpenBLAS.

The project makes use of several Continuous Integration (CI) services
conveniently interfaced with GitHub to automatically run tests on a number of
platforms and build configurations.

Also note that the test suites included with "numerically heavy" projects like
Julia, NumPy, SciPy, Octave or QuantumEspresso can be used for regression
testing, when those projects are built such that they use OpenBLAS.


## Benchmarking

A number of benchmarking methods are used by OpenBLAS:

- Several simple C benchmarks for performance testing individual BLAS functions
  are available in the `benchmark` folder. They can be run locally through the
  `Makefile` in that directory. And the `benchmark/scripts` subdirectory
  contains similar benchmarks that use OpenBLAS via NumPy, SciPy, Octave and R.
- On pull requests, a representative set of functions is tested for performance
  regressions with Codspeed; results can be viewed at
  [https://codspeed.io/OpenMathLib/OpenBLAS](https://codspeed.io/OpenMathLib/OpenBLAS).
- The [OpenMathLib/BLAS-Benchmarks](https://github.com/OpenMathLib/BLAS-Benchmarks) repository
  contains an [Airspeed Velocity](https://github.com/airspeed-velocity/asv/)-based benchmark
  suite which is run on several CPU architectures in cron jobs. Results are published
  to a dashboard: [http://www.openmathlib.org/BLAS-Benchmarks/](http://www.openmathlib.org/BLAS-Benchmarks/).

Benchmarking code for BLAS libraries, and specific performance analysis results, can be found
in a number of places. For example:

* [MatlabJuliaMatrixOperationsBenchmark](https://github.com/RoyiAvital/MatlabJuliaMatrixOperationsBenchmark)
  (various matrix operations in Julia and Matlab)
* [mmperf/mmperf](https://github.com/mmperf/mmperf/) (single-core matrix multiplication)


## Adding autodetection support for a new revision or variant of a supported CPU 

Especially relevant for x86-64, a new CPU model may be a "refresh" (die shrink and/or different number of cores) within an existing
model family without significant changes to its instruction set (e.g., Intel Skylake and Kaby Lake still are fundamentally the same architecture as Haswell,
low end Goldmont etc. are Nehalem). In this case, compilation with the appropriate older `TARGET` will already lead to a satisfactory build.

To achieve autodetection of the new model, its CPUID (or an equivalent identifier) needs to be added in the `cpuid_<architecture>.c`
relevant for its general architecture, with the returned name for the new type set appropriately. For x86, which has the most complex
`cpuid` file, there are two functions that need to be edited: `get_cpuname()` to return, e.g., `CPUTYPE_HASWELL` and `get_corename()` for the (broader)
core family returning, e.g., `CORE_HASWELL`.[^1]

[^1]:
    This information ends up in the `Makefile.conf` and `config.h` files generated by `getarch`. Failure to
    set either will typically lead to a missing definition of the `GEMM_UNROLL` parameters later in the build,
    as `getarch_2nd` will be unable to find a matching parameter section in `param.h`.

For architectures where `DYNAMIC_ARCH` builds are supported, a similar but simpler code section for the corresponding
runtime detection of the CPU exists in `driver/others/dynamic.c` (for x86), and `driver/others/dynamic_<arch>.c` for other architectures.
Note that for x86 the CPUID is compared after splitting it into its family, extended family, model and extended model parts, so the single decimal
number returned by Linux in `/proc/cpuinfo` for the model has to be converted back to hexadecimal before splitting into its constituent
digits. For example, `142 == 8E` translates to extended model 8, model 14.
 

## Adding dedicated support for a new CPU model

Usually it will be possible to start from an existing model, clone its `KERNEL` configuration file to the new name to use for this
`TARGET` and eventually replace individual kernels with versions better suited for peculiarities of the new CPU model.
In addition, it is necessary to add (or clone at first) the corresponding section of `GEMM_UNROLL` parameters in the top-level `param.h`,
and possibly to add definitions such as `USE_TRMM` (governing whether `TRMM` functions use the respective `GEMM` kernel or a separate source file)
to the `Makefile`s (and `CMakeLists.txt`) in the kernel directory. The new CPU name needs to be added to `TargetList.txt`,
and the CPU auto-detection code used by the `getarch` helper program - contained in
the `cpuid_<architecture>.c` file amended to include the CPUID (or equivalent) information processing required (see preceding section).


## Adding support for an entirely new architecture

This endeavour is best started by cloning the entire support structure for 32-bit ARM, and within that the ARMv5 CPU in particular,
as this is implemented through plain C kernels only. An example providing a convenient "shopping list" can be seen in pull request
[#1526](https://github.com/OpenMathLib/OpenBLAS/pull/1526).
