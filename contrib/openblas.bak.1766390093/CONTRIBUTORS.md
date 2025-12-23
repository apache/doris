# Contributions to the OpenBLAS project

## Creator & Maintainer

* Zhang Xianyi <traits.zhang@gmail.com>

## Active Developers

* Wang Qian <traz0824@gmail.com>
  * Optimize BLAS3 on ICT Loongson 3A.
  * Optimize BLAS3 on Intel Sandy Bridge.

* Werner Saar <wernsaar@googlemail.com>
  * [2013-03-04] Optimize AVX and FMA4 DGEMM on AMD Bulldozer
  * [2013-04-27] Optimize AVX and FMA4 TRSM on AMD Bulldozer
  * [2013-06-09] Optimize AVX and FMA4 SGEMM on AMD Bulldozer
  * [2013-06-11] Optimize AVX and FMA4 ZGEMM on AMD Bulldozer
  * [2013-06-12] Optimize AVX and FMA4 CGEMM on AMD Bulldozer
  * [2013-06-16] Optimize dgemv_n kernel on AMD Bulldozer
  * [2013-06-20] Optimize ddot, daxpy kernel on AMD Bulldozer
  * [2013-06-21] Optimize dcopy kernel on AMD Bulldozer
  * Porting and Optimization on ARM Cortex-A9
  * Optimization on AMD Piledriver
  * Optimization on Intel Haswell

* Chris Sidebottom <chris.sidebottom@arm.com>
  * Optimizations and other improvements targeting AArch64

* Annop Wongwathanarat <annop.wongwathanarat@arm.com>
  * Optimizations and other improvements targeting AArch64

## Previous Developers

* Zaheer Chothia <zaheer.chothia@gmail.com>
  * Improve the compatibility about complex number
  * Build LAPACKE: C interface to LAPACK
  * Improve the windows build.

* Chen Shaohu <huhumartinwar@gmail.com>
  * Optimize GEMV on the Loongson 3A processor.

* Luo Wen
  * Intern. Test Level-2 BLAS.

## Contributors

In chronological order:

* pipping <http://page.mi.fu-berlin.de/pipping>
  * [2011-06-11] Make USE_OPENMP=0 disable openmp.

* Stefan Karpinski <stefan@karpinski.org>
  * [2011-12-28] Fix a bug about SystemStubs on Mac OS X.

* Alexander Eberspächer <https://github.com/aeberspaecher>
  * [2012-05-02] Add note on patch for segfaults on Linux kernel 2.6.32.

* Mike Nolta <mike@nolta.net>
  * [2012-05-19] Fix building bug on FreeBSD and NetBSD.

* Sylvestre Ledru <https://github.com/sylvestre>
  * [2012-07-01] Improve the detection of sparc. Fix building bug under
    Hurd and kfreebsd.

* Jameson Nash <https://github.com/vtjnash>
  * [2012-08-20] Provide support for passing CFLAGS, FFLAGS, PFLAGS, FPFLAGS to
    make on the command line.

* Alexander Nasonov <alnsn@yandex.ru>
  * [2012-11-10] Fix NetBSD build.

* Sébastien Villemot <sebastien@debian.org>
  * [2012-11-14] Fix compilation with TARGET=GENERIC. Patch applied to Debian package.
  * [2013-08-28] Avoid failure on qemu guests declaring an Athlon CPU without 3dnow!

* Kang-Che Sung <Explorer09@gmail.com>
  * [2013-05-17] Fix typo in the document. Re-order the architecture list in getarch.c.

* Kenneth Hoste <kenneth.hoste@gmail.com>
  * [2013-05-22] Adjust Makefile about downloading LAPACK source files.

* Lei WANG <https://github.com/wlbksy>
  * [2013-05-22] Fix a bug about wget.

* Dan Luu <http://www.linkedin.com/in/danluu>
  * [2013-06-30] Add Intel Haswell support (using sandybridge optimizations).

* grisuthedragon <https://github.com/grisuthedragon>
  * [2013-07-11] create openblas_get_parallel to retrieve information which parallelization
    model is used by OpenBLAS.

* Elliot Saba <staticfloat@gmail.com>
  * [2013-07-22] Add in return value for `interface/trtri.c`

* Sébastien Fabbro <bicatali@gentoo.org>
  * [2013-07-24] Modify makefile to respect user's LDFLAGS
  * [2013-07-24] Add stack markings for GNU as arch-independent for assembler files

* Viral B. Shah <viral@mayin.org>
  * [2013-08-21] Patch LAPACK XLASD4.f as discussed in JuliaLang/julia#2340

* Lars Buitinck <https://github.com/larsmans>
  * [2013-08-28] get rid of the generated cblas_noconst.h file
  * [2013-08-28] Missing threshold in gemm.c
  * [2013-08-28] fix default prefix handling in makefiles

* yieldthought <https://github.com/yieldthought>
  * [2013-10-08] Remove -Wl,--retain-symbols-file from dynamic link line to fix tool support

* Keno Fischer <https://github.com/loladiro>
  * [2013-10-23] Use FC instead of CC to link the dynamic library on OS X

* Christopher Meng <cickumqt@gmail.com>
  * [2013-12-09] Add DESTDIR support for easier building on RPM based distros.
                 Use install command instead of cp to install files with permissions control.

* Lucas Beyer <lucasb.eyer.be@gmail.com>
  * [2013-12-10] Added support for NO_SHARED in make install.

* carlkl <https://github.com/carlkl>
  * [2013-12-13] Fixed LAPACKE building bug on Windows

* Isaac Dunham <https://github.com/idunham>
  * [2014-08-03] Fixed link error on Linux/musl

* Dave Nuechterlein
  * [2014-10-10] trmm and sgemm kernels (optimized for APM's X-Gene 1).
                 ARMv8 support.

* Jerome Robert <jeromerobert@gmx.com>
  * [2015-01-01] Speed-up small `ger` and `gemv` using stack allocation (bug #478)
  * [2015-12-23] `stack_check` in `gemv.c` (bug #722)
  * [2015-12-28] Allow to force the number of parallel make job
  * [2015-12-28] Fix detection of AMD E2-3200 detection
  * [2015-12-31] Let `make MAX_STACK_ALLOC=0` do what expected
  * [2016-01-19] Disable multi-threading in `ger` and `swap` for small matrices (bug #731)
  * [2016-01-24] Use `GEMM_MULTITHREAD_THRESHOLD` as a number of ops (bug #742)
  * [2016-01-26] Let `openblas_get_num_threads` return the number of active threads (bug #760)
  * [2016-01-30] Speed-up small `zger`, `zgemv`, `ztrmv` using stack allocation (bug #727)

* Dan Kortschak
  * [2015-01-07] Added test for drotmg bug #484.

* Ton van den Heuvel <https://github.com/ton>
  * [2015-03-18] Fix race condition during shutdown causing a crash in gotoblas_set_affinity().

* Martin Koehler <https://github.com/grisuthedragon/>
  * [2015-09-07] Improved imatcopy

* Ashwin Sekhar T K <https://github.com/ashwinyes/>
  * [2015-11-09] Assembly kernels for Cortex-A57 (ARMv8)
  * [2015-11-20] lapack-test fixes for Cortex-A57
  * [2016-03-14] Additional functional Assembly Kernels for Cortex-A57
  * [2016-03-14] Optimize Dgemm 4x4 for Cortex-A57

* theoractice <https://github.com/theoractice/>
  * [2016-03-20] Fix compiler error in VisualStudio with CMake
  * [2016-03-22] Fix access violation on Windows while static linking

* Paul Mustière <https://github.com/buffer51/>
  * [2016-02-04] Fix Android build on ARMV7
  * [2016-04-26] Android build with LAPACK for ARMV7 & ARMV8

* Shivraj Patil <https://github.com/sva-img/>
  * [2016-05-03] DGEMM optimization for MIPS P5600 and I6400 using MSA

* Kaustubh Raste <https://github.com/ksraste/>
  * [2016-05-09] DTRSM optimization for MIPS P5600 and I6400 using MSA
  * [2016-05-20] STRSM optimization for MIPS P5600 and I6400 using MSA

* Abdelrauf  <https://github.com/quickwritereader>
  * [2017-01-01] dgemm and dtrmm kernels for IBM z13
  * [2017-02-26] ztrmm kernel for IBM z13
  * [2017-03-13] strmm and ctrmm kernel for IBM z13
  * [2017-09-01] initial Blas Level-1,2 (double precision) for IBM z13
  * [2018-03-07] added missing Blas Level 1-2  (double precision) simd codes
  * [2019-02-01] added missing Blas Level-1,2 (single precision)  simd codes
  * [2019-03-14] power9 dgemm/dtrmm kernel
  * [2019-04-29] power9 sgemm/strmm kernel 

* Jiachen Wang <https://github.com/wjc404>
  * [2019-07-29] optimize AVX2 DGEMM
  * [2019-10-20] AVX512 DGEMM kernel (4x8)
  * [2019-11-06] optimize AVX512 SGEMM
  * [2019-11-12] AVX512 CGEMM & ZGEMM kernels
  * [2019-12-23] optimize AVX2 CGEMM and ZGEMM
  * [2019-12-30] AVX2 CGEMM3M & ZGEMM3M kernels
  * [2020-01-07] optimize AVX2 SGEMM and STRMM

* Rajalakshmi Srinivasaraghavan <https://github.com/RajalakshmiSR>
  * [2020-04-15] Half-precision GEMM for bfloat16

* Marius Hillenbrand <https://github.com/mhillenibm>
  * [2020-05-12] Revise dynamic architecture detection for IBM z
  * [2020-05-12] Add new sgemm and strmm kernel for IBM z14
  * [2020-09-07] Fix builds with clang on IBM z, including dynamic architecture support

* Danfeng Zhang <https://github.com/craft-zhang>
  * [2020-05-20] Improve performance of SGEMM and STRMM on Arm Cortex-A53

* PingTouGe Semiconductor Co., Ltd.
  * [2020-10] Add RISC-V Vector (0.7.1) support. Optimize BLAS kernels for Xuantie C910

* Jake Arkinstall <https://github.com/jake-arkinstall>
  * [2021-02-10] Remove in-source configure_file to enable builds in read-only contexts (issue #3100, PR #3101)

* River Dillon <oss@outerpassage.net>
  * [2021-07-10] fix compilation with musl libc

* Bine Brank <https://github.com/binebrank>
  * [2021-10-27] Add vector-length-agnostic DGEMM kernels for Arm SVE
  * [2021-11-20] Vector-length-agnostic Arm SVE copy routines for DGEMM, DTRMM, DSYMM
  * [2021-11-12] SVE kernels for SGEMM, STRMM and corresponding SVE copy functions
  * [2022-01-06] SVE kernels for CGEMM, ZGEMM, CTRMM, ZTRMM and corresponding SVE copy functions
  * [2022-01-18] SVE kernels and copy functions for TRSM

* Ilya Kurdyukov <https://github.com/ilyakurdyukov>
  * [2021-02-21] Add basic support for the Elbrus E2000 architecture

* PLCT Lab, Institute of Software Chinese Academy of Sciences
  * [2022-03] Support RISC-V Vector Intrinisc 1.0 version.
  
* Pablo Romero <https://github.com/pablorcum>
  * [2022-08] Fix building from sources for QNX

* Mark Seminatore <https://github.com/mseminatore>
  * [2023-11-09] Improve Windows threading performance scaling
  * [2024-02-09] Introduce MT_TRACE facility and improve code consistency

* Dirreke <https://github.com/mseminatore>
  * [2024-01-16] Add basic support for the CSKY architecture

* Christopher Daley <https://github.com/cdaley>
  * [2024-01-24] Optimize GEMV forwarding on ARM64 systems

* Aniket P. Garade <https://github.com/garadeaniket>   Sushil Pratap Singh <https://github.com/SushilPratap04>  Juliya James <https://github.com/Juliya32> 
  *  [2024-12-13] Optimized swap and rot  Level-1 BLAS routines with ARM SVE

* Annop Wongwathanarat <annop.wongwathanarat@arm.com>
  * [2025-01-10] Add thread throttling profile for SGEMM on NEOVERSEV1
  * [2025-01-21] Optimize gemv_t_sve_v1x3 kernel
  * [2025-02-26] Add sbgemv_t_bfdot kernel
  * [2025-03-12] Fix aarch64 sbgemv_t compilation error for GCC < 13
  * [2025-03-12] Optimize aarch64 sgemm_ncopy

* Marek Michalowski <marek.michalowski@arm.com>
  * [2025-01-21] Add thread throttling profile for SGEMV on `NEOVERSEV1`
  * [2025-02-18] Add thread throttling profile for SGEMM on `NEOVERSEV2`
  * [2025-02-19] Add thread throttling profile for SGEMV on `NEOVERSEV2`

* Ye Tao <ye.tao@arm.com>
  * [2025-02-03] Optimize SBGEMM kernel on NEOVERSEV1
  * [2025-02-27] Add sbgemv_n_neon kernel

* Abhishek Kumar <https://github.com/abhishek-iitmadras>
  * [2025-04-22] Optimise dot kernel for NEOVERSE V1