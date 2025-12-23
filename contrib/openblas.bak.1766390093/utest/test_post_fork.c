/*****************************************************************************
Copyright (c) 2011-2020, The OpenBLAS Project
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   1. Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
   3. Neither the name of the OpenBLAS project nor the names of
      its contributors may be used to endorse or promote products
      derived from this software without specific prior written
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

**********************************************************************************/

#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#include <cblas.h>
#ifdef USE_OPENMP
#include <omp.h>
#endif
#include "openblas_utest.h"

static void* xmalloc(size_t n)
{
    void* tmp;
    tmp = malloc(n);
    if (tmp == NULL) {
        fprintf(stderr, "Failed to allocate memory for the test payload.\n");
        exit(1);
    } else {
        return tmp;
    }
}

#ifdef BUILD_DOUBLE
static void check_dgemm(double *a, double *b, double *result, double *expected, blasint n)
{
    char trans1 = 'T';
    char trans2 = 'N';
    double zerod = 0, oned = 1;
    int i;
    BLASFUNC(dgemm)(&trans1, &trans2, &n, &n, &n, &oned, a, &n, b, &n, &zerod, result, &n);
    for(i = 0; i < n * n; ++i) {
        ASSERT_DBL_NEAR_TOL(expected[i], result[i], DOUBLE_EPS);
    }
}
#endif

CTEST(fork, safety_after_fork_in_parent)
{
#ifdef __UCLIBC__
#if !defined __UCLIBC_HAS_STUBS__ && !defined __ARCH_USE_MMU__
exit(0);
#endif
#endif
#ifndef BUILD_DOUBLE
exit(0);
#else
    blasint n = 100;
    int i, nthreads_omp;

    double *a, *b, *c, *d;
    size_t n_bytes;

    pid_t fork_pid;

    n_bytes = sizeof(*a) * n * n;

    a = xmalloc(n_bytes);
    b = xmalloc(n_bytes);
    c = xmalloc(n_bytes);
    d = xmalloc(n_bytes);

    // Put ones in a, b and n in c (result)
    for(i = 0; i < n * n; ++i) {
        a[i] = 1;
        b[i] = 1;
        c[i] = 1 * n;
    }

    // Test that OpenBLAS works after a fork.
    // This situation routinely happens with Pythons numpy where a
    // `sys.platform` calls `uname` in a forked process.
    // So we simulate this situation here.

    // There was an issue where a different number of OpenBLAS and OpenMP
    // threads triggered a memory leak. So run this multiple times
    // with different number of threads set.
#ifdef USE_OPENMP
    nthreads_omp = omp_get_max_threads();
    // Run with half the max OMP threads, the max threads and twice that
    for(i = (nthreads_omp + 1) / 2; i <= nthreads_omp * 2; i *= 2) {
        omp_set_num_threads(i);
#endif

        fork_pid = fork();
        if (fork_pid == -1) {
            perror("fork");
            CTEST_ERR("Failed to fork subprocesses in a loop.");
#ifdef USE_OPENMP
            CTEST_ERR("Number of OpenMP threads was %d in this attempt.",i);
#endif
        } else if (fork_pid == 0) {
            // Just pretend to do something, e.g. call `uname`, then exit
            exit(0);
        } else {
            // Wait for the child to finish and check the exit code.
            int child_status = 0;
            pid_t wait_pid = wait(&child_status);
            ASSERT_EQUAL(wait_pid, fork_pid);
            ASSERT_EQUAL(0, WEXITSTATUS (child_status));

            // Now OpenBLAS has to work
            check_dgemm(a, b, d, c, n);
        }
#ifdef USE_OPENMP
    }
#endif

#endif
}
