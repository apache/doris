OpenBLAS for the most part contains implementations of the reference (Netlib)
BLAS, CBLAS, LAPACK and LAPACKE interfaces. A few OpenBLAS-specific functions
are also provided however, which mostly can be seen as "BLAS extensions".
This page documents those non-standard APIs.

## BLAS-like extensions

| Routine       | Data Types    | Description                                     |
| ------------- |:------------- | :-----------------------------------------------|
| ?axpby        | s,d,c,z       | like `axpy` with a multiplier for `y`           |
| ?gemm3m       | c,z           | `gemm3m`                                        |
| ?imatcopy     | s,d,c,z       | in-place transposition/copying                  |
| ?omatcopy     | s,d,c,z       | out-of-place transposition/copying              |
| ?geadd        | s,d,c,z       | ATLAS-like matrix add `B = &alpha;*A+&beta;*B`  |
| ?gemmt        | s,d,c,z       | `gemm` but only a triangular part updated       |


## bfloat16 functionality

BLAS-like and conversion functions for `bfloat16` (available when OpenBLAS was compiled with `BUILD_BFLOAT16=1`):

* `void cblas_sbstobf16` converts a float array to an array of bfloat16 values by rounding
* `void cblas_sbdtobf16` converts a double array to an array of bfloat16 values by rounding
* `void cblas_sbf16tos` converts a bfloat16 array to an array of floats
* `void cblas_dbf16tod` converts a bfloat16 array to an array of doubles
* `float cblas_sbdot` computes the dot product of two bfloat16 arrays
* `void cblas_sbgemv` performs the matrix-vector operations of GEMV with the input matrix and X vector as bfloat16
* `void cblas_sbgemm` performs the matrix-matrix operations of GEMM with both input arrays containing bfloat16

## Utility functions

* `openblas_get_num_threads`
* `openblas_set_num_threads`
* `int openblas_get_num_procs(void)` returns the number of processors available on the system (may include "hyperthreading cores")
* `int openblas_get_parallel(void)` returns 0 for sequential use, 1 for platform-based threading and 2 for OpenMP-based threading
* `char * openblas_get_config()` returns the options OpenBLAS was built with, something like `NO_LAPACKE DYNAMIC_ARCH NO_AFFINITY Haswell`
* `int openblas_set_affinity(int thread_index, size_t cpusetsize, cpu_set_t *cpuset)` sets the CPU affinity mask of the given thread
  to the provided cpuset. Only available on Linux, with semantics identical to `pthread_setaffinity_np`.

