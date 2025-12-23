OpenBLAS checks the following environment variables on startup:

* `OPENBLAS_NUM_THREADS`: the number of threads to use (for non-OpenMP builds
  of OpenBLAS)
* `OMP_NUM_THREADS`: the number of threads to use (for OpenMP builds - note
  that setting this may also affect any other OpenMP code)
* `OPENBLAS_DEFAULT_NUM_THREADS`: the number of threads to use, irrespective if
  OpenBLAS was built for OpenMP or pthreads

* `OPENBLAS_MAIN_FREE=1`: this can be used to disable automatic assignment of
  cpu affinity in OpenBLAS builds that have it enabled by default
* `OPENBLAS_THREAD_TIMEOUT`: this can be used to define the length of time
  that idle threads should wait before exiting
* `OMP_ADAPTIVE=1`: this can be used in OpenMP builds to actually remove any
  surplus threads when the number of threads is decreased


`DYNAMIC_ARCH` builds also accept the following:

* `OPENBLAS_VERBOSE`:

    - set this to `1` to enable a warning when there is no exact match for the
      detected cpu in the library
    - set this to `2` to make OpenBLAS print the name of the cpu target it
      autodetected

* `OPENBLAS_CORETYPE`: set this to one of the supported target names to
  override autodetection, e.g., `OPENBLAS_CORETYPE=HASWELL`
* `OPENBLAS_L2_SIZE`: set this to override the autodetected size of the L2
  cache where it is not reported correctly (in virtual environments)


Deprecated variables still recognized for compatibilty:

* `GOTO_NUM_THREADS`: equivalent to `OPENBLAS_NUM_THREADS`
* `GOTOBLAS_MAIN_FREE`: equivalent to `OPENBLAS_MAIN_FREE`
* `OPENBLAS_BLOCK_FACTOR`: this applies a scale factor to the GEMM "P"
  parameter of the block matrix code, see file `driver/others/parameter.c`
