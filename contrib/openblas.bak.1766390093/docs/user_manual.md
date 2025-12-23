
This user manual covers compiling OpenBLAS itself, linking your code to OpenBLAS,
example code to use the C (CBLAS) and Fortran (BLAS) APIs, and some troubleshooting
tips. Compiling OpenBLAS is optional, since you may be able to install with a
package manager.

!!! Note BLAS API reference documentation

    The OpenBLAS documentation does not contain API reference documentation for
    BLAS or LAPACK, since these are standardized APIs, the documentation for
    which can be found in other places. If you want to understand every BLAS
    and LAPACK function and definition, we recommend reading the
    [Netlib BLAS ](http://netlib.org/blas/) and [Netlib LAPACK](http://netlib.org/lapack/)
    documentation.

    OpenBLAS does contain a limited number of functions that are non-standard,
    these are documented at [OpenBLAS extension functions](extensions.md).


## Compiling OpenBLAS

### Normal compile

The default way to build and install OpenBLAS from source is with Make:
```
make  # add `-j4` to compile in parallel with 4 processes
make install
```

By default, the CPU architecture is detected automatically when invoking
`make`, and the build is optimized for the detected CPU. To override the
autodetection, use the `TARGET` flag:

```
# `make TARGET=xxx` sets target CPU: e.g. for an Intel Nehalem CPU:
make TARGET=NEHALEM
```
The full list of known target CPU architectures can be found in
`TargetList.txt` in the root of the repository.

### Cross compile

For a basic cross-compilation with Make, three steps need to be taken:

- Set the `CC` and `FC` environment variables to select the cross toolchains
  for C and Fortran.
- Set the `HOSTCC` environment variable to select the host C compiler (i.e. the
  regular C compiler for the machine on which you are invoking the build).
- Set `TARGET` explicitly to the CPU architecture on which the produced
  OpenBLAS binaries will be used.

#### Cross-compilation examples

Compile the library for ARM Cortex-A9 linux on an x86-64 machine
_(note: install only `gnueabihf` versions of the cross toolchain - see
[this issue comment](https://github.com/OpenMathLib/OpenBLAS/issues/936#issuecomment-237596847)
for why_):
```
make CC=arm-linux-gnueabihf-gcc FC=arm-linux-gnueabihf-gfortran HOSTCC=gcc TARGET=CORTEXA9
```

Compile OpenBLAS for a loongson3a CPU on an x86-64 machine:
```
make BINARY=64 CC=mips64el-unknown-linux-gnu-gcc FC=mips64el-unknown-linux-gnu-gfortran HOSTCC=gcc TARGET=LOONGSON3A
```

Compile OpenBLAS for loongson3a CPU with the `loongcc` (based on Open64) compiler on an x86-64 machine:
```
make CC=loongcc FC=loongf95 HOSTCC=gcc TARGET=LOONGSON3A CROSS=1 CROSS_SUFFIX=mips64el-st-linux-gnu-   NO_LAPACKE=1 NO_SHARED=1 BINARY=32
```

### Building a debug version

Add `DEBUG=1` to your build command, e.g.:
```
make DEBUG=1
```

### Install to a specific directory

!!! note

    Installing to a directory is optional; it is also possible to use the shared or static
    libraries directly from the build directory.

Use `make install` with the `PREFIX` flag to install to a specific directory:

```
make install PREFIX=/path/to/installation/directory
```

The default directory is `/opt/OpenBLAS`.

!!! important

    Note that any flags passed to `make` during build should also be passed to
    `make install` to circumvent any install errors, i.e. some headers not
    being copied over correctly.

For more detailed information on building/installing from source, please read
the [Installation Guide](install.md).


## Linking to OpenBLAS

OpenBLAS can be used as a shared or a static library.

### Link a shared library

The shared library is normally called `libopenblas.so`, but not that the name
may be different as a result of build flags used or naming choices by a distro
packager (see [distributing.md] for details). To link a shared library named
`libopenblas.so`, the flag `-lopenblas` is needed. To find the OpenBLAS headers,
a `-I/path/to/includedir` is needed. And unless the library is installed in a
directory that the linker searches by default, also `-L` and `-Wl,-rpath` flags
are needed. For a source file `test.c` (e.g., the example code under _Call
CBLAS interface_ further down), the shared library can then be linked with:
```
gcc -o test test.c -I/your_path/OpenBLAS/include/ -L/your_path/OpenBLAS/lib -Wl,-rpath,/your_path/OpenBLAS/lib -lopenblas
```

The `-Wl,-rpath,/your_path/OpenBLAS/lib` linker flag can be omitted if you
ran `ldconfig` to update linker cache, put `/your_path/OpenBLAS/lib` in
`/etc/ld.so.conf` or a file in `/etc/ld.so.conf.d`, or installed OpenBLAS in a
location that is part of the `ld.so` default search path (usually `/lib`,
`/usr/lib` and `/usr/local/lib`). Alternatively, you can set the environment
variable `LD_LIBRARY_PATH` to point to the folder that contains `libopenblas.so`.
Otherwise, the build may succeed but at runtime loading the library will fail
with a message like:
```
cannot open shared object file: no such file or directory
```

More flags may be needed, depending on how OpenBLAS was built:

- If `libopenblas` is multi-threaded, please add `-lpthread`.
- If the library contains LAPACK functions (usually also true), please add
  `-lgfortran` (other Fortran libraries may also be needed, e.g. `-lquadmath`).
  Note that if you only make calls to LAPACKE routines, i.e. your code has
  `#include "lapacke.h"` and makes calls to methods like `LAPACKE_dgeqrf`,
  then `-lgfortran` is not needed.

!!! tip Use pkg-config

    Usually a pkg-config file (e.g., `openblas.pc`) is installed together
    with a `libopenblas` shared library. pkg-config is a tool that will
    tell you the exact flags needed for linking. For example:

    ```
    $ pkg-config --cflags openblas
    -I/usr/local/include
    $ pkg-config --libs openblas
    -L/usr/local/lib -lopenblas
    ```

### Link a static library

Linking a static library is simpler - add the path to the static OpenBLAS
library to the compile command:
```
gcc -o test test.c /your/path/libopenblas.a
```


## Code examples

### Call CBLAS interface

This example shows calling `cblas_dgemm` in C:

<!-- Source: https://gist.github.com/xianyi/6930656 -->
```c
#include <cblas.h>
#include <stdio.h>

void main()
{
  int i=0;
  double A[6] = {1.0,2.0,1.0,-3.0,4.0,-1.0};         
  double B[6] = {1.0,2.0,1.0,-3.0,4.0,-1.0};  
  double C[9] = {.5,.5,.5,.5,.5,.5,.5,.5,.5}; 
  cblas_dgemm(CblasColMajor, CblasNoTrans, CblasTrans,3,3,2,1,A, 3, B, 3,2,C,3);

  for(i=0; i<9; i++)
    printf("%lf ", C[i]);
  printf("\n");
}
```

To compile this file, save it as `test_cblas_dgemm.c` and then run:
```
gcc -o test_cblas_open test_cblas_dgemm.c -I/your_path/OpenBLAS/include/ -L/your_path/OpenBLAS/lib -lopenblas -lpthread -lgfortran
```
will result in a `test_cblas_open` executable.

### Call BLAS Fortran interface

This example shows calling the `dgemm` Fortran interface in C:

<!-- Source: https://gist.github.com/xianyi/5780018 -->
```c
#include "stdio.h"
#include "stdlib.h"
#include "sys/time.h"
#include "time.h"

extern void dgemm_(char*, char*, int*, int*,int*, double*, double*, int*, double*, int*, double*, double*, int*);

int main(int argc, char* argv[])
{
  int i;
  printf("test!\n");
  if(argc<4){
    printf("Input Error\n");
    return 1;
  }

  int m = atoi(argv[1]);
  int n = atoi(argv[2]);
  int k = atoi(argv[3]);
  int sizeofa = m * k;
  int sizeofb = k * n;
  int sizeofc = m * n;
  char ta = 'N';
  char tb = 'N';
  double alpha = 1.2;
  double beta = 0.001;

  struct timeval start,finish;
  double duration;

  double* A = (double*)malloc(sizeof(double) * sizeofa);
  double* B = (double*)malloc(sizeof(double) * sizeofb);
  double* C = (double*)malloc(sizeof(double) * sizeofc);

  srand((unsigned)time(NULL));

  for (i=0; i<sizeofa; i++)
    A[i] = i%3+1;//(rand()%100)/10.0;

  for (i=0; i<sizeofb; i++)
    B[i] = i%3+1;//(rand()%100)/10.0;

  for (i=0; i<sizeofc; i++)
    C[i] = i%3+1;//(rand()%100)/10.0;
  //#if 0
  printf("m=%d,n=%d,k=%d,alpha=%lf,beta=%lf,sizeofc=%d\n",m,n,k,alpha,beta,sizeofc);
  gettimeofday(&start, NULL);
  dgemm_(&ta, &tb, &m, &n, &k, &alpha, A, &m, B, &k, &beta, C, &m);
  gettimeofday(&finish, NULL);

  duration = ((double)(finish.tv_sec-start.tv_sec)*1000000 + (double)(finish.tv_usec-start.tv_usec)) / 1000000;
  double gflops = 2.0 * m *n*k;
  gflops = gflops/duration*1.0e-6;

  FILE *fp;
  fp = fopen("timeDGEMM.txt", "a");
  fprintf(fp, "%dx%dx%d\t%lf s\t%lf MFLOPS\n", m, n, k, duration, gflops);
  fclose(fp);

  free(A);
  free(B);
  free(C);
  return 0;
}
```

To compile this file, save it as `time_dgemm.c` and then run:
```
gcc -o time_dgemm time_dgemm.c /your/path/libopenblas.a -lpthread
```
You can then run it as: `./time_dgemm <m> <n> <k>`, with `m`, `n`, and `k` input
parameters to the `time_dgemm` executable.

!!! note

    When calling the Fortran interface from C, you have to deal with symbol name
    differences caused by compiler conventions. That is why the `dgemm_` function
    call in the example above has a trailing underscore. This is what it looks like
    when using `gcc`/`gfortran`, however such details may change for different
    compilers. Hence it requires extra support code. The CBLAS interface may be
    more portable when writing C code.

    When writing code that needs to be portable and work across different
    platforms and compilers, the above code example is not recommended for
    usage. Instead, we advise looking at how OpenBLAS (or BLAS in general, since
    this problem isn't specific to OpenBLAS) functions are called in widely
    used projects like Julia, SciPy, or R.


## Troubleshooting

* Please read the [FAQ](faq.md) first, your problem may be described there.
* Please ensure you are using a recent enough compiler, that supports the
  features your CPU provides (example: GCC versions before 4.6 were known to
  not support AVX kernels, and before 6.1 AVX512CD kernels).
* The number of CPU cores supported by default is <=256. On Linux x86-64, there
  is experimental support for up to 1024 cores and 128 NUMA nodes if you build
  the library with `BIGNUMA=1`.
* OpenBLAS does not set processor affinity by default. On Linux, you can enable
  processor affinity by commenting out the line `NO_AFFINITY=1` in
  `Makefile.rule`.
* On Loongson 3A, `make test` is known to fail with a `pthread_create` error
  and an `EAGAIN` error code. However, it will be OK when you run the same
  testcase in a shell.
