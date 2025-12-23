# Install OpenBLAS

OpenBLAS can be installed through package managers or from source. If you only
want to use OpenBLAS rather than make changes to it, we recommend installing a
pre-built binary package with your package manager of choice.

This page contains an overview of installing with package managers as well as
from source. For the latter, see [further down on this page](#building-from-source).


## Installing with a package manager

!!! note
    Almost every package manager provides OpenBLAS packages; the list on this
    page is not comprehensive. If your package manager of choice isn't shown
    here, please search its package database for `openblas` or `libopenblas`.


### Linux

On Linux, OpenBLAS can be installed with the system package manager, or with a
package manager like [Conda](https://docs.conda.io/en/latest/)
(or alternative package managers for the conda-forge ecosystem, like
[Mamba](https://mamba.readthedocs.io/en/latest/),
[Micromamba](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html),
or [Pixi](https://pixi.sh/latest/#windows-installer)),
[Spack](https://spack.io/), or [Nix](https://nixos.org/). For the latter set of
tools, the package name in all cases is `openblas`. Since package management in
quite a few of these tools is declarative (i.e., managed by adding `openblas`
to a metadata file describing the dependencies for your project or
environment), we won't attempt to give detailed instructions for these tools here.

Linux distributions typically split OpenBLAS up in two packages: one containing
the library itself (typically named `openblas` or `libopenblas`), and one containing headers,
pkg-config and CMake files (typically named the same as the package for the
library with `-dev` or `-devel` appended; e.g., `openblas-devel`). Please keep
in mind that if you want to install OpenBLAS in order to use it directly in
your own project, you will need to install both of those packages.

Distro-specific installation commands:

=== "Debian/Ubuntu/Mint/Kali"

    ```bash
    $ sudo apt update
    $ sudo apt install libopenblas-dev
    ```
    OpenBLAS can be configured as the default BLAS through the `update-alternatives` mechanism:

    ```bash
    $ sudo update-alternatives --config libblas.so.3
    ```

=== "openSUSE/SLE"

    ```bash
    $ sudo zypper refresh
    $ sudo zypper install openblas-devel
    ```

    OpenBLAS can be configured as the default BLAS through the `update-alternatives` mechanism:
    ```bash
    $ sudo update-alternatives --config libblas.so.3
    ```

=== "Fedora/CentOS/RHEL"

    ```bash
    $ dnf check-update
    $ dnf install openblas-devel
    ```

    !!! warning

        Fedora does not ship the pkg-config files for OpenBLAS. Instead, it wants you to
        link against [FlexiBLAS](https://www.mpi-magdeburg.mpg.de/projects/flexiblas) (which
        uses OpenBLAS by default as its backend on Fedora), which you can install with:

        ```bash
        $ dnf install flexiblas-devel
        ```

    For CentOS and RHEL, OpenBLAS packages are provided via the [Fedora EPEL repository](https://fedoraproject.org/wiki/EPEL).
    After adding that repository and its repository keys, you can install
    `openblas-devel` with either `dnf` or `yum`.

=== "Arch/Manjaro/Antergos"

    ```bash
    $ sudo pacman -S openblas
    ```


### Windows

=== "Conda-forge"

    OpenBLAS can be installed with `conda` (or `mamba`, `micromamba`, or
    `pixi`) from conda-forge:
    ```
    conda install openblas
    ```

    Conda-forge provides a method for switching the default BLAS implementation
    used by all packages. To use that for OpenBLAS, install `libblas=*=*openblas`
    (see [the docs on this mechanism](https://conda-forge.org/docs/maintainer/knowledge_base/#switching-blas-implementation)
    for more details).

=== "vcpkg"

    OpenBLAS can be installed with vcpkg:
    ```cmd
    # In classic mode:
    vcpkg install openblas

    # Or in manifest mode:
    vcpkg add port openblas
    ```

=== "OpenBLAS releases"

    Windows is the only platform for which binaries are made available by the
    OpenBLAS project itself. They can be downloaded from the GitHub
    Releases](https://github.com/OpenMathLib/OpenBLAS/releases) page. These
    binaries are built with MinGW, using the following build options:
    ```
    NUM_THREADS=64 TARGET=GENERIC DYNAMIC_ARCH=1 DYNAMIC_OLDER=1 CONSISTENT_FPCSR=1 INTERFACE=0
    ```
    There are separate packages for x86-64 and x86. The zip archive contains
    the include files, static and shared libraries, as well as configuration
    files for getting them found via CMake or pkg-config. To use these
    binaries, create a suitable folder for your OpenBLAS installation and unzip
    the `.zip` bundle there (note that you will need to edit the provided
    `openblas.pc` and `OpenBLASConfig.cmake` to reflect the installation path
    on your computer, as distributed they have "win" or "win64" reflecting the
    local paths on the system they were built on).

    Note that the same binaries can be downloaded
    [from SourceForge](http://sourceforge.net/projects/openblas/files); this is
    mostly of historical interest.


### macOS

To install OpenBLAS with a package manager on macOS, run:

=== "Homebrew"

    ```zsh
    % brew install openblas
    ```

=== "MacPorts"

    ```zsh
    % sudo port install OpenBLAS-devel
    ```

=== "Conda-forge"

    ```zsh
    % conda install openblas
    ```

    Conda-forge provides a method for switching the default BLAS implementation
    used by all packages. To use that for OpenBLAS, install `libblas=*=*openblas`
    (see [the docs on this mechanism](https://conda-forge.org/docs/maintainer/knowledge_base/#switching-blas-implementation)
    for more details).


### FreeBSD

You can install OpenBLAS from the FreeBSD [Ports collection](https://www.freebsd.org/ports/index.html):
```
pkg install openblas
```


## Building from source

We recommend download the latest [stable version](https://github.com/OpenMathLib/OpenBLAS/releases)
from the GitHub Releases page, or checking it out from a git tag, rather than a
dev version from the `develop` branch.

!!! tip

    The User manual contains [a section with detailed information on compiling OpenBLAS](user_manual.md#compiling-openblas),
    including how to customize builds and how to cross-compile. Please read
    that documentation first. This page contains only platform-specific build
    information, and assumes you already understand the general build system
    invocations to build OpenBLAS, with the specific build options you want to
    control multi-threading and other non-platform-specific behavior).


### Linux and macOS

Ensure you have C and Fortran compilers installed, then simply type `make` to compile the library.
There are no other build dependencies, nor unusual platform-specific
environment variables to set or other system setup to do.

!!! note

    When building in an emulator (KVM, QEMU, etc.), please make sure that the combination of CPU features exposed to
    the virtual environment matches that of an existing CPU to allow detection of the CPU model to succeed.
    (With `qemu`, this can be done by passing `-cpu host` or a supported model name at invocation).


### Windows

We support building OpenBLAS with either MinGW or Visual Studio on Windows.
Using MSVC will yield an OpenBLAS build with the Windows platform-native ABI.
Using MinGW will yield a different ABI. We'll describe both methods in detail
in this section, since the process for each is quite different.

#### Visual Studio & native Windows ABI

For Visual Studio, you can use CMake to generate Visual Studio solution files;
note that you will need at least CMake 3.11 for linking to work correctly).

Note that you need a Fortran compiler if you plan to build and use the LAPACK
functions included with OpenBLAS. The sections below describe using either
`flang` as an add-on to clang/LLVM or `gfortran` as part of MinGW for this
purpose. If you want to use the Intel Fortran compiler (`ifort` or `ifx`) for
this, be sure to also use the Intel C compiler (`icc` or `icx`) for building
the C parts, as the ABI imposed by `ifort` is incompatible with MSVC

A fully-optimized OpenBLAS that can be statically or dynamically linked to your
application can currently be built for the 64-bit architecture with the LLVM
compiler infrastructure. We're going to use [Miniconda3](https://docs.anaconda.com/miniconda/)
to grab all of the tools we need, since some of them are in an experimental
status. Before you begin, you'll need to have Microsoft Visual Studio 2015 or
newer installed.

1. Install Miniconda3 for 64-bit Windows using `winget install --id Anaconda.Miniconda3`,
   or easily download from [conda.io](https://docs.conda.io/en/latest/miniconda.html).
2. Open the "Anaconda Command Prompt" now available in the Start Menu, or at `%USERPROFILE%\miniconda3\shell\condabin\conda-hook.ps1`.
3. In that command prompt window, use `cd` to change to the directory where you want to build OpenBLAS.
4. Now install all of the tools we need:
   ```
   conda update -n base conda
   conda config --add channels conda-forge
   conda install -y cmake flang clangdev perl libflang ninja
   ```
5.  Still in the Anaconda Command Prompt window, activate the 64-bit MSVC environment with `vcvarsall x64`.
    On Windows 11 with Visual Studio 2022, this would be done by invoking:
    
    ```shell
    "c:\Program Files\Microsoft Visual Studio\2022\Community\vc\Auxiliary\Build\vcvars64.bat"
    ```
   
    With VS2019, the command should be the same (except for the year number of course).
    For other versions of MSVC, please check the Visual Studio documentation for
    exactly how to invoke the `vcvars64.bat` script.
   
    Confirm that the environment is active by typing `link`. This should return
    a long list of possible options for the `link` command. If it just returns
    _"command not found"_ or similar, review and retype the call to `vcvars64.bat`.

    !!! note

        if you are working from a Visual Studio command prompt window instead
        (so that you do not have to do the `vcvars` call), you need to invoke
        `conda activate` so that `CONDA_PREFIX` etc. get set up correctly before
        proceeding to step 6. Failing to do so will lead to link errors like
        `libflangmain.lib` not getting found later in the build.

6.  Now configure the project with CMake. Starting in the project directory, execute the following:
    ```
    set "LIB=%CONDA_PREFIX%\Library\lib;%LIB%"
    set "CPATH=%CONDA_PREFIX%\Library\include;%CPATH%"
    mkdir build
    cd build
    cmake .. -G "Ninja" -DCMAKE_CXX_COMPILER=clang-cl -DCMAKE_C_COMPILER=clang-cl -DCMAKE_Fortran_COMPILER=flang -DCMAKE_MT=mt -DBUILD_WITHOUT_LAPACK=no -DNOFORTRAN=0 -DDYNAMIC_ARCH=ON -DCMAKE_BUILD_TYPE=Release
    ```

    You may want to add further options in the `cmake` command here. For
    instance, the default only produces a static `.lib` version of the library.
    If you would rather have a DLL, add `-DBUILD_SHARED_LIBS=ON` above. Note that
    this step only creates some command files and directories, the actual build
    happens next.

7.  Build the project:

    ```
    cmake --build . --config Release
    ```
    This step will create the OpenBLAS library in the `lib` directory, and
    various build-time tests in the `test`, `ctest` and `openblas_utest`
    directories. However it will not separate the header files you might need
    for building your own programs from those used internally. To put all
    relevant files in a more convenient arrangement, run the next step.

8.  Install all relevant files created by the build:

    ```
    cmake --install . --prefix c:\opt -v
    ```
    This will copy all files that are needed for building and running your own
    programs with OpenBLAS to the given location, creating appropriate
    subdirectories for the individual kinds of files. In the case of `C:\opt` as
    given above, this would be:

    - `C:\opt\include\openblas` for the header files, 
    - `C:\opt\bin` for the `libopenblas.dll` shared library,
    - `C:\opt\lib` for the static library, and
    - `C:\opt\share` holds various support files that enable other cmake-based
      build scripts to find OpenBLAS automatically.


!!! tip "Change in complex types for Visual Studio 2017 and up"

    In newer Visual Studio versions, Microsoft has changed
    [how it handles complex types](https://docs.microsoft.com/en-us/cpp/c-runtime-library/complex-math-support?view=msvc-170#types-used-in-complex-math).
    Even when using a precompiled version of OpenBLAS, you might need to define
    `LAPACK_COMPLEX_CUSTOM` in order to define complex types properly for MSVC.
    For example, some variant of the following might help:

    ```c
    #if defined(_MSC_VER)
        #include <complex.h>
        #define LAPACK_COMPLEX_CUSTOM
        #define lapack_complex_float _Fcomplex
        #define lapack_complex_double _Dcomplex
    #endif
    ```

    For reference, see
    [openblas#3661](https://github.com/OpenMathLib/OpenBLAS/issues/3661),
    [lapack#683](https://github.com/Reference-LAPACK/lapack/issues/683), and
    [this Stack Overflow question](https://stackoverflow.com/questions/47520244/using-openblas-lapacke-in-visual-studio).


!!! warning "Building 32-bit binaries with MSVC"

    This method may produce binaries which demonstrate significantly lower
    performance than those built with the other methods. The Visual Studio
    compiler does not support the dialect of assembly used in the cpu-specific
    optimized files, so only the "generic" `TARGET` which is written in pure C
    will get built. For the same reason it is not possible (and not necessary)
    to use `-DDYNAMIC_ARCH=ON` in a Visual Studio build. You may consider
    building for the 32-bit architecture using the GNU (MinGW) ABI instead.

##### CMake & Visual Studio integration

To generate Visual Studio solution files, ensure CMake is installed and then run:
```
# Do this from Powershell so cmake can find visual studio
cmake -G "Visual Studio 14 Win64" -DCMAKE_BUILD_TYPE=Release .
```

To then build OpenBLAS using those solution files from within Visual Studio, we
also need Perl. Please install it and ensure it's on the `PATH` (see, e.g.,
[this Stack Overflow question for how](http://stackoverflow.com/questions/3051049/active-perl-installation-on-windows-operating-system)).

If you build from within Visual Studio, the dependencies may not be
automatically configured: if you try to build `libopenblas` directly, it may
fail with a message saying that some `.obj` files aren't found. If this
happens, you can work around the problem by building the projects that
`libopenblas` depends on before building `libopenblas` itself.

###### Build OpenBLAS for Universal Windows Platform

OpenBLAS can be built targeting [Universal Windows Platform](https://en.wikipedia.org/wiki/Universal_Windows_Platform)
(UWP) like this:

1.  Follow the steps above to build the Visual Studio solution files for
    Windows. This builds the helper executables which are required when building
    the OpenBLAS Visual Studio solution files for UWP in step 2.
2.  Remove the generated `CMakeCache.txt` and the `CMakeFiles` directory from
    the OpenBLAS source directory, then re-run CMake with the following options:

    ```
    # do this to build UWP compatible solution files
    cmake -G "Visual Studio 14 Win64" -DCMAKE_SYSTEM_NAME=WindowsStore -DCMAKE_SYSTEM_VERSION="10.0" -DCMAKE_SYSTEM_PROCESSOR=AMD64 -DVS_WINRT_COMPONENT=TRUE -DCMAKE_BUILD_TYPE=Release .
    ```
3.  Now build the solution with Visual Studio.


#### MinGW & GNU ABI

!!! note

    The resulting library from building with MinGW as described below can be
    used in Visual Studio, but it can only be linked dynamically. This
    configuration has not been thoroughly tested and should be considered
    experimental.


To build OpenBLAS on Windows with MinGW:

1.  Install the MinGW (GCC) compiler suite, either the 32-bit
    [MinGW]((http://www.mingw.org/) or the 64-bit
    [MinGW-w64](http://mingw-w64.sourceforge.net/) toolchain. Be sure to install
    its `gfortran` package as well (unless you really want to build the BLAS part
    of OpenBLAS only) and check that `gcc` and `gfortran` are the same version.
    In addition, please install MSYS2 with MinGW.
2.  Build OpenBLAS in the MSYS2 shell. Usually, you can just type `make`.
    OpenBLAS will detect the compiler and CPU automatically. 
3.  After the build is complete, OpenBLAS will generate the static library
    `libopenblas.a` and the shared library `libopenblas.dll` in the folder. You
    can type `make PREFIX=/your/installation/path install` to install the
    library to a certain location.

Note that OpenBLAS will generate the import library `libopenblas.dll.a` for
`libopenblas.dll` by default.

If you want to generate Windows-native PDB files from a MinGW build, you can
use the [cv2pdb](https://github.com/rainers/cv2pdb) tool to do so.

To then use the built OpenBLAS shared library in Visual Studio:

1.  Copy the import library (`OPENBLAS_TOP_DIR/libopenblas.dll.a`) and the
    shared library (`libopenblas.dll`) into the same folder (this must be the
    folder of your project that is going to use the BLAS library. You may need
    to add `libopenblas.dll.a` to the linker input list: `properties->Linker->Input`).
2.  Please follow the Visual Studio documentation about using third-party .dll
    libraries, and make sure to link against a library for the correct
    architecture.[^1]
3.  If you need CBLAS, you should include `cblas.h` in
    `/your/installation/path/include` in Visual Studio. Please see
    [openblas#95](http://github.com/OpenMathLib/OpenBLAS/issues/95) for more details.

[^1]:
   If the OpenBLAS DLLs are not linked correctly, you may see an error like
   _"The application was unable to start correctly (0xc000007b)"_, which typically
   indicates a mismatch between 32-bit and 64-bit libraries.

!!! info "Limitations of using the MinGW build within Visual Studio"

    - Both static and dynamic linking are supported with MinGW.  With Visual
      Studio, however, only dynamic linking is supported and so you should use
      the import library.
    - Debugging from Visual Studio does not work because MinGW and Visual
      Studio have incompatible formats for debug information (PDB vs.
      DWARF/STABS).  You should either debug with GDB on the command line or
      with a visual frontend, for instance [Eclipse](http://www.eclipse.org/cdt/) or
      [Qt Creator](http://qt.nokia.com/products/developer-tools/).


### Windows on Arm

A fully functional native OpenBLAS for WoA that can be built as both a static and dynamic library using LLVM toolchain and Visual Studio 2022. Before starting to build, make sure that you have installed Visual Studio 2022 on your ARM device, including the "Desktop Development with C++" component (that contains the cmake tool).
(Note that you can use the free "Visual Studio 2022 Community Edition" for this task. In principle it would be possible to build with VisualStudio alone, but using
the LLVM toolchain enables native compilation of the Fortran sources of LAPACK and of all the optimized assembly files, which VisualStudio cannot handle on its own)

1. Clone OpenBLAS to your local machine and checkout to latest release of
   OpenBLAS (unless you want to build the latest development snapshot - here we
   are using  the 0.3.28 release as the example, of course this exact version
   may be outdated by the time you read this)
  
       ```cmd
       git clone https://github.com/OpenMathLib/OpenBLAS.git
       cd OpenBLAS
       git checkout v0.3.28
       ```
  
2. Install Latest LLVM toolchain for WoA:

       Download the Latest LLVM toolchain for WoA from [the Release
       page](https://github.com/llvm/llvm-project/releases/tag/llvmorg-19.1.5). At
       the time of writing, this is version 19.1.5 - be sure to select the
       latest release for which you can find a precompiled package whose name ends
       in "-woa64.exe" (precompiled packages usually lag a week or two behind their
       corresponding source release).  Make sure to enable the option
       *“Add LLVM to the system PATH for all the users”*.

       Note: Make sure that the path of LLVM toolchain is at the top of Environment
       Variables section to avoid conflicts between the set of compilers available
       in the system path

3. Launch the Native Command Prompt for Windows ARM64:

       From the start menu search for *"ARM64 Native Tools Command Prompt for Visual
       Studio 2022"*. Alternatively open command prompt, run the following command to
       activate the environment:

       ```cmd
       C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsarm64.bat
       ```

4. Navigate to the OpenBLAS source code directory and start building OpenBLAS
   by invoking Ninja:

       ```cmd
       cd OpenBLAS
       mkdir build
       cd build

       cmake .. -G Ninja -DCMAKE_BUILD_TYPE=Release -DTARGET=ARMV8 -DBINARY=64 -DCMAKE_C_COMPILER=clang-cl -DCMAKE_Fortran_COMPILER=flang-new

       ninja -j16
       ```
       
       Note: You might want to include additional options in the cmake command
       here. For example, the default configuration only generates a
       `static.lib` version of the library. If you prefer a DLL, you can add
       `-DBUILD_SHARED_LIBS=ON`.

       Note that it is also possible to use the same setup to build OpenBLAS
       with Make, if you prefer Makefiles over the CMake build for some
       reason:

       ```cmd
       $ make CC=clang-cl FC=flang-new AR="llvm-ar" TARGET=ARMV8 ARCH=arm64 RANLIB="llvm-ranlib" MAKE=make
       ```


#### Generating an import library

Microsoft Windows has this thing called "import libraries". You need it for
MSVC; you don't need it for MinGW because the `ld` linker is smart enough -
however, you may still want it for some reason, so we'll describe the process
for both MSVC and MinGW.

Import libraries are compiled from a list of what symbols to use, which are
contained in a `.def` file. A `.def` file should be already be present in the
`exports` directory under the top-level OpenBLAS directory after you've run a build.
In your shell, move to this directory: `cd exports`.

=== "MSVC"

    Unlike MinGW, MSVC absolutely requires an import library. Now the C ABI of
    MSVC and MinGW are actually identical, so linking is actually okay (any
    incompatibility in the C ABI would be a bug).

    The import libraries of MSVC have the suffix `.lib`. They are generated
    from a `.def` file using MSVC's `lib.exe`.

=== "MinGW"

    MinGW import libraries have the suffix `.a`, just like static libraries.
    Our goal is to produce the file `libopenblas.dll.a`.

    You need to first insert a line `LIBRARY libopenblas.dll` in `libopenblas.def`:
    ```
    cat <(echo "LIBRARY libopenblas.dll") libopenblas.def > libopenblas.def.1
    mv libopenblas.def.1 libopenblas.def
    ```

    Now the `.def` file probably looks like:
    ```
    LIBRARY libopenblas.dll
    EXPORTS
	   caxpy=caxpy_  @1
	   caxpy_=caxpy_  @2
           ...
    ```
    Then, generate the import library: `dlltool -d libopenblas.def -l libopenblas.dll.a`

    _Again, there is basically **no point** in making an import library for use in MinGW. It actually slows down linking._


### Android

To build OpenBLAS for Android, you will need the following tools installed on your machine:

- [The Android NDK](https://developer.android.com/ndk/)
- Clang compiler on the build machine

The next two sections below describe how to build with Clang for ARMV7 and
ARMV8 targets, respectively. The same basic principles as described below for
ARMV8 should also apply to building an x86 or x86-64 version (substitute
something like `NEHALEM` for the target instead of `ARMV8`, and replace all the
`aarch64` in the toolchain paths with `x86` or `x96_64` as appropriate).

!!! info "Historic note"

    Since NDK version 19, the default toolchain is provided as a standalone
    toolchain, so building one yourself following
    [building a standalone toolchain](http://developer.android.com/ndk/guides/standalone_toolchain.html)
    should no longer be necessary.


#### Building for ARMV7

```bash
# Set path to ndk-bundle
export NDK_BUNDLE_DIR=/path/to/ndk-bundle

# Set the PATH to contain paths to clang and arm-linux-androideabi-* utilities
export PATH=${NDK_BUNDLE_DIR}/toolchains/arm-linux-androideabi-4.9/prebuilt/linux-x86_64/bin:${NDK_BUNDLE_DIR}/toolchains/llvm/prebuilt/linux-x86_64/bin:$PATH

# Set LDFLAGS so that the linker finds the appropriate libgcc
export LDFLAGS="-L${NDK_BUNDLE_DIR}/toolchains/arm-linux-androideabi-4.9/prebuilt/linux-x86_64/lib/gcc/arm-linux-androideabi/4.9.x"

# Set the clang cross compile flags
export CLANG_FLAGS="-target arm-linux-androideabi -marm -mfpu=vfp -mfloat-abi=softfp --sysroot ${NDK_BUNDLE_DIR}/platforms/android-23/arch-arm -gcc-toolchain ${NDK_BUNDLE_DIR}/toolchains/arm-linux-androideabi-4.9/prebuilt/linux-x86_64/"

#OpenBLAS Compile
make TARGET=ARMV7 ONLY_CBLAS=1 AR=ar CC="clang ${CLANG_FLAGS}" HOSTCC=gcc ARM_SOFTFP_ABI=1 -j4
```

On macOS, it may also be necessary to give the complete path to the `ar`
utility in the make command above, like so:
```bash
AR=${NDK_BUNDLE_DIR}/toolchains/arm-linux-androideabi-4.9/prebuilt/darwin-x86_64/bin/arm-linux-androideabi-gcc-ar
```
otherwise you may get a linker error complaining like `malformed archive header
name at 8` when the native macOS `ar` command was invoked instead. Note that
with recent NDK versions, the AR tool may be named `llvm-ar` rather than what
is assumed above.

 
#### Building for ARMV8

```bash
# Set path to ndk-bundle
export NDK_BUNDLE_DIR=/path/to/ndk-bundle/

# Export PATH to contain directories of clang and aarch64-linux-android-* utilities
export PATH=${NDK_BUNDLE_DIR}/toolchains/aarch64-linux-android-4.9/prebuilt/linux-x86_64/bin/:${NDK_BUNDLE_DIR}/toolchains/llvm/prebuilt/linux-x86_64/bin:$PATH

# Setup LDFLAGS so that loader can find libgcc and pass -lm for sqrt
export LDFLAGS="-L${NDK_BUNDLE_DIR}/toolchains/aarch64-linux-android-4.9/prebuilt/linux-x86_64/lib/gcc/aarch64-linux-android/4.9.x -lm"

# Setup the clang cross compile options
export CLANG_FLAGS="-target aarch64-linux-android --sysroot ${NDK_BUNDLE_DIR}/platforms/android-23/arch-arm64 -gcc-toolchain ${NDK_BUNDLE_DIR}/toolchains/aarch64-linux-android-4.9/prebuilt/linux-x86_64/"

# Compile
make TARGET=ARMV8 ONLY_CBLAS=1 AR=ar CC="clang ${CLANG_FLAGS}" HOSTCC=gcc -j4
```
Note: using `TARGET=CORTEXA57` in place of `ARMV8` will pick up better
optimized routines. Implementations for the `CORTEXA57` target are compatible
with all other `ARMV8` targets.

Note: for NDK 23b, something as simple as:
```bash
export PATH=/opt/android-ndk-r23b/toolchains/llvm/prebuilt/linux-x86_64/bin/:$PATH
make HOSTCC=gcc CC=/opt/android-ndk-r23b/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android31-clang ONLY_CBLAS=1 TARGET=ARMV8
```
appears to be sufficient on Linux. On OSX, setting AR to the ar provided in the
"bin" path of the NDK (probably `llvm-ar`) is also necessary.


??? note "Alternative build script for 3 architectures"

    This script will build OpenBLAS for 3 architecture (`ARMV7`, `ARMV8`,
    `X86`) and install them to `/opt/OpenBLAS/lib`. Of course you can also copy
    only the section that is of interest to you - also notice that the `AR=`
    line may need adapting to the name of the ar tool provided in your
    `$TOOLCHAIN/bin` - for example `llvm-ar` in some recent NDK versions.
    It was tested on macOS with NDK version 21.3.6528147.

    ```bash
    export NDK=YOUR_PATH_TO_SDK/Android/sdk/ndk/21.3.6528147
    export TOOLCHAIN=$NDK/toolchains/llvm/prebuilt/darwin-x86_64

    make clean
    make \
        TARGET=ARMV7 \
        ONLY_CBLAS=1 \
        CC="$TOOLCHAIN"/bin/armv7a-linux-androideabi21-clang \
        AR="$TOOLCHAIN"/bin/arm-linux-androideabi-ar \
        HOSTCC=gcc \
        ARM_SOFTFP_ABI=1 \
        -j4
    sudo make install

    make clean
    make \
        TARGET=CORTEXA57 \
        ONLY_CBLAS=1 \
        CC=$TOOLCHAIN/bin/aarch64-linux-android21-clang \
        AR=$TOOLCHAIN/bin/aarch64-linux-android-ar \
        HOSTCC=gcc \
    -j4
    sudo make install

    make clean
    make \
        TARGET=ATOM \
        ONLY_CBLAS=1 \
    CC="$TOOLCHAIN"/bin/i686-linux-android21-clang \
    AR="$TOOLCHAIN"/bin/i686-linux-android-ar \
    HOSTCC=gcc \
    ARM_SOFTFP_ABI=1 \
    -j4
    sudo make install

    ## This will build for x86_64 
    make clean
    make \
        TARGET=ATOM BINARY=64\
    ONLY_CBLAS=1 \
    CC="$TOOLCHAIN"/bin/x86_64-linux-android21-clang \
    AR="$TOOLCHAIN"/bin/x86_64-linux-android-ar \
    HOSTCC=gcc \
    ARM_SOFTFP_ABI=1 \
    -j4
    sudo make install
    ```
    You can find full list of target architectures in [TargetList.txt](https://github.com/OpenMathLib/OpenBLAS/blob/develop/TargetList.txt)


### iPhone/iOS

As none of the current developers uses iOS, the following instructions are what
was found to work in our Azure CI setup, but as far as we know this builds a
fully working OpenBLAS for this platform.

Go to the directory where you unpacked OpenBLAS,and enter the following commands:
```bash
CC=/Applications/Xcode_12.4.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/clang

CFLAGS= -O2 -Wno-macro-redefined -isysroot /Applications/Xcode_12.4.app/Contents/Developer/Platforms/iPhoneOS.platform/Developer/SDKs/iPhoneOS14.4.sdk -arch arm64 -miphoneos-version-min=10.0

make TARGET=ARMV8 DYNAMIC_ARCH=1 NUM_THREADS=32 HOSTCC=clang NOFORTRAN=1
```
Adjust `MIN_IOS_VERSION` as necessary for your installation. E.g., change the version number
to the minimum iOS version you want to target and execute this file to build the library.

### HarmonyOS

For this target you will need the cross-compiler toolchain package by Huawei,
which contains solutions for both Windows and Linux. Only the Linux-based
toolchain has been tested so far, but the following instructions may apply
similarly to Windows:

Download [this HarmonyOS 4.1.1 SDK](https://repo.huaweicloud.com/harmonyos/os/4.1.1-Release/ohos-sdk-windows_linux-public.tar.gz),
or whatever newer version may be available in the future). Use `tar -xvf
ohos-sdk-windows_linux_public.tar.gz` to unpack it somewhere on your system.
This will create a folder named "ohos-sdk" with subfolders "linux" and
"windows". In the linux one you will find a ZIP archive named
`native-linux-x64-4.1.7.8-Release.zip` - you need to unzip this where you want
to install the cross-compiler, for example in `/opt/ohos-sdk`.

In the directory where you unpacked OpenBLAS, create a build directory for cmake, and change into it :
```bash
mkdir build
cd build
```
Use the version of `cmake` that came with the SDK, and specify the location of
its toolchain file as a cmake option. Also set the build target for OpenBLAS to
`ARMV8` and specify `NOFORTRAN=1` (at least as of version 4.1.1, the SDK
contains no Fortran compiler):
```bash
/opt/ohos-sdk/linux/native/build-tools/cmake/bin/cmake \
      -DCMAKE_TOOLCHAIN_FILE=/opt/ohos-sdk/linux/native/build/cmake/ohos.toolchain.cmake \
      -DOHOS_ARCH="arm64-v8a" -DTARGET=ARMV8 -DNOFORTRAN=1 ..
```
Additional other OpenBLAS build options like `USE_OPENMP=1` or `DYNAMIC_ARCH=1`
will probably work too. Finally do the build:
```bash
/opt/ohos-sdk/linux/native/build-tools/cmake/bin/cmake --build .
```

### MIPS

For MIPS targets you will need latest toolchains:

- P5600 - MTI GNU/Linux Toolchain
- I6400, P6600 - IMG GNU/Linux Toolchain

You can use following commandlines for builds:

```bash
IMG_TOOLCHAIN_DIR={full IMG GNU/Linux Toolchain path including "bin" directory -- for example, /opt/linux_toolchain/bin}
IMG_GCC_PREFIX=mips-img-linux-gnu
IMG_TOOLCHAIN=${IMG_TOOLCHAIN_DIR}/${IMG_GCC_PREFIX}

# I6400 Build (n32):
make BINARY=32 BINARY32=1 CC=$IMG_TOOLCHAIN-gcc AR=$IMG_TOOLCHAIN-ar FC="$IMG_TOOLCHAIN-gfortran -EL -mabi=n32" RANLIB=$IMG_TOOLCHAIN-ranlib HOSTCC=gcc CFLAGS="-EL" FFLAGS=$CFLAGS LDFLAGS=$CFLAGS TARGET=I6400

# I6400 Build (n64):
make BINARY=64 BINARY64=1 CC=$IMG_TOOLCHAIN-gcc AR=$IMG_TOOLCHAIN-ar FC="$IMG_TOOLCHAIN-gfortran -EL" RANLIB=$IMG_TOOLCHAIN-ranlib HOSTCC=gcc CFLAGS="-EL" FFLAGS=$CFLAGS LDFLAGS=$CFLAGS TARGET=I6400

# P6600 Build (n32):
make BINARY=32 BINARY32=1 CC=$IMG_TOOLCHAIN-gcc AR=$IMG_TOOLCHAIN-ar FC="$IMG_TOOLCHAIN-gfortran -EL -mabi=n32" RANLIB=$IMG_TOOLCHAIN-ranlib HOSTCC=gcc CFLAGS="-EL" FFLAGS=$CFLAGS LDFLAGS=$CFLAGS TARGET=P6600

# P6600 Build (n64):
make BINARY=64 BINARY64=1 CC=$IMG_TOOLCHAIN-gcc AR=$IMG_TOOLCHAIN-ar FC="$IMG_TOOLCHAIN-gfortran -EL" RANLIB=$IMG_TOOLCHAIN-ranlib HOSTCC=gcc CFLAGS="-EL" FFLAGS="$CFLAGS" LDFLAGS="$CFLAGS" TARGET=P6600

MTI_TOOLCHAIN_DIR={full MTI GNU/Linux Toolchain path including "bin" directory -- for example, /opt/linux_toolchain/bin}
MTI_GCC_PREFIX=mips-mti-linux-gnu
MTI_TOOLCHAIN=${IMG_TOOLCHAIN_DIR}/${IMG_GCC_PREFIX}

# P5600 Build:

make BINARY=32 BINARY32=1 CC=$MTI_TOOLCHAIN-gcc AR=$MTI_TOOLCHAIN-ar FC="$MTI_TOOLCHAIN-gfortran -EL"    RANLIB=$MTI_TOOLCHAIN-ranlib HOSTCC=gcc CFLAGS="-EL" FFLAGS=$CFLAGS LDFLAGS=$CFLAGS TARGET=P5600
```


### FreeBSD

You will need to install the following tools from the FreeBSD ports tree:

* lang/gcc
* lang/perl5.12
* ftp/curl
* devel/gmake
* devel/patch

To compile run the command:
```bash
$ gmake CC=gcc FC=gfortran
```


### Cortex-M

Cortex-M is a widely used microcontroller that is present in a variety of
industrial and consumer electronics. A common variant of the Cortex-M is the
`STM32F4xx` series. Here, we will give instructions for building for that
series.

First, install the embedded Arm GCC compiler from the Arm website. Then, create
the following `toolchain.cmake` file:

```cmake
set(CMAKE_SYSTEM_NAME Generic)
set(CMAKE_SYSTEM_PROCESSOR arm)

set(CMAKE_C_COMPILER "arm-none-eabi-gcc.exe")
set(CMAKE_CXX_COMPILER "arm-none-eabi-g++.exe")

set(CMAKE_EXE_LINKER_FLAGS "--specs=nosys.specs" CACHE INTERNAL "")

set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
```

Then build OpenBLAS with:
```bash
$ cmake .. -G Ninja -DCMAKE_C_COMPILER=arm-none-eabi-gcc -DCMAKE_TOOLCHAIN_FILE:PATH="toolchain.cmake" -DNOFORTRAN=1 -DTARGET=ARMV5 -DEMBEDDED=1
```

In your embedded application, the following functions need to be provided for OpenBLAS to work correctly:
```C
void free(void* ptr);
void* malloc(size_t size);
```

!!! note

    If you are developing for an embedded platform, it is your responsibility
    to make sure that the device has sufficient memory for `malloc` calls.
    [Libmemory](https://github.com/embeddedartistry/libmemory)
    provides one implementation of `malloc` for embedded platforms.
