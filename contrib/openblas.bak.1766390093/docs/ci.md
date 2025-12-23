# CI jobs

| Arch|Target CPU|OS|Build system|XComp to|C Compiler|Fortran Compiler|threading|DYN_ARCH|INT64|Libraries| CI Provider| CPU  count|
| ------------|---|---|-----------|-------------|----------|----------------|------|------------|----------|-----------|----------|-------|
| x86_64      |Intel 32bit|Windows|CMAKE/VS2015| -|mingw6.3| - | pthreads | - | - | static | Appveyor|   |
| x86_64      |Intel      |Windows|CMAKE/VS2015| -|mingw5.3| - | pthreads | - | - | static | Appveyor|  |
| x86_64      |Intel      |Centos5|gmake       | -|gcc 4.8 |gfortran| pthreads | + | - | both | Azure |   |
| x86_64      |SDE (SkylakeX)|Ubuntu| CMAKE| - | gcc | gfortran | pthreads | - | - | both | Azure |   |
| x86_64      |Haswell/ SkylakeX|Windows|CMAKE/VS2017| - | VS2017| - |  | - | - | static | Azure |   |
| x86_64      | " | Windows|mingw32-make| - |gcc | gfortran |  | list | - | both | Azure |  |
| x86_64      | " |Windows|CMAKE/Ninja| - |LLVM | - |  | - | - | static | Azure |   |
| x86_64      | " |Windows|CMAKE/Ninja| - |LLVM | flang | | - | - | static | Azure |   |
| x86_64      | " |Windows|CMAKE/Ninja| - |VS2022| flang* |  | - | - | static | Azure |   |
| x86_64      | " |macOS11|gmake | - | gcc-10|gfortran| OpenMP | + | - | both | Azure |  |
| x86_64      | " |macOS11|gmake | - | gcc-10|gfortran| none | - | - | both | Azure |  |
| x86_64      | " |macOS12|gmake | - | gcc-12|gfortran|pthreads| - | - | both | Azure |  |
| x86_64      | " |macOS11|gmake | - | llvm  | - | OpenMP | + | - | both | Azure |  |
| x86_64      | " |macOS11|CMAKE | - | llvm  | - | OpenMP | no_avx512 | - | static | Azure |  |
| x86_64      | " |macOS11|CMAKE | - | gcc-10| gfortran| pthreads | list | - | shared | Azure |  |
| x86_64      | " |macOS11|gmake | - | llvm | ifort | pthreads | - | - | both | Azure |  |
| x86_64      | " |macOS11|gmake |arm| AndroidNDK-llvm | - | | - | - | both | Azure |  |
| x86_64      | " |macOS11|gmake |arm64| XCode 12.4 | - | | + | - | both | Azure |  |
| x86_64      | " |macOS11|gmake |arm | XCode 12.4 | - | | + | - | both | Azure |  |
| x86_64      | " |Alpine Linux(musl)|gmake| - | gcc | gfortran | pthreads | + | - | both | Azure |  |
| arm64       |Apple M1   |OSX    |CMAKE/XCode| - | LLVM   | - | OpenMP | - | - | static | Cirrus |   |
| arm64       |Apple M1   |OSX    |CMAKE/Xcode| - | LLVM   | - | OpenMP | - | + | static | Cirrus |  |  
| arm64       |Apple M1   |OSX    |CMAKE/XCode|x86_64| LLVM| - | - | + | - | static | Cirrus |   |
| arm64       |Neoverse N1|Linux  |gmake      | -    |gcc10.2| -| pthreads| - | - | both   | Cirrus |   |
| arm64       |Neoverse N1|Linux  |gmake      | -    |gcc10.2| -| pthreads| - | + | both   | Cirrus |  |
| arm64       |Neoverse N1|Linux  |gmake      |-     |gcc10.2| -| OpenMP | - | - | both   |Cirrus | 8 |
| x86_64      | Ryzen|   FreeBSD  |gmake      | - | gcc12.2|gfortran| pthreads| - | - | both | Cirrus | |
| x86_64      | Ryzen|   FreeBSD  |gmake      |   | gcc12.2|gfortran| pthreads| - | + | both | Cirrus | |
| x86_64       |GENERIC    |QEMU   |gmake| mips64 | gcc | gfortran | pthreads | - | - | static | Github |  |
| x86_64      |SICORTEX   |QEMU   |gmake| mips64 | gcc | gfortran | pthreads | - | - | static | Github |  |
| x86_64      |I6400      |QEMU   |gmake| mips64 | gcc | gfortran | pthreads | - | - | static | Github |  |
| x86_64      |P6600      |QEMU   |gmake| mips64 | gcc | gfortran | pthreads | - | - | static | Github |  |
| x86_64      |I6500      |QEMU   |gmake| mips64 | gcc | gfortran | pthreads | - | - | static | Github |  |
| x86_64      |Intel      |Ubuntu |CMAKE| - | gcc-11.3 | gfortran | pthreads | + | - | static | Github |  |
| x86_64      |Intel      |Ubuntu |gmake| - | gcc-11.3 | gfortran | pthreads | + | - | both   | Github |  |
| x86_64      |Intel      |Ubuntu |CMAKE| - | gcc-11.3 | flang-classic | pthreads | + | - | static | Github |  |
| x86_64      |Intel      |Ubuntu |gmake| - | gcc-11.3 | flang-classic | pthreads | + | - | both   | Github |  |
| x86_64      |Intel      |macOS12 | CMAKE| - | AppleClang 14 | gfortran | pthreads | + | - | static | Github |  |
| x86_64      |Intel      |macOS12 | gmake| - | AppleClang 14 | gfortran | pthreads | + | - | both   | Github |  |
| x86_64      |Intel      |Windows2022 | CMAKE/Ninja| - | mingw gcc 13  | gfortran |  | + | - | static | Github |  |
| x86_64      |Intel      |Windows2022 | CMAKE/Ninja| - | mingw gcc 13  | gfortran |  | + | + | static | Github |  |
| x86_64      |Intel 32bit|Windows2022 | CMAKE/Ninja| - | mingw gcc 13  | gfortran |  | + | - | static | Github | |
| x86_64      |Intel |Windows2022 | CMAKE/Ninja| - | LLVM 16 | - |  |  + | - | static | Github |  |
| x86_64      |Intel | Windows2022 |CMAKE/Ninja| - | LLVM 16 | - |  | + | + | static | Github |  | 
| x86_64      |Intel | Windows2022 |CMAKE/Ninja| - | gcc 13| - |  | + | - | static | Github |   |
| x86_64      |Intel| Ubuntu        |gmake      |mips64|gcc|gfortran|pthreads|+|-|both|Github|   |
| x86_64      |generic|Ubuntu        |gmake      |riscv64|gcc|gfortran|pthreads|-|-|both|Github|  |
| x86_64      |Intel|Ubuntu        |gmake      |mips32|gcc|gfortran|pthreads|-|-|both|Github |  |
| x86_64      |Intel|Ubuntu        |gmake      |ia64|gcc|gfortran|pthreads|-|-|both|Github|  |
| x86_64      |C910V|QEmu         |gmake      |riscv64|gcc|gfortran|pthreads|-|-|both|Github| |
|power        |pwr9| Ubuntu        |gmake      | - |gcc|gfortran|OpenMP|-|-|both|OSUOSL|  |
|zarch        |z14 | Ubuntu        |gmake      | - |gcc|gfortran|OpenMP|-|-|both|OSUOSL|  |
