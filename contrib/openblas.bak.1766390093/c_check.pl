#!/usr/bin/env perl

#use File::Basename;
# use File::Temp qw(tempfile);

# Checking cross compile
$hostos   = `uname -s | sed -e s/\-.*//`;    chop($hostos);
$hostarch = `uname -m | sed -e s/i.86/x86/`;
$hostarch = `uname -p` if ($hostos eq "AIX" || $hostos eq "SunOS");
chop($hostarch);
$hostarch = "x86_64" if ($hostarch eq "amd64");
$hostarch = "arm" if ($hostarch ne "arm64" && $hostarch =~ /^arm.*/);
$hostarch = "arm64" if ($hostarch eq "aarch64");
$hostarch = "power" if ($hostarch =~ /^(powerpc|ppc).*/);
$hostarch = "zarch" if ($hostarch eq "s390x");

#$tmpf = new File::Temp( UNLINK => 1 );
$binary = $ENV{"BINARY"};

$makefile = shift(@ARGV);
$config   = shift(@ARGV);

$compiler_name = shift(@ARGV);
$flags = join(" ", @ARGV);

# First, we need to know the target OS and compiler name

$data = `$compiler_name $flags -E ctest.c`;

if ($?) {
    printf STDERR "C Compiler ($compiler_name) is something wrong.\n";
    die 1;
}

$cross_suffix = "";

eval "use File::Basename";
if ($@){ 
    warn "could not load PERL module File::Basename, emulating its functionality";
    my $dirnam = substr($compiler_name, 0, rindex($compiler_name, "/")-1 );
    if ($dirnam ne ".") {
	$cross_suffix .= $dirnam . "/";
    }
    my $basnam = substr($compiler_name, rindex($compiler_name,"/")+1, length($compiler_name)-rindex($compiler_name,"/")-1);
	if ($basnam =~ /([^\s]*-)(.*)/) {
	$cross_suffix .= $1;
    }
} else {
    if (dirname($compiler_name) ne ".") {
	$cross_suffix .= dirname($compiler_name) . "/";
    }

    if (basename($compiler_name) =~ /([^\s]*-)(.*)/) {
	$cross_suffix .= $1;
    }
}

$compiler = "";
$compiler = LSB       if ($data =~ /COMPILER_LSB/);
$compiler = CLANG     if ($data =~ /COMPILER_CLANG/);
$compiler = PGI       if ($data =~ /COMPILER_PGI/);
$compiler = PATHSCALE if ($data =~ /COMPILER_PATHSCALE/);
$compiler = INTEL     if ($data =~ /COMPILER_INTEL/);
$compiler = OPEN64    if ($data =~ /COMPILER_OPEN64/);
$compiler = SUN       if ($data =~ /COMPILER_SUN/);
$compiler = IBM       if ($data =~ /COMPILER_IBM/);
$compiler = DEC       if ($data =~ /COMPILER_DEC/);
$compiler = FUJITSU   if ($data =~ /COMPILER_FUJITSU/);
$compiler = GCC       if ($compiler eq "");

$os = Linux           if ($data =~ /OS_LINUX/);
$os = FreeBSD         if ($data =~ /OS_FREEBSD/);
$os = NetBSD          if ($data =~ /OS_NETBSD/);
$os = OpenBSD         if ($data =~ /OS_OPENBSD/);
$os = DragonFly       if ($data =~ /OS_DRAGONFLY/);
$os = Darwin          if ($data =~ /OS_DARWIN/);
$os = SunOS           if ($data =~ /OS_SUNOS/);
$os = AIX             if ($data =~ /OS_AIX/);
$os = osf             if ($data =~ /OS_OSF/);
$os = WINNT           if ($data =~ /OS_WINNT/);
$os = CYGWIN_NT       if ($data =~ /OS_CYGWIN_NT/);
$os = Interix         if ($data =~ /OS_INTERIX/);
$os = Android         if ($data =~ /OS_ANDROID/);
$os = Haiku           if ($data =~ /OS_HAIKU/);

$architecture = x86          if ($data =~ /ARCH_X86/);
$architecture = x86_64       if ($data =~ /ARCH_X86_64/);
$architecture = e2k          if ($data =~ /ARCH_E2K/);
$architecture = power        if ($data =~ /ARCH_POWER/);
$architecture = mips         if ($data =~ /ARCH_MIPS/);
$architecture = mips64       if ($data =~ /ARCH_MIPS64/);
$architecture = alpha        if ($data =~ /ARCH_ALPHA/);
$architecture = sparc        if ($data =~ /ARCH_SPARC/);
$architecture = ia64         if ($data =~ /ARCH_IA64/);
$architecture = arm          if ($data =~ /ARCH_ARM/);
$architecture = arm64        if ($data =~ /ARCH_ARM64/);
$architecture = zarch        if ($data =~ /ARCH_ZARCH/);
$architecture = riscv64      if ($data =~ /ARCH_RISCV64/);
$architecture = loongarch64  if ($data =~ /ARCH_LOONGARCH64/);
$architecture = csky         if ($data =~ /ARCH_CSKY/);

$defined = 0;

if ($os eq "AIX") {
    $compiler_name .= " -maix32" if ($binary eq "32");
    $compiler_name .= " -maix64" if ($binary eq "64");
    $defined = 1;
}

if ($architecture eq "mips") {
    $compiler_name .= " -mabi=32";
    $defined = 1;
}

if ($architecture eq "mips64") {
    $compiler_name .= " -mabi=n32" if ($binary eq "32");
    $compiler_name .= " -mabi=64" if ($binary eq "64");
    $defined = 1;
}

if (($architecture eq "arm") || ($architecture eq "arm64")) {
    $defined = 1;
}

if ($architecture eq "zarch") {
    $defined = 1;
    $binary = 64;
}

if ($architecture eq "e2k") {
    $defined = 1;
    $binary = 64;
}

if ($architecture eq "alpha") {
    $defined = 1;
    $binary = 64;
}

if ($architecture eq "ia64") {
    $defined = 1;
    $binary = 64;
}

if (($architecture eq "x86") && ($os ne Darwin) && ($os ne SunOS)) {
    $defined = 1;
    $binary =32;
}

if ($architecture eq "riscv64") {
    $defined = 1;
    $binary = 64;
}

if ($architecture eq "loongarch64") {
    $defined = 1;
    $binary = 64;
}

if ($architecture eq "csky") {
    $defined = 1;
    $binary = 32;
}

if ($compiler eq "PGI") {
    $compiler_name .= " -tp p7"    if ($binary eq "32");
    $compiler_name .= " -tp p7-64" if ($binary eq "64");
    $openmp = "-mp";
    $defined = 1;
}

if ($compiler eq "IBM") {
    $compiler_name .= " -q32"  if ($binary eq "32");
    $compiler_name .= " -q64"  if ($binary eq "64");
    $openmp = "-qsmp=omp";
    $defined = 1;
}

if ($compiler eq "INTEL") {
    $openmp = "-openmp";
}

if ($compiler eq "PATHSCALE") {
    $openmp = "-mp";
}

if ($compiler eq "OPEN64") {
    $openmp = "-mp";
}

if ($compiler eq "CLANG") {
    $openmp = "-fopenmp";
}

if ($compiler eq "GCC" || $compiler eq "LSB") {
    $openmp = "-fopenmp";
}

if ($compiler eq "FUJITSU") {
    $openmp = "-Kopenmp";
}

if ($defined == 0) {
    $compiler_name .= " -m32" if ($binary eq "32");
    $compiler_name .= " -m64" if ($binary eq "64");
}

# Do again

$data = `$compiler_name $flags -E ctest.c`;

if ($?) {
    printf STDERR "C Compiler ($compiler_name) is something wrong.\n";
    die 1;
}

$have_msa = 0;
if (($architecture eq "mips") || ($architecture eq "mips64")) {
    eval "use File::Temp qw(tempfile)";
    if ($@){ 
	warn "could not load PERL module File::Temp, so could not check MSA capatibility";
    } else {
	$tmpf = new File::Temp( SUFFIX => '.c' , UNLINK => 1 );
	$code = '"addvi.b $w0, $w1, 1"';
	$msa_flags = "-mmsa -mfp64 -mload-store-pairs";
	print $tmpf "#include <msa.h>\n\n";
	print $tmpf "void main(void){ __asm__ volatile($code); }\n";

	$args = "$msa_flags -o $tmpf.o $tmpf";
	my @cmd = ("$compiler_name $flags $args >/dev/null 2>/dev/null");
	system(@cmd) == 0;
	if ($? != 0) {
	    $have_msa = 0;
	} else {
	    $have_msa = 1;
	}
	unlink("$tmpf.o");
    }
}

$no_lsx = 0;
$no_lasx = 0;
if (($architecture eq "loongarch64")) {
    eval "use File::Temp qw(tempfile)";
    if ($@){
	warn "could not load PERL module File::Temp, so could not check LSX and LASX capatibility";
    } else {
	$tmplsx = new File::Temp( SUFFIX => '.c' , UNLINK => 1 );
	$codelsx = '"vadd.b $vr0, $vr0, $vr0"';
	$lsx_flags = "-march=loongarch64";
	print $tmplsx "void main(void){ __asm__ volatile($codelsx); }\n";

	$args = "$lsx_flags -o $tmplsx.o $tmplsx";
	my @cmd = ("$compiler_name $flags $args >/dev/null 2>/dev/null");
	system(@cmd) == 0;
	if ($? != 0) {
	    $no_lsx = 1;
	} else {
	    $no_lsx = 0;
	}
	unlink("$tmplsx.o");

	$tmplasx = new File::Temp( SUFFIX => '.c' , UNLINK => 1 );
	$codelasx = '"xvadd.b $xr0, $xr0, $xr0"';
	$lasx_flags = "-march=loongarch64";
	print $tmplasx "void main(void){ __asm__ volatile($codelasx); }\n";

	$args = "$lasx_flags -o $tmplasx.o $tmplasx";
	my @cmd = ("$compiler_name $flags $args >/dev/null 2>/dev/null");
	system(@cmd) == 0;
	if ($? != 0) {
	    $no_lasx = 1;
	} else {
	    $no_lasx = 0;
	}
	unlink("$tmplasx.o");
    }
}

$architecture = x86          if ($data =~ /ARCH_X86/);
$architecture = x86_64       if ($data =~ /ARCH_X86_64/);
$architecture = e2k          if ($data =~ /ARCH_E2K/);
$architecture = power        if ($data =~ /ARCH_POWER/);
$architecture = mips         if ($data =~ /ARCH_MIPS/);
$architecture = mips64       if ($data =~ /ARCH_MIPS64/);
$architecture = alpha        if ($data =~ /ARCH_ALPHA/);
$architecture = sparc        if ($data =~ /ARCH_SPARC/);
$architecture = ia64         if ($data =~ /ARCH_IA64/);
$architecture = arm          if ($data =~ /ARCH_ARM/);
$architecture = arm64        if ($data =~ /ARCH_ARM64/);
$architecture = zarch        if ($data =~ /ARCH_ZARCH/);
$architecture = loongarch64  if ($data =~ /ARCH_LOONGARCH64/);
$architecture = csky         if ($data =~ /ARCH_CSKY/);

$binformat    = bin32;
$binformat    = bin64  if ($data =~ /BINARY_64/);

$no_avx512= 0;
if (($architecture eq "x86") || ($architecture eq "x86_64")) {
    eval "use File::Temp qw(tempfile)";
    if ($@){ 
	warn "could not load PERL module File::Temp, so could not check compiler compatibility with AVX512";
	$no_avx512 = 0;
    } else {
#	$tmpf = new File::Temp( UNLINK => 1 );
	($fh,$tmpf) = tempfile( SUFFIX => '.c' , UNLINK => 1 );
	$code = '"vbroadcastss -4 * 4(%rsi), %zmm2"';
	print $fh "#include <immintrin.h>\n\nint main(void){ __asm__ volatile($code); }\n";
	$args = " -march=skylake-avx512 -c -o $tmpf.o $tmpf";
	if ($compiler eq "PGI") {
	    $args = " -tp skylake -c -o $tmpf.o $tmpf";
	}
	my @cmd = ("$compiler_name $flags $args >/dev/null 2>/dev/null");
	system(@cmd) == 0;
	if ($? != 0) {
	    $no_avx512 = 1;
	} else {
	    $no_avx512 = 0;
	}
	unlink("$tmpf.o");
    }
}

$no_rv64gv= 0;
if (($architecture eq "riscv64")) {
    eval "use File::Temp qw(tempfile)";
    if ($@){ 
	warn "could not load PERL module File::Temp, so could not check compiler compatibility with the RISCV vector extension";
	$no_rv64gv = 0;
    } else {
#	$tmpf = new File::Temp( UNLINK => 1 );
	($fh,$tmpf) = tempfile( SUFFIX => '.c' , UNLINK => 1 );
	$code = '"vsetvli    zero, zero, e8, m1\n"';
	print $fh "int main(void){ __asm__ volatile($code); }\n";
	$args = " -march=rv64gv -c -o $tmpf.o $tmpf";
	my @cmd = ("$compiler_name $flags $args >/dev/null 2>/dev/null");
	system(@cmd) == 0;
	if ($? != 0) {
	    $no_rv64gv = 1;
	} else {
	    $no_rv64gv = 0;
	}
	unlink("$tmpf.o");
    }
}

$c11_atomics = 0;
if ($data =~ /HAVE_C11/) {
    eval "use File::Temp qw(tempfile)";
    if ($@){ 
       warn "could not load PERL module File::Temp, so could not check compiler compatibility with C11";
       $c11_atomics = 0;
    } else {
       ($fh,$tmpf) = tempfile( SUFFIX => '.c' , UNLINK => 1 );
       print $fh "#include <stdatomic.h>\nint main(void){}\n";
       $args = " -c -o $tmpf.o $tmpf";
       my @cmd = ("$compiler_name $flags $args >/dev/null 2>/dev/null");
       system(@cmd) == 0;
       if ($? != 0) {
           $c11_atomics = 0;
       } else {
           $c11_atomics = 1;
       }
       unlink("$tmpf.o");
    }
}

if ($compiler eq "GCC" &&( ($architecture eq "x86") || ($architecture eq "x86_64"))) {
	$no_avx2 = 0;
	$oldgcc = 0;
	$data = `$compiler_name -dumpversion`;
	if ($data <= 4.6) {
		$no_avx2 = 1;
		$oldgcc = 1;
	}
}

$data = `$compiler_name $flags -S ctest1.c && grep globl ctest1.s | head -n 1 && rm -f ctest1.s`;

$data =~ /globl\s([_\.]*)(.*)/;

$need_fu      = $1;

$cross = 0;

if ($architecture ne $hostarch) {
    $cross = 1;
    $cross = 0 if (($hostarch eq "x86_64") && ($architecture eq "x86"));
    $cross = 0 if (($hostarch eq "mips64") && ($architecture eq "mips"));
}

$cross = 1 if ($os ne $hostos);
$cross = 0 if (($os eq "Android") && ($hostos eq "Linux") && ($ENV{TERMUX_APP_PID} != ""));

$openmp = "" if $ENV{USE_OPENMP} != 1;

$linker_L = "";
$linker_l = "";
$linker_a = "";

{
    $link = `$compiler_name $flags -c ctest2.c -o ctest2.o 2>&1 && $compiler_name $flags $openmp -v ctest2.o -o ctest2 2>&1 && rm -f ctest2.o ctest2 ctest2.exe`;

    $link =~ s/\-Y\sP\,/\-Y/g;

    @flags = split(/[\s\,\n]/, $link);
    # remove leading and trailing quotes from each flag.
    @flags = map {s/^['"]|['"]$//g; $_} @flags;

    foreach $flags (@flags) {
	if (
	    ($flags =~ /^\-L/)
	    && ($flags !~ /^-LIST:/)
	    && ($flags !~ /^-LANG:/)
	    ) {
	    $linker_L .= $flags . " "
	    }

	if ($flags =~ /^\-Y/) {
	    $linker_L .= "-Wl,". $flags . " "
	    }

	if ($flags =~ /^\--exclude-libs/) {
	    $linker_L .= "-Wl,". $flags . " ";
	    $flags="";
	   }

	if (
	    ($flags =~ /^\-l/)
	    && ($flags !~ /gfortranbegin/)
	    && ($flags !~ /frtbegin/)
	    && ($flags !~ /pathfstart/)
	    && ($flags !~ /numa/)
	    && ($flags !~ /crt[0-9]/)
	    && ($flags !~ /gcc/)
	    && ($flags !~ /user32/)
	    && ($flags !~ /kernel32/)
	    && ($flags !~ /advapi32/)
	    && ($flags !~ /shell32/)
	    && ($flags !~ /omp/)
	    && ($flags !~ /[0-9]+/)
	    ) {
	    $linker_l .= $flags . " "
	}

	$linker_a .= $flags . " " if $flags =~ /\.a$/;
    }

}

open(MAKEFILE, "> $makefile") || die "Can't create $makefile";
open(CONFFILE, "> $config"  ) || die "Can't create $config";

# print $data, "\n";

print MAKEFILE "OSNAME=$os\n";
print MAKEFILE "ARCH=$architecture\n";
print MAKEFILE "C_COMPILER=$compiler\n";
print MAKEFILE "BINARY32=\n" if $binformat ne bin32;
print MAKEFILE "BINARY64=\n" if $binformat ne bin64;
print MAKEFILE "BINARY32=1\n" if $binformat eq bin32;
print MAKEFILE "BINARY64=1\n" if $binformat eq bin64;
print MAKEFILE "FU=$need_fu\n" if $need_fu ne "";
print MAKEFILE "CROSS_SUFFIX=$cross_suffix\n" if $cross != 0 && $cross_suffix ne "";
print MAKEFILE "CROSS=1\n" if $cross != 0;
print MAKEFILE "CEXTRALIB=$linker_L $linker_l $linker_a\n";
print MAKEFILE "HAVE_MSA=1\n" if $have_msa eq 1;
print MAKEFILE "MSA_FLAGS=$msa_flags\n" if $have_msa eq 1;
print MAKEFILE "NO_RV64GV=1\n" if $no_rv64gv eq 1;
print MAKEFILE "NO_AVX512=1\n" if $no_avx512 eq 1;
print MAKEFILE "NO_AVX2=1\n" if $no_avx2 eq 1;
print MAKEFILE "OLDGCC=1\n" if $oldgcc eq 1;
print MAKEFILE "NO_LSX=1\n" if $no_lsx eq 1;
print MAKEFILE "NO_LASX=1\n" if $no_lasx eq 1;

$os           =~ tr/[a-z]/[A-Z]/;
$architecture =~ tr/[a-z]/[A-Z]/;
$compiler     =~ tr/[a-z]/[A-Z]/;

print CONFFILE "#define OS_$os\t1\n";
print CONFFILE "#define ARCH_$architecture\t1\n";
print CONFFILE "#define C_$compiler\t1\n";
print CONFFILE "#define __32BIT__\t1\n"  if $binformat eq bin32;
print CONFFILE "#define __64BIT__\t1\n"  if $binformat eq bin64;
print CONFFILE "#define FUNDERSCORE\t$need_fu\n" if $need_fu ne "";
print CONFFILE "#define HAVE_MSA\t1\n"  if $have_msa eq 1;
print CONFFILE "#define HAVE_C11\t1\n" if $c11_atomics eq 1;
print CONFFILE "#define NO_LSX\t1\n" if $no_lsx eq 1;
print CONFFILE "#define NO_LASX\t1\n" if $no_lasx eq 1;


if ($os eq "LINUX") {

#    @pthread = split(/\s+/, `nm /lib/libpthread.so* | grep _pthread_create`);

#    if ($pthread[2] ne "") {
#	print CONFFILE "#define PTHREAD_CREATE_FUNC	$pthread[2]\n";
#    } else {
	print CONFFILE "#define PTHREAD_CREATE_FUNC	pthread_create\n";
#    }
} else {
    print CONFFILE "#define PTHREAD_CREATE_FUNC	pthread_create\n";
}

close(MAKEFILE);
close(CONFFILE);
