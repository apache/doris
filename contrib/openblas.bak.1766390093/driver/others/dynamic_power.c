
#include "common.h"

extern gotoblas_t gotoblas_POWER6;
extern gotoblas_t gotoblas_POWER8;
#if ((!defined __GNUC__) || ( __GNUC__ >= 6)) || defined(__clang__)
extern gotoblas_t gotoblas_POWER9;
#endif
#ifdef HAVE_P10_SUPPORT
extern gotoblas_t gotoblas_POWER10;
#endif

extern void openblas_warning(int verbose, const char *msg);

static char *corename[] = {
	"unknown",
	"POWER6",
	"POWER8",
	"POWER9",
	"POWER10"
};

#define NUM_CORETYPES 5

char *gotoblas_corename(void) {
#ifndef C_PGI
	if (gotoblas == &gotoblas_POWER6)	return corename[1];
#endif
	if (gotoblas == &gotoblas_POWER8)	return corename[2];
#if ((!defined __GNUC__) || ( __GNUC__ >= 6)) || defined(__clang__)
	if (gotoblas == &gotoblas_POWER9)	return corename[3];
#endif
#ifdef HAVE_P10_SUPPORT
	if (gotoblas == &gotoblas_POWER10)	return corename[4];
#endif
	return corename[0];
}

#define CPU_UNKNOWN  0
#define CPU_POWER5   5
#define CPU_POWER6   6
#define CPU_POWER8   8
#define CPU_POWER9   9
#define CPU_POWER10 10

#ifndef POWER_9
#define POWER_9         0x20000         /* 9 class CPU */
#endif
#ifndef POWER_10
#define POWER_10        0x40000         /* 10 class CPU */
#endif

#ifdef _AIX
#include <sys/systemcfg.h>

static int cpuid(void)
{
    int arch = _system_configuration.implementation;
#ifdef POWER_6
    if (arch == POWER_6) return CPU_POWER6;
#endif
#ifdef POWER_7
    else if (arch == POWER_7) return CPU_POWER6;
#endif
#ifdef POWER_8
    else if (arch == POWER_8) return CPU_POWER8;
#endif
#ifdef POWER_9
    else if (arch == POWER_9) return CPU_POWER9;
#endif
#ifdef POWER_10
    else if (arch >= POWER_10) return CPU_POWER10;
#endif
    return CPU_UNKNOWN;
}
#elif defined(C_PGI) || defined(__clang__)
/*
 * NV HPC compilers do not yet implement __builtin_cpu_is().
 * Fake a version here for use in the CPU detection code below.
 *
 * Strategy here is to first check the CPU to see what it actually is,
 * and then test the input to see if what the CPU actually is matches
 * what was requested.
 */

/*
 *  Define POWER processor version table.
 *
 *  NOTE NV HPC SDK compilers only support POWER8 and POWER9 at this time
 */

static  struct {
    uint32_t    pvr_mask;
    uint32_t    pvr_value;
    const char* cpu_name;
    uint32_t    cpu_type;
} pvrPOWER [] = {

    {   /* POWER6 in P5+ mode; 2.04-compliant processor */
        .pvr_mask       = 0xffffffff,
        .pvr_value      = 0x0f000001,
        .cpu_name       = "POWER5+",
        .cpu_type       = CPU_POWER5,
    },

    {   /* Power6 aka POWER6X*/
        .pvr_mask       = 0xffff0000,
        .pvr_value      = 0x003e0000,
        .cpu_name       = "POWER6 (raw)",
        .cpu_type       = CPU_POWER6,
    },

    {   /* Power7 */
        .pvr_mask       = 0xffff0000,
        .pvr_value      = 0x003f0000,
        .cpu_name       = "POWER7 (raw)",
        .cpu_type       = CPU_POWER6,
    },

    {   /* Power7+ */
        .pvr_mask       = 0xffff0000,
        .pvr_value      = 0x004A0000,
        .cpu_name       = "POWER7+ (raw)",
        .cpu_type       = CPU_POWER6,
    },

    {   /* Power8E */
        .pvr_mask       = 0xffff0000,
        .pvr_value      = 0x004b0000,
        .cpu_name       = "POWER8E (raw)",
        .cpu_type       = CPU_POWER8,
    },

    {   /* Power8NVL */
        .pvr_mask       = 0xffff0000,
        .pvr_value      = 0x004c0000,
        .cpu_name       = "POWER8NVL (raw)",
        .cpu_type       = CPU_POWER8,
    },

    {   /* Power8 */
        .pvr_mask       = 0xffff0000,
        .pvr_value      = 0x004d0000,
        .cpu_name       = "POWER8 (raw)",
        .cpu_type       = CPU_POWER8,
    },

    {   /* Power9 DD2.0 */
        .pvr_mask       = 0xffffefff,
        .pvr_value      = 0x004e0200,
        .cpu_name       = "POWER9 (raw)",
        .cpu_type       = CPU_POWER9,
    },

    {   /* Power9 DD 2.1 */
        .pvr_mask       = 0xffffefff,
        .pvr_value      = 0x004e0201,
        .cpu_name       = "POWER9 (raw)",
        .cpu_type       = CPU_POWER9,
    },

    {   /* Power9 DD2.2 or later */
        .pvr_mask       = 0xffff0000,
        .pvr_value      = 0x004e0000,
        .cpu_name       = "POWER9 (raw)",
        .cpu_type       = CPU_POWER9,
    },

    {   /* Power10 */
        .pvr_mask       = 0xffff0000,
        .pvr_value      = 0x00800000,
        .cpu_name       = "POWER10 (raw)",
        .cpu_type       = CPU_POWER10,
    },

    {   /* End of table, pvr_mask and pvr_value must be zero */
        .pvr_mask       = 0x0,
        .pvr_value      = 0x0,
        .cpu_name       = "Unknown",
        .cpu_type       = CPU_UNKNOWN,
    },
};

static int cpuid(void)
{
	int i;
	uint32_t pvr;
	uint32_t cpu_type;

	asm("mfpvr    %0" : "=r"(pvr));

	for (i = 0 ; i < sizeof pvrPOWER / sizeof *pvrPOWER ; ++i) {
		if ((pvr & pvrPOWER[i].pvr_mask) == pvrPOWER[i].pvr_value) {
			break;
		}
	}

#if defined(DEBUG)
	printf("%s: returning CPU=%s, cpu_type=%p\n", __func__,
		pvrPOWER[i].cpu_name, pvrPOWER[i].cpu_type);
#endif
	cpu_type = pvrPOWER[i].cpu_type;
	return (int)(cpu_type);
}
#elif !defined(__BUILTIN_CPU_SUPPORTS__)
static int cpuid(void)
{
    return CPU_UNKNOWN;
}
#endif  /* _AIX */

#ifndef __BUILTIN_CPU_SUPPORTS__
#include <string.h>

#ifndef __has_builtin
#define __has_builtin(x)   0
#endif

#if defined(_AIX) || !__has_builtin(__builtin_cpu_is)
static int __builtin_cpu_is(const char *arg)
{
    static int ipinfo = -1;
    if (ipinfo < 0) {
        ipinfo = cpuid();
    }
#ifdef HAVE_P10_SUPPORT
    if (ipinfo == CPU_POWER10) {
        if (!strcmp(arg, "power10")) return 1;
    }
#endif
    if (ipinfo == CPU_POWER9) {
        if (!strcmp(arg, "power9")) return 1;
    } else if (ipinfo == CPU_POWER8) {
        if (!strcmp(arg, "power8")) return 1;
#ifndef C_PGI
    } else if (ipinfo == CPU_POWER6) {
        if (!strcmp(arg, "power6")) return 1;
#endif
    }
    return 0;
}
#endif

#if defined(_AIX) || !__has_builtin(__builtin_cpu_supports)
static int __builtin_cpu_supports(const char *arg)
{
    return 0;
}
#endif
#endif

static gotoblas_t *get_coretype(void) {

#ifndef C_PGI
	if (__builtin_cpu_is("power6") || __builtin_cpu_is("power6x"))
		return &gotoblas_POWER6;
#endif
	if (__builtin_cpu_is("power8"))
		return &gotoblas_POWER8;
#if ((!defined __GNUC__) || ( __GNUC__ >= 6)) || defined(__clang__)
	if (__builtin_cpu_is("power9"))
		return &gotoblas_POWER9;
#endif
#ifdef HAVE_P10_SUPPORT
#if defined(_AIX) || defined(__clang__)
	if (__builtin_cpu_is("power10"))
#else
	if (__builtin_cpu_supports ("arch_3_1") && __builtin_cpu_supports ("mma"))
#endif
		return &gotoblas_POWER10;
#endif
	/* Fall back to the POWER9 implementation if the toolchain is too old or the MMA feature is not set */
#if (!defined __GNUC__) || ( __GNUC__ >= 11) || (__GNUC__ == 10 && __GNUC_MINOR__ >= 2)
	if (__builtin_cpu_is("power10"))
		return &gotoblas_POWER9;
#endif
	return NULL;
}

static gotoblas_t *force_coretype(char * coretype) {

	int i ;
	int found = -1;
	char message[128];

	for ( i = 0 ; i < NUM_CORETYPES; i++)
	{
		if (!strncasecmp(coretype, corename[i], 20))
		{
			found = i;
			break;
		}
	}

	switch (found)
	{
#ifndef C_PGI
	case  1: return (&gotoblas_POWER6);
#endif
	case  2: return (&gotoblas_POWER8);
#if ((!defined __GNUC__) || ( __GNUC__ >= 6)) || defined(__clang__)
	case  3: return (&gotoblas_POWER9);
#endif
#ifdef HAVE_P10_SUPPORT
	case  4: return (&gotoblas_POWER10);
#endif
	default: return NULL;
	}
	snprintf(message, 128, "Core not found: %s\n", coretype);
	openblas_warning(1, message);
}

void gotoblas_dynamic_init(void) {

	char coremsg[128];
	char coren[22];
	char *p;


	if (gotoblas) return;

	p = getenv("OPENBLAS_CORETYPE");
	if ( p )
	{
		gotoblas = force_coretype(p);
	}
	else
	{
		gotoblas = get_coretype();
	}

	if (gotoblas == NULL)
	{
		snprintf(coremsg, 128, "Falling back to POWER8 core\n");
		openblas_warning(1, coremsg);
		gotoblas = &gotoblas_POWER8;
	}

	if (gotoblas && gotoblas -> init) {
		strncpy(coren,gotoblas_corename(),20);
		sprintf(coremsg, "Core: %s\n",coren);
		if (getenv("GET_OPENBLAS_CORETYPE")) {
			fprintf(stderr, "%s", coremsg);
		}
		openblas_warning(2, coremsg);
		gotoblas -> init();
	} else {
		openblas_warning(0, "OpenBLAS : Architecture Initialization failed. No initialization function found.\n");
		exit(1);
	}
}

void gotoblas_dynamic_quit(void) {
	gotoblas = NULL;
}
