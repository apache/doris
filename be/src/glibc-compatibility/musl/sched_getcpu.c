#define _GNU_SOURCE
#include <errno.h>
#include <sched.h>
#include "syscall.h"
#include "atomic.h"

#ifndef __NR_getcpu
#if defined(__x86_64__)
#define __NR_getcpu 309
#elif defined(__i386__)
#define __NR_getcpu 318
#elif defined(__aarch64__)
#define __NR_getcpu 168
#endif
#endif

#ifndef SYS_getcpu
#ifdef __NR_getcpu
#define SYS_getcpu __NR_getcpu
#endif
#endif

#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
#endif
#endif

#ifdef VDSO_GETCPU_SYM

static void *volatile vdso_func;

typedef long (*getcpu_f)(unsigned *, unsigned *, void *);

static long getcpu_init(unsigned *cpu, unsigned *node, void *unused)
{
	void *p = __vdsosym(VDSO_GETCPU_VER, VDSO_GETCPU_SYM);
	getcpu_f f = (getcpu_f)p;
	a_cas_p(&vdso_func, (void *)getcpu_init, p);
	return f ? f(cpu, node, unused) : -ENOSYS;
}

static void *volatile vdso_func = (void *)getcpu_init;

#endif

int sched_getcpu(void)
{
	int r;
	unsigned cpu = 0;

#ifdef VDSO_GETCPU_SYM
	getcpu_f f = (getcpu_f)vdso_func;
	if (f) {
		r = f(&cpu, 0, 0);
		if (!r) return cpu;
		if (r != -ENOSYS) return __syscall_ret(r);
	}
#endif

	r = __syscall(SYS_getcpu, &cpu, 0, 0);
	if (!r) {
#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
        __msan_unpoison(&cpu, sizeof(cpu));
#endif
#endif
        return cpu;
    }
	return __syscall_ret(r);
}
