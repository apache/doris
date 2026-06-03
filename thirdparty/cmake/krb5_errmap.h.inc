/*
 * This file is generated, please don't edit it.
 * script: /mnt/disk7/sunchenyang/doris/thirdparty/src/krb5-1.19/src/util/gen.pl
 * args:   bimap errmap.h NAME=mecherrmap LEFT=OM_uint32 RIGHT=struct mecherror LEFTPRINT=print_OM_uint32 RIGHTPRINT=mecherror_print LEFTCMP=cmp_OM_uint32 RIGHTCMP=mecherror_cmp
 * The rest of this file is copied from a template, with
 * substitutions.  See the template for copyright info.
 */
/* start of t_bimap header template */
/*
 * bidirectional mapping table, add-only
 *
 * Parameters:
 * NAME
 * LEFT, RIGHT - types
 * LEFTCMP, RIGHTCMP - comparison functions
 *
 * Methods:
 * int init() - nonzero is error code, if any possible
 * long size()
 * void foreach(int (*)(LEFT, RIGHT, void*), void*)
 * int add(LEFT, RIGHT) - 0 = success, -1 = allocation failure
 * const struct mecherror *findleft(OM_uint32) - null iff not found
 * const OM_uint32 *findright(struct mecherror)
 * void destroy() - destroys container, doesn't delete elements
 *
 * initial implementation: flat array of (left,right) pairs
 */

struct mecherrmap__pair {
    OM_uint32 l;
    struct mecherror r;
};
/* end of t_bimap header template */
/* start of t_array template */

/*
 * array type, derived from template
 *
 * parameters:
 * NAME: mecherrmap__pairarray
 * TYPE: struct mecherrmap__pair
 *
 * methods:
 * int init() -> nonzero if fail initial allocation
 * unsigned long size() -> nonnegative number of values stored
 * int grow(newsize) -> negative if fail allocation, memset(,0,) new space
 * struct mecherrmap__pair *getaddr(idx) -> aborts if out of range
 * void set(idx, value) -> aborts if out of range
 * struct mecherrmap__pair get(idx) -> value, or aborts if out of range
 */

#include <stdlib.h>
#include <errno.h>
#include <limits.h>
#include <string.h>
#include <stdint.h>

struct mecherrmap__pairarray__header {
    size_t allocated;
    struct mecherrmap__pair *elts;
};
typedef struct mecherrmap__pairarray__header mecherrmap__pairarray;

static inline int
mecherrmap__pairarray_init(mecherrmap__pairarray *arr)
{
    arr->elts = calloc(10, sizeof(struct mecherrmap__pair));
    if (arr->elts == NULL)
	return ENOMEM;
    arr->allocated = 10;
    return 0;
}

static inline long
mecherrmap__pairarray_size(mecherrmap__pairarray *arr)
{
    return arr->allocated;
}

static inline unsigned long
mecherrmap__pairarray_max_size(mecherrmap__pairarray *arr)
{
    size_t upper_bound;

    upper_bound = SIZE_MAX / sizeof(*arr->elts);
    if (upper_bound > ULONG_MAX)
	upper_bound = ULONG_MAX;
    return (unsigned long) upper_bound;
}

static inline int
mecherrmap__pairarray_grow(mecherrmap__pairarray *arr, unsigned long newcount)
{
    size_t oldsize = sizeof(*arr->elts) * arr->allocated;
    size_t newsize;
    void *ptr;

    if (newcount > LONG_MAX)
	return -1;
    if (newcount < arr->allocated)
	return 0;
    if (newcount > mecherrmap__pairarray_max_size(arr))
	return -1;

    newsize = sizeof(*arr->elts) * newcount;
    ptr = realloc(arr->elts, newsize);
    if (ptr == NULL)
	return -1;
    memset((char *)ptr + oldsize, 0, newsize - oldsize);
    arr->elts = ptr;
    arr->allocated = newcount;
    return 0;
}

static inline struct mecherrmap__pair *
mecherrmap__pairarray_getaddr (mecherrmap__pairarray *arr, long idx)
{
    if (idx < 0 || (unsigned long) idx >= arr->allocated)
	abort();
    return arr->elts + idx;
}

static inline void
mecherrmap__pairarray_set (mecherrmap__pairarray *arr, long idx, struct mecherrmap__pair value)
{
    struct mecherrmap__pair *newvalp;
    newvalp = mecherrmap__pairarray_getaddr(arr, idx);
    *newvalp = value;
}

static inline struct mecherrmap__pair
mecherrmap__pairarray_get (mecherrmap__pairarray *arr, long idx)
{
    return *mecherrmap__pairarray_getaddr(arr, idx);
}

static inline void
mecherrmap__pairarray_destroy (mecherrmap__pairarray *arr)
{
    free(arr->elts);
    arr->elts = 0;
}
/* end of t_array template */
/* start of t_bimap body template */

/* for use in cases where text substitutions may not work, like putting
   "const" before a type that turns out to be "char *"  */
typedef OM_uint32 mecherrmap__left_t;
typedef struct mecherror mecherrmap__right_t;

typedef struct {
    mecherrmap__pairarray a;
    long nextidx;
} mecherrmap;

static inline int
mecherrmap_init (mecherrmap *m)
{
    m->nextidx = 0;
    return mecherrmap__pairarray_init (&m->a);
}

static inline long
mecherrmap_size (mecherrmap *m)
{
    return mecherrmap__pairarray_size (&m->a);
}

static inline void
mecherrmap_foreach (mecherrmap *m, int (*fn)(OM_uint32, struct mecherror, void *), void *p)
{
    long i, sz;
    sz = m->nextidx;
    for (i = 0; i < sz; i++) {
	struct mecherrmap__pair *pair;
	pair = mecherrmap__pairarray_getaddr (&m->a, i);
	if ((*fn)(pair->l, pair->r, p) != 0)
	    break;
    }
}

static inline int
mecherrmap_add (mecherrmap *m, OM_uint32 l, struct mecherror r)
{
    long i, sz;
    struct mecherrmap__pair newpair;
    int err;

    sz = m->nextidx;
    /* Make sure we're not duplicating.  */
    for (i = 0; i < sz; i++) {
	struct mecherrmap__pair *pair;
	pair = mecherrmap__pairarray_getaddr (&m->a, i);
	assert ((*cmp_OM_uint32)(l, pair->l) != 0);
	if ((*cmp_OM_uint32)(l, pair->l) == 0)
	    abort();
	assert ((*mecherror_cmp)(r, pair->r) != 0);
	if ((*mecherror_cmp)(r, pair->r) == 0)
	    abort();
    }
    newpair.l = l;
    newpair.r = r;
    if (sz >= LONG_MAX - 1)
	return ENOMEM;
    err = mecherrmap__pairarray_grow (&m->a, sz+1);
    if (err)
	return err;
    mecherrmap__pairarray_set (&m->a, sz, newpair);
    m->nextidx++;
    return 0;
}

static inline const mecherrmap__right_t *
mecherrmap_findleft (mecherrmap *m, OM_uint32 l)
{
    long i, sz;
    sz = mecherrmap_size (m);
    for (i = 0; i < sz; i++) {
	struct mecherrmap__pair *pair;
	pair = mecherrmap__pairarray_getaddr (&m->a, i);
	if ((*cmp_OM_uint32)(l, pair->l) == 0)
	    return &pair->r;
    }
    return 0;
}

static inline const mecherrmap__left_t *
mecherrmap_findright (mecherrmap *m, struct mecherror r)
{
    long i, sz;
    sz = mecherrmap_size (m);
    for (i = 0; i < sz; i++) {
	struct mecherrmap__pair *pair;
	pair = mecherrmap__pairarray_getaddr (&m->a, i);
	if ((*mecherror_cmp)(r, pair->r) == 0)
	    return &pair->l;
    }
    return 0;
}

struct mecherrmap__printstat {
    FILE *f;
    int comma;
};
static inline int
mecherrmap__printone (OM_uint32 l, struct mecherror r, void *p)
{
    struct mecherrmap__printstat *ps = p;
    fprintf(ps->f, ps->comma ? ", (" : "(");
    ps->comma = 1;
    (*print_OM_uint32)(l, ps->f);
    fprintf(ps->f, ",");
    (*mecherror_print)(r, ps->f);
    fprintf(ps->f, ")");
    return 0;
}

static inline void
mecherrmap_printmap (mecherrmap *m, FILE *f)
{
    struct mecherrmap__printstat ps;
    ps.comma = 0;
    ps.f = f;
    fprintf(f, "(");
    mecherrmap_foreach (m, mecherrmap__printone, &ps);
    fprintf(f, ")");
}

static inline void
mecherrmap_destroy (mecherrmap *m)
{
    mecherrmap__pairarray_destroy (&m->a);
}
/* end of t_bimap body template */
