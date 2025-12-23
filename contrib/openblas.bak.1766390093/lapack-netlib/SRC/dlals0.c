#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <complex.h>
#ifdef complex
#undef complex
#endif
#ifdef I
#undef I
#endif

#if defined(_WIN64)
typedef long long BLASLONG;
typedef unsigned long long BLASULONG;
#else
typedef long BLASLONG;
typedef unsigned long BLASULONG;
#endif

#ifdef LAPACK_ILP64
typedef BLASLONG blasint;
#if defined(_WIN64)
#define blasabs(x) llabs(x)
#else
#define blasabs(x) labs(x)
#endif
#else
typedef int blasint;
#define blasabs(x) abs(x)
#endif

typedef blasint integer;

typedef unsigned int uinteger;
typedef char *address;
typedef short int shortint;
typedef float real;
typedef double doublereal;
typedef struct { real r, i; } complex;
typedef struct { doublereal r, i; } doublecomplex;
#ifdef _MSC_VER
static inline _Fcomplex Cf(complex *z) {_Fcomplex zz={z->r , z->i}; return zz;}
static inline _Dcomplex Cd(doublecomplex *z) {_Dcomplex zz={z->r , z->i};return zz;}
static inline _Fcomplex * _pCf(complex *z) {return (_Fcomplex*)z;}
static inline _Dcomplex * _pCd(doublecomplex *z) {return (_Dcomplex*)z;}
#else
static inline _Complex float Cf(complex *z) {return z->r + z->i*_Complex_I;}
static inline _Complex double Cd(doublecomplex *z) {return z->r + z->i*_Complex_I;}
static inline _Complex float * _pCf(complex *z) {return (_Complex float*)z;}
static inline _Complex double * _pCd(doublecomplex *z) {return (_Complex double*)z;}
#endif
#define pCf(z) (*_pCf(z))
#define pCd(z) (*_pCd(z))
typedef blasint logical;

typedef char logical1;
typedef char integer1;

#define TRUE_ (1)
#define FALSE_ (0)

/* Extern is for use with -E */
#ifndef Extern
#define Extern extern
#endif

/* I/O stuff */

typedef int flag;
typedef int ftnlen;
typedef int ftnint;

/*external read, write*/
typedef struct
{	flag cierr;
	ftnint ciunit;
	flag ciend;
	char *cifmt;
	ftnint cirec;
} cilist;

/*internal read, write*/
typedef struct
{	flag icierr;
	char *iciunit;
	flag iciend;
	char *icifmt;
	ftnint icirlen;
	ftnint icirnum;
} icilist;

/*open*/
typedef struct
{	flag oerr;
	ftnint ounit;
	char *ofnm;
	ftnlen ofnmlen;
	char *osta;
	char *oacc;
	char *ofm;
	ftnint orl;
	char *oblnk;
} olist;

/*close*/
typedef struct
{	flag cerr;
	ftnint cunit;
	char *csta;
} cllist;

/*rewind, backspace, endfile*/
typedef struct
{	flag aerr;
	ftnint aunit;
} alist;

/* inquire */
typedef struct
{	flag inerr;
	ftnint inunit;
	char *infile;
	ftnlen infilen;
	ftnint	*inex;	/*parameters in standard's order*/
	ftnint	*inopen;
	ftnint	*innum;
	ftnint	*innamed;
	char	*inname;
	ftnlen	innamlen;
	char	*inacc;
	ftnlen	inacclen;
	char	*inseq;
	ftnlen	inseqlen;
	char 	*indir;
	ftnlen	indirlen;
	char	*infmt;
	ftnlen	infmtlen;
	char	*inform;
	ftnint	informlen;
	char	*inunf;
	ftnlen	inunflen;
	ftnint	*inrecl;
	ftnint	*innrec;
	char	*inblank;
	ftnlen	inblanklen;
} inlist;

#define VOID void

union Multitype {	/* for multiple entry points */
	integer1 g;
	shortint h;
	integer i;
	/* longint j; */
	real r;
	doublereal d;
	complex c;
	doublecomplex z;
	};

typedef union Multitype Multitype;

struct Vardesc {	/* for Namelist */
	char *name;
	char *addr;
	ftnlen *dims;
	int  type;
	};
typedef struct Vardesc Vardesc;

struct Namelist {
	char *name;
	Vardesc **vars;
	int nvars;
	};
typedef struct Namelist Namelist;

#define abs(x) ((x) >= 0 ? (x) : -(x))
#define dabs(x) (fabs(x))
#define f2cmin(a,b) ((a) <= (b) ? (a) : (b))
#define f2cmax(a,b) ((a) >= (b) ? (a) : (b))
#define dmin(a,b) (f2cmin(a,b))
#define dmax(a,b) (f2cmax(a,b))
#define bit_test(a,b)	((a) >> (b) & 1)
#define bit_clear(a,b)	((a) & ~((uinteger)1 << (b)))
#define bit_set(a,b)	((a) |  ((uinteger)1 << (b)))

#define abort_() { sig_die("Fortran abort routine called", 1); }
#define c_abs(z) (cabsf(Cf(z)))
#define c_cos(R,Z) { pCf(R)=ccos(Cf(Z)); }
#ifdef _MSC_VER
#define c_div(c, a, b) {Cf(c)._Val[0] = (Cf(a)._Val[0]/Cf(b)._Val[0]); Cf(c)._Val[1]=(Cf(a)._Val[1]/Cf(b)._Val[1]);}
#define z_div(c, a, b) {Cd(c)._Val[0] = (Cd(a)._Val[0]/Cd(b)._Val[0]); Cd(c)._Val[1]=(Cd(a)._Val[1]/df(b)._Val[1]);}
#else
#define c_div(c, a, b) {pCf(c) = Cf(a)/Cf(b);}
#define z_div(c, a, b) {pCd(c) = Cd(a)/Cd(b);}
#endif
#define c_exp(R, Z) {pCf(R) = cexpf(Cf(Z));}
#define c_log(R, Z) {pCf(R) = clogf(Cf(Z));}
#define c_sin(R, Z) {pCf(R) = csinf(Cf(Z));}
//#define c_sqrt(R, Z) {*(R) = csqrtf(Cf(Z));}
#define c_sqrt(R, Z) {pCf(R) = csqrtf(Cf(Z));}
#define d_abs(x) (fabs(*(x)))
#define d_acos(x) (acos(*(x)))
#define d_asin(x) (asin(*(x)))
#define d_atan(x) (atan(*(x)))
#define d_atn2(x, y) (atan2(*(x),*(y)))
#define d_cnjg(R, Z) { pCd(R) = conj(Cd(Z)); }
#define r_cnjg(R, Z) { pCf(R) = conjf(Cf(Z)); }
#define d_cos(x) (cos(*(x)))
#define d_cosh(x) (cosh(*(x)))
#define d_dim(__a, __b) ( *(__a) > *(__b) ? *(__a) - *(__b) : 0.0 )
#define d_exp(x) (exp(*(x)))
#define d_imag(z) (cimag(Cd(z)))
#define r_imag(z) (cimagf(Cf(z)))
#define d_int(__x) (*(__x)>0 ? floor(*(__x)) : -floor(- *(__x)))
#define r_int(__x) (*(__x)>0 ? floor(*(__x)) : -floor(- *(__x)))
#define d_lg10(x) ( 0.43429448190325182765 * log(*(x)) )
#define r_lg10(x) ( 0.43429448190325182765 * log(*(x)) )
#define d_log(x) (log(*(x)))
#define d_mod(x, y) (fmod(*(x), *(y)))
#define u_nint(__x) ((__x)>=0 ? floor((__x) + .5) : -floor(.5 - (__x)))
#define d_nint(x) u_nint(*(x))
#define u_sign(__a,__b) ((__b) >= 0 ? ((__a) >= 0 ? (__a) : -(__a)) : -((__a) >= 0 ? (__a) : -(__a)))
#define d_sign(a,b) u_sign(*(a),*(b))
#define r_sign(a,b) u_sign(*(a),*(b))
#define d_sin(x) (sin(*(x)))
#define d_sinh(x) (sinh(*(x)))
#define d_sqrt(x) (sqrt(*(x)))
#define d_tan(x) (tan(*(x)))
#define d_tanh(x) (tanh(*(x)))
#define i_abs(x) abs(*(x))
#define i_dnnt(x) ((integer)u_nint(*(x)))
#define i_len(s, n) (n)
#define i_nint(x) ((integer)u_nint(*(x)))
#define i_sign(a,b) ((integer)u_sign((integer)*(a),(integer)*(b)))
#define pow_dd(ap, bp) ( pow(*(ap), *(bp)))
#define pow_si(B,E) spow_ui(*(B),*(E))
#define pow_ri(B,E) spow_ui(*(B),*(E))
#define pow_di(B,E) dpow_ui(*(B),*(E))
#define pow_zi(p, a, b) {pCd(p) = zpow_ui(Cd(a), *(b));}
#define pow_ci(p, a, b) {pCf(p) = cpow_ui(Cf(a), *(b));}
#define pow_zz(R,A,B) {pCd(R) = cpow(Cd(A),*(B));}
#define s_cat(lpp, rpp, rnp, np, llp) { 	ftnlen i, nc, ll; char *f__rp, *lp; 	ll = (llp); lp = (lpp); 	for(i=0; i < (int)*(np); ++i) {         	nc = ll; 	        if((rnp)[i] < nc) nc = (rnp)[i]; 	        ll -= nc;         	f__rp = (rpp)[i]; 	        while(--nc >= 0) *lp++ = *(f__rp)++;         } 	while(--ll >= 0) *lp++ = ' '; }
#define s_cmp(a,b,c,d) ((integer)strncmp((a),(b),f2cmin((c),(d))))
#define s_copy(A,B,C,D) { int __i,__m; for (__i=0, __m=f2cmin((C),(D)); __i<__m && (B)[__i] != 0; ++__i) (A)[__i] = (B)[__i]; }
#define sig_die(s, kill) { exit(1); }
#define s_stop(s, n) {exit(0);}
static char junk[] = "\n@(#)LIBF77 VERSION 19990503\n";
#define z_abs(z) (cabs(Cd(z)))
#define z_exp(R, Z) {pCd(R) = cexp(Cd(Z));}
#define z_sqrt(R, Z) {pCd(R) = csqrt(Cd(Z));}
#define myexit_() break;
#define mycycle() continue;
#define myceiling(w) {ceil(w)}
#define myhuge(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc(w,s,e,n) {dmaxloc_(w,*(s),*(e),n)}

/* procedure parameter types for -A and -C++ */


#ifdef __cplusplus
typedef logical (*L_fp)(...);
#else
typedef logical (*L_fp)();
#endif

static float spow_ui(float x, integer n) {
	float pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
static double dpow_ui(double x, integer n) {
	double pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
#ifdef _MSC_VER
static _Fcomplex cpow_ui(complex x, integer n) {
	complex pow={1.0,0.0}; unsigned long int u;
		if(n != 0) {
		if(n < 0) n = -n, x.r = 1/x.r, x.i=1/x.i;
		for(u = n; ; ) {
			if(u & 01) pow.r *= x.r, pow.i *= x.i;
			if(u >>= 1) x.r *= x.r, x.i *= x.i;
			else break;
		}
	}
	_Fcomplex p={pow.r, pow.i};
	return p;
}
#else
static _Complex float cpow_ui(_Complex float x, integer n) {
	_Complex float pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
#endif
#ifdef _MSC_VER
static _Dcomplex zpow_ui(_Dcomplex x, integer n) {
	_Dcomplex pow={1.0,0.0}; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x._Val[0] = 1/x._Val[0], x._Val[1] =1/x._Val[1];
		for(u = n; ; ) {
			if(u & 01) pow._Val[0] *= x._Val[0], pow._Val[1] *= x._Val[1];
			if(u >>= 1) x._Val[0] *= x._Val[0], x._Val[1] *= x._Val[1];
			else break;
		}
	}
	_Dcomplex p = {pow._Val[0], pow._Val[1]};
	return p;
}
#else
static _Complex double zpow_ui(_Complex double x, integer n) {
	_Complex double pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
#endif
static integer pow_ii(integer x, integer n) {
	integer pow; unsigned long int u;
	if (n <= 0) {
		if (n == 0 || x == 1) pow = 1;
		else if (x != -1) pow = x == 0 ? 1/x : 0;
		else n = -n;
	}
	if ((n > 0) || !(n == 0 || x == 1 || x != -1)) {
		u = n;
		for(pow = 1; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
static integer dmaxloc_(double *w, integer s, integer e, integer *n)
{
	double m; integer i, mi;
	for(m=w[s-1], mi=s, i=s+1; i<=e; i++)
		if (w[i-1]>m) mi=i ,m=w[i-1];
	return mi-s+1;
}
static integer smaxloc_(float *w, integer s, integer e, integer *n)
{
	float m; integer i, mi;
	for(m=w[s-1], mi=s, i=s+1; i<=e; i++)
		if (w[i-1]>m) mi=i ,m=w[i-1];
	return mi-s+1;
}
static inline void cdotc_(complex *z, integer *n_, complex *x, integer *incx_, complex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Fcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conjf(Cf(&x[i]))._Val[0] * Cf(&y[i])._Val[0];
			zdotc._Val[1] += conjf(Cf(&x[i]))._Val[1] * Cf(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conjf(Cf(&x[i*incx]))._Val[0] * Cf(&y[i*incy])._Val[0];
			zdotc._Val[1] += conjf(Cf(&x[i*incx]))._Val[1] * Cf(&y[i*incy])._Val[1];
		}
	}
	pCf(z) = zdotc;
}
#else
	_Complex float zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conjf(Cf(&x[i])) * Cf(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conjf(Cf(&x[i*incx])) * Cf(&y[i*incy]);
		}
	}
	pCf(z) = zdotc;
}
#endif
static inline void zdotc_(doublecomplex *z, integer *n_, doublecomplex *x, integer *incx_, doublecomplex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Dcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conj(Cd(&x[i]))._Val[0] * Cd(&y[i])._Val[0];
			zdotc._Val[1] += conj(Cd(&x[i]))._Val[1] * Cd(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conj(Cd(&x[i*incx]))._Val[0] * Cd(&y[i*incy])._Val[0];
			zdotc._Val[1] += conj(Cd(&x[i*incx]))._Val[1] * Cd(&y[i*incy])._Val[1];
		}
	}
	pCd(z) = zdotc;
}
#else
	_Complex double zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conj(Cd(&x[i])) * Cd(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conj(Cd(&x[i*incx])) * Cd(&y[i*incy]);
		}
	}
	pCd(z) = zdotc;
}
#endif	
static inline void cdotu_(complex *z, integer *n_, complex *x, integer *incx_, complex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Fcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cf(&x[i])._Val[0] * Cf(&y[i])._Val[0];
			zdotc._Val[1] += Cf(&x[i])._Val[1] * Cf(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cf(&x[i*incx])._Val[0] * Cf(&y[i*incy])._Val[0];
			zdotc._Val[1] += Cf(&x[i*incx])._Val[1] * Cf(&y[i*incy])._Val[1];
		}
	}
	pCf(z) = zdotc;
}
#else
	_Complex float zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cf(&x[i]) * Cf(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cf(&x[i*incx]) * Cf(&y[i*incy]);
		}
	}
	pCf(z) = zdotc;
}
#endif
static inline void zdotu_(doublecomplex *z, integer *n_, doublecomplex *x, integer *incx_, doublecomplex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Dcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cd(&x[i])._Val[0] * Cd(&y[i])._Val[0];
			zdotc._Val[1] += Cd(&x[i])._Val[1] * Cd(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cd(&x[i*incx])._Val[0] * Cd(&y[i*incy])._Val[0];
			zdotc._Val[1] += Cd(&x[i*incx])._Val[1] * Cd(&y[i*incy])._Val[1];
		}
	}
	pCd(z) = zdotc;
}
#else
	_Complex double zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cd(&x[i]) * Cd(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cd(&x[i*incx]) * Cd(&y[i*incy]);
		}
	}
	pCd(z) = zdotc;
}
#endif
/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/



/* Table of constant values */

static doublereal c_b5 = -1.;
static integer c__1 = 1;
static doublereal c_b11 = 1.;
static doublereal c_b13 = 0.;
static integer c__0 = 0;

/* > \brief \b DLALS0 applies back multiplying factors in solving the least squares problem using divide and c
onquer SVD approach. Used by sgelsd. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DLALS0 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlals0.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlals0.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlals0.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DLALS0( ICOMPQ, NL, NR, SQRE, NRHS, B, LDB, BX, LDBX, */
/*                          PERM, GIVPTR, GIVCOL, LDGCOL, GIVNUM, LDGNUM, */
/*                          POLES, DIFL, DIFR, Z, K, C, S, WORK, INFO ) */

/*       INTEGER            GIVPTR, ICOMPQ, INFO, K, LDB, LDBX, LDGCOL, */
/*      $                   LDGNUM, NL, NR, NRHS, SQRE */
/*       DOUBLE PRECISION   C, S */
/*       INTEGER            GIVCOL( LDGCOL, * ), PERM( * ) */
/*       DOUBLE PRECISION   B( LDB, * ), BX( LDBX, * ), DIFL( * ), */
/*      $                   DIFR( LDGNUM, * ), GIVNUM( LDGNUM, * ), */
/*      $                   POLES( LDGNUM, * ), WORK( * ), Z( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DLALS0 applies back the multiplying factors of either the left or the */
/* > right singular vector matrix of a diagonal matrix appended by a row */
/* > to the right hand side matrix B in solving the least squares problem */
/* > using the divide-and-conquer SVD approach. */
/* > */
/* > For the left singular vector matrix, three types of orthogonal */
/* > matrices are involved: */
/* > */
/* > (1L) Givens rotations: the number of such rotations is GIVPTR; the */
/* >      pairs of columns/rows they were applied to are stored in GIVCOL; */
/* >      and the C- and S-values of these rotations are stored in GIVNUM. */
/* > */
/* > (2L) Permutation. The (NL+1)-st row of B is to be moved to the first */
/* >      row, and for J=2:N, PERM(J)-th row of B is to be moved to the */
/* >      J-th row. */
/* > */
/* > (3L) The left singular vector matrix of the remaining matrix. */
/* > */
/* > For the right singular vector matrix, four types of orthogonal */
/* > matrices are involved: */
/* > */
/* > (1R) The right singular vector matrix of the remaining matrix. */
/* > */
/* > (2R) If SQRE = 1, one extra Givens rotation to generate the right */
/* >      null space. */
/* > */
/* > (3R) The inverse transformation of (2L). */
/* > */
/* > (4R) The inverse transformation of (1L). */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] ICOMPQ */
/* > \verbatim */
/* >          ICOMPQ is INTEGER */
/* >         Specifies whether singular vectors are to be computed in */
/* >         factored form: */
/* >         = 0: Left singular vector matrix. */
/* >         = 1: Right singular vector matrix. */
/* > \endverbatim */
/* > */
/* > \param[in] NL */
/* > \verbatim */
/* >          NL is INTEGER */
/* >         The row dimension of the upper block. NL >= 1. */
/* > \endverbatim */
/* > */
/* > \param[in] NR */
/* > \verbatim */
/* >          NR is INTEGER */
/* >         The row dimension of the lower block. NR >= 1. */
/* > \endverbatim */
/* > */
/* > \param[in] SQRE */
/* > \verbatim */
/* >          SQRE is INTEGER */
/* >         = 0: the lower block is an NR-by-NR square matrix. */
/* >         = 1: the lower block is an NR-by-(NR+1) rectangular matrix. */
/* > */
/* >         The bidiagonal matrix has row dimension N = NL + NR + 1, */
/* >         and column dimension M = N + SQRE. */
/* > \endverbatim */
/* > */
/* > \param[in] NRHS */
/* > \verbatim */
/* >          NRHS is INTEGER */
/* >         The number of columns of B and BX. NRHS must be at least 1. */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is DOUBLE PRECISION array, dimension ( LDB, NRHS ) */
/* >         On input, B contains the right hand sides of the least */
/* >         squares problem in rows 1 through M. On output, B contains */
/* >         the solution X in rows 1 through N. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >         The leading dimension of B. LDB must be at least */
/* >         f2cmax(1,MAX( M, N ) ). */
/* > \endverbatim */
/* > */
/* > \param[out] BX */
/* > \verbatim */
/* >          BX is DOUBLE PRECISION array, dimension ( LDBX, NRHS ) */
/* > \endverbatim */
/* > */
/* > \param[in] LDBX */
/* > \verbatim */
/* >          LDBX is INTEGER */
/* >         The leading dimension of BX. */
/* > \endverbatim */
/* > */
/* > \param[in] PERM */
/* > \verbatim */
/* >          PERM is INTEGER array, dimension ( N ) */
/* >         The permutations (from deflation and sorting) applied */
/* >         to the two blocks. */
/* > \endverbatim */
/* > */
/* > \param[in] GIVPTR */
/* > \verbatim */
/* >          GIVPTR is INTEGER */
/* >         The number of Givens rotations which took place in this */
/* >         subproblem. */
/* > \endverbatim */
/* > */
/* > \param[in] GIVCOL */
/* > \verbatim */
/* >          GIVCOL is INTEGER array, dimension ( LDGCOL, 2 ) */
/* >         Each pair of numbers indicates a pair of rows/columns */
/* >         involved in a Givens rotation. */
/* > \endverbatim */
/* > */
/* > \param[in] LDGCOL */
/* > \verbatim */
/* >          LDGCOL is INTEGER */
/* >         The leading dimension of GIVCOL, must be at least N. */
/* > \endverbatim */
/* > */
/* > \param[in] GIVNUM */
/* > \verbatim */
/* >          GIVNUM is DOUBLE PRECISION array, dimension ( LDGNUM, 2 ) */
/* >         Each number indicates the C or S value used in the */
/* >         corresponding Givens rotation. */
/* > \endverbatim */
/* > */
/* > \param[in] LDGNUM */
/* > \verbatim */
/* >          LDGNUM is INTEGER */
/* >         The leading dimension of arrays DIFR, POLES and */
/* >         GIVNUM, must be at least K. */
/* > \endverbatim */
/* > */
/* > \param[in] POLES */
/* > \verbatim */
/* >          POLES is DOUBLE PRECISION array, dimension ( LDGNUM, 2 ) */
/* >         On entry, POLES(1:K, 1) contains the new singular */
/* >         values obtained from solving the secular equation, and */
/* >         POLES(1:K, 2) is an array containing the poles in the secular */
/* >         equation. */
/* > \endverbatim */
/* > */
/* > \param[in] DIFL */
/* > \verbatim */
/* >          DIFL is DOUBLE PRECISION array, dimension ( K ). */
/* >         On entry, DIFL(I) is the distance between I-th updated */
/* >         (undeflated) singular value and the I-th (undeflated) old */
/* >         singular value. */
/* > \endverbatim */
/* > */
/* > \param[in] DIFR */
/* > \verbatim */
/* >          DIFR is DOUBLE PRECISION array, dimension ( LDGNUM, 2 ). */
/* >         On entry, DIFR(I, 1) contains the distances between I-th */
/* >         updated (undeflated) singular value and the I+1-th */
/* >         (undeflated) old singular value. And DIFR(I, 2) is the */
/* >         normalizing factor for the I-th right singular vector. */
/* > \endverbatim */
/* > */
/* > \param[in] Z */
/* > \verbatim */
/* >          Z is DOUBLE PRECISION array, dimension ( K ) */
/* >         Contain the components of the deflation-adjusted updating row */
/* >         vector. */
/* > \endverbatim */
/* > */
/* > \param[in] K */
/* > \verbatim */
/* >          K is INTEGER */
/* >         Contains the dimension of the non-deflated matrix, */
/* >         This is the order of the related secular equation. 1 <= K <=N. */
/* > \endverbatim */
/* > */
/* > \param[in] C */
/* > \verbatim */
/* >          C is DOUBLE PRECISION */
/* >         C contains garbage if SQRE =0 and the C-value of a Givens */
/* >         rotation related to the right null space if SQRE = 1. */
/* > \endverbatim */
/* > */
/* > \param[in] S */
/* > \verbatim */
/* >          S is DOUBLE PRECISION */
/* >         S contains garbage if SQRE =0 and the S-value of a Givens */
/* >         rotation related to the right null space if SQRE = 1. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension ( K ) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleOTHERcomputational */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >     Ming Gu and Ren-Cang Li, Computer Science Division, University of */
/* >       California at Berkeley, USA \n */
/* >     Osni Marques, LBNL/NERSC, USA \n */

/*  ===================================================================== */
/* Subroutine */ void dlals0_(integer *icompq, integer *nl, integer *nr, 
	integer *sqre, integer *nrhs, doublereal *b, integer *ldb, doublereal 
	*bx, integer *ldbx, integer *perm, integer *givptr, integer *givcol, 
	integer *ldgcol, doublereal *givnum, integer *ldgnum, doublereal *
	poles, doublereal *difl, doublereal *difr, doublereal *z__, integer *
	k, doublereal *c__, doublereal *s, doublereal *work, integer *info)
{
    /* System generated locals */
    integer givcol_dim1, givcol_offset, b_dim1, b_offset, bx_dim1, bx_offset, 
	    difr_dim1, difr_offset, givnum_dim1, givnum_offset, poles_dim1, 
	    poles_offset, i__1, i__2;
    doublereal d__1;

    /* Local variables */
    doublereal temp;
    extern /* Subroutine */ void drot_(integer *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *);
    extern doublereal dnrm2_(integer *, doublereal *, integer *);
    integer i__, j, m, n;
    extern /* Subroutine */ void dscal_(integer *, doublereal *, doublereal *, 
	    integer *);
    doublereal diflj, difrj, dsigj;
    extern /* Subroutine */ void dgemv_(char *, integer *, integer *, 
	    doublereal *, doublereal *, integer *, doublereal *, integer *, 
	    doublereal *, doublereal *, integer *), dcopy_(integer *, 
	    doublereal *, integer *, doublereal *, integer *);
    extern doublereal dlamc3_(doublereal *, doublereal *);
    doublereal dj;
    extern /* Subroutine */ void dlascl_(char *, integer *, integer *, 
	    doublereal *, doublereal *, integer *, integer *, doublereal *, 
	    integer *, integer *), dlacpy_(char *, integer *, integer 
	    *, doublereal *, integer *, doublereal *, integer *); 
    extern int xerbla_(char *, integer *, ftnlen);
    doublereal dsigjp;
    integer nlp1;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input parameters. */

    /* Parameter adjustments */
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    bx_dim1 = *ldbx;
    bx_offset = 1 + bx_dim1 * 1;
    bx -= bx_offset;
    --perm;
    givcol_dim1 = *ldgcol;
    givcol_offset = 1 + givcol_dim1 * 1;
    givcol -= givcol_offset;
    difr_dim1 = *ldgnum;
    difr_offset = 1 + difr_dim1 * 1;
    difr -= difr_offset;
    poles_dim1 = *ldgnum;
    poles_offset = 1 + poles_dim1 * 1;
    poles -= poles_offset;
    givnum_dim1 = *ldgnum;
    givnum_offset = 1 + givnum_dim1 * 1;
    givnum -= givnum_offset;
    --difl;
    --z__;
    --work;

    /* Function Body */
    *info = 0;
    n = *nl + *nr + 1;

    if (*icompq < 0 || *icompq > 1) {
	*info = -1;
    } else if (*nl < 1) {
	*info = -2;
    } else if (*nr < 1) {
	*info = -3;
    } else if (*sqre < 0 || *sqre > 1) {
	*info = -4;
    } else if (*nrhs < 1) {
	*info = -5;
    } else if (*ldb < n) {
	*info = -7;
    } else if (*ldbx < n) {
	*info = -9;
    } else if (*givptr < 0) {
	*info = -11;
    } else if (*ldgcol < n) {
	*info = -13;
    } else if (*ldgnum < n) {
	*info = -15;
    //} else if (*k < 1) {
    } else if (*k < 0) {
	*info = -20;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DLALS0", &i__1, (ftnlen)6);
	return;
    }

    m = n + *sqre;
    nlp1 = *nl + 1;

    if (*icompq == 0) {

/*        Apply back orthogonal transformations from the left. */

/*        Step (1L): apply back the Givens rotations performed. */

	i__1 = *givptr;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    drot_(nrhs, &b[givcol[i__ + (givcol_dim1 << 1)] + b_dim1], ldb, &
		    b[givcol[i__ + givcol_dim1] + b_dim1], ldb, &givnum[i__ + 
		    (givnum_dim1 << 1)], &givnum[i__ + givnum_dim1]);
/* L10: */
	}

/*        Step (2L): permute rows of B. */

	dcopy_(nrhs, &b[nlp1 + b_dim1], ldb, &bx[bx_dim1 + 1], ldbx);
	i__1 = n;
	for (i__ = 2; i__ <= i__1; ++i__) {
	    dcopy_(nrhs, &b[perm[i__] + b_dim1], ldb, &bx[i__ + bx_dim1], 
		    ldbx);
/* L20: */
	}

/*        Step (3L): apply the inverse of the left singular vector */
/*        matrix to BX. */

	if (*k == 1) {
	    dcopy_(nrhs, &bx[bx_offset], ldbx, &b[b_offset], ldb);
	    if (z__[1] < 0.) {
		dscal_(nrhs, &c_b5, &b[b_offset], ldb);
	    }
	} else {
	    i__1 = *k;
	    for (j = 1; j <= i__1; ++j) {
		diflj = difl[j];
		dj = poles[j + poles_dim1];
		dsigj = -poles[j + (poles_dim1 << 1)];
		if (j < *k) {
		    difrj = -difr[j + difr_dim1];
		    dsigjp = -poles[j + 1 + (poles_dim1 << 1)];
		}
		if (z__[j] == 0. || poles[j + (poles_dim1 << 1)] == 0.) {
		    work[j] = 0.;
		} else {
		    work[j] = -poles[j + (poles_dim1 << 1)] * z__[j] / diflj /
			     (poles[j + (poles_dim1 << 1)] + dj);
		}
		i__2 = j - 1;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    if (z__[i__] == 0. || poles[i__ + (poles_dim1 << 1)] == 
			    0.) {
			work[i__] = 0.;
		    } else {
			work[i__] = poles[i__ + (poles_dim1 << 1)] * z__[i__] 
				/ (dlamc3_(&poles[i__ + (poles_dim1 << 1)], &
				dsigj) - diflj) / (poles[i__ + (poles_dim1 << 
				1)] + dj);
		    }
/* L30: */
		}
		i__2 = *k;
		for (i__ = j + 1; i__ <= i__2; ++i__) {
		    if (z__[i__] == 0. || poles[i__ + (poles_dim1 << 1)] == 
			    0.) {
			work[i__] = 0.;
		    } else {
			work[i__] = poles[i__ + (poles_dim1 << 1)] * z__[i__] 
				/ (dlamc3_(&poles[i__ + (poles_dim1 << 1)], &
				dsigjp) + difrj) / (poles[i__ + (poles_dim1 <<
				 1)] + dj);
		    }
/* L40: */
		}
		work[1] = -1.;
		temp = dnrm2_(k, &work[1], &c__1);
		dgemv_("T", k, nrhs, &c_b11, &bx[bx_offset], ldbx, &work[1], &
			c__1, &c_b13, &b[j + b_dim1], ldb);
		dlascl_("G", &c__0, &c__0, &temp, &c_b11, &c__1, nrhs, &b[j + 
			b_dim1], ldb, info);
/* L50: */
	    }
	}

/*        Move the deflated rows of BX to B also. */

	if (*k < f2cmax(m,n)) {
	    i__1 = n - *k;
	    dlacpy_("A", &i__1, nrhs, &bx[*k + 1 + bx_dim1], ldbx, &b[*k + 1 
		    + b_dim1], ldb);
	}
    } else {

/*        Apply back the right orthogonal transformations. */

/*        Step (1R): apply back the new right singular vector matrix */
/*        to B. */

	if (*k == 1) {
	    dcopy_(nrhs, &b[b_offset], ldb, &bx[bx_offset], ldbx);
	} else {
	    i__1 = *k;
	    for (j = 1; j <= i__1; ++j) {
		dsigj = poles[j + (poles_dim1 << 1)];
		if (z__[j] == 0.) {
		    work[j] = 0.;
		} else {
		    work[j] = -z__[j] / difl[j] / (dsigj + poles[j + 
			    poles_dim1]) / difr[j + (difr_dim1 << 1)];
		}
		i__2 = j - 1;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    if (z__[j] == 0.) {
			work[i__] = 0.;
		    } else {
			d__1 = -poles[i__ + 1 + (poles_dim1 << 1)];
			work[i__] = z__[j] / (dlamc3_(&dsigj, &d__1) - difr[
				i__ + difr_dim1]) / (dsigj + poles[i__ + 
				poles_dim1]) / difr[i__ + (difr_dim1 << 1)];
		    }
/* L60: */
		}
		i__2 = *k;
		for (i__ = j + 1; i__ <= i__2; ++i__) {
		    if (z__[j] == 0.) {
			work[i__] = 0.;
		    } else {
			d__1 = -poles[i__ + (poles_dim1 << 1)];
			work[i__] = z__[j] / (dlamc3_(&dsigj, &d__1) - difl[
				i__]) / (dsigj + poles[i__ + poles_dim1]) / 
				difr[i__ + (difr_dim1 << 1)];
		    }
/* L70: */
		}
		dgemv_("T", k, nrhs, &c_b11, &b[b_offset], ldb, &work[1], &
			c__1, &c_b13, &bx[j + bx_dim1], ldbx);
/* L80: */
	    }
	}

/*        Step (2R): if SQRE = 1, apply back the rotation that is */
/*        related to the right null space of the subproblem. */

	if (*sqre == 1) {
	    dcopy_(nrhs, &b[m + b_dim1], ldb, &bx[m + bx_dim1], ldbx);
	    drot_(nrhs, &bx[bx_dim1 + 1], ldbx, &bx[m + bx_dim1], ldbx, c__, 
		    s);
	}
	if (*k < f2cmax(m,n)) {
	    i__1 = n - *k;
	    dlacpy_("A", &i__1, nrhs, &b[*k + 1 + b_dim1], ldb, &bx[*k + 1 + 
		    bx_dim1], ldbx);
	}

/*        Step (3R): permute rows of B. */

	dcopy_(nrhs, &bx[bx_dim1 + 1], ldbx, &b[nlp1 + b_dim1], ldb);
	if (*sqre == 1) {
	    dcopy_(nrhs, &bx[m + bx_dim1], ldbx, &b[m + b_dim1], ldb);
	}
	i__1 = n;
	for (i__ = 2; i__ <= i__1; ++i__) {
	    dcopy_(nrhs, &bx[i__ + bx_dim1], ldbx, &b[perm[i__] + b_dim1], 
		    ldb);
/* L90: */
	}

/*        Step (4R): apply back the Givens rotations performed. */

	for (i__ = *givptr; i__ >= 1; --i__) {
	    d__1 = -givnum[i__ + givnum_dim1];
	    drot_(nrhs, &b[givcol[i__ + (givcol_dim1 << 1)] + b_dim1], ldb, &
		    b[givcol[i__ + givcol_dim1] + b_dim1], ldb, &givnum[i__ + 
		    (givnum_dim1 << 1)], &d__1);
/* L100: */
	}
    }

    return;

/*     End of DLALS0 */

} /* dlals0_ */

