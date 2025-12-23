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

static integer c__1 = 1;
static real c_b34 = 0.f;
static real c_b35 = 1.f;
static integer c__0 = 0;
static integer c_n1 = -1;

/* > \brief \b SGEJSV */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SGEJSV + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgejsv.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgejsv.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgejsv.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SGEJSV( JOBA, JOBU, JOBV, JOBR, JOBT, JOBP, */
/*                          M, N, A, LDA, SVA, U, LDU, V, LDV, */
/*                          WORK, LWORK, IWORK, INFO ) */

/*       IMPLICIT    NONE */
/*       INTEGER     INFO, LDA, LDU, LDV, LWORK, M, N */
/*       REAL        A( LDA, * ), SVA( N ), U( LDU, * ), V( LDV, * ), */
/*      $            WORK( LWORK ) */
/*       INTEGER     IWORK( * ) */
/*       CHARACTER*1 JOBA, JOBP, JOBR, JOBT, JOBU, JOBV */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SGEJSV computes the singular value decomposition (SVD) of a real M-by-N */
/* > matrix [A], where M >= N. The SVD of [A] is written as */
/* > */
/* >              [A] = [U] * [SIGMA] * [V]^t, */
/* > */
/* > where [SIGMA] is an N-by-N (M-by-N) matrix which is zero except for its N */
/* > diagonal elements, [U] is an M-by-N (or M-by-M) orthonormal matrix, and */
/* > [V] is an N-by-N orthogonal matrix. The diagonal elements of [SIGMA] are */
/* > the singular values of [A]. The columns of [U] and [V] are the left and */
/* > the right singular vectors of [A], respectively. The matrices [U] and [V] */
/* > are computed and stored in the arrays U and V, respectively. The diagonal */
/* > of [SIGMA] is computed and stored in the array SVA. */
/* > SGEJSV can sometimes compute tiny singular values and their singular vectors much */
/* > more accurately than other SVD routines, see below under Further Details. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOBA */
/* > \verbatim */
/* >          JOBA is CHARACTER*1 */
/* >         Specifies the level of accuracy: */
/* >       = 'C': This option works well (high relative accuracy) if A = B * D, */
/* >              with well-conditioned B and arbitrary diagonal matrix D. */
/* >              The accuracy cannot be spoiled by COLUMN scaling. The */
/* >              accuracy of the computed output depends on the condition of */
/* >              B, and the procedure aims at the best theoretical accuracy. */
/* >              The relative error max_{i=1:N}|d sigma_i| / sigma_i is */
/* >              bounded by f(M,N)*epsilon* cond(B), independent of D. */
/* >              The input matrix is preprocessed with the QRF with column */
/* >              pivoting. This initial preprocessing and preconditioning by */
/* >              a rank revealing QR factorization is common for all values of */
/* >              JOBA. Additional actions are specified as follows: */
/* >       = 'E': Computation as with 'C' with an additional estimate of the */
/* >              condition number of B. It provides a realistic error bound. */
/* >       = 'F': If A = D1 * C * D2 with ill-conditioned diagonal scalings */
/* >              D1, D2, and well-conditioned matrix C, this option gives */
/* >              higher accuracy than the 'C' option. If the structure of the */
/* >              input matrix is not known, and relative accuracy is */
/* >              desirable, then this option is advisable. The input matrix A */
/* >              is preprocessed with QR factorization with FULL (row and */
/* >              column) pivoting. */
/* >       = 'G': Computation as with 'F' with an additional estimate of the */
/* >              condition number of B, where A=D*B. If A has heavily weighted */
/* >              rows, then using this condition number gives too pessimistic */
/* >              error bound. */
/* >       = 'A': Small singular values are the noise and the matrix is treated */
/* >              as numerically rank deficient. The error in the computed */
/* >              singular values is bounded by f(m,n)*epsilon*||A||. */
/* >              The computed SVD A = U * S * V^t restores A up to */
/* >              f(m,n)*epsilon*||A||. */
/* >              This gives the procedure the licence to discard (set to zero) */
/* >              all singular values below N*epsilon*||A||. */
/* >       = 'R': Similar as in 'A'. Rank revealing property of the initial */
/* >              QR factorization is used do reveal (using triangular factor) */
/* >              a gap sigma_{r+1} < epsilon * sigma_r in which case the */
/* >              numerical RANK is declared to be r. The SVD is computed with */
/* >              absolute error bounds, but more accurately than with 'A'. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBU */
/* > \verbatim */
/* >          JOBU is CHARACTER*1 */
/* >         Specifies whether to compute the columns of U: */
/* >       = 'U': N columns of U are returned in the array U. */
/* >       = 'F': full set of M left sing. vectors is returned in the array U. */
/* >       = 'W': U may be used as workspace of length M*N. See the description */
/* >              of U. */
/* >       = 'N': U is not computed. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBV */
/* > \verbatim */
/* >          JOBV is CHARACTER*1 */
/* >         Specifies whether to compute the matrix V: */
/* >       = 'V': N columns of V are returned in the array V; Jacobi rotations */
/* >              are not explicitly accumulated. */
/* >       = 'J': N columns of V are returned in the array V, but they are */
/* >              computed as the product of Jacobi rotations. This option is */
/* >              allowed only if JOBU .NE. 'N', i.e. in computing the full SVD. */
/* >       = 'W': V may be used as workspace of length N*N. See the description */
/* >              of V. */
/* >       = 'N': V is not computed. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBR */
/* > \verbatim */
/* >          JOBR is CHARACTER*1 */
/* >         Specifies the RANGE for the singular values. Issues the licence to */
/* >         set to zero small positive singular values if they are outside */
/* >         specified range. If A .NE. 0 is scaled so that the largest singular */
/* >         value of c*A is around SQRT(BIG), BIG=SLAMCH('O'), then JOBR issues */
/* >         the licence to kill columns of A whose norm in c*A is less than */
/* >         SQRT(SFMIN) (for JOBR = 'R'), or less than SMALL=SFMIN/EPSLN, */
/* >         where SFMIN=SLAMCH('S'), EPSLN=SLAMCH('E'). */
/* >       = 'N': Do not kill small columns of c*A. This option assumes that */
/* >              BLAS and QR factorizations and triangular solvers are */
/* >              implemented to work in that range. If the condition of A */
/* >              is greater than BIG, use SGESVJ. */
/* >       = 'R': RESTRICTED range for sigma(c*A) is [SQRT(SFMIN), SQRT(BIG)] */
/* >              (roughly, as described above). This option is recommended. */
/* >                                             =========================== */
/* >         For computing the singular values in the FULL range [SFMIN,BIG] */
/* >         use SGESVJ. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBT */
/* > \verbatim */
/* >          JOBT is CHARACTER*1 */
/* >         If the matrix is square then the procedure may determine to use */
/* >         transposed A if A^t seems to be better with respect to convergence. */
/* >         If the matrix is not square, JOBT is ignored. This is subject to */
/* >         changes in the future. */
/* >         The decision is based on two values of entropy over the adjoint */
/* >         orbit of A^t * A. See the descriptions of WORK(6) and WORK(7). */
/* >       = 'T': transpose if entropy test indicates possibly faster */
/* >         convergence of Jacobi process if A^t is taken as input. If A is */
/* >         replaced with A^t, then the row pivoting is included automatically. */
/* >       = 'N': do not speculate. */
/* >         This option can be used to compute only the singular values, or the */
/* >         full SVD (U, SIGMA and V). For only one set of singular vectors */
/* >         (U or V), the caller should provide both U and V, as one of the */
/* >         matrices is used as workspace if the matrix A is transposed. */
/* >         The implementer can easily remove this constraint and make the */
/* >         code more complicated. See the descriptions of U and V. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBP */
/* > \verbatim */
/* >          JOBP is CHARACTER*1 */
/* >         Issues the licence to introduce structured perturbations to drown */
/* >         denormalized numbers. This licence should be active if the */
/* >         denormals are poorly implemented, causing slow computation, */
/* >         especially in cases of fast convergence (!). For details see [1,2]. */
/* >         For the sake of simplicity, this perturbations are included only */
/* >         when the full SVD or only the singular values are requested. The */
/* >         implementer/user can easily add the perturbation for the cases of */
/* >         computing one set of singular vectors. */
/* >       = 'P': introduce perturbation */
/* >       = 'N': do not perturb */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >         The number of rows of the input matrix A.  M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >         The number of columns of the input matrix A. M >= N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is REAL array, dimension (LDA,N) */
/* >          On entry, the M-by-N matrix A. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[out] SVA */
/* > \verbatim */
/* >          SVA is REAL array, dimension (N) */
/* >          On exit, */
/* >          - For WORK(1)/WORK(2) = ONE: The singular values of A. During the */
/* >            computation SVA contains Euclidean column norms of the */
/* >            iterated matrices in the array A. */
/* >          - For WORK(1) .NE. WORK(2): The singular values of A are */
/* >            (WORK(1)/WORK(2)) * SVA(1:N). This factored form is used if */
/* >            sigma_max(A) overflows or if small singular values have been */
/* >            saved from underflow by scaling the input matrix A. */
/* >          - If JOBR='R' then some of the singular values may be returned */
/* >            as exact zeros obtained by "set to zero" because they are */
/* >            below the numerical rank threshold or are denormalized numbers. */
/* > \endverbatim */
/* > */
/* > \param[out] U */
/* > \verbatim */
/* >          U is REAL array, dimension ( LDU, N ) */
/* >          If JOBU = 'U', then U contains on exit the M-by-N matrix of */
/* >                         the left singular vectors. */
/* >          If JOBU = 'F', then U contains on exit the M-by-M matrix of */
/* >                         the left singular vectors, including an ONB */
/* >                         of the orthogonal complement of the Range(A). */
/* >          If JOBU = 'W'  .AND. (JOBV = 'V' .AND. JOBT = 'T' .AND. M = N), */
/* >                         then U is used as workspace if the procedure */
/* >                         replaces A with A^t. In that case, [V] is computed */
/* >                         in U as left singular vectors of A^t and then */
/* >                         copied back to the V array. This 'W' option is just */
/* >                         a reminder to the caller that in this case U is */
/* >                         reserved as workspace of length N*N. */
/* >          If JOBU = 'N'  U is not referenced, unless JOBT='T'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU */
/* > \verbatim */
/* >          LDU is INTEGER */
/* >          The leading dimension of the array U,  LDU >= 1. */
/* >          IF  JOBU = 'U' or 'F' or 'W',  then LDU >= M. */
/* > \endverbatim */
/* > */
/* > \param[out] V */
/* > \verbatim */
/* >          V is REAL array, dimension ( LDV, N ) */
/* >          If JOBV = 'V', 'J' then V contains on exit the N-by-N matrix of */
/* >                         the right singular vectors; */
/* >          If JOBV = 'W', AND (JOBU = 'U' AND JOBT = 'T' AND M = N), */
/* >                         then V is used as workspace if the pprocedure */
/* >                         replaces A with A^t. In that case, [U] is computed */
/* >                         in V as right singular vectors of A^t and then */
/* >                         copied back to the U array. This 'W' option is just */
/* >                         a reminder to the caller that in this case V is */
/* >                         reserved as workspace of length N*N. */
/* >          If JOBV = 'N'  V is not referenced, unless JOBT='T'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDV */
/* > \verbatim */
/* >          LDV is INTEGER */
/* >          The leading dimension of the array V,  LDV >= 1. */
/* >          If JOBV = 'V' or 'J' or 'W', then LDV >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (LWORK) */
/* >          On exit, */
/* >          WORK(1) = SCALE = WORK(2) / WORK(1) is the scaling factor such */
/* >                    that SCALE*SVA(1:N) are the computed singular values */
/* >                    of A. (See the description of SVA().) */
/* >          WORK(2) = See the description of WORK(1). */
/* >          WORK(3) = SCONDA is an estimate for the condition number of */
/* >                    column equilibrated A. (If JOBA = 'E' or 'G') */
/* >                    SCONDA is an estimate of SQRT(||(R^t * R)^(-1)||_1). */
/* >                    It is computed using SPOCON. It holds */
/* >                    N^(-1/4) * SCONDA <= ||R^(-1)||_2 <= N^(1/4) * SCONDA */
/* >                    where R is the triangular factor from the QRF of A. */
/* >                    However, if R is truncated and the numerical rank is */
/* >                    determined to be strictly smaller than N, SCONDA is */
/* >                    returned as -1, thus indicating that the smallest */
/* >                    singular values might be lost. */
/* > */
/* >          If full SVD is needed, the following two condition numbers are */
/* >          useful for the analysis of the algorithm. They are provied for */
/* >          a developer/implementer who is familiar with the details of */
/* >          the method. */
/* > */
/* >          WORK(4) = an estimate of the scaled condition number of the */
/* >                    triangular factor in the first QR factorization. */
/* >          WORK(5) = an estimate of the scaled condition number of the */
/* >                    triangular factor in the second QR factorization. */
/* >          The following two parameters are computed if JOBT = 'T'. */
/* >          They are provided for a developer/implementer who is familiar */
/* >          with the details of the method. */
/* > */
/* >          WORK(6) = the entropy of A^t*A :: this is the Shannon entropy */
/* >                    of diag(A^t*A) / Trace(A^t*A) taken as point in the */
/* >                    probability simplex. */
/* >          WORK(7) = the entropy of A*A^t. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          Length of WORK to confirm proper allocation of work space. */
/* >          LWORK depends on the job: */
/* > */
/* >          If only SIGMA is needed ( JOBU = 'N', JOBV = 'N' ) and */
/* >            -> .. no scaled condition estimate required (JOBE = 'N'): */
/* >               LWORK >= f2cmax(2*M+N,4*N+1,7). This is the minimal requirement. */
/* >               ->> For optimal performance (blocked code) the optimal value */
/* >               is LWORK >= f2cmax(2*M+N,3*N+(N+1)*NB,7). Here NB is the optimal */
/* >               block size for DGEQP3 and DGEQRF. */
/* >               In general, optimal LWORK is computed as */
/* >               LWORK >= f2cmax(2*M+N,N+LWORK(DGEQP3),N+LWORK(DGEQRF), 7). */
/* >            -> .. an estimate of the scaled condition number of A is */
/* >               required (JOBA='E', 'G'). In this case, LWORK is the maximum */
/* >               of the above and N*N+4*N, i.e. LWORK >= f2cmax(2*M+N,N*N+4*N,7). */
/* >               ->> For optimal performance (blocked code) the optimal value */
/* >               is LWORK >= f2cmax(2*M+N,3*N+(N+1)*NB, N*N+4*N, 7). */
/* >               In general, the optimal length LWORK is computed as */
/* >               LWORK >= f2cmax(2*M+N,N+LWORK(DGEQP3),N+LWORK(DGEQRF), */
/* >                                                     N+N*N+LWORK(DPOCON),7). */
/* > */
/* >          If SIGMA and the right singular vectors are needed (JOBV = 'V'), */
/* >            -> the minimal requirement is LWORK >= f2cmax(2*M+N,4*N+1,7). */
/* >            -> For optimal performance, LWORK >= f2cmax(2*M+N,3*N+(N+1)*NB,7), */
/* >               where NB is the optimal block size for DGEQP3, DGEQRF, DGELQ, */
/* >               DORMLQ. In general, the optimal length LWORK is computed as */
/* >               LWORK >= f2cmax(2*M+N,N+LWORK(DGEQP3), N+LWORK(DPOCON), */
/* >                       N+LWORK(DGELQ), 2*N+LWORK(DGEQRF), N+LWORK(DORMLQ)). */
/* > */
/* >          If SIGMA and the left singular vectors are needed */
/* >            -> the minimal requirement is LWORK >= f2cmax(2*M+N,4*N+1,7). */
/* >            -> For optimal performance: */
/* >               if JOBU = 'U' :: LWORK >= f2cmax(2*M+N,3*N+(N+1)*NB,7), */
/* >               if JOBU = 'F' :: LWORK >= f2cmax(2*M+N,3*N+(N+1)*NB,N+M*NB,7), */
/* >               where NB is the optimal block size for DGEQP3, DGEQRF, DORMQR. */
/* >               In general, the optimal length LWORK is computed as */
/* >               LWORK >= f2cmax(2*M+N,N+LWORK(DGEQP3),N+LWORK(DPOCON), */
/* >                        2*N+LWORK(DGEQRF), N+LWORK(DORMQR)). */
/* >               Here LWORK(DORMQR) equals N*NB (for JOBU = 'U') or */
/* >               M*NB (for JOBU = 'F'). */
/* > */
/* >          If the full SVD is needed: (JOBU = 'U' or JOBU = 'F') and */
/* >            -> if JOBV = 'V' */
/* >               the minimal requirement is LWORK >= f2cmax(2*M+N,6*N+2*N*N). */
/* >            -> if JOBV = 'J' the minimal requirement is */
/* >               LWORK >= f2cmax(2*M+N, 4*N+N*N,2*N+N*N+6). */
/* >            -> For optimal performance, LWORK should be additionally */
/* >               larger than N+M*NB, where NB is the optimal block size */
/* >               for DORMQR. */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (M+3*N). */
/* >          On exit, */
/* >          IWORK(1) = the numerical rank determined after the initial */
/* >                     QR factorization with pivoting. See the descriptions */
/* >                     of JOBA and JOBR. */
/* >          IWORK(2) = the number of the computed nonzero singular values */
/* >          IWORK(3) = if nonzero, a warning message: */
/* >                     If IWORK(3) = 1 then some of the column norms of A */
/* >                     were denormalized floats. The requested high accuracy */
/* >                     is not warranted by the data. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >           < 0:  if INFO = -i, then the i-th argument had an illegal value. */
/* >           = 0:  successful exit; */
/* >           > 0:  SGEJSV  did not converge in the maximal allowed number */
/* >                 of sweeps. The computed values may be inaccurate. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup realGEsing */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  SGEJSV implements a preconditioned Jacobi SVD algorithm. It uses SGEQP3, */
/* >  SGEQRF, and SGELQF as preprocessors and preconditioners. Optionally, an */
/* >  additional row pivoting can be used as a preprocessor, which in some */
/* >  cases results in much higher accuracy. An example is matrix A with the */
/* >  structure A = D1 * C * D2, where D1, D2 are arbitrarily ill-conditioned */
/* >  diagonal matrices and C is well-conditioned matrix. In that case, complete */
/* >  pivoting in the first QR factorizations provides accuracy dependent on the */
/* >  condition number of C, and independent of D1, D2. Such higher accuracy is */
/* >  not completely understood theoretically, but it works well in practice. */
/* >  Further, if A can be written as A = B*D, with well-conditioned B and some */
/* >  diagonal D, then the high accuracy is guaranteed, both theoretically and */
/* >  in software, independent of D. For more details see [1], [2]. */
/* >     The computational range for the singular values can be the full range */
/* >  ( UNDERFLOW,OVERFLOW ), provided that the machine arithmetic and the BLAS */
/* >  & LAPACK routines called by SGEJSV are implemented to work in that range. */
/* >  If that is not the case, then the restriction for safe computation with */
/* >  the singular values in the range of normalized IEEE numbers is that the */
/* >  spectral condition number kappa(A)=sigma_max(A)/sigma_min(A) does not */
/* >  overflow. This code (SGEJSV) is best used in this restricted range, */
/* >  meaning that singular values of magnitude below ||A||_2 / SLAMCH('O') are */
/* >  returned as zeros. See JOBR for details on this. */
/* >     Further, this implementation is somewhat slower than the one described */
/* >  in [1,2] due to replacement of some non-LAPACK components, and because */
/* >  the choice of some tuning parameters in the iterative part (SGESVJ) is */
/* >  left to the implementer on a particular machine. */
/* >     The rank revealing QR factorization (in this code: SGEQP3) should be */
/* >  implemented as in [3]. We have a new version of SGEQP3 under development */
/* >  that is more robust than the current one in LAPACK, with a cleaner cut in */
/* >  rank deficient cases. It will be available in the SIGMA library [4]. */
/* >  If M is much larger than N, it is obvious that the initial QRF with */
/* >  column pivoting can be preprocessed by the QRF without pivoting. That */
/* >  well known trick is not used in SGEJSV because in some cases heavy row */
/* >  weighting can be treated with complete pivoting. The overhead in cases */
/* >  M much larger than N is then only due to pivoting, but the benefits in */
/* >  terms of accuracy have prevailed. The implementer/user can incorporate */
/* >  this extra QRF step easily. The implementer can also improve data movement */
/* >  (matrix transpose, matrix copy, matrix transposed copy) - this */
/* >  implementation of SGEJSV uses only the simplest, naive data movement. */
/* > \endverbatim */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >  Zlatko Drmac (Zagreb, Croatia) and Kresimir Veselic (Hagen, Germany) */

/* > \par References: */
/*  ================ */
/* > */
/* > \verbatim */
/* > */
/* > [1] Z. Drmac and K. Veselic: New fast and accurate Jacobi SVD algorithm I. */
/* >     SIAM J. Matrix Anal. Appl. Vol. 35, No. 2 (2008), pp. 1322-1342. */
/* >     LAPACK Working note 169. */
/* > [2] Z. Drmac and K. Veselic: New fast and accurate Jacobi SVD algorithm II. */
/* >     SIAM J. Matrix Anal. Appl. Vol. 35, No. 2 (2008), pp. 1343-1362. */
/* >     LAPACK Working note 170. */
/* > [3] Z. Drmac and Z. Bujanovic: On the failure of rank-revealing QR */
/* >     factorization software - a case study. */
/* >     ACM Trans. Math. Softw. Vol. 35, No 2 (2008), pp. 1-28. */
/* >     LAPACK Working note 176. */
/* > [4] Z. Drmac: SIGMA - mathematical software library for accurate SVD, PSV, */
/* >     QSVD, (H,K)-SVD computations. */
/* >     Department of Mathematics, University of Zagreb, 2008. */
/* > \endverbatim */

/* >  \par Bugs, examples and comments: */
/*   ================================= */
/* > */
/* >  Please report all bugs and send interesting examples and/or comments to */
/* >  drmac@math.hr. Thank you. */
/* > */
/*  ===================================================================== */
/* Subroutine */ void sgejsv_(char *joba, char *jobu, char *jobv, char *jobr, 
	char *jobt, char *jobp, integer *m, integer *n, real *a, integer *lda,
	 real *sva, real *u, integer *ldu, real *v, integer *ldv, real *work, 
	integer *lwork, integer *iwork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, u_dim1, u_offset, v_dim1, v_offset, i__1, i__2, 
	    i__3, i__4, i__5, i__6, i__7, i__8, i__9, i__10, i__11, i__12;
    real r__1, r__2, r__3, r__4;

    /* Local variables */
    logical defr;
    real aapp, aaqq;
    logical kill;
    integer ierr;
    real temp1;
    extern real snrm2_(integer *, real *, integer *);
    integer p, q;
    logical jracc;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void sscal_(integer *, real *, real *, integer *);
    real small, entra, sfmin;
    logical lsvec;
    real epsln;
    logical rsvec;
    extern /* Subroutine */ void scopy_(integer *, real *, integer *, real *, 
	    integer *);
    integer n1;
    extern /* Subroutine */ void sswap_(integer *, real *, integer *, real *, 
	    integer *);
    logical l2aber;
    extern /* Subroutine */ void strsm_(char *, char *, char *, char *, 
	    integer *, integer *, real *, real *, integer *, real *, integer *
	    );
    real condr1, condr2, uscal1, uscal2;
    logical l2kill, l2rank, l2tran;
    extern /* Subroutine */ void sgeqp3_(integer *, integer *, real *, integer 
	    *, integer *, real *, real *, integer *, integer *);
    logical l2pert;
    integer nr;
    real scalem, sconda;
    logical goscal;
    real aatmin;
    extern real slamch_(char *);
    real aatmax;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    logical noscal;
    extern /* Subroutine */ void sgelqf_(integer *, integer *, real *, integer 
	    *, real *, real *, integer *, integer *);
    extern integer isamax_(integer *, real *, integer *);
    extern /* Subroutine */ void slascl_(char *, integer *, integer *, real *, 
	    real *, integer *, integer *, real *, integer *, integer *), sgeqrf_(integer *, integer *, real *, integer *, real *, 
	    real *, integer *, integer *), slacpy_(char *, integer *, integer 
	    *, real *, integer *, real *, integer *), slaset_(char *, 
	    integer *, integer *, real *, real *, real *, integer *);
    real entrat;
    logical almort;
    real maxprj;
    extern /* Subroutine */ void spocon_(char *, integer *, real *, integer *, 
	    real *, real *, real *, integer *, integer *);
    logical errest;
    extern /* Subroutine */ void sgesvj_(char *, char *, char *, integer *, 
	    integer *, real *, integer *, real *, integer *, real *, integer *
	    , real *, integer *, integer *), slassq_(
	    integer *, real *, integer *, real *, real *);
    logical transp;
    extern /* Subroutine */ int slaswp_(integer *, real *, integer *, integer 
	    *, integer *, integer *, integer *); 
    extern void sorgqr_(integer *, integer *,
	     integer *, real *, integer *, real *, real *, integer *, integer 
	    *), sormlq_(char *, char *, integer *, integer *, integer *, real 
	    *, integer *, real *, real *, integer *, real *, integer *, 
	    integer *), sormqr_(char *, char *, integer *, 
	    integer *, integer *, real *, integer *, real *, real *, integer *
	    , real *, integer *, integer *);
    logical rowpiv;
    real big, cond_ok__, xsc, big1;
    integer warning, numrank;


/*  -- LAPACK computational routine (version 3.7.1) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */


/*  =========================================================================== */



/*     Test the input arguments */

    /* Parameter adjustments */
    --sva;
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    u_dim1 = *ldu;
    u_offset = 1 + u_dim1 * 1;
    u -= u_offset;
    v_dim1 = *ldv;
    v_offset = 1 + v_dim1 * 1;
    v -= v_offset;
    --work;
    --iwork;

    /* Function Body */
    lsvec = lsame_(jobu, "U") || lsame_(jobu, "F");
    jracc = lsame_(jobv, "J");
    rsvec = lsame_(jobv, "V") || jracc;
    rowpiv = lsame_(joba, "F") || lsame_(joba, "G");
    l2rank = lsame_(joba, "R");
    l2aber = lsame_(joba, "A");
    errest = lsame_(joba, "E") || lsame_(joba, "G");
    l2tran = lsame_(jobt, "T");
    l2kill = lsame_(jobr, "R");
    defr = lsame_(jobr, "N");
    l2pert = lsame_(jobp, "P");

    if (! (rowpiv || l2rank || l2aber || errest || lsame_(joba, "C"))) {
	*info = -1;
    } else if (! (lsvec || lsame_(jobu, "N") || lsame_(
	    jobu, "W"))) {
	*info = -2;
    } else if (! (rsvec || lsame_(jobv, "N") || lsame_(
	    jobv, "W")) || jracc && ! lsvec) {
	*info = -3;
    } else if (! (l2kill || defr)) {
	*info = -4;
    } else if (! (l2tran || lsame_(jobt, "N"))) {
	*info = -5;
    } else if (! (l2pert || lsame_(jobp, "N"))) {
	*info = -6;
    } else if (*m < 0) {
	*info = -7;
    } else if (*n < 0 || *n > *m) {
	*info = -8;
    } else if (*lda < *m) {
	*info = -10;
    } else if (lsvec && *ldu < *m) {
	*info = -13;
    } else if (rsvec && *ldv < *n) {
	*info = -15;
    } else /* if(complicated condition) */ {
/* Computing MAX */
	i__1 = 7, i__2 = (*n << 2) + 1, i__1 = f2cmax(i__1,i__2), i__2 = (*m << 
		1) + *n;
/* Computing MAX */
	i__3 = 7, i__4 = (*n << 2) + *n * *n, i__3 = f2cmax(i__3,i__4), i__4 = (*
		m << 1) + *n;
/* Computing MAX */
	i__5 = 7, i__6 = (*m << 1) + *n, i__5 = f2cmax(i__5,i__6), i__6 = (*n << 
		2) + 1;
/* Computing MAX */
	i__7 = 7, i__8 = (*m << 1) + *n, i__7 = f2cmax(i__7,i__8), i__8 = (*n << 
		2) + 1;
/* Computing MAX */
	i__9 = (*m << 1) + *n, i__10 = *n * 6 + (*n << 1) * *n;
/* Computing MAX */
	i__11 = (*m << 1) + *n, i__12 = (*n << 2) + *n * *n, i__11 = f2cmax(
		i__11,i__12), i__12 = (*n << 1) + *n * *n + 6;
	if (! (lsvec || rsvec || errest) && *lwork < f2cmax(i__1,i__2) || ! (
		lsvec || rsvec) && errest && *lwork < f2cmax(i__3,i__4) || lsvec 
		&& ! rsvec && *lwork < f2cmax(i__5,i__6) || rsvec && ! lsvec && *
		lwork < f2cmax(i__7,i__8) || lsvec && rsvec && ! jracc && *lwork 
		< f2cmax(i__9,i__10) || lsvec && rsvec && jracc && *lwork < f2cmax(
		i__11,i__12)) {
	    *info = -17;
	} else {
/*        #:) */
	    *info = 0;
	}
    }

    if (*info != 0) {
/*       #:( */
	i__1 = -(*info);
	xerbla_("SGEJSV", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return for void matrix (Y3K safe) */
/* #:) */
    if (*m == 0 || *n == 0) {
	iwork[1] = 0;
	iwork[2] = 0;
	iwork[3] = 0;
	work[1] = 0.f;
	work[2] = 0.f;
	work[3] = 0.f;
	work[4] = 0.f;
	work[5] = 0.f;
	work[6] = 0.f;
	work[7] = 0.f;
	return;
    }

/*     Determine whether the matrix U should be M x N or M x M */

    if (lsvec) {
	n1 = *n;
	if (lsame_(jobu, "F")) {
	    n1 = *m;
	}
    }

/*     Set numerical parameters */

/* !    NOTE: Make sure SLAMCH() does not fail on the target architecture. */

    epsln = slamch_("Epsilon");
    sfmin = slamch_("SafeMinimum");
    small = sfmin / epsln;
    big = slamch_("O");
/*     BIG   = ONE / SFMIN */

/*     Initialize SVA(1:N) = diag( ||A e_i||_2 )_1^N */

/* (!)  If necessary, scale SVA() to protect the largest norm from */
/*     overflow. It is possible that this scaling pushes the smallest */
/*     column norm left from the underflow threshold (extreme case). */

    scalem = 1.f / sqrt((real) (*m) * (real) (*n));
    noscal = TRUE_;
    goscal = TRUE_;
    i__1 = *n;
    for (p = 1; p <= i__1; ++p) {
	aapp = 0.f;
	aaqq = 1.f;
	slassq_(m, &a[p * a_dim1 + 1], &c__1, &aapp, &aaqq);
	if (aapp > big) {
	    *info = -9;
	    i__2 = -(*info);
	    xerbla_("SGEJSV", &i__2, (ftnlen)6);
	    return;
	}
	aaqq = sqrt(aaqq);
	if (aapp < big / aaqq && noscal) {
	    sva[p] = aapp * aaqq;
	} else {
	    noscal = FALSE_;
	    sva[p] = aapp * (aaqq * scalem);
	    if (goscal) {
		goscal = FALSE_;
		i__2 = p - 1;
		sscal_(&i__2, &scalem, &sva[1], &c__1);
	    }
	}
/* L1874: */
    }

    if (noscal) {
	scalem = 1.f;
    }

    aapp = 0.f;
    aaqq = big;
    i__1 = *n;
    for (p = 1; p <= i__1; ++p) {
/* Computing MAX */
	r__1 = aapp, r__2 = sva[p];
	aapp = f2cmax(r__1,r__2);
	if (sva[p] != 0.f) {
/* Computing MIN */
	    r__1 = aaqq, r__2 = sva[p];
	    aaqq = f2cmin(r__1,r__2);
	}
/* L4781: */
    }

/*     Quick return for zero M x N matrix */
/* #:) */
    if (aapp == 0.f) {
	if (lsvec) {
	    slaset_("G", m, &n1, &c_b34, &c_b35, &u[u_offset], ldu)
		    ;
	}
	if (rsvec) {
	    slaset_("G", n, n, &c_b34, &c_b35, &v[v_offset], ldv);
	}
	work[1] = 1.f;
	work[2] = 1.f;
	if (errest) {
	    work[3] = 1.f;
	}
	if (lsvec && rsvec) {
	    work[4] = 1.f;
	    work[5] = 1.f;
	}
	if (l2tran) {
	    work[6] = 0.f;
	    work[7] = 0.f;
	}
	iwork[1] = 0;
	iwork[2] = 0;
	iwork[3] = 0;
	return;
    }

/*     Issue warning if denormalized column norms detected. Override the */
/*     high relative accuracy request. Issue licence to kill columns */
/*     (set them to zero) whose norm is less than sigma_max / BIG (roughly). */
/* #:( */
    warning = 0;
    if (aaqq <= sfmin) {
	l2rank = TRUE_;
	l2kill = TRUE_;
	warning = 1;
    }

/*     Quick return for one-column matrix */
/* #:) */
    if (*n == 1) {

	if (lsvec) {
	    slascl_("G", &c__0, &c__0, &sva[1], &scalem, m, &c__1, &a[a_dim1 
		    + 1], lda, &ierr);
	    slacpy_("A", m, &c__1, &a[a_offset], lda, &u[u_offset], ldu);
/*           computing all M left singular vectors of the M x 1 matrix */
	    if (n1 != *n) {
		i__1 = *lwork - *n;
		sgeqrf_(m, n, &u[u_offset], ldu, &work[1], &work[*n + 1], &
			i__1, &ierr);
		i__1 = *lwork - *n;
		sorgqr_(m, &n1, &c__1, &u[u_offset], ldu, &work[1], &work[*n 
			+ 1], &i__1, &ierr);
		scopy_(m, &a[a_dim1 + 1], &c__1, &u[u_dim1 + 1], &c__1);
	    }
	}
	if (rsvec) {
	    v[v_dim1 + 1] = 1.f;
	}
	if (sva[1] < big * scalem) {
	    sva[1] /= scalem;
	    scalem = 1.f;
	}
	work[1] = 1.f / scalem;
	work[2] = 1.f;
	if (sva[1] != 0.f) {
	    iwork[1] = 1;
	    if (sva[1] / scalem >= sfmin) {
		iwork[2] = 1;
	    } else {
		iwork[2] = 0;
	    }
	} else {
	    iwork[1] = 0;
	    iwork[2] = 0;
	}
	iwork[3] = 0;
	if (errest) {
	    work[3] = 1.f;
	}
	if (lsvec && rsvec) {
	    work[4] = 1.f;
	    work[5] = 1.f;
	}
	if (l2tran) {
	    work[6] = 0.f;
	    work[7] = 0.f;
	}
	return;

    }

    transp = FALSE_;
    l2tran = l2tran && *m == *n;

    aatmax = -1.f;
    aatmin = big;
    if (rowpiv || l2tran) {

/*     Compute the row norms, needed to determine row pivoting sequence */
/*     (in the case of heavily row weighted A, row pivoting is strongly */
/*     advised) and to collect information needed to compare the */
/*     structures of A * A^t and A^t * A (in the case L2TRAN.EQ..TRUE.). */

	if (l2tran) {
	    i__1 = *m;
	    for (p = 1; p <= i__1; ++p) {
		xsc = 0.f;
		temp1 = 1.f;
		slassq_(n, &a[p + a_dim1], lda, &xsc, &temp1);
/*              SLASSQ gets both the ell_2 and the ell_infinity norm */
/*              in one pass through the vector */
		work[*m + *n + p] = xsc * scalem;
		work[*n + p] = xsc * (scalem * sqrt(temp1));
/* Computing MAX */
		r__1 = aatmax, r__2 = work[*n + p];
		aatmax = f2cmax(r__1,r__2);
		if (work[*n + p] != 0.f) {
/* Computing MIN */
		    r__1 = aatmin, r__2 = work[*n + p];
		    aatmin = f2cmin(r__1,r__2);
		}
/* L1950: */
	    }
	} else {
	    i__1 = *m;
	    for (p = 1; p <= i__1; ++p) {
		work[*m + *n + p] = scalem * (r__1 = a[p + isamax_(n, &a[p + 
			a_dim1], lda) * a_dim1], abs(r__1));
/* Computing MAX */
		r__1 = aatmax, r__2 = work[*m + *n + p];
		aatmax = f2cmax(r__1,r__2);
/* Computing MIN */
		r__1 = aatmin, r__2 = work[*m + *n + p];
		aatmin = f2cmin(r__1,r__2);
/* L1904: */
	    }
	}

    }

/*     For square matrix A try to determine whether A^t  would be  better */
/*     input for the preconditioned Jacobi SVD, with faster convergence. */
/*     The decision is based on an O(N) function of the vector of column */
/*     and row norms of A, based on the Shannon entropy. This should give */
/*     the right choice in most cases when the difference actually matters. */
/*     It may fail and pick the slower converging side. */

    entra = 0.f;
    entrat = 0.f;
    if (l2tran) {

	xsc = 0.f;
	temp1 = 1.f;
	slassq_(n, &sva[1], &c__1, &xsc, &temp1);
	temp1 = 1.f / temp1;

	entra = 0.f;
	i__1 = *n;
	for (p = 1; p <= i__1; ++p) {
/* Computing 2nd power */
	    r__1 = sva[p] / xsc;
	    big1 = r__1 * r__1 * temp1;
	    if (big1 != 0.f) {
		entra += big1 * log(big1);
	    }
/* L1113: */
	}
	entra = -entra / log((real) (*n));

/*        Now, SVA().^2/Trace(A^t * A) is a point in the probability simplex. */
/*        It is derived from the diagonal of  A^t * A.  Do the same with the */
/*        diagonal of A * A^t, compute the entropy of the corresponding */
/*        probability distribution. Note that A * A^t and A^t * A have the */
/*        same trace. */

	entrat = 0.f;
	i__1 = *n + *m;
	for (p = *n + 1; p <= i__1; ++p) {
/* Computing 2nd power */
	    r__1 = work[p] / xsc;
	    big1 = r__1 * r__1 * temp1;
	    if (big1 != 0.f) {
		entrat += big1 * log(big1);
	    }
/* L1114: */
	}
	entrat = -entrat / log((real) (*m));

/*        Analyze the entropies and decide A or A^t. Smaller entropy */
/*        usually means better input for the algorithm. */

	transp = entrat < entra;

/*        If A^t is better than A, transpose A. */

	if (transp) {
/*           In an optimal implementation, this trivial transpose */
/*           should be replaced with faster transpose. */
	    i__1 = *n - 1;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = *n;
		for (q = p + 1; q <= i__2; ++q) {
		    temp1 = a[q + p * a_dim1];
		    a[q + p * a_dim1] = a[p + q * a_dim1];
		    a[p + q * a_dim1] = temp1;
/* L1116: */
		}
/* L1115: */
	    }
	    i__1 = *n;
	    for (p = 1; p <= i__1; ++p) {
		work[*m + *n + p] = sva[p];
		sva[p] = work[*n + p];
/* L1117: */
	    }
	    temp1 = aapp;
	    aapp = aatmax;
	    aatmax = temp1;
	    temp1 = aaqq;
	    aaqq = aatmin;
	    aatmin = temp1;
	    kill = lsvec;
	    lsvec = rsvec;
	    rsvec = kill;
	    if (lsvec) {
		n1 = *n;
	    }

	    rowpiv = TRUE_;
	}

    }
/*     END IF L2TRAN */

/*     Scale the matrix so that its maximal singular value remains less */
/*     than SQRT(BIG) -- the matrix is scaled so that its maximal column */
/*     has Euclidean norm equal to SQRT(BIG/N). The only reason to keep */
/*     SQRT(BIG) instead of BIG is the fact that SGEJSV uses LAPACK and */
/*     BLAS routines that, in some implementations, are not capable of */
/*     working in the full interval [SFMIN,BIG] and that they may provoke */
/*     overflows in the intermediate results. If the singular values spread */
/*     from SFMIN to BIG, then SGESVJ will compute them. So, in that case, */
/*     one should use SGESVJ instead of SGEJSV. */

    big1 = sqrt(big);
    temp1 = sqrt(big / (real) (*n));

    slascl_("G", &c__0, &c__0, &aapp, &temp1, n, &c__1, &sva[1], n, &ierr);
    if (aaqq > aapp * sfmin) {
	aaqq = aaqq / aapp * temp1;
    } else {
	aaqq = aaqq * temp1 / aapp;
    }
    temp1 *= scalem;
    slascl_("G", &c__0, &c__0, &aapp, &temp1, m, n, &a[a_offset], lda, &ierr);

/*     To undo scaling at the end of this procedure, multiply the */
/*     computed singular values with USCAL2 / USCAL1. */

    uscal1 = temp1;
    uscal2 = aapp;

    if (l2kill) {
/*        L2KILL enforces computation of nonzero singular values in */
/*        the restricted range of condition number of the initial A, */
/*        sigma_max(A) / sigma_min(A) approx. SQRT(BIG)/SQRT(SFMIN). */
	xsc = sqrt(sfmin);
    } else {
	xsc = small;

/*        Now, if the condition number of A is too big, */
/*        sigma_max(A) / sigma_min(A) .GT. SQRT(BIG/N) * EPSLN / SFMIN, */
/*        as a precaution measure, the full SVD is computed using SGESVJ */
/*        with accumulated Jacobi rotations. This provides numerically */
/*        more robust computation, at the cost of slightly increased run */
/*        time. Depending on the concrete implementation of BLAS and LAPACK */
/*        (i.e. how they behave in presence of extreme ill-conditioning) the */
/*        implementor may decide to remove this switch. */
	if (aaqq < sqrt(sfmin) && lsvec && rsvec) {
	    jracc = TRUE_;
	}

    }
    if (aaqq < xsc) {
	i__1 = *n;
	for (p = 1; p <= i__1; ++p) {
	    if (sva[p] < xsc) {
		slaset_("A", m, &c__1, &c_b34, &c_b34, &a[p * a_dim1 + 1], 
			lda);
		sva[p] = 0.f;
	    }
/* L700: */
	}
    }

/*     Preconditioning using QR factorization with pivoting */

    if (rowpiv) {
/*        Optional row permutation (Bjoerck row pivoting): */
/*        A result by Cox and Higham shows that the Bjoerck's */
/*        row pivoting combined with standard column pivoting */
/*        has similar effect as Powell-Reid complete pivoting. */
/*        The ell-infinity norms of A are made nonincreasing. */
	i__1 = *m - 1;
	for (p = 1; p <= i__1; ++p) {
	    i__2 = *m - p + 1;
	    q = isamax_(&i__2, &work[*m + *n + p], &c__1) + p - 1;
	    iwork[(*n << 1) + p] = q;
	    if (p != q) {
		temp1 = work[*m + *n + p];
		work[*m + *n + p] = work[*m + *n + q];
		work[*m + *n + q] = temp1;
	    }
/* L1952: */
	}
	i__1 = *m - 1;
	slaswp_(n, &a[a_offset], lda, &c__1, &i__1, &iwork[(*n << 1) + 1], &
		c__1);
    }

/*     End of the preparation phase (scaling, optional sorting and */
/*     transposing, optional flushing of small columns). */

/*     Preconditioning */

/*     If the full SVD is needed, the right singular vectors are computed */
/*     from a matrix equation, and for that we need theoretical analysis */
/*     of the Businger-Golub pivoting. So we use SGEQP3 as the first RR QRF. */
/*     In all other cases the first RR QRF can be chosen by other criteria */
/*     (eg speed by replacing global with restricted window pivoting, such */
/*     as in SGEQPX from TOMS # 782). Good results will be obtained using */
/*     SGEQPX with properly (!) chosen numerical parameters. */
/*     Any improvement of SGEQP3 improves overal performance of SGEJSV. */

/*     A * P1 = Q1 * [ R1^t 0]^t: */
    i__1 = *n;
    for (p = 1; p <= i__1; ++p) {
	iwork[p] = 0;
/* L1963: */
    }
    i__1 = *lwork - *n;
    sgeqp3_(m, n, &a[a_offset], lda, &iwork[1], &work[1], &work[*n + 1], &
	    i__1, &ierr);

/*     The upper triangular matrix R1 from the first QRF is inspected for */
/*     rank deficiency and possibilities for deflation, or possible */
/*     ill-conditioning. Depending on the user specified flag L2RANK, */
/*     the procedure explores possibilities to reduce the numerical */
/*     rank by inspecting the computed upper triangular factor. If */
/*     L2RANK or L2ABER are up, then SGEJSV will compute the SVD of */
/*     A + dA, where ||dA|| <= f(M,N)*EPSLN. */

    nr = 1;
    if (l2aber) {
/*        Standard absolute error bound suffices. All sigma_i with */
/*        sigma_i < N*EPSLN*||A|| are flushed to zero. This is an */
/*        aggressive enforcement of lower numerical rank by introducing a */
/*        backward error of the order of N*EPSLN*||A||. */
	temp1 = sqrt((real) (*n)) * epsln;
	i__1 = *n;
	for (p = 2; p <= i__1; ++p) {
	    if ((r__2 = a[p + p * a_dim1], abs(r__2)) >= temp1 * (r__1 = a[
		    a_dim1 + 1], abs(r__1))) {
		++nr;
	    } else {
		goto L3002;
	    }
/* L3001: */
	}
L3002:
	;
    } else if (l2rank) {
/*        Sudden drop on the diagonal of R1 is used as the criterion for */
/*        close-to-rank-deficient. */
	temp1 = sqrt(sfmin);
	i__1 = *n;
	for (p = 2; p <= i__1; ++p) {
	    if ((r__2 = a[p + p * a_dim1], abs(r__2)) < epsln * (r__1 = a[p - 
		    1 + (p - 1) * a_dim1], abs(r__1)) || (r__3 = a[p + p * 
		    a_dim1], abs(r__3)) < small || l2kill && (r__4 = a[p + p *
		     a_dim1], abs(r__4)) < temp1) {
		goto L3402;
	    }
	    ++nr;
/* L3401: */
	}
L3402:

	;
    } else {
/*        The goal is high relative accuracy. However, if the matrix */
/*        has high scaled condition number the relative accuracy is in */
/*        general not feasible. Later on, a condition number estimator */
/*        will be deployed to estimate the scaled condition number. */
/*        Here we just remove the underflowed part of the triangular */
/*        factor. This prevents the situation in which the code is */
/*        working hard to get the accuracy not warranted by the data. */
	temp1 = sqrt(sfmin);
	i__1 = *n;
	for (p = 2; p <= i__1; ++p) {
	    if ((r__1 = a[p + p * a_dim1], abs(r__1)) < small || l2kill && (
		    r__2 = a[p + p * a_dim1], abs(r__2)) < temp1) {
		goto L3302;
	    }
	    ++nr;
/* L3301: */
	}
L3302:

	;
    }

    almort = FALSE_;
    if (nr == *n) {
	maxprj = 1.f;
	i__1 = *n;
	for (p = 2; p <= i__1; ++p) {
	    temp1 = (r__1 = a[p + p * a_dim1], abs(r__1)) / sva[iwork[p]];
	    maxprj = f2cmin(maxprj,temp1);
/* L3051: */
	}
/* Computing 2nd power */
	r__1 = maxprj;
	if (r__1 * r__1 >= 1.f - (real) (*n) * epsln) {
	    almort = TRUE_;
	}
    }


    sconda = -1.f;
    condr1 = -1.f;
    condr2 = -1.f;

    if (errest) {
	if (*n == nr) {
	    if (rsvec) {
		slacpy_("U", n, n, &a[a_offset], lda, &v[v_offset], ldv);
		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    temp1 = sva[iwork[p]];
		    r__1 = 1.f / temp1;
		    sscal_(&p, &r__1, &v[p * v_dim1 + 1], &c__1);
/* L3053: */
		}
		spocon_("U", n, &v[v_offset], ldv, &c_b35, &temp1, &work[*n + 
			1], &iwork[(*n << 1) + *m + 1], &ierr);
	    } else if (lsvec) {
		slacpy_("U", n, n, &a[a_offset], lda, &u[u_offset], ldu);
		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    temp1 = sva[iwork[p]];
		    r__1 = 1.f / temp1;
		    sscal_(&p, &r__1, &u[p * u_dim1 + 1], &c__1);
/* L3054: */
		}
		spocon_("U", n, &u[u_offset], ldu, &c_b35, &temp1, &work[*n + 
			1], &iwork[(*n << 1) + *m + 1], &ierr);
	    } else {
		slacpy_("U", n, n, &a[a_offset], lda, &work[*n + 1], n);
		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    temp1 = sva[iwork[p]];
		    r__1 = 1.f / temp1;
		    sscal_(&p, &r__1, &work[*n + (p - 1) * *n + 1], &c__1);
/* L3052: */
		}
		spocon_("U", n, &work[*n + 1], n, &c_b35, &temp1, &work[*n + *
			n * *n + 1], &iwork[(*n << 1) + *m + 1], &ierr);
	    }
	    sconda = 1.f / sqrt(temp1);
/*           SCONDA is an estimate of SQRT(||(R^t * R)^(-1)||_1). */
/*           N^(-1/4) * SCONDA <= ||R^(-1)||_2 <= N^(1/4) * SCONDA */
	} else {
	    sconda = -1.f;
	}
    }

    l2pert = l2pert && (r__1 = a[a_dim1 + 1] / a[nr + nr * a_dim1], abs(r__1))
	     > sqrt(big1);
/*     If there is no violent scaling, artificial perturbation is not needed. */

/*     Phase 3: */

    if (! (rsvec || lsvec)) {

/*         Singular Values only */

/* Computing MIN */
	i__2 = *n - 1;
	i__1 = f2cmin(i__2,nr);
	for (p = 1; p <= i__1; ++p) {
	    i__2 = *n - p;
	    scopy_(&i__2, &a[p + (p + 1) * a_dim1], lda, &a[p + 1 + p * 
		    a_dim1], &c__1);
/* L1946: */
	}

/*        The following two DO-loops introduce small relative perturbation */
/*        into the strict upper triangle of the lower triangular matrix. */
/*        Small entries below the main diagonal are also changed. */
/*        This modification is useful if the computing environment does not */
/*        provide/allow FLUSH TO ZERO underflow, for it prevents many */
/*        annoying denormalized numbers in case of strongly scaled matrices. */
/*        The perturbation is structured so that it does not introduce any */
/*        new perturbation of the singular values, and it does not destroy */
/*        the job done by the preconditioner. */
/*        The licence for this perturbation is in the variable L2PERT, which */
/*        should be .FALSE. if FLUSH TO ZERO underflow is active. */

	if (! almort) {

	    if (l2pert) {
/*              XSC = SQRT(SMALL) */
		xsc = epsln / (real) (*n);
		i__1 = nr;
		for (q = 1; q <= i__1; ++q) {
		    temp1 = xsc * (r__1 = a[q + q * a_dim1], abs(r__1));
		    i__2 = *n;
		    for (p = 1; p <= i__2; ++p) {
			if (p > q && (r__1 = a[p + q * a_dim1], abs(r__1)) <= 
				temp1 || p < q) {
			    a[p + q * a_dim1] = r_sign(&temp1, &a[p + q * 
				    a_dim1]);
			}
/* L4949: */
		    }
/* L4947: */
		}
	    } else {
		i__1 = nr - 1;
		i__2 = nr - 1;
		slaset_("U", &i__1, &i__2, &c_b34, &c_b34, &a[(a_dim1 << 1) + 
			1], lda);
	    }


	    i__1 = *lwork - *n;
	    sgeqrf_(n, &nr, &a[a_offset], lda, &work[1], &work[*n + 1], &i__1,
		     &ierr);

	    i__1 = nr - 1;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = nr - p;
		scopy_(&i__2, &a[p + (p + 1) * a_dim1], lda, &a[p + 1 + p * 
			a_dim1], &c__1);
/* L1948: */
	    }

	}

/*           Row-cyclic Jacobi SVD algorithm with column pivoting */

/*           to drown denormals */
	if (l2pert) {
/*              XSC = SQRT(SMALL) */
	    xsc = epsln / (real) (*n);
	    i__1 = nr;
	    for (q = 1; q <= i__1; ++q) {
		temp1 = xsc * (r__1 = a[q + q * a_dim1], abs(r__1));
		i__2 = nr;
		for (p = 1; p <= i__2; ++p) {
		    if (p > q && (r__1 = a[p + q * a_dim1], abs(r__1)) <= 
			    temp1 || p < q) {
			a[p + q * a_dim1] = r_sign(&temp1, &a[p + q * a_dim1])
				;
		    }
/* L1949: */
		}
/* L1947: */
	    }
	} else {
	    i__1 = nr - 1;
	    i__2 = nr - 1;
	    slaset_("U", &i__1, &i__2, &c_b34, &c_b34, &a[(a_dim1 << 1) + 1], 
		    lda);
	}

/*           triangular matrix (plus perturbation which is ignored in */
/*           the part which destroys triangular form (confusing?!)) */

	sgesvj_("L", "NoU", "NoV", &nr, &nr, &a[a_offset], lda, &sva[1], n, &
		v[v_offset], ldv, &work[1], lwork, info);

	scalem = work[1];
	numrank = i_nint(&work[2]);


    } else if (rsvec && ! lsvec) {

/*        -> Singular Values and Right Singular Vectors <- */

	if (almort) {

	    i__1 = nr;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = *n - p + 1;
		scopy_(&i__2, &a[p + p * a_dim1], lda, &v[p + p * v_dim1], &
			c__1);
/* L1998: */
	    }
	    i__1 = nr - 1;
	    i__2 = nr - 1;
	    slaset_("Upper", &i__1, &i__2, &c_b34, &c_b34, &v[(v_dim1 << 1) + 
		    1], ldv);

	    sgesvj_("L", "U", "N", n, &nr, &v[v_offset], ldv, &sva[1], &nr, &
		    a[a_offset], lda, &work[1], lwork, info);
	    scalem = work[1];
	    numrank = i_nint(&work[2]);
	} else {

/*        accumulated product of Jacobi rotations, three are perfect ) */

	    i__1 = nr - 1;
	    i__2 = nr - 1;
	    slaset_("Lower", &i__1, &i__2, &c_b34, &c_b34, &a[a_dim1 + 2], 
		    lda);
	    i__1 = *lwork - *n;
	    sgelqf_(&nr, n, &a[a_offset], lda, &work[1], &work[*n + 1], &i__1,
		     &ierr);
	    slacpy_("Lower", &nr, &nr, &a[a_offset], lda, &v[v_offset], ldv);
	    i__1 = nr - 1;
	    i__2 = nr - 1;
	    slaset_("Upper", &i__1, &i__2, &c_b34, &c_b34, &v[(v_dim1 << 1) + 
		    1], ldv);
	    i__1 = *lwork - (*n << 1);
	    sgeqrf_(&nr, &nr, &v[v_offset], ldv, &work[*n + 1], &work[(*n << 
		    1) + 1], &i__1, &ierr);
	    i__1 = nr;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = nr - p + 1;
		scopy_(&i__2, &v[p + p * v_dim1], ldv, &v[p + p * v_dim1], &
			c__1);
/* L8998: */
	    }
	    i__1 = nr - 1;
	    i__2 = nr - 1;
	    slaset_("Upper", &i__1, &i__2, &c_b34, &c_b34, &v[(v_dim1 << 1) + 
		    1], ldv);

	    i__1 = *lwork - *n;
	    sgesvj_("Lower", "U", "N", &nr, &nr, &v[v_offset], ldv, &sva[1], &
		    nr, &u[u_offset], ldu, &work[*n + 1], &i__1, info);
	    scalem = work[*n + 1];
	    numrank = i_nint(&work[*n + 2]);
	    if (nr < *n) {
		i__1 = *n - nr;
		slaset_("A", &i__1, &nr, &c_b34, &c_b34, &v[nr + 1 + v_dim1], 
			ldv);
		i__1 = *n - nr;
		slaset_("A", &nr, &i__1, &c_b34, &c_b34, &v[(nr + 1) * v_dim1 
			+ 1], ldv);
		i__1 = *n - nr;
		i__2 = *n - nr;
		slaset_("A", &i__1, &i__2, &c_b34, &c_b35, &v[nr + 1 + (nr + 
			1) * v_dim1], ldv);
	    }

	    i__1 = *lwork - *n;
	    sormlq_("Left", "Transpose", n, n, &nr, &a[a_offset], lda, &work[
		    1], &v[v_offset], ldv, &work[*n + 1], &i__1, &ierr);

	}

	i__1 = *n;
	for (p = 1; p <= i__1; ++p) {
	    scopy_(n, &v[p + v_dim1], ldv, &a[iwork[p] + a_dim1], lda);
/* L8991: */
	}
	slacpy_("All", n, n, &a[a_offset], lda, &v[v_offset], ldv);

	if (transp) {
	    slacpy_("All", n, n, &v[v_offset], ldv, &u[u_offset], ldu);
	}

    } else if (lsvec && ! rsvec) {


/*        Jacobi rotations in the Jacobi iterations. */
	i__1 = nr;
	for (p = 1; p <= i__1; ++p) {
	    i__2 = *n - p + 1;
	    scopy_(&i__2, &a[p + p * a_dim1], lda, &u[p + p * u_dim1], &c__1);
/* L1965: */
	}
	i__1 = nr - 1;
	i__2 = nr - 1;
	slaset_("Upper", &i__1, &i__2, &c_b34, &c_b34, &u[(u_dim1 << 1) + 1], 
		ldu);

	i__1 = *lwork - (*n << 1);
	sgeqrf_(n, &nr, &u[u_offset], ldu, &work[*n + 1], &work[(*n << 1) + 1]
		, &i__1, &ierr);

	i__1 = nr - 1;
	for (p = 1; p <= i__1; ++p) {
	    i__2 = nr - p;
	    scopy_(&i__2, &u[p + (p + 1) * u_dim1], ldu, &u[p + 1 + p * 
		    u_dim1], &c__1);
/* L1967: */
	}
	i__1 = nr - 1;
	i__2 = nr - 1;
	slaset_("Upper", &i__1, &i__2, &c_b34, &c_b34, &u[(u_dim1 << 1) + 1], 
		ldu);

	i__1 = *lwork - *n;
	sgesvj_("Lower", "U", "N", &nr, &nr, &u[u_offset], ldu, &sva[1], &nr, 
		&a[a_offset], lda, &work[*n + 1], &i__1, info);
	scalem = work[*n + 1];
	numrank = i_nint(&work[*n + 2]);

	if (nr < *m) {
	    i__1 = *m - nr;
	    slaset_("A", &i__1, &nr, &c_b34, &c_b34, &u[nr + 1 + u_dim1], ldu);
	    if (nr < n1) {
		i__1 = n1 - nr;
		slaset_("A", &nr, &i__1, &c_b34, &c_b34, &u[(nr + 1) * u_dim1 
			+ 1], ldu);
		i__1 = *m - nr;
		i__2 = n1 - nr;
		slaset_("A", &i__1, &i__2, &c_b34, &c_b35, &u[nr + 1 + (nr + 
			1) * u_dim1], ldu);
	    }
	}

	i__1 = *lwork - *n;
	sormqr_("Left", "No Tr", m, &n1, n, &a[a_offset], lda, &work[1], &u[
		u_offset], ldu, &work[*n + 1], &i__1, &ierr);

	if (rowpiv) {
	    i__1 = *m - 1;
	    slaswp_(&n1, &u[u_offset], ldu, &c__1, &i__1, &iwork[(*n << 1) + 
		    1], &c_n1);
	}

	i__1 = n1;
	for (p = 1; p <= i__1; ++p) {
	    xsc = 1.f / snrm2_(m, &u[p * u_dim1 + 1], &c__1);
	    sscal_(m, &xsc, &u[p * u_dim1 + 1], &c__1);
/* L1974: */
	}

	if (transp) {
	    slacpy_("All", n, n, &u[u_offset], ldu, &v[v_offset], ldv);
	}

    } else {


	if (! jracc) {

	    if (! almort) {

/*           Second Preconditioning Step (QRF [with pivoting]) */
/*           Note that the composition of TRANSPOSE, QRF and TRANSPOSE is */
/*           equivalent to an LQF CALL. Since in many libraries the QRF */
/*           seems to be better optimized than the LQF, we do explicit */
/*           transpose and use the QRF. This is subject to changes in an */
/*           optimized implementation of SGEJSV. */

		i__1 = nr;
		for (p = 1; p <= i__1; ++p) {
		    i__2 = *n - p + 1;
		    scopy_(&i__2, &a[p + p * a_dim1], lda, &v[p + p * v_dim1],
			     &c__1);
/* L1968: */
		}

/*           denormals in the second QR factorization, where they are */
/*           as good as zeros. This is done to avoid painfully slow */
/*           computation with denormals. The relative size of the perturbation */
/*           is a parameter that can be changed by the implementer. */
/*           This perturbation device will be obsolete on machines with */
/*           properly implemented arithmetic. */
/*           To switch it off, set L2PERT=.FALSE. To remove it from  the */
/*           code, remove the action under L2PERT=.TRUE., leave the ELSE part. */
/*           The following two loops should be blocked and fused with the */
/*           transposed copy above. */

		if (l2pert) {
		    xsc = sqrt(small);
		    i__1 = nr;
		    for (q = 1; q <= i__1; ++q) {
			temp1 = xsc * (r__1 = v[q + q * v_dim1], abs(r__1));
			i__2 = *n;
			for (p = 1; p <= i__2; ++p) {
			    if (p > q && (r__1 = v[p + q * v_dim1], abs(r__1))
				     <= temp1 || p < q) {
				v[p + q * v_dim1] = r_sign(&temp1, &v[p + q * 
					v_dim1]);
			    }
			    if (p < q) {
				v[p + q * v_dim1] = -v[p + q * v_dim1];
			    }
/* L2968: */
			}
/* L2969: */
		    }
		} else {
		    i__1 = nr - 1;
		    i__2 = nr - 1;
		    slaset_("U", &i__1, &i__2, &c_b34, &c_b34, &v[(v_dim1 << 
			    1) + 1], ldv);
		}

/*           Estimate the row scaled condition number of R1 */
/*           (If R1 is rectangular, N > NR, then the condition number */
/*           of the leading NR x NR submatrix is estimated.) */

		slacpy_("L", &nr, &nr, &v[v_offset], ldv, &work[(*n << 1) + 1]
			, &nr);
		i__1 = nr;
		for (p = 1; p <= i__1; ++p) {
		    i__2 = nr - p + 1;
		    temp1 = snrm2_(&i__2, &work[(*n << 1) + (p - 1) * nr + p],
			     &c__1);
		    i__2 = nr - p + 1;
		    r__1 = 1.f / temp1;
		    sscal_(&i__2, &r__1, &work[(*n << 1) + (p - 1) * nr + p], 
			    &c__1);
/* L3950: */
		}
		spocon_("Lower", &nr, &work[(*n << 1) + 1], &nr, &c_b35, &
			temp1, &work[(*n << 1) + nr * nr + 1], &iwork[*m + (*
			n << 1) + 1], &ierr);
		condr1 = 1.f / sqrt(temp1);
/*           R1 is OK for inverse <=> CONDR1 .LT. FLOAT(N) */
/*           more conservative    <=> CONDR1 .LT. SQRT(FLOAT(N)) */

		cond_ok__ = sqrt((real) nr);
/* [TP]       COND_OK is a tuning parameter. */
		if (condr1 < cond_ok__) {
/*              implementation, this QRF should be implemented as the QRF */
/*              of a lower triangular matrix. */
/*              R1^t = Q2 * R2 */
		    i__1 = *lwork - (*n << 1);
		    sgeqrf_(n, &nr, &v[v_offset], ldv, &work[*n + 1], &work[(*
			    n << 1) + 1], &i__1, &ierr);

		    if (l2pert) {
			xsc = sqrt(small) / epsln;
			i__1 = nr;
			for (p = 2; p <= i__1; ++p) {
			    i__2 = p - 1;
			    for (q = 1; q <= i__2; ++q) {
/* Computing MIN */
				r__3 = (r__1 = v[p + p * v_dim1], abs(r__1)), 
					r__4 = (r__2 = v[q + q * v_dim1], abs(
					r__2));
				temp1 = xsc * f2cmin(r__3,r__4);
				if ((r__1 = v[q + p * v_dim1], abs(r__1)) <= 
					temp1) {
				    v[q + p * v_dim1] = r_sign(&temp1, &v[q + 
					    p * v_dim1]);
				}
/* L3958: */
			    }
/* L3959: */
			}
		    }

		    if (nr != *n) {
			slacpy_("A", n, &nr, &v[v_offset], ldv, &work[(*n << 
				1) + 1], n);
		    }

		    i__1 = nr - 1;
		    for (p = 1; p <= i__1; ++p) {
			i__2 = nr - p;
			scopy_(&i__2, &v[p + (p + 1) * v_dim1], ldv, &v[p + 1 
				+ p * v_dim1], &c__1);
/* L1969: */
		    }

		    condr2 = condr1;

		} else {

/*              Note that windowed pivoting would be equally good */
/*              numerically, and more run-time efficient. So, in */
/*              an optimal implementation, the next call to SGEQP3 */
/*              should be replaced with eg. CALL SGEQPX (ACM TOMS #782) */
/*              with properly (carefully) chosen parameters. */

/*              R1^t * P2 = Q2 * R2 */
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			iwork[*n + p] = 0;
/* L3003: */
		    }
		    i__1 = *lwork - (*n << 1);
		    sgeqp3_(n, &nr, &v[v_offset], ldv, &iwork[*n + 1], &work[*
			    n + 1], &work[(*n << 1) + 1], &i__1, &ierr);
/* *               CALL SGEQRF( N, NR, V, LDV, WORK(N+1), WORK(2*N+1), */
/* *     $              LWORK-2*N, IERR ) */
		    if (l2pert) {
			xsc = sqrt(small);
			i__1 = nr;
			for (p = 2; p <= i__1; ++p) {
			    i__2 = p - 1;
			    for (q = 1; q <= i__2; ++q) {
/* Computing MIN */
				r__3 = (r__1 = v[p + p * v_dim1], abs(r__1)), 
					r__4 = (r__2 = v[q + q * v_dim1], abs(
					r__2));
				temp1 = xsc * f2cmin(r__3,r__4);
				if ((r__1 = v[q + p * v_dim1], abs(r__1)) <= 
					temp1) {
				    v[q + p * v_dim1] = r_sign(&temp1, &v[q + 
					    p * v_dim1]);
				}
/* L3968: */
			    }
/* L3969: */
			}
		    }

		    slacpy_("A", n, &nr, &v[v_offset], ldv, &work[(*n << 1) + 
			    1], n);

		    if (l2pert) {
			xsc = sqrt(small);
			i__1 = nr;
			for (p = 2; p <= i__1; ++p) {
			    i__2 = p - 1;
			    for (q = 1; q <= i__2; ++q) {
/* Computing MIN */
				r__3 = (r__1 = v[p + p * v_dim1], abs(r__1)), 
					r__4 = (r__2 = v[q + q * v_dim1], abs(
					r__2));
				temp1 = xsc * f2cmin(r__3,r__4);
				v[p + q * v_dim1] = -r_sign(&temp1, &v[q + p *
					 v_dim1]);
/* L8971: */
			    }
/* L8970: */
			}
		    } else {
			i__1 = nr - 1;
			i__2 = nr - 1;
			slaset_("L", &i__1, &i__2, &c_b34, &c_b34, &v[v_dim1 
				+ 2], ldv);
		    }
/*              Now, compute R2 = L3 * Q3, the LQ factorization. */
		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    sgelqf_(&nr, &nr, &v[v_offset], ldv, &work[(*n << 1) + *n 
			    * nr + 1], &work[(*n << 1) + *n * nr + nr + 1], &
			    i__1, &ierr);
		    slacpy_("L", &nr, &nr, &v[v_offset], ldv, &work[(*n << 1) 
			    + *n * nr + nr + 1], &nr);
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			temp1 = snrm2_(&p, &work[(*n << 1) + *n * nr + nr + p]
				, &nr);
			r__1 = 1.f / temp1;
			sscal_(&p, &r__1, &work[(*n << 1) + *n * nr + nr + p],
				 &nr);
/* L4950: */
		    }
		    spocon_("L", &nr, &work[(*n << 1) + *n * nr + nr + 1], &
			    nr, &c_b35, &temp1, &work[(*n << 1) + *n * nr + 
			    nr + nr * nr + 1], &iwork[*m + (*n << 1) + 1], &
			    ierr);
		    condr2 = 1.f / sqrt(temp1);

		    if (condr2 >= cond_ok__) {
/*                 (this overwrites the copy of R2, as it will not be */
/*                 needed in this branch, but it does not overwritte the */
/*                 Huseholder vectors of Q2.). */
			slacpy_("U", &nr, &nr, &v[v_offset], ldv, &work[(*n <<
				 1) + 1], n);
/*                 WORK(2*N+N*NR+1:2*N+N*NR+N) */
		    }

		}

		if (l2pert) {
		    xsc = sqrt(small);
		    i__1 = nr;
		    for (q = 2; q <= i__1; ++q) {
			temp1 = xsc * v[q + q * v_dim1];
			i__2 = q - 1;
			for (p = 1; p <= i__2; ++p) {
/*                    V(p,q) = - SIGN( TEMP1, V(q,p) ) */
			    v[p + q * v_dim1] = -r_sign(&temp1, &v[p + q * 
				    v_dim1]);
/* L4969: */
			}
/* L4968: */
		    }
		} else {
		    i__1 = nr - 1;
		    i__2 = nr - 1;
		    slaset_("U", &i__1, &i__2, &c_b34, &c_b34, &v[(v_dim1 << 
			    1) + 1], ldv);
		}

/*        Second preconditioning finished; continue with Jacobi SVD */
/*        The input matrix is lower trinagular. */

/*        Recover the right singular vectors as solution of a well */
/*        conditioned triangular matrix equation. */

		if (condr1 < cond_ok__) {

		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    sgesvj_("L", "U", "N", &nr, &nr, &v[v_offset], ldv, &sva[
			    1], &nr, &u[u_offset], ldu, &work[(*n << 1) + *n *
			     nr + nr + 1], &i__1, info);
		    scalem = work[(*n << 1) + *n * nr + nr + 1];
		    numrank = i_nint(&work[(*n << 1) + *n * nr + nr + 2]);
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			scopy_(&nr, &v[p * v_dim1 + 1], &c__1, &u[p * u_dim1 
				+ 1], &c__1);
			sscal_(&nr, &sva[p], &v[p * v_dim1 + 1], &c__1);
/* L3970: */
		    }

		    if (nr == *n) {
/* :))             .. best case, R1 is inverted. The solution of this matrix */
/*                 equation is Q2*V2 = the product of the Jacobi rotations */
/*                 used in SGESVJ, premultiplied with the orthogonal matrix */
/*                 from the second QR factorization. */
			strsm_("L", "U", "N", "N", &nr, &nr, &c_b35, &a[
				a_offset], lda, &v[v_offset], ldv);
		    } else {
/*                 is inverted to get the product of the Jacobi rotations */
/*                 used in SGESVJ. The Q-factor from the second QR */
/*                 factorization is then built in explicitly. */
			strsm_("L", "U", "T", "N", &nr, &nr, &c_b35, &work[(*
				n << 1) + 1], n, &v[v_offset], ldv);
			if (nr < *n) {
			    i__1 = *n - nr;
			    slaset_("A", &i__1, &nr, &c_b34, &c_b34, &v[nr + 
				    1 + v_dim1], ldv);
			    i__1 = *n - nr;
			    slaset_("A", &nr, &i__1, &c_b34, &c_b34, &v[(nr + 
				    1) * v_dim1 + 1], ldv);
			    i__1 = *n - nr;
			    i__2 = *n - nr;
			    slaset_("A", &i__1, &i__2, &c_b34, &c_b35, &v[nr 
				    + 1 + (nr + 1) * v_dim1], ldv);
			}
			i__1 = *lwork - (*n << 1) - *n * nr - nr;
			sormqr_("L", "N", n, n, &nr, &work[(*n << 1) + 1], n, 
				&work[*n + 1], &v[v_offset], ldv, &work[(*n <<
				 1) + *n * nr + nr + 1], &i__1, &ierr);
		    }

		} else if (condr2 < cond_ok__) {

/* :)           .. the input matrix A is very likely a relative of */
/*              the Kahan matrix :) */
/*              The matrix R2 is inverted. The solution of the matrix equation */
/*              is Q3^T*V3 = the product of the Jacobi rotations (appplied to */
/*              the lower triangular L3 from the LQ factorization of */
/*              R2=L3*Q3), pre-multiplied with the transposed Q3. */
		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    sgesvj_("L", "U", "N", &nr, &nr, &v[v_offset], ldv, &sva[
			    1], &nr, &u[u_offset], ldu, &work[(*n << 1) + *n *
			     nr + nr + 1], &i__1, info);
		    scalem = work[(*n << 1) + *n * nr + nr + 1];
		    numrank = i_nint(&work[(*n << 1) + *n * nr + nr + 2]);
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			scopy_(&nr, &v[p * v_dim1 + 1], &c__1, &u[p * u_dim1 
				+ 1], &c__1);
			sscal_(&nr, &sva[p], &u[p * u_dim1 + 1], &c__1);
/* L3870: */
		    }
		    strsm_("L", "U", "N", "N", &nr, &nr, &c_b35, &work[(*n << 
			    1) + 1], n, &u[u_offset], ldu);
		    i__1 = nr;
		    for (q = 1; q <= i__1; ++q) {
			i__2 = nr;
			for (p = 1; p <= i__2; ++p) {
			    work[(*n << 1) + *n * nr + nr + iwork[*n + p]] = 
				    u[p + q * u_dim1];
/* L872: */
			}
			i__2 = nr;
			for (p = 1; p <= i__2; ++p) {
			    u[p + q * u_dim1] = work[(*n << 1) + *n * nr + nr 
				    + p];
/* L874: */
			}
/* L873: */
		    }
		    if (nr < *n) {
			i__1 = *n - nr;
			slaset_("A", &i__1, &nr, &c_b34, &c_b34, &v[nr + 1 + 
				v_dim1], ldv);
			i__1 = *n - nr;
			slaset_("A", &nr, &i__1, &c_b34, &c_b34, &v[(nr + 1) *
				 v_dim1 + 1], ldv);
			i__1 = *n - nr;
			i__2 = *n - nr;
			slaset_("A", &i__1, &i__2, &c_b34, &c_b35, &v[nr + 1 
				+ (nr + 1) * v_dim1], ldv);
		    }
		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    sormqr_("L", "N", n, n, &nr, &work[(*n << 1) + 1], n, &
			    work[*n + 1], &v[v_offset], ldv, &work[(*n << 1) 
			    + *n * nr + nr + 1], &i__1, &ierr);
		} else {
/*              Last line of defense. */
/* #:(          This is a rather pathological case: no scaled condition */
/*              improvement after two pivoted QR factorizations. Other */
/*              possibility is that the rank revealing QR factorization */
/*              or the condition estimator has failed, or the COND_OK */
/*              is set very close to ONE (which is unnecessary). Normally, */
/*              this branch should never be executed, but in rare cases of */
/*              failure of the RRQR or condition estimator, the last line of */
/*              defense ensures that SGEJSV completes the task. */
/*              Compute the full SVD of L3 using SGESVJ with explicit */
/*              accumulation of Jacobi rotations. */
		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    sgesvj_("L", "U", "V", &nr, &nr, &v[v_offset], ldv, &sva[
			    1], &nr, &u[u_offset], ldu, &work[(*n << 1) + *n *
			     nr + nr + 1], &i__1, info);
		    scalem = work[(*n << 1) + *n * nr + nr + 1];
		    numrank = i_nint(&work[(*n << 1) + *n * nr + nr + 2]);
		    if (nr < *n) {
			i__1 = *n - nr;
			slaset_("A", &i__1, &nr, &c_b34, &c_b34, &v[nr + 1 + 
				v_dim1], ldv);
			i__1 = *n - nr;
			slaset_("A", &nr, &i__1, &c_b34, &c_b34, &v[(nr + 1) *
				 v_dim1 + 1], ldv);
			i__1 = *n - nr;
			i__2 = *n - nr;
			slaset_("A", &i__1, &i__2, &c_b34, &c_b35, &v[nr + 1 
				+ (nr + 1) * v_dim1], ldv);
		    }
		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    sormqr_("L", "N", n, n, &nr, &work[(*n << 1) + 1], n, &
			    work[*n + 1], &v[v_offset], ldv, &work[(*n << 1) 
			    + *n * nr + nr + 1], &i__1, &ierr);

		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    sormlq_("L", "T", &nr, &nr, &nr, &work[(*n << 1) + 1], n, 
			    &work[(*n << 1) + *n * nr + 1], &u[u_offset], ldu,
			     &work[(*n << 1) + *n * nr + nr + 1], &i__1, &
			    ierr);
		    i__1 = nr;
		    for (q = 1; q <= i__1; ++q) {
			i__2 = nr;
			for (p = 1; p <= i__2; ++p) {
			    work[(*n << 1) + *n * nr + nr + iwork[*n + p]] = 
				    u[p + q * u_dim1];
/* L772: */
			}
			i__2 = nr;
			for (p = 1; p <= i__2; ++p) {
			    u[p + q * u_dim1] = work[(*n << 1) + *n * nr + nr 
				    + p];
/* L774: */
			}
/* L773: */
		    }

		}

/*           Permute the rows of V using the (column) permutation from the */
/*           first QRF. Also, scale the columns to make them unit in */
/*           Euclidean norm. This applies to all cases. */

		temp1 = sqrt((real) (*n)) * epsln;
		i__1 = *n;
		for (q = 1; q <= i__1; ++q) {
		    i__2 = *n;
		    for (p = 1; p <= i__2; ++p) {
			work[(*n << 1) + *n * nr + nr + iwork[p]] = v[p + q * 
				v_dim1];
/* L972: */
		    }
		    i__2 = *n;
		    for (p = 1; p <= i__2; ++p) {
			v[p + q * v_dim1] = work[(*n << 1) + *n * nr + nr + p]
				;
/* L973: */
		    }
		    xsc = 1.f / snrm2_(n, &v[q * v_dim1 + 1], &c__1);
		    if (xsc < 1.f - temp1 || xsc > temp1 + 1.f) {
			sscal_(n, &xsc, &v[q * v_dim1 + 1], &c__1);
		    }
/* L1972: */
		}
/*           At this moment, V contains the right singular vectors of A. */
/*           Next, assemble the left singular vector matrix U (M x N). */
		if (nr < *m) {
		    i__1 = *m - nr;
		    slaset_("A", &i__1, &nr, &c_b34, &c_b34, &u[nr + 1 + 
			    u_dim1], ldu);
		    if (nr < n1) {
			i__1 = n1 - nr;
			slaset_("A", &nr, &i__1, &c_b34, &c_b34, &u[(nr + 1) *
				 u_dim1 + 1], ldu);
			i__1 = *m - nr;
			i__2 = n1 - nr;
			slaset_("A", &i__1, &i__2, &c_b34, &c_b35, &u[nr + 1 
				+ (nr + 1) * u_dim1], ldu);
		    }
		}

/*           The Q matrix from the first QRF is built into the left singular */
/*           matrix U. This applies to all cases. */

		i__1 = *lwork - *n;
		sormqr_("Left", "No_Tr", m, &n1, n, &a[a_offset], lda, &work[
			1], &u[u_offset], ldu, &work[*n + 1], &i__1, &ierr);
/*           The columns of U are normalized. The cost is O(M*N) flops. */
		temp1 = sqrt((real) (*m)) * epsln;
		i__1 = nr;
		for (p = 1; p <= i__1; ++p) {
		    xsc = 1.f / snrm2_(m, &u[p * u_dim1 + 1], &c__1);
		    if (xsc < 1.f - temp1 || xsc > temp1 + 1.f) {
			sscal_(m, &xsc, &u[p * u_dim1 + 1], &c__1);
		    }
/* L1973: */
		}

/*           If the initial QRF is computed with row pivoting, the left */
/*           singular vectors must be adjusted. */

		if (rowpiv) {
		    i__1 = *m - 1;
		    slaswp_(&n1, &u[u_offset], ldu, &c__1, &i__1, &iwork[(*n 
			    << 1) + 1], &c_n1);
		}

	    } else {

/*        the second QRF is not needed */

		slacpy_("Upper", n, n, &a[a_offset], lda, &work[*n + 1], n);
		if (l2pert) {
		    xsc = sqrt(small);
		    i__1 = *n;
		    for (p = 2; p <= i__1; ++p) {
			temp1 = xsc * work[*n + (p - 1) * *n + p];
			i__2 = p - 1;
			for (q = 1; q <= i__2; ++q) {
			    work[*n + (q - 1) * *n + p] = -r_sign(&temp1, &
				    work[*n + (p - 1) * *n + q]);
/* L5971: */
			}
/* L5970: */
		    }
		} else {
		    i__1 = *n - 1;
		    i__2 = *n - 1;
		    slaset_("Lower", &i__1, &i__2, &c_b34, &c_b34, &work[*n + 
			    2], n);
		}

		i__1 = *lwork - *n - *n * *n;
		sgesvj_("Upper", "U", "N", n, n, &work[*n + 1], n, &sva[1], n,
			 &u[u_offset], ldu, &work[*n + *n * *n + 1], &i__1, 
			info);

		scalem = work[*n + *n * *n + 1];
		numrank = i_nint(&work[*n + *n * *n + 2]);
		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    scopy_(n, &work[*n + (p - 1) * *n + 1], &c__1, &u[p * 
			    u_dim1 + 1], &c__1);
		    sscal_(n, &sva[p], &work[*n + (p - 1) * *n + 1], &c__1);
/* L6970: */
		}

		strsm_("Left", "Upper", "NoTrans", "No UD", n, n, &c_b35, &a[
			a_offset], lda, &work[*n + 1], n);
		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    scopy_(n, &work[*n + p], n, &v[iwork[p] + v_dim1], ldv);
/* L6972: */
		}
		temp1 = sqrt((real) (*n)) * epsln;
		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    xsc = 1.f / snrm2_(n, &v[p * v_dim1 + 1], &c__1);
		    if (xsc < 1.f - temp1 || xsc > temp1 + 1.f) {
			sscal_(n, &xsc, &v[p * v_dim1 + 1], &c__1);
		    }
/* L6971: */
		}

/*           Assemble the left singular vector matrix U (M x N). */

		if (*n < *m) {
		    i__1 = *m - *n;
		    slaset_("A", &i__1, n, &c_b34, &c_b34, &u[*n + 1 + u_dim1]
			    , ldu);
		    if (*n < n1) {
			i__1 = n1 - *n;
			slaset_("A", n, &i__1, &c_b34, &c_b34, &u[(*n + 1) * 
				u_dim1 + 1], ldu);
			i__1 = *m - *n;
			i__2 = n1 - *n;
			slaset_("A", &i__1, &i__2, &c_b34, &c_b35, &u[*n + 1 
				+ (*n + 1) * u_dim1], ldu);
		    }
		}
		i__1 = *lwork - *n;
		sormqr_("Left", "No Tr", m, &n1, n, &a[a_offset], lda, &work[
			1], &u[u_offset], ldu, &work[*n + 1], &i__1, &ierr);
		temp1 = sqrt((real) (*m)) * epsln;
		i__1 = n1;
		for (p = 1; p <= i__1; ++p) {
		    xsc = 1.f / snrm2_(m, &u[p * u_dim1 + 1], &c__1);
		    if (xsc < 1.f - temp1 || xsc > temp1 + 1.f) {
			sscal_(m, &xsc, &u[p * u_dim1 + 1], &c__1);
		    }
/* L6973: */
		}

		if (rowpiv) {
		    i__1 = *m - 1;
		    slaswp_(&n1, &u[u_offset], ldu, &c__1, &i__1, &iwork[(*n 
			    << 1) + 1], &c_n1);
		}

	    }

/*        end of the  >> almost orthogonal case <<  in the full SVD */

	} else {

/*        This branch deploys a preconditioned Jacobi SVD with explicitly */
/*        accumulated rotations. It is included as optional, mainly for */
/*        experimental purposes. It does perform well, and can also be used. */
/*        In this implementation, this branch will be automatically activated */
/*        if the  condition number sigma_max(A) / sigma_min(A) is predicted */
/*        to be greater than the overflow threshold. This is because the */
/*        a posteriori computation of the singular vectors assumes robust */
/*        implementation of BLAS and some LAPACK procedures, capable of working */
/*        in presence of extreme values. Since that is not always the case, ... */

	    i__1 = nr;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = *n - p + 1;
		scopy_(&i__2, &a[p + p * a_dim1], lda, &v[p + p * v_dim1], &
			c__1);
/* L7968: */
	    }

	    if (l2pert) {
		xsc = sqrt(small / epsln);
		i__1 = nr;
		for (q = 1; q <= i__1; ++q) {
		    temp1 = xsc * (r__1 = v[q + q * v_dim1], abs(r__1));
		    i__2 = *n;
		    for (p = 1; p <= i__2; ++p) {
			if (p > q && (r__1 = v[p + q * v_dim1], abs(r__1)) <= 
				temp1 || p < q) {
			    v[p + q * v_dim1] = r_sign(&temp1, &v[p + q * 
				    v_dim1]);
			}
			if (p < q) {
			    v[p + q * v_dim1] = -v[p + q * v_dim1];
			}
/* L5968: */
		    }
/* L5969: */
		}
	    } else {
		i__1 = nr - 1;
		i__2 = nr - 1;
		slaset_("U", &i__1, &i__2, &c_b34, &c_b34, &v[(v_dim1 << 1) + 
			1], ldv);
	    }
	    i__1 = *lwork - (*n << 1);
	    sgeqrf_(n, &nr, &v[v_offset], ldv, &work[*n + 1], &work[(*n << 1) 
		    + 1], &i__1, &ierr);
	    slacpy_("L", n, &nr, &v[v_offset], ldv, &work[(*n << 1) + 1], n);

	    i__1 = nr;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = nr - p + 1;
		scopy_(&i__2, &v[p + p * v_dim1], ldv, &u[p + p * u_dim1], &
			c__1);
/* L7969: */
	    }
	    if (l2pert) {
		xsc = sqrt(small / epsln);
		i__1 = nr;
		for (q = 2; q <= i__1; ++q) {
		    i__2 = q - 1;
		    for (p = 1; p <= i__2; ++p) {
/* Computing MIN */
			r__3 = (r__1 = u[p + p * u_dim1], abs(r__1)), r__4 = (
				r__2 = u[q + q * u_dim1], abs(r__2));
			temp1 = xsc * f2cmin(r__3,r__4);
			u[p + q * u_dim1] = -r_sign(&temp1, &u[q + p * u_dim1]
				);
/* L9971: */
		    }
/* L9970: */
		}
	    } else {
		i__1 = nr - 1;
		i__2 = nr - 1;
		slaset_("U", &i__1, &i__2, &c_b34, &c_b34, &u[(u_dim1 << 1) + 
			1], ldu);
	    }
	    i__1 = *lwork - (*n << 1) - *n * nr;
	    sgesvj_("L", "U", "V", &nr, &nr, &u[u_offset], ldu, &sva[1], n, &
		    v[v_offset], ldv, &work[(*n << 1) + *n * nr + 1], &i__1, 
		    info);
	    scalem = work[(*n << 1) + *n * nr + 1];
	    numrank = i_nint(&work[(*n << 1) + *n * nr + 2]);
	    if (nr < *n) {
		i__1 = *n - nr;
		slaset_("A", &i__1, &nr, &c_b34, &c_b34, &v[nr + 1 + v_dim1], 
			ldv);
		i__1 = *n - nr;
		slaset_("A", &nr, &i__1, &c_b34, &c_b34, &v[(nr + 1) * v_dim1 
			+ 1], ldv);
		i__1 = *n - nr;
		i__2 = *n - nr;
		slaset_("A", &i__1, &i__2, &c_b34, &c_b35, &v[nr + 1 + (nr + 
			1) * v_dim1], ldv);
	    }
	    i__1 = *lwork - (*n << 1) - *n * nr - nr;
	    sormqr_("L", "N", n, n, &nr, &work[(*n << 1) + 1], n, &work[*n + 
		    1], &v[v_offset], ldv, &work[(*n << 1) + *n * nr + nr + 1]
		    , &i__1, &ierr);

/*           Permute the rows of V using the (column) permutation from the */
/*           first QRF. Also, scale the columns to make them unit in */
/*           Euclidean norm. This applies to all cases. */

	    temp1 = sqrt((real) (*n)) * epsln;
	    i__1 = *n;
	    for (q = 1; q <= i__1; ++q) {
		i__2 = *n;
		for (p = 1; p <= i__2; ++p) {
		    work[(*n << 1) + *n * nr + nr + iwork[p]] = v[p + q * 
			    v_dim1];
/* L8972: */
		}
		i__2 = *n;
		for (p = 1; p <= i__2; ++p) {
		    v[p + q * v_dim1] = work[(*n << 1) + *n * nr + nr + p];
/* L8973: */
		}
		xsc = 1.f / snrm2_(n, &v[q * v_dim1 + 1], &c__1);
		if (xsc < 1.f - temp1 || xsc > temp1 + 1.f) {
		    sscal_(n, &xsc, &v[q * v_dim1 + 1], &c__1);
		}
/* L7972: */
	    }

/*           At this moment, V contains the right singular vectors of A. */
/*           Next, assemble the left singular vector matrix U (M x N). */

	    if (nr < *m) {
		i__1 = *m - nr;
		slaset_("A", &i__1, &nr, &c_b34, &c_b34, &u[nr + 1 + u_dim1], 
			ldu);
		if (nr < n1) {
		    i__1 = n1 - nr;
		    slaset_("A", &nr, &i__1, &c_b34, &c_b34, &u[(nr + 1) * 
			    u_dim1 + 1], ldu);
		    i__1 = *m - nr;
		    i__2 = n1 - nr;
		    slaset_("A", &i__1, &i__2, &c_b34, &c_b35, &u[nr + 1 + (
			    nr + 1) * u_dim1], ldu);
		}
	    }

	    i__1 = *lwork - *n;
	    sormqr_("Left", "No Tr", m, &n1, n, &a[a_offset], lda, &work[1], &
		    u[u_offset], ldu, &work[*n + 1], &i__1, &ierr);

	    if (rowpiv) {
		i__1 = *m - 1;
		slaswp_(&n1, &u[u_offset], ldu, &c__1, &i__1, &iwork[(*n << 1)
			 + 1], &c_n1);
	    }


	}
	if (transp) {
	    i__1 = *n;
	    for (p = 1; p <= i__1; ++p) {
		sswap_(n, &u[p * u_dim1 + 1], &c__1, &v[p * v_dim1 + 1], &
			c__1);
/* L6974: */
	    }
	}

    }
/*     end of the full SVD */

/*     Undo scaling, if necessary (and possible) */

    if (uscal2 <= big / sva[1] * uscal1) {
	slascl_("G", &c__0, &c__0, &uscal1, &uscal2, &nr, &c__1, &sva[1], n, &
		ierr);
	uscal1 = 1.f;
	uscal2 = 1.f;
    }

    if (nr < *n) {
	i__1 = *n;
	for (p = nr + 1; p <= i__1; ++p) {
	    sva[p] = 0.f;
/* L3004: */
	}
    }

    work[1] = uscal2 * scalem;
    work[2] = uscal1;
    if (errest) {
	work[3] = sconda;
    }
    if (lsvec && rsvec) {
	work[4] = condr1;
	work[5] = condr2;
    }
    if (l2tran) {
	work[6] = entra;
	work[7] = entrat;
    }

    iwork[1] = nr;
    iwork[2] = numrank;
    iwork[3] = warning;

    return;
} /* sgejsv_ */

