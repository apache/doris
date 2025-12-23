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
static doublereal c_b35 = 10.;
static doublereal c_b71 = .5;

/* > \brief \b DGGBAL */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DGGBAL + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dggbal.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dggbal.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dggbal.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DGGBAL( JOB, N, A, LDA, B, LDB, ILO, IHI, LSCALE, */
/*                          RSCALE, WORK, INFO ) */

/*       CHARACTER          JOB */
/*       INTEGER            IHI, ILO, INFO, LDA, LDB, N */
/*       DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), LSCALE( * ), */
/*      $                   RSCALE( * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DGGBAL balances a pair of general real matrices (A,B).  This */
/* > involves, first, permuting A and B by similarity transformations to */
/* > isolate eigenvalues in the first 1 to ILO$-$1 and last IHI+1 to N */
/* > elements on the diagonal; and second, applying a diagonal similarity */
/* > transformation to rows and columns ILO to IHI to make the rows */
/* > and columns as close in norm as possible. Both steps are optional. */
/* > */
/* > Balancing may reduce the 1-norm of the matrices, and improve the */
/* > accuracy of the computed eigenvalues and/or eigenvectors in the */
/* > generalized eigenvalue problem A*x = lambda*B*x. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOB */
/* > \verbatim */
/* >          JOB is CHARACTER*1 */
/* >          Specifies the operations to be performed on A and B: */
/* >          = 'N':  none:  simply set ILO = 1, IHI = N, LSCALE(I) = 1.0 */
/* >                  and RSCALE(I) = 1.0 for i = 1,...,N. */
/* >          = 'P':  permute only; */
/* >          = 'S':  scale only; */
/* >          = 'B':  both permute and scale. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices A and B.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is DOUBLE PRECISION array, dimension (LDA,N) */
/* >          On entry, the input matrix A. */
/* >          On exit,  A is overwritten by the balanced matrix. */
/* >          If JOB = 'N', A is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A. LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is DOUBLE PRECISION array, dimension (LDB,N) */
/* >          On entry, the input matrix B. */
/* >          On exit,  B is overwritten by the balanced matrix. */
/* >          If JOB = 'N', B is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B. LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] ILO */
/* > \verbatim */
/* >          ILO is INTEGER */
/* > \endverbatim */
/* > */
/* > \param[out] IHI */
/* > \verbatim */
/* >          IHI is INTEGER */
/* >          ILO and IHI are set to integers such that on exit */
/* >          A(i,j) = 0 and B(i,j) = 0 if i > j and */
/* >          j = 1,...,ILO-1 or i = IHI+1,...,N. */
/* >          If JOB = 'N' or 'S', ILO = 1 and IHI = N. */
/* > \endverbatim */
/* > */
/* > \param[out] LSCALE */
/* > \verbatim */
/* >          LSCALE is DOUBLE PRECISION array, dimension (N) */
/* >          Details of the permutations and scaling factors applied */
/* >          to the left side of A and B.  If P(j) is the index of the */
/* >          row interchanged with row j, and D(j) */
/* >          is the scaling factor applied to row j, then */
/* >            LSCALE(j) = P(j)    for J = 1,...,ILO-1 */
/* >                      = D(j)    for J = ILO,...,IHI */
/* >                      = P(j)    for J = IHI+1,...,N. */
/* >          The order in which the interchanges are made is N to IHI+1, */
/* >          then 1 to ILO-1. */
/* > \endverbatim */
/* > */
/* > \param[out] RSCALE */
/* > \verbatim */
/* >          RSCALE is DOUBLE PRECISION array, dimension (N) */
/* >          Details of the permutations and scaling factors applied */
/* >          to the right side of A and B.  If P(j) is the index of the */
/* >          column interchanged with column j, and D(j) */
/* >          is the scaling factor applied to column j, then */
/* >            LSCALE(j) = P(j)    for J = 1,...,ILO-1 */
/* >                      = D(j)    for J = ILO,...,IHI */
/* >                      = P(j)    for J = IHI+1,...,N. */
/* >          The order in which the interchanges are made is N to IHI+1, */
/* >          then 1 to ILO-1. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension (lwork) */
/* >          lwork must be at least f2cmax(1,6*N) when JOB = 'S' or 'B', and */
/* >          at least 1 when JOB = 'N' or 'P'. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleGBcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  See R.C. WARD, Balancing the generalized eigenvalue problem, */
/* >                 SIAM J. Sci. Stat. Comp. 2 (1981), 141-152. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void dggbal_(char *job, integer *n, doublereal *a, integer *
	lda, doublereal *b, integer *ldb, integer *ilo, integer *ihi, 
	doublereal *lscale, doublereal *rscale, doublereal *work, integer *
	info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, i__1, i__2, i__3;
    doublereal d__1, d__2, d__3;

    /* Local variables */
    integer lcab;
    doublereal beta, coef;
    integer irab, lrab;
    doublereal basl, cmax;
    extern doublereal ddot_(integer *, doublereal *, integer *, doublereal *, 
	    integer *);
    doublereal coef2, coef5;
    integer i__, j, k, l, m;
    doublereal gamma, t, alpha;
    extern /* Subroutine */ void dscal_(integer *, doublereal *, doublereal *, 
	    integer *);
    extern logical lsame_(char *, char *);
    doublereal sfmin, sfmax;
    extern /* Subroutine */ void dswap_(integer *, doublereal *, integer *, 
	    doublereal *, integer *);
    integer iflow;
    extern /* Subroutine */ void daxpy_(integer *, doublereal *, doublereal *, 
	    integer *, doublereal *, integer *);
    integer kount, jc;
    doublereal ta, tb, tc;
    extern doublereal dlamch_(char *);
    integer ir, it;
    doublereal ew;
    integer nr;
    doublereal pgamma;
    extern integer idamax_(integer *, doublereal *, integer *);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    integer lsfmin, lsfmax, ip1, jp1, lm1;
    doublereal cab, rab, ewc, cor, sum;
    integer nrp2, icab;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input parameters */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --lscale;
    --rscale;
    --work;

    /* Function Body */
    *info = 0;
    if (! lsame_(job, "N") && ! lsame_(job, "P") && ! lsame_(job, "S") 
	    && ! lsame_(job, "B")) {
	*info = -1;
    } else if (*n < 0) {
	*info = -2;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -4;
    } else if (*ldb < f2cmax(1,*n)) {
	*info = -6;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DGGBAL", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	*ilo = 1;
	*ihi = *n;
	return;
    }

    if (*n == 1) {
	*ilo = 1;
	*ihi = *n;
	lscale[1] = 1.;
	rscale[1] = 1.;
	return;
    }

    if (lsame_(job, "N")) {
	*ilo = 1;
	*ihi = *n;
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    lscale[i__] = 1.;
	    rscale[i__] = 1.;
/* L10: */
	}
	return;
    }

    k = 1;
    l = *n;
    if (lsame_(job, "S")) {
	goto L190;
    }

    goto L30;

/*     Permute the matrices A and B to isolate the eigenvalues. */

/*     Find row with one nonzero in columns 1 through L */

L20:
    l = lm1;
    if (l != 1) {
	goto L30;
    }

    rscale[1] = 1.;
    lscale[1] = 1.;
    goto L190;

L30:
    lm1 = l - 1;
    for (i__ = l; i__ >= 1; --i__) {
	i__1 = lm1;
	for (j = 1; j <= i__1; ++j) {
	    jp1 = j + 1;
	    if (a[i__ + j * a_dim1] != 0. || b[i__ + j * b_dim1] != 0.) {
		goto L50;
	    }
/* L40: */
	}
	j = l;
	goto L70;

L50:
	i__1 = l;
	for (j = jp1; j <= i__1; ++j) {
	    if (a[i__ + j * a_dim1] != 0. || b[i__ + j * b_dim1] != 0.) {
		goto L80;
	    }
/* L60: */
	}
	j = jp1 - 1;

L70:
	m = l;
	iflow = 1;
	goto L160;
L80:
	;
    }
    goto L100;

/*     Find column with one nonzero in rows K through N */

L90:
    ++k;

L100:
    i__1 = l;
    for (j = k; j <= i__1; ++j) {
	i__2 = lm1;
	for (i__ = k; i__ <= i__2; ++i__) {
	    ip1 = i__ + 1;
	    if (a[i__ + j * a_dim1] != 0. || b[i__ + j * b_dim1] != 0.) {
		goto L120;
	    }
/* L110: */
	}
	i__ = l;
	goto L140;
L120:
	i__2 = l;
	for (i__ = ip1; i__ <= i__2; ++i__) {
	    if (a[i__ + j * a_dim1] != 0. || b[i__ + j * b_dim1] != 0.) {
		goto L150;
	    }
/* L130: */
	}
	i__ = ip1 - 1;
L140:
	m = k;
	iflow = 2;
	goto L160;
L150:
	;
    }
    goto L190;

/*     Permute rows M and I */

L160:
    lscale[m] = (doublereal) i__;
    if (i__ == m) {
	goto L170;
    }
    i__1 = *n - k + 1;
    dswap_(&i__1, &a[i__ + k * a_dim1], lda, &a[m + k * a_dim1], lda);
    i__1 = *n - k + 1;
    dswap_(&i__1, &b[i__ + k * b_dim1], ldb, &b[m + k * b_dim1], ldb);

/*     Permute columns M and J */

L170:
    rscale[m] = (doublereal) j;
    if (j == m) {
	goto L180;
    }
    dswap_(&l, &a[j * a_dim1 + 1], &c__1, &a[m * a_dim1 + 1], &c__1);
    dswap_(&l, &b[j * b_dim1 + 1], &c__1, &b[m * b_dim1 + 1], &c__1);

L180:
    switch (iflow) {
	case 1:  goto L20;
	case 2:  goto L90;
    }

L190:
    *ilo = k;
    *ihi = l;

    if (lsame_(job, "P")) {
	i__1 = *ihi;
	for (i__ = *ilo; i__ <= i__1; ++i__) {
	    lscale[i__] = 1.;
	    rscale[i__] = 1.;
/* L195: */
	}
	return;
    }

    if (*ilo == *ihi) {
	return;
    }

/*     Balance the submatrix in rows ILO to IHI. */

    nr = *ihi - *ilo + 1;
    i__1 = *ihi;
    for (i__ = *ilo; i__ <= i__1; ++i__) {
	rscale[i__] = 0.;
	lscale[i__] = 0.;

	work[i__] = 0.;
	work[i__ + *n] = 0.;
	work[i__ + (*n << 1)] = 0.;
	work[i__ + *n * 3] = 0.;
	work[i__ + (*n << 2)] = 0.;
	work[i__ + *n * 5] = 0.;
/* L200: */
    }

/*     Compute right side vector in resulting linear equations */

    basl = d_lg10(&c_b35);
    i__1 = *ihi;
    for (i__ = *ilo; i__ <= i__1; ++i__) {
	i__2 = *ihi;
	for (j = *ilo; j <= i__2; ++j) {
	    tb = b[i__ + j * b_dim1];
	    ta = a[i__ + j * a_dim1];
	    if (ta == 0.) {
		goto L210;
	    }
	    d__1 = abs(ta);
	    ta = d_lg10(&d__1) / basl;
L210:
	    if (tb == 0.) {
		goto L220;
	    }
	    d__1 = abs(tb);
	    tb = d_lg10(&d__1) / basl;
L220:
	    work[i__ + (*n << 2)] = work[i__ + (*n << 2)] - ta - tb;
	    work[j + *n * 5] = work[j + *n * 5] - ta - tb;
/* L230: */
	}
/* L240: */
    }

    coef = 1. / (doublereal) (nr << 1);
    coef2 = coef * coef;
    coef5 = coef2 * .5;
    nrp2 = nr + 2;
    beta = 0.;
    it = 1;

/*     Start generalized conjugate gradient iteration */

L250:

    gamma = ddot_(&nr, &work[*ilo + (*n << 2)], &c__1, &work[*ilo + (*n << 2)]
	    , &c__1) + ddot_(&nr, &work[*ilo + *n * 5], &c__1, &work[*ilo + *
	    n * 5], &c__1);

    ew = 0.;
    ewc = 0.;
    i__1 = *ihi;
    for (i__ = *ilo; i__ <= i__1; ++i__) {
	ew += work[i__ + (*n << 2)];
	ewc += work[i__ + *n * 5];
/* L260: */
    }

/* Computing 2nd power */
    d__1 = ew;
/* Computing 2nd power */
    d__2 = ewc;
/* Computing 2nd power */
    d__3 = ew - ewc;
    gamma = coef * gamma - coef2 * (d__1 * d__1 + d__2 * d__2) - coef5 * (
	    d__3 * d__3);
    if (gamma == 0.) {
	goto L350;
    }
    if (it != 1) {
	beta = gamma / pgamma;
    }
    t = coef5 * (ewc - ew * 3.);
    tc = coef5 * (ew - ewc * 3.);

    dscal_(&nr, &beta, &work[*ilo], &c__1);
    dscal_(&nr, &beta, &work[*ilo + *n], &c__1);

    daxpy_(&nr, &coef, &work[*ilo + (*n << 2)], &c__1, &work[*ilo + *n], &
	    c__1);
    daxpy_(&nr, &coef, &work[*ilo + *n * 5], &c__1, &work[*ilo], &c__1);

    i__1 = *ihi;
    for (i__ = *ilo; i__ <= i__1; ++i__) {
	work[i__] += tc;
	work[i__ + *n] += t;
/* L270: */
    }

/*     Apply matrix to vector */

    i__1 = *ihi;
    for (i__ = *ilo; i__ <= i__1; ++i__) {
	kount = 0;
	sum = 0.;
	i__2 = *ihi;
	for (j = *ilo; j <= i__2; ++j) {
	    if (a[i__ + j * a_dim1] == 0.) {
		goto L280;
	    }
	    ++kount;
	    sum += work[j];
L280:
	    if (b[i__ + j * b_dim1] == 0.) {
		goto L290;
	    }
	    ++kount;
	    sum += work[j];
L290:
	    ;
	}
	work[i__ + (*n << 1)] = (doublereal) kount * work[i__ + *n] + sum;
/* L300: */
    }

    i__1 = *ihi;
    for (j = *ilo; j <= i__1; ++j) {
	kount = 0;
	sum = 0.;
	i__2 = *ihi;
	for (i__ = *ilo; i__ <= i__2; ++i__) {
	    if (a[i__ + j * a_dim1] == 0.) {
		goto L310;
	    }
	    ++kount;
	    sum += work[i__ + *n];
L310:
	    if (b[i__ + j * b_dim1] == 0.) {
		goto L320;
	    }
	    ++kount;
	    sum += work[i__ + *n];
L320:
	    ;
	}
	work[j + *n * 3] = (doublereal) kount * work[j] + sum;
/* L330: */
    }

    sum = ddot_(&nr, &work[*ilo + *n], &c__1, &work[*ilo + (*n << 1)], &c__1) 
	    + ddot_(&nr, &work[*ilo], &c__1, &work[*ilo + *n * 3], &c__1);
    alpha = gamma / sum;

/*     Determine correction to current iteration */

    cmax = 0.;
    i__1 = *ihi;
    for (i__ = *ilo; i__ <= i__1; ++i__) {
	cor = alpha * work[i__ + *n];
	if (abs(cor) > cmax) {
	    cmax = abs(cor);
	}
	lscale[i__] += cor;
	cor = alpha * work[i__];
	if (abs(cor) > cmax) {
	    cmax = abs(cor);
	}
	rscale[i__] += cor;
/* L340: */
    }
    if (cmax < .5) {
	goto L350;
    }

    d__1 = -alpha;
    daxpy_(&nr, &d__1, &work[*ilo + (*n << 1)], &c__1, &work[*ilo + (*n << 2)]
	    , &c__1);
    d__1 = -alpha;
    daxpy_(&nr, &d__1, &work[*ilo + *n * 3], &c__1, &work[*ilo + *n * 5], &
	    c__1);

    pgamma = gamma;
    ++it;
    if (it <= nrp2) {
	goto L250;
    }

/*     End generalized conjugate gradient iteration */

L350:
    sfmin = dlamch_("S");
    sfmax = 1. / sfmin;
    lsfmin = (integer) (d_lg10(&sfmin) / basl + 1.);
    lsfmax = (integer) (d_lg10(&sfmax) / basl);
    i__1 = *ihi;
    for (i__ = *ilo; i__ <= i__1; ++i__) {
	i__2 = *n - *ilo + 1;
	irab = idamax_(&i__2, &a[i__ + *ilo * a_dim1], lda);
	rab = (d__1 = a[i__ + (irab + *ilo - 1) * a_dim1], abs(d__1));
	i__2 = *n - *ilo + 1;
	irab = idamax_(&i__2, &b[i__ + *ilo * b_dim1], ldb);
/* Computing MAX */
	d__2 = rab, d__3 = (d__1 = b[i__ + (irab + *ilo - 1) * b_dim1], abs(
		d__1));
	rab = f2cmax(d__2,d__3);
	d__1 = rab + sfmin;
	lrab = (integer) (d_lg10(&d__1) / basl + 1.);
	ir = (integer) (lscale[i__] + d_sign(&c_b71, &lscale[i__]));
/* Computing MIN */
	i__2 = f2cmax(ir,lsfmin), i__2 = f2cmin(i__2,lsfmax), i__3 = lsfmax - lrab;
	ir = f2cmin(i__2,i__3);
	lscale[i__] = pow_di(&c_b35, &ir);
	icab = idamax_(ihi, &a[i__ * a_dim1 + 1], &c__1);
	cab = (d__1 = a[icab + i__ * a_dim1], abs(d__1));
	icab = idamax_(ihi, &b[i__ * b_dim1 + 1], &c__1);
/* Computing MAX */
	d__2 = cab, d__3 = (d__1 = b[icab + i__ * b_dim1], abs(d__1));
	cab = f2cmax(d__2,d__3);
	d__1 = cab + sfmin;
	lcab = (integer) (d_lg10(&d__1) / basl + 1.);
	jc = (integer) (rscale[i__] + d_sign(&c_b71, &rscale[i__]));
/* Computing MIN */
	i__2 = f2cmax(jc,lsfmin), i__2 = f2cmin(i__2,lsfmax), i__3 = lsfmax - lcab;
	jc = f2cmin(i__2,i__3);
	rscale[i__] = pow_di(&c_b35, &jc);
/* L360: */
    }

/*     Row scaling of matrices A and B */

    i__1 = *ihi;
    for (i__ = *ilo; i__ <= i__1; ++i__) {
	i__2 = *n - *ilo + 1;
	dscal_(&i__2, &lscale[i__], &a[i__ + *ilo * a_dim1], lda);
	i__2 = *n - *ilo + 1;
	dscal_(&i__2, &lscale[i__], &b[i__ + *ilo * b_dim1], ldb);
/* L370: */
    }

/*     Column scaling of matrices A and B */

    i__1 = *ihi;
    for (j = *ilo; j <= i__1; ++j) {
	dscal_(ihi, &rscale[j], &a[j * a_dim1 + 1], &c__1);
	dscal_(ihi, &rscale[j], &b[j * b_dim1 + 1], &c__1);
/* L380: */
    }

    return;

/*     End of DGGBAL */

} /* dggbal_ */

