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
#define z_div(c, a, b) {Cd(c)._Val[0] = (Cd(a)._Val[0]/Cd(b)._Val[0]); Cd(c)._Val[1]=(Cd(a)._Val[1]/Cd(b)._Val[1]);}
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
#define mycycle_() continue;
#define myceiling_(w) {ceil(w)}
#define myhuge_(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc_(w,s,e,n) {dmaxloc_(w,*(s),*(e),n)}

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

static doublereal c_b1 = 0.;
static doublereal c_b15 = 1.;
static integer c__1 = 1;
static doublereal c_b44 = -1.;

/* > \brief \b DTGSJA */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DTGSJA + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtgsja.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtgsja.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtgsja.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DTGSJA( JOBU, JOBV, JOBQ, M, P, N, K, L, A, LDA, B, */
/*                          LDB, TOLA, TOLB, ALPHA, BETA, U, LDU, V, LDV, */
/*                          Q, LDQ, WORK, NCALL MYCYCLE, INFO ) */

/*       CHARACTER          JOBQ, JOBU, JOBV */
/*       INTEGER            INFO, K, L, LDA, LDB, LDQ, LDU, LDV, M, N, */
/*      $                   NCALL MYCYCLE, P */
/*       DOUBLE PRECISION   TOLA, TOLB */
/*       DOUBLE PRECISION   A( LDA, * ), ALPHA( * ), B( LDB, * ), */
/*      $                   BETA( * ), Q( LDQ, * ), U( LDU, * ), */
/*      $                   V( LDV, * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DTGSJA computes the generalized singular value decomposition (GSVD) */
/* > of two real upper triangular (or trapezoidal) matrices A and B. */
/* > */
/* > On entry, it is assumed that matrices A and B have the following */
/* > forms, which may be obtained by the preprocessing subroutine DGGSVP */
/* > from a general M-by-N matrix A and P-by-N matrix B: */
/* > */
/* >              N-K-L  K    L */
/* >    A =    K ( 0    A12  A13 ) if M-K-L >= 0; */
/* >           L ( 0     0   A23 ) */
/* >       M-K-L ( 0     0    0  ) */
/* > */
/* >            N-K-L  K    L */
/* >    A =  K ( 0    A12  A13 ) if M-K-L < 0; */
/* >       M-K ( 0     0   A23 ) */
/* > */
/* >            N-K-L  K    L */
/* >    B =  L ( 0     0   B13 ) */
/* >       P-L ( 0     0    0  ) */
/* > */
/* > where the K-by-K matrix A12 and L-by-L matrix B13 are nonsingular */
/* > upper triangular; A23 is L-by-L upper triangular if M-K-L >= 0, */
/* > otherwise A23 is (M-K)-by-L upper trapezoidal. */
/* > */
/* > On exit, */
/* > */
/* >        U**T *A*Q = D1*( 0 R ),    V**T *B*Q = D2*( 0 R ), */
/* > */
/* > where U, V and Q are orthogonal matrices. */
/* > R is a nonsingular upper triangular matrix, and D1 and D2 are */
/* > ``diagonal'' matrices, which are of the following structures: */
/* > */
/* > If M-K-L >= 0, */
/* > */
/* >                     K  L */
/* >        D1 =     K ( I  0 ) */
/* >                 L ( 0  C ) */
/* >             M-K-L ( 0  0 ) */
/* > */
/* >                   K  L */
/* >        D2 = L   ( 0  S ) */
/* >             P-L ( 0  0 ) */
/* > */
/* >                N-K-L  K    L */
/* >   ( 0 R ) = K (  0   R11  R12 ) K */
/* >             L (  0    0   R22 ) L */
/* > */
/* > where */
/* > */
/* >   C = diag( ALPHA(K+1), ... , ALPHA(K+L) ), */
/* >   S = diag( BETA(K+1),  ... , BETA(K+L) ), */
/* >   C**2 + S**2 = I. */
/* > */
/* >   R is stored in A(1:K+L,N-K-L+1:N) on exit. */
/* > */
/* > If M-K-L < 0, */
/* > */
/* >                K M-K K+L-M */
/* >     D1 =   K ( I  0    0   ) */
/* >          M-K ( 0  C    0   ) */
/* > */
/* >                  K M-K K+L-M */
/* >     D2 =   M-K ( 0  S    0   ) */
/* >          K+L-M ( 0  0    I   ) */
/* >            P-L ( 0  0    0   ) */
/* > */
/* >                N-K-L  K   M-K  K+L-M */
/* > ( 0 R ) =    K ( 0    R11  R12  R13  ) */
/* >           M-K ( 0     0   R22  R23  ) */
/* >         K+L-M ( 0     0    0   R33  ) */
/* > */
/* > where */
/* > C = diag( ALPHA(K+1), ... , ALPHA(M) ), */
/* > S = diag( BETA(K+1),  ... , BETA(M) ), */
/* > C**2 + S**2 = I. */
/* > */
/* > R = ( R11 R12 R13 ) is stored in A(1:M, N-K-L+1:N) and R33 is stored */
/* >     (  0  R22 R23 ) */
/* > in B(M-K+1:L,N+M-K-L+1:N) on exit. */
/* > */
/* > The computation of the orthogonal transformation matrices U, V or Q */
/* > is optional.  These matrices may either be formed explicitly, or they */
/* > may be postmultiplied into input matrices U1, V1, or Q1. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOBU */
/* > \verbatim */
/* >          JOBU is CHARACTER*1 */
/* >          = 'U':  U must contain an orthogonal matrix U1 on entry, and */
/* >                  the product U1*U is returned; */
/* >          = 'I':  U is initialized to the unit matrix, and the */
/* >                  orthogonal matrix U is returned; */
/* >          = 'N':  U is not computed. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBV */
/* > \verbatim */
/* >          JOBV is CHARACTER*1 */
/* >          = 'V':  V must contain an orthogonal matrix V1 on entry, and */
/* >                  the product V1*V is returned; */
/* >          = 'I':  V is initialized to the unit matrix, and the */
/* >                  orthogonal matrix V is returned; */
/* >          = 'N':  V is not computed. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBQ */
/* > \verbatim */
/* >          JOBQ is CHARACTER*1 */
/* >          = 'Q':  Q must contain an orthogonal matrix Q1 on entry, and */
/* >                  the product Q1*Q is returned; */
/* >          = 'I':  Q is initialized to the unit matrix, and the */
/* >                  orthogonal matrix Q is returned; */
/* >          = 'N':  Q is not computed. */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the matrix A.  M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] P */
/* > \verbatim */
/* >          P is INTEGER */
/* >          The number of rows of the matrix B.  P >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the matrices A and B.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] K */
/* > \verbatim */
/* >          K is INTEGER */
/* > \endverbatim */
/* > */
/* > \param[in] L */
/* > \verbatim */
/* >          L is INTEGER */
/* > */
/* >          K and L specify the subblocks in the input matrices A and B: */
/* >          A23 = A(K+1:MIN(K+L,M),N-L+1:N) and B13 = B(1:L,N-L+1:N) */
/* >          of A and B, whose GSVD is going to be computed by DTGSJA. */
/* >          See Further Details. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is DOUBLE PRECISION array, dimension (LDA,N) */
/* >          On entry, the M-by-N matrix A. */
/* >          On exit, A(N-K+1:N,1:MIN(K+L,M) ) contains the triangular */
/* >          matrix R or part of R.  See Purpose for details. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A. LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is DOUBLE PRECISION array, dimension (LDB,N) */
/* >          On entry, the P-by-N matrix B. */
/* >          On exit, if necessary, B(M-K+1:L,N+M-K-L+1:N) contains */
/* >          a part of R.  See Purpose for details. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B. LDB >= f2cmax(1,P). */
/* > \endverbatim */
/* > */
/* > \param[in] TOLA */
/* > \verbatim */
/* >          TOLA is DOUBLE PRECISION */
/* > \endverbatim */
/* > */
/* > \param[in] TOLB */
/* > \verbatim */
/* >          TOLB is DOUBLE PRECISION */
/* > */
/* >          TOLA and TOLB are the convergence criteria for the Jacobi- */
/* >          Kogbetliantz iteration procedure. Generally, they are the */
/* >          same as used in the preprocessing step, say */
/* >              TOLA = f2cmax(M,N)*norm(A)*MAZHEPS, */
/* >              TOLB = f2cmax(P,N)*norm(B)*MAZHEPS. */
/* > \endverbatim */
/* > */
/* > \param[out] ALPHA */
/* > \verbatim */
/* >          ALPHA is DOUBLE PRECISION array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] BETA */
/* > \verbatim */
/* >          BETA is DOUBLE PRECISION array, dimension (N) */
/* > */
/* >          On exit, ALPHA and BETA contain the generalized singular */
/* >          value pairs of A and B; */
/* >            ALPHA(1:K) = 1, */
/* >            BETA(1:K)  = 0, */
/* >          and if M-K-L >= 0, */
/* >            ALPHA(K+1:K+L) = diag(C), */
/* >            BETA(K+1:K+L)  = diag(S), */
/* >          or if M-K-L < 0, */
/* >            ALPHA(K+1:M)= C, ALPHA(M+1:K+L)= 0 */
/* >            BETA(K+1:M) = S, BETA(M+1:K+L) = 1. */
/* >          Furthermore, if K+L < N, */
/* >            ALPHA(K+L+1:N) = 0 and */
/* >            BETA(K+L+1:N)  = 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] U */
/* > \verbatim */
/* >          U is DOUBLE PRECISION array, dimension (LDU,M) */
/* >          On entry, if JOBU = 'U', U must contain a matrix U1 (usually */
/* >          the orthogonal matrix returned by DGGSVP). */
/* >          On exit, */
/* >          if JOBU = 'I', U contains the orthogonal matrix U; */
/* >          if JOBU = 'U', U contains the product U1*U. */
/* >          If JOBU = 'N', U is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU */
/* > \verbatim */
/* >          LDU is INTEGER */
/* >          The leading dimension of the array U. LDU >= f2cmax(1,M) if */
/* >          JOBU = 'U'; LDU >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[in,out] V */
/* > \verbatim */
/* >          V is DOUBLE PRECISION array, dimension (LDV,P) */
/* >          On entry, if JOBV = 'V', V must contain a matrix V1 (usually */
/* >          the orthogonal matrix returned by DGGSVP). */
/* >          On exit, */
/* >          if JOBV = 'I', V contains the orthogonal matrix V; */
/* >          if JOBV = 'V', V contains the product V1*V. */
/* >          If JOBV = 'N', V is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDV */
/* > \verbatim */
/* >          LDV is INTEGER */
/* >          The leading dimension of the array V. LDV >= f2cmax(1,P) if */
/* >          JOBV = 'V'; LDV >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Q */
/* > \verbatim */
/* >          Q is DOUBLE PRECISION array, dimension (LDQ,N) */
/* >          On entry, if JOBQ = 'Q', Q must contain a matrix Q1 (usually */
/* >          the orthogonal matrix returned by DGGSVP). */
/* >          On exit, */
/* >          if JOBQ = 'I', Q contains the orthogonal matrix Q; */
/* >          if JOBQ = 'Q', Q contains the product Q1*Q. */
/* >          If JOBQ = 'N', Q is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDQ */
/* > \verbatim */
/* >          LDQ is INTEGER */
/* >          The leading dimension of the array Q. LDQ >= f2cmax(1,N) if */
/* >          JOBQ = 'Q'; LDQ >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension (2*N) */
/* > \endverbatim */
/* > */
/* > \param[out] NCALL MYCYCLE */
/* > \verbatim */
/* >          NCALL MYCYCLE is INTEGER */
/* >          The number of cycles required for convergence. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          = 1:  the procedure does not converge after MAXIT cycles. */
/* > \endverbatim */
/* > */
/* > \verbatim */
/* >  Internal Parameters */
/* >  =================== */
/* > */
/* >  MAXIT   INTEGER */
/* >          MAXIT specifies the total loops that the iterative procedure */
/* >          may take. If after MAXIT cycles, the routine fails to */
/* >          converge, we return INFO = 1. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleOTHERcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  DTGSJA essentially uses a variant of Kogbetliantz algorithm to reduce */
/* >  f2cmin(L,M-K)-by-L triangular (or trapezoidal) matrix A23 and L-by-L */
/* >  matrix B13 to the form: */
/* > */
/* >           U1**T *A13*Q1 = C1*R1; V1**T *B13*Q1 = S1*R1, */
/* > */
/* >  where U1, V1 and Q1 are orthogonal matrix, and Z**T is the transpose */
/* >  of Z.  C1 and S1 are diagonal matrices satisfying */
/* > */
/* >                C1**2 + S1**2 = I, */
/* > */
/* >  and R1 is an L-by-L nonsingular upper triangular matrix. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void dtgsja_(char *jobu, char *jobv, char *jobq, integer *m, 
	integer *p, integer *n, integer *k, integer *l, doublereal *a, 
	integer *lda, doublereal *b, integer *ldb, doublereal *tola, 
	doublereal *tolb, doublereal *alpha, doublereal *beta, doublereal *u, 
	integer *ldu, doublereal *v, integer *ldv, doublereal *q, integer *
	ldq, doublereal *work, integer *ncallmycycle, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, q_dim1, q_offset, u_dim1, 
	    u_offset, v_dim1, v_offset, i__1, i__2, i__3, i__4;
    doublereal d__1;

    /* Local variables */
    extern /* Subroutine */ void drot_(integer *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *);
    integer kcallmycycle, i__, j;
    doublereal gamma;
    extern /* Subroutine */ void dscal_(integer *, doublereal *, doublereal *, 
	    integer *);
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void dcopy_(integer *, doublereal *, integer *, 
	    doublereal *, integer *);
    doublereal a1;
    logical initq;
    doublereal a2, a3, b1;
    logical initu, initv, wantq, upper;
    doublereal b2, b3;
    logical wantu, wantv;
    doublereal error, ssmin;
    extern /* Subroutine */ void dlags2_(logical *, doublereal *, doublereal *,
	     doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *, doublereal *), dlapll_(integer *, doublereal *, 
	    integer *, doublereal *, integer *, doublereal *), dlartg_(
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *), dlaset_(char *, integer *, integer *, doublereal *,
	     doublereal *, doublereal *, integer *);
     extern int xerbla_(char *, integer *, ftnlen);
//    extern integer myhuge_(doublereal *);
    doublereal csq, csu, csv, snq, rwk, snu, snv;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */



/*     Decode and test the input parameters */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --alpha;
    --beta;
    u_dim1 = *ldu;
    u_offset = 1 + u_dim1 * 1;
    u -= u_offset;
    v_dim1 = *ldv;
    v_offset = 1 + v_dim1 * 1;
    v -= v_offset;
    q_dim1 = *ldq;
    q_offset = 1 + q_dim1 * 1;
    q -= q_offset;
    --work;

    /* Function Body */
    initu = lsame_(jobu, "I");
    wantu = initu || lsame_(jobu, "U");

    initv = lsame_(jobv, "I");
    wantv = initv || lsame_(jobv, "V");

    initq = lsame_(jobq, "I");
    wantq = initq || lsame_(jobq, "Q");

    *info = 0;
    if (! (initu || wantu || lsame_(jobu, "N"))) {
	*info = -1;
    } else if (! (initv || wantv || lsame_(jobv, "N"))) 
	    {
	*info = -2;
    } else if (! (initq || wantq || lsame_(jobq, "N"))) 
	    {
	*info = -3;
    } else if (*m < 0) {
	*info = -4;
    } else if (*p < 0) {
	*info = -5;
    } else if (*n < 0) {
	*info = -6;
    } else if (*lda < f2cmax(1,*m)) {
	*info = -10;
    } else if (*ldb < f2cmax(1,*p)) {
	*info = -12;
    } else if (*ldu < 1 || wantu && *ldu < *m) {
	*info = -18;
    } else if (*ldv < 1 || wantv && *ldv < *p) {
	*info = -20;
    } else if (*ldq < 1 || wantq && *ldq < *n) {
	*info = -22;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DTGSJA", &i__1, (ftnlen)6);
	return;
    }

/*     Initialize U, V and Q, if necessary */

    if (initu) {
	dlaset_("Full", m, m, &c_b1, &c_b15, &u[u_offset], ldu);
    }
    if (initv) {
	dlaset_("Full", p, p, &c_b1, &c_b15, &v[v_offset], ldv);
    }
    if (initq) {
	dlaset_("Full", n, n, &c_b1, &c_b15, &q[q_offset], ldq);
    }

/*     Loop until convergence */

    upper = FALSE_;
    for (kcallmycycle = 1; kcallmycycle <= 40; ++kcallmycycle) {

	upper = ! upper;

	i__1 = *l - 1;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = *l;
	    for (j = i__ + 1; j <= i__2; ++j) {

		a1 = 0.;
		a2 = 0.;
		a3 = 0.;
		if (*k + i__ <= *m) {
		    a1 = a[*k + i__ + (*n - *l + i__) * a_dim1];
		}
		if (*k + j <= *m) {
		    a3 = a[*k + j + (*n - *l + j) * a_dim1];
		}

		b1 = b[i__ + (*n - *l + i__) * b_dim1];
		b3 = b[j + (*n - *l + j) * b_dim1];

		if (upper) {
		    if (*k + i__ <= *m) {
			a2 = a[*k + i__ + (*n - *l + j) * a_dim1];
		    }
		    b2 = b[i__ + (*n - *l + j) * b_dim1];
		} else {
		    if (*k + j <= *m) {
			a2 = a[*k + j + (*n - *l + i__) * a_dim1];
		    }
		    b2 = b[j + (*n - *l + i__) * b_dim1];
		}

		dlags2_(&upper, &a1, &a2, &a3, &b1, &b2, &b3, &csu, &snu, &
			csv, &snv, &csq, &snq);

/*              Update (K+I)-th and (K+J)-th rows of matrix A: U**T *A */

		if (*k + j <= *m) {
		    drot_(l, &a[*k + j + (*n - *l + 1) * a_dim1], lda, &a[*k 
			    + i__ + (*n - *l + 1) * a_dim1], lda, &csu, &snu);
		}

/*              Update I-th and J-th rows of matrix B: V**T *B */

		drot_(l, &b[j + (*n - *l + 1) * b_dim1], ldb, &b[i__ + (*n - *
			l + 1) * b_dim1], ldb, &csv, &snv);

/*              Update (N-L+I)-th and (N-L+J)-th columns of matrices */
/*              A and B: A*Q and B*Q */

/* Computing MIN */
		i__4 = *k + *l;
		i__3 = f2cmin(i__4,*m);
		drot_(&i__3, &a[(*n - *l + j) * a_dim1 + 1], &c__1, &a[(*n - *
			l + i__) * a_dim1 + 1], &c__1, &csq, &snq);

		drot_(l, &b[(*n - *l + j) * b_dim1 + 1], &c__1, &b[(*n - *l + 
			i__) * b_dim1 + 1], &c__1, &csq, &snq);

		if (upper) {
		    if (*k + i__ <= *m) {
			a[*k + i__ + (*n - *l + j) * a_dim1] = 0.;
		    }
		    b[i__ + (*n - *l + j) * b_dim1] = 0.;
		} else {
		    if (*k + j <= *m) {
			a[*k + j + (*n - *l + i__) * a_dim1] = 0.;
		    }
		    b[j + (*n - *l + i__) * b_dim1] = 0.;
		}

/*              Update orthogonal matrices U, V, Q, if desired. */

		if (wantu && *k + j <= *m) {
		    drot_(m, &u[(*k + j) * u_dim1 + 1], &c__1, &u[(*k + i__) *
			     u_dim1 + 1], &c__1, &csu, &snu);
		}

		if (wantv) {
		    drot_(p, &v[j * v_dim1 + 1], &c__1, &v[i__ * v_dim1 + 1], 
			    &c__1, &csv, &snv);
		}

		if (wantq) {
		    drot_(n, &q[(*n - *l + j) * q_dim1 + 1], &c__1, &q[(*n - *
			    l + i__) * q_dim1 + 1], &c__1, &csq, &snq);
		}

/* L10: */
	    }
/* L20: */
	}

	if (! upper) {

/*           The matrices A13 and B13 were lower triangular at the start */
/*           of the cycle, and are now upper triangular. */

/*           Convergence test: test the parallelism of the corresponding */
/*           rows of A and B. */

	    error = 0.;
/* Computing MIN */
	    i__2 = *l, i__3 = *m - *k;
	    i__1 = f2cmin(i__2,i__3);
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = *l - i__ + 1;
		dcopy_(&i__2, &a[*k + i__ + (*n - *l + i__) * a_dim1], lda, &
			work[1], &c__1);
		i__2 = *l - i__ + 1;
		dcopy_(&i__2, &b[i__ + (*n - *l + i__) * b_dim1], ldb, &work[*
			l + 1], &c__1);
		i__2 = *l - i__ + 1;
		dlapll_(&i__2, &work[1], &c__1, &work[*l + 1], &c__1, &ssmin);
		error = f2cmax(error,ssmin);
/* L30: */
	    }

	    if (abs(error) <= f2cmin(*tola,*tolb)) {
		goto L50;
	    }
	}

/*        End of cycle loop */

/* L40: */
    }

/*     The algorithm has not converged after MAXIT cycles. */

    *info = 1;
    goto L100;

L50:

/*     If ERROR <= MIN(TOLA,TOLB), then the algorithm has converged. */
/*     Compute the generalized singular value pairs (ALPHA, BETA), and */
/*     set the triangular matrix R to array A. */

    i__1 = *k;
    for (i__ = 1; i__ <= i__1; ++i__) {
	alpha[i__] = 1.;
	beta[i__] = 0.;
/* L60: */
    }

/* Computing MIN */
    i__2 = *l, i__3 = *m - *k;
    i__1 = f2cmin(i__2,i__3);
    for (i__ = 1; i__ <= i__1; ++i__) {

	a1 = a[*k + i__ + (*n - *l + i__) * a_dim1];
	b1 = b[i__ + (*n - *l + i__) * b_dim1];
	gamma = b1 / a1;

	if (gamma <= (doublereal) myhuge_(&c_b1) && gamma >= -((doublereal) 
		myhuge_(&c_b1))) {

/*           change sign if necessary */

	    if (gamma < 0.) {
		i__2 = *l - i__ + 1;
		dscal_(&i__2, &c_b44, &b[i__ + (*n - *l + i__) * b_dim1], ldb)
			;
		if (wantv) {
		    dscal_(p, &c_b44, &v[i__ * v_dim1 + 1], &c__1);
		}
	    }

	    d__1 = abs(gamma);
	    dlartg_(&d__1, &c_b15, &beta[*k + i__], &alpha[*k + i__], &rwk);

	    if (alpha[*k + i__] >= beta[*k + i__]) {
		i__2 = *l - i__ + 1;
		d__1 = 1. / alpha[*k + i__];
		dscal_(&i__2, &d__1, &a[*k + i__ + (*n - *l + i__) * a_dim1], 
			lda);
	    } else {
		i__2 = *l - i__ + 1;
		d__1 = 1. / beta[*k + i__];
		dscal_(&i__2, &d__1, &b[i__ + (*n - *l + i__) * b_dim1], ldb);
		i__2 = *l - i__ + 1;
		dcopy_(&i__2, &b[i__ + (*n - *l + i__) * b_dim1], ldb, &a[*k 
			+ i__ + (*n - *l + i__) * a_dim1], lda);
	    }

	} else {

	    alpha[*k + i__] = 0.;
	    beta[*k + i__] = 1.;
	    i__2 = *l - i__ + 1;
	    dcopy_(&i__2, &b[i__ + (*n - *l + i__) * b_dim1], ldb, &a[*k + 
		    i__ + (*n - *l + i__) * a_dim1], lda);

	}

/* L70: */
    }

/*     Post-assignment */

    i__1 = *k + *l;
    for (i__ = *m + 1; i__ <= i__1; ++i__) {
	alpha[i__] = 0.;
	beta[i__] = 1.;
/* L80: */
    }

    if (*k + *l < *n) {
	i__1 = *n;
	for (i__ = *k + *l + 1; i__ <= i__1; ++i__) {
	    alpha[i__] = 0.;
	    beta[i__] = 0.;
/* L90: */
	}
    }

L100:
    *ncallmycycle = kcallmycycle;
    return;

/*     End of DTGSJA */

} /* dtgsja_ */

