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

static doublereal c_b7 = 1.;
static integer c__1 = 1;
static doublereal c_b10 = -1.;

/* > \brief \b DORHR_COL */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DORHR_COL + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dorhr_c
ol.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dorhr_c
ol.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dorhr_c
ol.f"> */
/* > [TXT]</a> */
/* > */
/*  Definition: */
/*  =========== */

/*       SUBROUTINE DORHR_COL( M, N, NB, A, LDA, T, LDT, D, INFO ) */

/*       INTEGER           INFO, LDA, LDT, M, N, NB */
/*       DOUBLE PRECISION  A( LDA, * ), D( * ), T( LDT, * ) */

/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >  DORHR_COL takes an M-by-N real matrix Q_in with orthonormal columns */
/* >  as input, stored in A, and performs Householder Reconstruction (HR), */
/* >  i.e. reconstructs Householder vectors V(i) implicitly representing */
/* >  another M-by-N matrix Q_out, with the property that Q_in = Q_out*S, */
/* >  where S is an N-by-N diagonal matrix with diagonal entries */
/* >  equal to +1 or -1. The Householder vectors (columns V(i) of V) are */
/* >  stored in A on output, and the diagonal entries of S are stored in D. */
/* >  Block reflectors are also returned in T */
/* >  (same output format as DGEQRT). */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the matrix A. M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the matrix A. M >= N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] NB */
/* > \verbatim */
/* >          NB is INTEGER */
/* >          The column block size to be used in the reconstruction */
/* >          of Householder column vector blocks in the array A and */
/* >          corresponding block reflectors in the array T. NB >= 1. */
/* >          (Note that if NB > N, then N is used instead of NB */
/* >          as the column block size.) */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is DOUBLE PRECISION array, dimension (LDA,N) */
/* > */
/* >          On entry: */
/* > */
/* >             The array A contains an M-by-N orthonormal matrix Q_in, */
/* >             i.e the columns of A are orthogonal unit vectors. */
/* > */
/* >          On exit: */
/* > */
/* >             The elements below the diagonal of A represent the unit */
/* >             lower-trapezoidal matrix V of Householder column vectors */
/* >             V(i). The unit diagonal entries of V are not stored */
/* >             (same format as the output below the diagonal in A from */
/* >             DGEQRT). The matrix T and the matrix V stored on output */
/* >             in A implicitly define Q_out. */
/* > */
/* >             The elements above the diagonal contain the factor U */
/* >             of the "modified" LU-decomposition: */
/* >                Q_in - ( S ) = V * U */
/* >                       ( 0 ) */
/* >             where 0 is a (M-N)-by-(M-N) zero matrix. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[out] T */
/* > \verbatim */
/* >          T is DOUBLE PRECISION array, */
/* >          dimension (LDT, N) */
/* > */
/* >          Let NOCB = Number_of_output_col_blocks */
/* >                   = CEIL(N/NB) */
/* > */
/* >          On exit, T(1:NB, 1:N) contains NOCB upper-triangular */
/* >          block reflectors used to define Q_out stored in compact */
/* >          form as a sequence of upper-triangular NB-by-NB column */
/* >          blocks (same format as the output T in DGEQRT). */
/* >          The matrix T and the matrix V stored on output in A */
/* >          implicitly define Q_out. NOTE: The lower triangles */
/* >          below the upper-triangular blcoks will be filled with */
/* >          zeros. See Further Details. */
/* > \endverbatim */
/* > */
/* > \param[in] LDT */
/* > \verbatim */
/* >          LDT is INTEGER */
/* >          The leading dimension of the array T. */
/* >          LDT >= f2cmax(1,f2cmin(NB,N)). */
/* > \endverbatim */
/* > */
/* > \param[out] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array, dimension f2cmin(M,N). */
/* >          The elements can be only plus or minus one. */
/* > */
/* >          D(i) is constructed as D(i) = -SIGN(Q_in_i(i,i)), where */
/* >          1 <= i <= f2cmin(M,N), and Q_in_i is Q_in after performing */
/* >          i-1 steps of “modified” Gaussian elimination. */
/* >          See Further Details. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value */
/* > \endverbatim */
/* > */
/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* > The computed M-by-M orthogonal factor Q_out is defined implicitly as */
/* > a product of orthogonal matrices Q_out(i). Each Q_out(i) is stored in */
/* > the compact WY-representation format in the corresponding blocks of */
/* > matrices V (stored in A) and T. */
/* > */
/* > The M-by-N unit lower-trapezoidal matrix V stored in the M-by-N */
/* > matrix A contains the column vectors V(i) in NB-size column */
/* > blocks VB(j). For example, VB(1) contains the columns */
/* > V(1), V(2), ... V(NB). NOTE: The unit entries on */
/* > the diagonal of Y are not stored in A. */
/* > */
/* > The number of column blocks is */
/* > */
/* >     NOCB = Number_of_output_col_blocks = CEIL(N/NB) */
/* > */
/* > where each block is of order NB except for the last block, which */
/* > is of order LAST_NB = N - (NOCB-1)*NB. */
/* > */
/* > For example, if M=6,  N=5 and NB=2, the matrix V is */
/* > */
/* > */
/* >     V = (    VB(1),   VB(2), VB(3) ) = */
/* > */
/* >       = (   1                      ) */
/* >         ( v21    1                 ) */
/* >         ( v31  v32    1            ) */
/* >         ( v41  v42  v43   1        ) */
/* >         ( v51  v52  v53  v54    1  ) */
/* >         ( v61  v62  v63  v54   v65 ) */
/* > */
/* > */
/* > For each of the column blocks VB(i), an upper-triangular block */
/* > reflector TB(i) is computed. These blocks are stored as */
/* > a sequence of upper-triangular column blocks in the NB-by-N */
/* > matrix T. The size of each TB(i) block is NB-by-NB, except */
/* > for the last block, whose size is LAST_NB-by-LAST_NB. */
/* > */
/* > For example, if M=6,  N=5 and NB=2, the matrix T is */
/* > */
/* >     T  = (    TB(1),    TB(2), TB(3) ) = */
/* > */
/* >        = ( t11  t12  t13  t14   t15  ) */
/* >          (      t22       t24        ) */
/* > */
/* > */
/* > The M-by-M factor Q_out is given as a product of NOCB */
/* > orthogonal M-by-M matrices Q_out(i). */
/* > */
/* >     Q_out = Q_out(1) * Q_out(2) * ... * Q_out(NOCB), */
/* > */
/* > where each matrix Q_out(i) is given by the WY-representation */
/* > using corresponding blocks from the matrices V and T: */
/* > */
/* >     Q_out(i) = I - VB(i) * TB(i) * (VB(i))**T, */
/* > */
/* > where I is the identity matrix. Here is the formula with matrix */
/* > dimensions: */
/* > */
/* >  Q(i){M-by-M} = I{M-by-M} - */
/* >    VB(i){M-by-INB} * TB(i){INB-by-INB} * (VB(i))**T {INB-by-M}, */
/* > */
/* > where INB = NB, except for the last block NOCB */
/* > for which INB=LAST_NB. */
/* > */
/* > ===== */
/* > NOTE: */
/* > ===== */
/* > */
/* > If Q_in is the result of doing a QR factorization */
/* > B = Q_in * R_in, then: */
/* > */
/* > B = (Q_out*S) * R_in = Q_out * (S * R_in) = O_out * R_out. */
/* > */
/* > So if one wants to interpret Q_out as the result */
/* > of the QR factorization of B, then corresponding R_out */
/* > should be obtained by R_out = S * R_in, i.e. some rows of R_in */
/* > should be multiplied by -1. */
/* > */
/* > For the details of the algorithm, see [1]. */
/* > */
/* > [1] "Reconstructing Householder vectors from tall-skinny QR", */
/* >     G. Ballard, J. Demmel, L. Grigori, M. Jacquelin, H.D. Nguyen, */
/* >     E. Solomonik, J. Parallel Distrib. Comput., */
/* >     vol. 85, pp. 3-31, 2015. */
/* > \endverbatim */
/* > */
/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date November 2019 */

/* > \ingroup doubleOTHERcomputational */

/* > \par Contributors: */
/*  ================== */
/* > */
/* > \verbatim */
/* > */
/* > November   2019, Igor Kozachenko, */
/* >            Computer Science Division, */
/* >            University of California, Berkeley */
/* > */
/* > \endverbatim */

/*  ===================================================================== */
/* Subroutine */ void dorhr_col_(integer *m, integer *n, integer *nb, 
	doublereal *a, integer *lda, doublereal *t, integer *ldt, doublereal *
	d__, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, t_dim1, t_offset, i__1, i__2, i__3, i__4;

    /* Local variables */
    extern /* Subroutine */ void dlaorhr_col_getrfnp_(integer *, integer *, 
	    doublereal *, integer *, doublereal *, integer *);
    integer nplusone, i__, j;
    extern /* Subroutine */ void dscal_(integer *, doublereal *, doublereal *, 
	    integer *);
    integer iinfo;
    extern /* Subroutine */ void dcopy_(integer *, doublereal *, integer *, 
	    doublereal *, integer *), dtrsm_(char *, char *, char *, char *, 
	    integer *, integer *, doublereal *, doublereal *, integer *, 
	    doublereal *, integer *);
    integer jb;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    integer jbtemp1, jbtemp2, jnb;


/*  -- LAPACK computational routine (version 3.9.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     November 2019 */


/*  ===================================================================== */


/*     Test the input parameters */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    t_dim1 = *ldt;
    t_offset = 1 + t_dim1 * 1;
    t -= t_offset;
    --d__;

    /* Function Body */
    *info = 0;
    if (*m < 0) {
	*info = -1;
    } else if (*n < 0 || *n > *m) {
	*info = -2;
    } else if (*nb < 1) {
	*info = -3;
    } else if (*lda < f2cmax(1,*m)) {
	*info = -5;
    } else /* if(complicated condition) */ {
/* Computing MAX */
	i__1 = 1, i__2 = f2cmin(*nb,*n);
	if (*ldt < f2cmax(i__1,i__2)) {
	    *info = -7;
	}
    }

/*     Handle error in the input parameters. */

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DORHR_COL", &i__1, (ftnlen)9);
	return;
    }

/*     Quick return if possible */

    if (f2cmin(*m,*n) == 0) {
	return;
    }

/*     On input, the M-by-N matrix A contains the orthogonal */
/*     M-by-N matrix Q_in. */

/*     (1) Compute the unit lower-trapezoidal V (ones on the diagonal */
/*     are not stored) by performing the "modified" LU-decomposition. */

/*     Q_in - ( S ) = V * U = ( V1 ) * U, */
/*            ( 0 )           ( V2 ) */

/*     where 0 is an (M-N)-by-N zero matrix. */

/*     (1-1) Factor V1 and U. */
    dlaorhr_col_getrfnp_(n, n, &a[a_offset], lda, &d__[1], &iinfo);

/*     (1-2) Solve for V2. */

    if (*m > *n) {
	i__1 = *m - *n;
	dtrsm_("R", "U", "N", "N", &i__1, n, &c_b7, &a[a_offset], lda, &a[*n 
		+ 1 + a_dim1], lda);
    }

/*     (2) Reconstruct the block reflector T stored in T(1:NB, 1:N) */
/*     as a sequence of upper-triangular blocks with NB-size column */
/*     blocking. */

/*     Loop over the column blocks of size NB of the array A(1:M,1:N) */
/*     and the array T(1:NB,1:N), JB is the column index of a column */
/*     block, JNB is the column block size at each step JB. */

    nplusone = *n + 1;
    i__1 = *n;
    i__2 = *nb;
    for (jb = 1; i__2 < 0 ? jb >= i__1 : jb <= i__1; jb += i__2) {

/*        (2-0) Determine the column block size JNB. */

/* Computing MIN */
	i__3 = nplusone - jb;
	jnb = f2cmin(i__3,*nb);

/*        (2-1) Copy the upper-triangular part of the current JNB-by-JNB */
/*        diagonal block U(JB) (of the N-by-N matrix U) stored */
/*        in A(JB:JB+JNB-1,JB:JB+JNB-1) into the upper-triangular part */
/*        of the current JNB-by-JNB block T(1:JNB,JB:JB+JNB-1) */
/*        column-by-column, total JNB*(JNB+1)/2 elements. */

	jbtemp1 = jb - 1;
	i__3 = jb + jnb - 1;
	for (j = jb; j <= i__3; ++j) {
	    i__4 = j - jbtemp1;
	    dcopy_(&i__4, &a[jb + j * a_dim1], &c__1, &t[j * t_dim1 + 1], &
		    c__1);
	}

/*        (2-2) Perform on the upper-triangular part of the current */
/*        JNB-by-JNB diagonal block U(JB) (of the N-by-N matrix U) stored */
/*        in T(1:JNB,JB:JB+JNB-1) the following operation in place: */
/*        (-1)*U(JB)*S(JB), i.e the result will be stored in the upper- */
/*        triangular part of T(1:JNB,JB:JB+JNB-1). This multiplication */
/*        of the JNB-by-JNB diagonal block U(JB) by the JNB-by-JNB */
/*        diagonal block S(JB) of the N-by-N sign matrix S from the */
/*        right means changing the sign of each J-th column of the block */
/*        U(JB) according to the sign of the diagonal element of the block */
/*        S(JB), i.e. S(J,J) that is stored in the array element D(J). */

	i__3 = jb + jnb - 1;
	for (j = jb; j <= i__3; ++j) {
	    if (d__[j] == 1.) {
		i__4 = j - jbtemp1;
		dscal_(&i__4, &c_b10, &t[j * t_dim1 + 1], &c__1);
	    }
	}

/*        (2-3) Perform the triangular solve for the current block */
/*        matrix X(JB): */

/*               X(JB) * (A(JB)**T) = B(JB), where: */

/*               A(JB)**T  is a JNB-by-JNB unit upper-triangular */
/*                         coefficient block, and A(JB)=V1(JB), which */
/*                         is a JNB-by-JNB unit lower-triangular block */
/*                         stored in A(JB:JB+JNB-1,JB:JB+JNB-1). */
/*                         The N-by-N matrix V1 is the upper part */
/*                         of the M-by-N lower-trapezoidal matrix V */
/*                         stored in A(1:M,1:N); */

/*               B(JB)     is a JNB-by-JNB  upper-triangular right-hand */
/*                         side block, B(JB) = (-1)*U(JB)*S(JB), and */
/*                         B(JB) is stored in T(1:JNB,JB:JB+JNB-1); */

/*               X(JB)     is a JNB-by-JNB upper-triangular solution */
/*                         block, X(JB) is the upper-triangular block */
/*                         reflector T(JB), and X(JB) is stored */
/*                         in T(1:JNB,JB:JB+JNB-1). */

/*             In other words, we perform the triangular solve for the */
/*             upper-triangular block T(JB): */

/*               T(JB) * (V1(JB)**T) = (-1)*U(JB)*S(JB). */

/*             Even though the blocks X(JB) and B(JB) are upper- */
/*             triangular, the routine DTRSM will access all JNB**2 */
/*             elements of the square T(1:JNB,JB:JB+JNB-1). Therefore, */
/*             we need to set to zero the elements of the block */
/*             T(1:JNB,JB:JB+JNB-1) below the diagonal before the call */
/*             to DTRSM. */

/*        (2-3a) Set the elements to zero. */

	jbtemp2 = jb - 2;
	i__3 = jb + jnb - 2;
	for (j = jb; j <= i__3; ++j) {
	    i__4 = *nb;
	    for (i__ = j - jbtemp2; i__ <= i__4; ++i__) {
		t[i__ + j * t_dim1] = 0.;
	    }
	}

/*        (2-3b) Perform the triangular solve. */

	dtrsm_("R", "L", "T", "U", &jnb, &jnb, &c_b7, &a[jb + jb * a_dim1], 
		lda, &t[jb * t_dim1 + 1], ldt);

    }

    return;

/*     End of DORHR_COL */

} /* dorhr_col__ */

