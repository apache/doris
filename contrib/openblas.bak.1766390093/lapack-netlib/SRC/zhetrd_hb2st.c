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

static doublecomplex c_b1 = {0.,0.};
static integer c__2 = 2;
static integer c_n1 = -1;
static integer c__3 = 3;
static integer c__4 = 4;

/* > \brief \b ZHETRD_HB2ST reduces a complex Hermitian band matrix A to real symmetric tridiagonal form T */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZHETRD_HB2ST + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zhbtrd_
hb2st.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zhbtrd_
hb2st.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zhbtrd_
hb2st.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZHETRD_HB2ST( STAGE1, VECT, UPLO, N, KD, AB, LDAB, */
/*                               D, E, HOUS, LHOUS, WORK, LWORK, INFO ) */

/*       #if defined(_OPENMP) */
/*       use omp_lib */
/*       #endif */

/*       IMPLICIT NONE */

/*       CHARACTER          STAGE1, UPLO, VECT */
/*       INTEGER            N, KD, IB, LDAB, LHOUS, LWORK, INFO */
/*       DOUBLE PRECISION   D( * ), E( * ) */
/*       COMPLEX*16         AB( LDAB, * ), HOUS( * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZHETRD_HB2ST reduces a complex Hermitian band matrix A to real symmetric */
/* > tridiagonal form T by a unitary similarity transformation: */
/* > Q**H * A * Q = T. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] STAGE1 */
/* > \verbatim */
/* >          STAGE1 is CHARACTER*1 */
/* >          = 'N':  "No": to mention that the stage 1 of the reduction */
/* >                  from dense to band using the zhetrd_he2hb routine */
/* >                  was not called before this routine to reproduce AB. */
/* >                  In other term this routine is called as standalone. */
/* >          = 'Y':  "Yes": to mention that the stage 1 of the */
/* >                  reduction from dense to band using the zhetrd_he2hb */
/* >                  routine has been called to produce AB (e.g., AB is */
/* >                  the output of zhetrd_he2hb. */
/* > \endverbatim */
/* > */
/* > \param[in] VECT */
/* > \verbatim */
/* >          VECT is CHARACTER*1 */
/* >          = 'N':  No need for the Housholder representation, */
/* >                  and thus LHOUS is of size f2cmax(1, 4*N); */
/* >          = 'V':  the Householder representation is needed to */
/* >                  either generate or to apply Q later on, */
/* >                  then LHOUS is to be queried and computed. */
/* >                  (NOT AVAILABLE IN THIS RELEASE). */
/* > \endverbatim */
/* > */
/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >          = 'U':  Upper triangle of A is stored; */
/* >          = 'L':  Lower triangle of A is stored. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] KD */
/* > \verbatim */
/* >          KD is INTEGER */
/* >          The number of superdiagonals of the matrix A if UPLO = 'U', */
/* >          or the number of subdiagonals if UPLO = 'L'.  KD >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] AB */
/* > \verbatim */
/* >          AB is COMPLEX*16 array, dimension (LDAB,N) */
/* >          On entry, the upper or lower triangle of the Hermitian band */
/* >          matrix A, stored in the first KD+1 rows of the array.  The */
/* >          j-th column of A is stored in the j-th column of the array AB */
/* >          as follows: */
/* >          if UPLO = 'U', AB(kd+1+i-j,j) = A(i,j) for f2cmax(1,j-kd)<=i<=j; */
/* >          if UPLO = 'L', AB(1+i-j,j)    = A(i,j) for j<=i<=f2cmin(n,j+kd). */
/* >          On exit, the diagonal elements of AB are overwritten by the */
/* >          diagonal elements of the tridiagonal matrix T; if KD > 0, the */
/* >          elements on the first superdiagonal (if UPLO = 'U') or the */
/* >          first subdiagonal (if UPLO = 'L') are overwritten by the */
/* >          off-diagonal elements of T; the rest of AB is overwritten by */
/* >          values generated during the reduction. */
/* > \endverbatim */
/* > */
/* > \param[in] LDAB */
/* > \verbatim */
/* >          LDAB is INTEGER */
/* >          The leading dimension of the array AB.  LDAB >= KD+1. */
/* > \endverbatim */
/* > */
/* > \param[out] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array, dimension (N) */
/* >          The diagonal elements of the tridiagonal matrix T. */
/* > \endverbatim */
/* > */
/* > \param[out] E */
/* > \verbatim */
/* >          E is DOUBLE PRECISION array, dimension (N-1) */
/* >          The off-diagonal elements of the tridiagonal matrix T: */
/* >          E(i) = T(i,i+1) if UPLO = 'U'; E(i) = T(i+1,i) if UPLO = 'L'. */
/* > \endverbatim */
/* > */
/* > \param[out] HOUS */
/* > \verbatim */
/* >          HOUS is COMPLEX*16 array, dimension LHOUS, that */
/* >          store the Householder representation. */
/* > \endverbatim */
/* > */
/* > \param[in] LHOUS */
/* > \verbatim */
/* >          LHOUS is INTEGER */
/* >          The dimension of the array HOUS. LHOUS = MAX(1, dimension) */
/* >          If LWORK = -1, or LHOUS=-1, */
/* >          then a query is assumed; the routine */
/* >          only calculates the optimal size of the HOUS array, returns */
/* >          this value as the first entry of the HOUS array, and no error */
/* >          message related to LHOUS is issued by XERBLA. */
/* >          LHOUS = MAX(1, dimension) where */
/* >          dimension = 4*N if VECT='N' */
/* >          not available now if VECT='H' */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. LWORK = MAX(1, dimension) */
/* >          If LWORK = -1, or LHOUS=-1, */
/* >          then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* >          LWORK = MAX(1, dimension) where */
/* >          dimension   = (2KD+1)*N + KD*NTHREADS */
/* >          where KD is the blocking size of the reduction, */
/* >          FACTOPTNB is the blocking used by the QR or LQ */
/* >          algorithm, usually FACTOPTNB=128 is a good choice */
/* >          NTHREADS is the number of threads used when */
/* >          openMP compilation is enabled, otherwise =1. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date November 2017 */

/* > \ingroup complex16OTHERcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  Implemented by Azzam Haidar. */
/* > */
/* >  All details are available on technical report, SC11, SC13 papers. */
/* > */
/* >  Azzam Haidar, Hatem Ltaief, and Jack Dongarra. */
/* >  Parallel reduction to condensed forms for symmetric eigenvalue problems */
/* >  using aggregated fine-grained and memory-aware kernels. In Proceedings */
/* >  of 2011 International Conference for High Performance Computing, */
/* >  Networking, Storage and Analysis (SC '11), New York, NY, USA, */
/* >  Article 8 , 11 pages. */
/* >  http://doi.acm.org/10.1145/2063384.2063394 */
/* > */
/* >  A. Haidar, J. Kurzak, P. Luszczek, 2013. */
/* >  An improved parallel singular value algorithm and its implementation */
/* >  for multicore hardware, In Proceedings of 2013 International Conference */
/* >  for High Performance Computing, Networking, Storage and Analysis (SC '13). */
/* >  Denver, Colorado, USA, 2013. */
/* >  Article 90, 12 pages. */
/* >  http://doi.acm.org/10.1145/2503210.2503292 */
/* > */
/* >  A. Haidar, R. Solca, S. Tomov, T. Schulthess and J. Dongarra. */
/* >  A novel hybrid CPU-GPU generalized eigensolver for electronic structure */
/* >  calculations based on fine-grained memory aware tasks. */
/* >  International Journal of High Performance Computing Applications. */
/* >  Volume 28 Issue 2, Pages 196-209, May 2014. */
/* >  http://hpc.sagepub.com/content/28/2/196 */
/* > */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void zhetrd_hb2st_(char *stage1, char *vect, char *uplo, 
	integer *n, integer *kd, doublecomplex *ab, integer *ldab, doublereal 
	*d__, doublereal *e, doublecomplex *hous, integer *lhous, 
	doublecomplex *work, integer *lwork, integer *info)
{
    /* System generated locals */
    integer ab_dim1, ab_offset, i__1, i__2, i__3, i__4, i__5;
    real r__1;
    doublecomplex z__1;

    /* Local variables */
    integer inda;
    extern integer ilaenv2stage_(integer *, char *, char *, integer *, 
	    integer *, integer *, integer *);
    integer thed, myid, indw, apos, dpos, indv, abofdpos, nthreads, i__, k, m,
	     edind, debug;
    extern logical lsame_(char *, char *);
    integer lhmin, sizea, shift, stind, colpt, lwmin, awpos;
    logical wantq, upper;
    integer grsiz, sizev, ttype, stepercol, ed, ib, st, abdpos;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    integer thgrid, thgrnb, indtau;
    doublereal abstmp;
    integer ofdpos;
    extern /* Subroutine */ void zhb2st_kernels_(char *, logical *, integer *,
	     integer *, integer *, integer *, integer *, integer *, integer *,
	     doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *, doublecomplex *), zlacpy_(char *, integer *, 
	    integer *, doublecomplex *, integer *, doublecomplex *, integer *), zlaset_(char *, integer *, integer *, doublecomplex *, 
	    doublecomplex *, doublecomplex *, integer *);
    integer blklastind;
    extern /* Subroutine */ void mecago_();
    logical lquery, afters1;
    integer lda, tid, ldv;
    doublecomplex tmp;
    integer stt, sweepid, nbtiles, sizetau, thgrsiz;





/*  -- LAPACK computational routine (version 3.8.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     November 2017 */


/*  ===================================================================== */


/*     Determine the minimal workspace size required. */
/*     Test the input parameters */

    /* Parameter adjustments */
    ab_dim1 = *ldab;
    ab_offset = 1 + ab_dim1 * 1;
    ab -= ab_offset;
    --d__;
    --e;
    --hous;
    --work;

    /* Function Body */
    debug = 0;
    *info = 0;
    afters1 = lsame_(stage1, "Y");
    wantq = lsame_(vect, "V");
    upper = lsame_(uplo, "U");
    lquery = *lwork == -1 || *lhous == -1;

/*     Determine the block size, the workspace size and the hous size. */

    ib = ilaenv2stage_(&c__2, "ZHETRD_HB2ST", vect, n, kd, &c_n1, &c_n1);
    lhmin = ilaenv2stage_(&c__3, "ZHETRD_HB2ST", vect, n, kd, &ib, &c_n1);
    lwmin = ilaenv2stage_(&c__4, "ZHETRD_HB2ST", vect, n, kd, &ib, &c_n1);

    if (! afters1 && ! lsame_(stage1, "N")) {
	*info = -1;
    } else if (! lsame_(vect, "N")) {
	*info = -2;
    } else if (! upper && ! lsame_(uplo, "L")) {
	*info = -3;
    } else if (*n < 0) {
	*info = -4;
    } else if (*kd < 0) {
	*info = -5;
    } else if (*ldab < *kd + 1) {
	*info = -7;
    } else if (*lhous < lhmin && ! lquery) {
	*info = -11;
    } else if (*lwork < lwmin && ! lquery) {
	*info = -13;
    }

    if (*info == 0) {
	hous[1].r = (doublereal) lhmin, hous[1].i = 0.;
	work[1].r = (doublereal) lwmin, work[1].i = 0.;
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZHETRD_HB2ST", &i__1, (ftnlen)12);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	hous[1].r = 1., hous[1].i = 0.;
	work[1].r = 1., work[1].i = 0.;
	return;
    }

/*     Determine pointer position */

    ldv = *kd + ib;
    sizetau = *n << 1;
    sizev = *n << 1;
    indtau = 1;
    indv = indtau + sizetau;
    lda = (*kd << 1) + 1;
    sizea = lda * *n;
    inda = 1;
    indw = inda + sizea;
    nthreads = 1;
    tid = 0;

    if (upper) {
	apos = inda + *kd;
	awpos = inda;
	dpos = apos + *kd;
	ofdpos = dpos - 1;
	abdpos = *kd + 1;
	abofdpos = *kd;
    } else {
	apos = inda;
	awpos = inda + *kd + 1;
	dpos = apos;
	ofdpos = dpos + 1;
	abdpos = 1;
	abofdpos = 2;
    }

/*     Case KD=0: */
/*     The matrix is diagonal. We just copy it (convert to "real" for */
/*     complex because D is double and the imaginary part should be 0) */
/*     and store it in D. A sequential code here is better or */
/*     in a parallel environment it might need two cores for D and E */

    if (*kd == 0) {
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = abdpos + i__ * ab_dim1;
	    d__[i__] = ab[i__2].r;
/* L30: */
	}
	i__1 = *n - 1;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    e[i__] = 0.;
/* L40: */
	}

	hous[1].r = 1., hous[1].i = 0.;
	work[1].r = 1., work[1].i = 0.;
	return;
    }

/*     Case KD=1: */
/*     The matrix is already Tridiagonal. We have to make diagonal */
/*     and offdiagonal elements real, and store them in D and E. */
/*     For that, for real precision just copy the diag and offdiag */
/*     to D and E while for the COMPLEX case the bulge chasing is */
/*     performed to convert the hermetian tridiagonal to symmetric */
/*     tridiagonal. A simpler coversion formula might be used, but then */
/*     updating the Q matrix will be required and based if Q is generated */
/*     or not this might complicate the story. */

    if (*kd == 1) {
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = abdpos + i__ * ab_dim1;
	    d__[i__] = ab[i__2].r;
/* L50: */
	}

/*         make off-diagonal elements real and copy them to E */

	if (upper) {
	    i__1 = *n - 1;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = abofdpos + (i__ + 1) * ab_dim1;
		tmp.r = ab[i__2].r, tmp.i = ab[i__2].i;
		abstmp = z_abs(&tmp);
		i__2 = abofdpos + (i__ + 1) * ab_dim1;
		ab[i__2].r = abstmp, ab[i__2].i = 0.;
		e[i__] = abstmp;
		if (abstmp != 0.) {
		    z__1.r = tmp.r / abstmp, z__1.i = tmp.i / abstmp;
		    tmp.r = z__1.r, tmp.i = z__1.i;
		} else {
		    tmp.r = 1., tmp.i = 0.;
		}
		if (i__ < *n - 1) {
		    i__2 = abofdpos + (i__ + 2) * ab_dim1;
		    i__3 = abofdpos + (i__ + 2) * ab_dim1;
		    z__1.r = ab[i__3].r * tmp.r - ab[i__3].i * tmp.i, z__1.i =
			     ab[i__3].r * tmp.i + ab[i__3].i * tmp.r;
		    ab[i__2].r = z__1.r, ab[i__2].i = z__1.i;
		}
/*                  IF( WANTZ ) THEN */
/*                     CALL ZSCAL( N, DCONJG( TMP ), Q( 1, I+1 ), 1 ) */
/*                  END IF */
/* L60: */
	    }
	} else {
	    i__1 = *n - 1;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = abofdpos + i__ * ab_dim1;
		tmp.r = ab[i__2].r, tmp.i = ab[i__2].i;
		abstmp = z_abs(&tmp);
		i__2 = abofdpos + i__ * ab_dim1;
		ab[i__2].r = abstmp, ab[i__2].i = 0.;
		e[i__] = abstmp;
		if (abstmp != 0.) {
		    z__1.r = tmp.r / abstmp, z__1.i = tmp.i / abstmp;
		    tmp.r = z__1.r, tmp.i = z__1.i;
		} else {
		    tmp.r = 1., tmp.i = 0.;
		}
		if (i__ < *n - 1) {
		    i__2 = abofdpos + (i__ + 1) * ab_dim1;
		    i__3 = abofdpos + (i__ + 1) * ab_dim1;
		    z__1.r = ab[i__3].r * tmp.r - ab[i__3].i * tmp.i, z__1.i =
			     ab[i__3].r * tmp.i + ab[i__3].i * tmp.r;
		    ab[i__2].r = z__1.r, ab[i__2].i = z__1.i;
		}
/*                 IF( WANTQ ) THEN */
/*                    CALL ZSCAL( N, TMP, Q( 1, I+1 ), 1 ) */
/*                 END IF */
/* L70: */
	    }
	}

	hous[1].r = 1., hous[1].i = 0.;
	work[1].r = 1., work[1].i = 0.;
	return;
    }

/*     Main code start here. */
/*     Reduce the hermitian band of A to a tridiagonal matrix. */

    thgrsiz = *n;
    grsiz = 1;
    shift = 3;
    r__1 = (real) (*n) / (real) (*kd) + .5f;
    nbtiles = r_int(&r__1);
    r__1 = (real) shift / (real) grsiz + .5f;
    stepercol = r_int(&r__1);
    r__1 = (real) (*n - 1) / (real) thgrsiz + .5f;
    thgrnb = r_int(&r__1);

    i__1 = *kd + 1;
    zlacpy_("A", &i__1, n, &ab[ab_offset], ldab, &work[apos], &lda)
	    ;
    zlaset_("A", kd, n, &c_b1, &c_b1, &work[awpos], &lda);


/*     openMP parallelisation start here */


/*     main bulge chasing loop */

    i__1 = thgrnb;
    for (thgrid = 1; thgrid <= i__1; ++thgrid) {
	stt = (thgrid - 1) * thgrsiz + 1;
/* Computing MIN */
	i__2 = stt + thgrsiz - 1, i__3 = *n - 1;
	thed = f2cmin(i__2,i__3);
	i__2 = *n - 1;
	for (i__ = stt; i__ <= i__2; ++i__) {
	    ed = f2cmin(i__,thed);
	    if (stt > ed) {
		myexit_();
	    }
	    i__3 = stepercol;
	    for (m = 1; m <= i__3; ++m) {
		st = stt;
		i__4 = ed;
		for (sweepid = st; sweepid <= i__4; ++sweepid) {
		    i__5 = grsiz;
		    for (k = 1; k <= i__5; ++k) {
			myid = (i__ - sweepid) * (stepercol * grsiz) + (m - 1)
				 * grsiz + k;
			if (myid == 1) {
			    ttype = 1;
			} else {
			    ttype = myid % 2 + 2;
			}
			if (ttype == 2) {
			    colpt = myid / 2 * *kd + sweepid;
			    stind = colpt - *kd + 1;
			    edind = f2cmin(colpt,*n);
			    blklastind = colpt;
			} else {
			    colpt = (myid + 1) / 2 * *kd + sweepid;
			    stind = colpt - *kd + 1;
			    edind = f2cmin(colpt,*n);
			    if (stind >= edind - 1 && edind == *n) {
				blklastind = *n;
			    } else {
				blklastind = 0;
			    }
			}

/*                         Call the kernel */

			zhb2st_kernels_(uplo, &wantq, &ttype, &stind, &edind,
				 &sweepid, n, kd, &ib, &work[inda], &lda, &
				hous[indv], &hous[indtau], &ldv, &work[indw + 
				tid * *kd]);
			if (blklastind >= *n - 1) {
			    ++stt;
			    myexit_();
			}
/* L140: */
		    }
/* L130: */
		}
/* L120: */
	    }
/* L110: */
	}
/* L100: */
    }


/*     Copy the diagonal from A to D. Note that D is REAL thus only */
/*     the Real part is needed, the imaginary part should be zero. */

    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	i__2 = dpos + (i__ - 1) * lda;
	d__[i__] = work[i__2].r;
/* L150: */
    }

/*     Copy the off diagonal from A to E. Note that E is REAL thus only */
/*     the Real part is needed, the imaginary part should be zero. */

    if (upper) {
	i__1 = *n - 1;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = ofdpos + i__ * lda;
	    e[i__] = work[i__2].r;
/* L160: */
	}
    } else {
	i__1 = *n - 1;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = ofdpos + (i__ - 1) * lda;
	    e[i__] = work[i__2].r;
/* L170: */
	}
    }

    hous[1].r = (doublereal) lhmin, hous[1].i = 0.;
    work[1].r = (doublereal) lwmin, work[1].i = 0.;
    return;

/*     End of ZHETRD_HB2ST */

} /* zhetrd_hb2st__ */

