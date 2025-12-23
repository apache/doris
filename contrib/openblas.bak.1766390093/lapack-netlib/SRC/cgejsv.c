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

static complex c_b1 = {0.f,0.f};
static complex c_b2 = {1.f,0.f};
static integer c_n1 = -1;
static integer c__1 = 1;
static integer c__0 = 0;
static real c_b141 = 1.f;
static logical c_false = FALSE_;

/* > \brief \b CGEJSV */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CGEJSV + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cgejsv.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cgejsv.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cgejsv.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*     SUBROUTINE CGEJSV( JOBA, JOBU, JOBV, JOBR, JOBT, JOBP, */
/*                         M, N, A, LDA, SVA, U, LDU, V, LDV, */
/*                         CWORK, LWORK, RWORK, LRWORK, IWORK, INFO ) */

/*     IMPLICIT    NONE */
/*     INTEGER     INFO, LDA, LDU, LDV, LWORK, M, N */
/*     COMPLEX     A( LDA, * ),  U( LDU, * ), V( LDV, * ), CWORK( LWORK ) */
/*     REAL        SVA( N ), RWORK( LRWORK ) */
/*     INTEGER     IWORK( * ) */
/*     CHARACTER*1 JOBA, JOBP, JOBR, JOBT, JOBU, JOBV */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CGEJSV computes the singular value decomposition (SVD) of a complex M-by-N */
/* > matrix [A], where M >= N. The SVD of [A] is written as */
/* > */
/* >              [A] = [U] * [SIGMA] * [V]^*, */
/* > */
/* > where [SIGMA] is an N-by-N (M-by-N) matrix which is zero except for its N */
/* > diagonal elements, [U] is an M-by-N (or M-by-M) unitary matrix, and */
/* > [V] is an N-by-N unitary matrix. The diagonal elements of [SIGMA] are */
/* > the singular values of [A]. The columns of [U] and [V] are the left and */
/* > the right singular vectors of [A], respectively. The matrices [U] and [V] */
/* > are computed and stored in the arrays U and V, respectively. The diagonal */
/* > of [SIGMA] is computed and stored in the array SVA. */
/* > \endverbatim */
/* > */
/* >  Arguments: */
/* >  ========== */
/* > */
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
/* >              condition number of B, where A=B*D. If A has heavily weighted */
/* >              rows, then using this condition number gives too pessimistic */
/* >              error bound. */
/* >       = 'A': Small singular values are not well determined by the data */
/* >              and are considered as noisy; the matrix is treated as */
/* >              numerically rank deficient. The error in the computed */
/* >              singular values is bounded by f(m,n)*epsilon*||A||. */
/* >              The computed SVD A = U * S * V^* restores A up to */
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
/* >              computed as the product of Jacobi rotations, if JOBT = 'N'. */
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
/* >              is greater than BIG, use CGESVJ. */
/* >       = 'R': RESTRICTED range for sigma(c*A) is [SQRT(SFMIN), SQRT(BIG)] */
/* >              (roughly, as described above). This option is recommended. */
/* >                                             =========================== */
/* >         For computing the singular values in the FULL range [SFMIN,BIG] */
/* >         use CGESVJ. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBT */
/* > \verbatim */
/* >          JOBT is CHARACTER*1 */
/* >         If the matrix is square then the procedure may determine to use */
/* >         transposed A if A^* seems to be better with respect to convergence. */
/* >         If the matrix is not square, JOBT is ignored. */
/* >         The decision is based on two values of entropy over the adjoint */
/* >         orbit of A^* * A. See the descriptions of WORK(6) and WORK(7). */
/* >       = 'T': transpose if entropy test indicates possibly faster */
/* >         convergence of Jacobi process if A^* is taken as input. If A is */
/* >         replaced with A^*, then the row pivoting is included automatically. */
/* >       = 'N': do not speculate. */
/* >         The option 'T' can be used to compute only the singular values, or */
/* >         the full SVD (U, SIGMA and V). For only one set of singular vectors */
/* >         (U or V), the caller should provide both U and V, as one of the */
/* >         matrices is used as workspace if the matrix A is transposed. */
/* >         The implementer can easily remove this constraint and make the */
/* >         code more complicated. See the descriptions of U and V. */
/* >         In general, this option is considered experimental, and 'N'; should */
/* >         be preferred. This is subject to changes in the future. */
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
/* >          A is COMPLEX array, dimension (LDA,N) */
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
/* >          U is COMPLEX array, dimension ( LDU, N ) or ( LDU, M ) */
/* >          If JOBU = 'U', then U contains on exit the M-by-N matrix of */
/* >                         the left singular vectors. */
/* >          If JOBU = 'F', then U contains on exit the M-by-M matrix of */
/* >                         the left singular vectors, including an ONB */
/* >                         of the orthogonal complement of the Range(A). */
/* >          If JOBU = 'W'  .AND. (JOBV = 'V' .AND. JOBT = 'T' .AND. M = N), */
/* >                         then U is used as workspace if the procedure */
/* >                         replaces A with A^*. In that case, [V] is computed */
/* >                         in U as left singular vectors of A^* and then */
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
/* >          V is COMPLEX array, dimension ( LDV, N ) */
/* >          If JOBV = 'V', 'J' then V contains on exit the N-by-N matrix of */
/* >                         the right singular vectors; */
/* >          If JOBV = 'W', AND (JOBU = 'U' AND JOBT = 'T' AND M = N), */
/* >                         then V is used as workspace if the pprocedure */
/* >                         replaces A with A^*. In that case, [U] is computed */
/* >                         in V as right singular vectors of A^* and then */
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
/* > \param[out] CWORK */
/* > \verbatim */
/* >          CWORK is COMPLEX array, dimension (MAX(2,LWORK)) */
/* >          If the call to CGEJSV is a workspace query (indicated by LWORK=-1 or */
/* >          LRWORK=-1), then on exit CWORK(1) contains the required length of */
/* >          CWORK for the job parameters used in the call. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          Length of CWORK to confirm proper allocation of workspace. */
/* >          LWORK depends on the job: */
/* > */
/* >          1. If only SIGMA is needed ( JOBU = 'N', JOBV = 'N' ) and */
/* >            1.1 .. no scaled condition estimate required (JOBA.NE.'E'.AND.JOBA.NE.'G'): */
/* >               LWORK >= 2*N+1. This is the minimal requirement. */
/* >               ->> For optimal performance (blocked code) the optimal value */
/* >               is LWORK >= N + (N+1)*NB. Here NB is the optimal */
/* >               block size for CGEQP3 and CGEQRF. */
/* >               In general, optimal LWORK is computed as */
/* >               LWORK >= f2cmax(N+LWORK(CGEQP3),N+LWORK(CGEQRF), LWORK(CGESVJ)). */
/* >            1.2. .. an estimate of the scaled condition number of A is */
/* >               required (JOBA='E', or 'G'). In this case, LWORK the minimal */
/* >               requirement is LWORK >= N*N + 2*N. */
/* >               ->> For optimal performance (blocked code) the optimal value */
/* >               is LWORK >= f2cmax(N+(N+1)*NB, N*N+2*N)=N**2+2*N. */
/* >               In general, the optimal length LWORK is computed as */
/* >               LWORK >= f2cmax(N+LWORK(CGEQP3),N+LWORK(CGEQRF), LWORK(CGESVJ), */
/* >                            N*N+LWORK(CPOCON)). */
/* >          2. If SIGMA and the right singular vectors are needed (JOBV = 'V'), */
/* >             (JOBU = 'N') */
/* >            2.1   .. no scaled condition estimate requested (JOBE = 'N'): */
/* >            -> the minimal requirement is LWORK >= 3*N. */
/* >            -> For optimal performance, */
/* >               LWORK >= f2cmax(N+(N+1)*NB, 2*N+N*NB)=2*N+N*NB, */
/* >               where NB is the optimal block size for CGEQP3, CGEQRF, CGELQ, */
/* >               CUNMLQ. In general, the optimal length LWORK is computed as */
/* >               LWORK >= f2cmax(N+LWORK(CGEQP3), N+LWORK(CGESVJ), */
/* >                       N+LWORK(CGELQF), 2*N+LWORK(CGEQRF), N+LWORK(CUNMLQ)). */
/* >            2.2 .. an estimate of the scaled condition number of A is */
/* >               required (JOBA='E', or 'G'). */
/* >            -> the minimal requirement is LWORK >= 3*N. */
/* >            -> For optimal performance, */
/* >               LWORK >= f2cmax(N+(N+1)*NB, 2*N,2*N+N*NB)=2*N+N*NB, */
/* >               where NB is the optimal block size for CGEQP3, CGEQRF, CGELQ, */
/* >               CUNMLQ. In general, the optimal length LWORK is computed as */
/* >               LWORK >= f2cmax(N+LWORK(CGEQP3), LWORK(CPOCON), N+LWORK(CGESVJ), */
/* >                       N+LWORK(CGELQF), 2*N+LWORK(CGEQRF), N+LWORK(CUNMLQ)). */
/* >          3. If SIGMA and the left singular vectors are needed */
/* >            3.1  .. no scaled condition estimate requested (JOBE = 'N'): */
/* >            -> the minimal requirement is LWORK >= 3*N. */
/* >            -> For optimal performance: */
/* >               if JOBU = 'U' :: LWORK >= f2cmax(3*N, N+(N+1)*NB, 2*N+N*NB)=2*N+N*NB, */
/* >               where NB is the optimal block size for CGEQP3, CGEQRF, CUNMQR. */
/* >               In general, the optimal length LWORK is computed as */
/* >               LWORK >= f2cmax(N+LWORK(CGEQP3), 2*N+LWORK(CGEQRF), N+LWORK(CUNMQR)). */
/* >            3.2  .. an estimate of the scaled condition number of A is */
/* >               required (JOBA='E', or 'G'). */
/* >            -> the minimal requirement is LWORK >= 3*N. */
/* >            -> For optimal performance: */
/* >               if JOBU = 'U' :: LWORK >= f2cmax(3*N, N+(N+1)*NB, 2*N+N*NB)=2*N+N*NB, */
/* >               where NB is the optimal block size for CGEQP3, CGEQRF, CUNMQR. */
/* >               In general, the optimal length LWORK is computed as */
/* >               LWORK >= f2cmax(N+LWORK(CGEQP3),N+LWORK(CPOCON), */
/* >                        2*N+LWORK(CGEQRF), N+LWORK(CUNMQR)). */
/* > */
/* >          4. If the full SVD is needed: (JOBU = 'U' or JOBU = 'F') and */
/* >            4.1. if JOBV = 'V' */
/* >               the minimal requirement is LWORK >= 5*N+2*N*N. */
/* >            4.2. if JOBV = 'J' the minimal requirement is */
/* >               LWORK >= 4*N+N*N. */
/* >            In both cases, the allocated CWORK can accommodate blocked runs */
/* >            of CGEQP3, CGEQRF, CGELQF, CUNMQR, CUNMLQ. */
/* > */
/* >          If the call to CGEJSV is a workspace query (indicated by LWORK=-1 or */
/* >          LRWORK=-1), then on exit CWORK(1) contains the optimal and CWORK(2) contains the */
/* >          minimal length of CWORK for the job parameters used in the call. */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is REAL array, dimension (MAX(7,LWORK)) */
/* >          On exit, */
/* >          RWORK(1) = Determines the scaling factor SCALE = RWORK(2) / RWORK(1) */
/* >                    such that SCALE*SVA(1:N) are the computed singular values */
/* >                    of A. (See the description of SVA().) */
/* >          RWORK(2) = See the description of RWORK(1). */
/* >          RWORK(3) = SCONDA is an estimate for the condition number of */
/* >                    column equilibrated A. (If JOBA = 'E' or 'G') */
/* >                    SCONDA is an estimate of SQRT(||(R^* * R)^(-1)||_1). */
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
/* >          RWORK(4) = an estimate of the scaled condition number of the */
/* >                    triangular factor in the first QR factorization. */
/* >          RWORK(5) = an estimate of the scaled condition number of the */
/* >                    triangular factor in the second QR factorization. */
/* >          The following two parameters are computed if JOBT = 'T'. */
/* >          They are provided for a developer/implementer who is familiar */
/* >          with the details of the method. */
/* >          RWORK(6) = the entropy of A^* * A :: this is the Shannon entropy */
/* >                    of diag(A^* * A) / Trace(A^* * A) taken as point in the */
/* >                    probability simplex. */
/* >          RWORK(7) = the entropy of A * A^*. (See the description of RWORK(6).) */
/* >          If the call to CGEJSV is a workspace query (indicated by LWORK=-1 or */
/* >          LRWORK=-1), then on exit RWORK(1) contains the required length of */
/* >          RWORK for the job parameters used in the call. */
/* > \endverbatim */
/* > */
/* > \param[in] LRWORK */
/* > \verbatim */
/* >          LRWORK is INTEGER */
/* >          Length of RWORK to confirm proper allocation of workspace. */
/* >          LRWORK depends on the job: */
/* > */
/* >       1. If only the singular values are requested i.e. if */
/* >          LSAME(JOBU,'N') .AND. LSAME(JOBV,'N') */
/* >          then: */
/* >          1.1. If LSAME(JOBT,'T') .OR. LSAME(JOBA,'F') .OR. LSAME(JOBA,'G'), */
/* >               then: LRWORK = f2cmax( 7, 2 * M ). */
/* >          1.2. Otherwise, LRWORK  = f2cmax( 7,  N ). */
/* >       2. If singular values with the right singular vectors are requested */
/* >          i.e. if */
/* >          (LSAME(JOBV,'V').OR.LSAME(JOBV,'J')) .AND. */
/* >          .NOT.(LSAME(JOBU,'U').OR.LSAME(JOBU,'F')) */
/* >          then: */
/* >          2.1. If LSAME(JOBT,'T') .OR. LSAME(JOBA,'F') .OR. LSAME(JOBA,'G'), */
/* >          then LRWORK = f2cmax( 7, 2 * M ). */
/* >          2.2. Otherwise, LRWORK  = f2cmax( 7,  N ). */
/* >       3. If singular values with the left singular vectors are requested, i.e. if */
/* >          (LSAME(JOBU,'U').OR.LSAME(JOBU,'F')) .AND. */
/* >          .NOT.(LSAME(JOBV,'V').OR.LSAME(JOBV,'J')) */
/* >          then: */
/* >          3.1. If LSAME(JOBT,'T') .OR. LSAME(JOBA,'F') .OR. LSAME(JOBA,'G'), */
/* >          then LRWORK = f2cmax( 7, 2 * M ). */
/* >          3.2. Otherwise, LRWORK  = f2cmax( 7,  N ). */
/* >       4. If singular values with both the left and the right singular vectors */
/* >          are requested, i.e. if */
/* >          (LSAME(JOBU,'U').OR.LSAME(JOBU,'F')) .AND. */
/* >          (LSAME(JOBV,'V').OR.LSAME(JOBV,'J')) */
/* >          then: */
/* >          4.1. If LSAME(JOBT,'T') .OR. LSAME(JOBA,'F') .OR. LSAME(JOBA,'G'), */
/* >          then LRWORK = f2cmax( 7, 2 * M ). */
/* >          4.2. Otherwise, LRWORK  = f2cmax( 7, N ). */
/* > */
/* >          If, on entry, LRWORK = -1 or LWORK=-1, a workspace query is assumed and */
/* >          the length of RWORK is returned in RWORK(1). */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, of dimension at least 4, that further depends */
/* >          on the job: */
/* > */
/* >          1. If only the singular values are requested then: */
/* >             If ( LSAME(JOBT,'T') .OR. LSAME(JOBA,'F') .OR. LSAME(JOBA,'G') ) */
/* >             then the length of IWORK is N+M; otherwise the length of IWORK is N. */
/* >          2. If the singular values and the right singular vectors are requested then: */
/* >             If ( LSAME(JOBT,'T') .OR. LSAME(JOBA,'F') .OR. LSAME(JOBA,'G') ) */
/* >             then the length of IWORK is N+M; otherwise the length of IWORK is N. */
/* >          3. If the singular values and the left singular vectors are requested then: */
/* >             If ( LSAME(JOBT,'T') .OR. LSAME(JOBA,'F') .OR. LSAME(JOBA,'G') ) */
/* >             then the length of IWORK is N+M; otherwise the length of IWORK is N. */
/* >          4. If the singular values with both the left and the right singular vectors */
/* >             are requested, then: */
/* >             4.1. If LSAME(JOBV,'J') the length of IWORK is determined as follows: */
/* >                  If ( LSAME(JOBT,'T') .OR. LSAME(JOBA,'F') .OR. LSAME(JOBA,'G') ) */
/* >                  then the length of IWORK is N+M; otherwise the length of IWORK is N. */
/* >             4.2. If LSAME(JOBV,'V') the length of IWORK is determined as follows: */
/* >                  If ( LSAME(JOBT,'T') .OR. LSAME(JOBA,'F') .OR. LSAME(JOBA,'G') ) */
/* >                  then the length of IWORK is 2*N+M; otherwise the length of IWORK is 2*N. */
/* > */
/* >          On exit, */
/* >          IWORK(1) = the numerical rank determined after the initial */
/* >                     QR factorization with pivoting. See the descriptions */
/* >                     of JOBA and JOBR. */
/* >          IWORK(2) = the number of the computed nonzero singular values */
/* >          IWORK(3) = if nonzero, a warning message: */
/* >                     If IWORK(3) = 1 then some of the column norms of A */
/* >                     were denormalized floats. The requested high accuracy */
/* >                     is not warranted by the data. */
/* >          IWORK(4) = 1 or -1. If IWORK(4) = 1, then the procedure used A^* to */
/* >                     do the job as specified by the JOB parameters. */
/* >          If the call to CGEJSV is a workspace query (indicated by LWORK = -1 and */
/* >          LRWORK = -1), then on exit IWORK(1) contains the required length of */
/* >          IWORK for the job parameters used in the call. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >           < 0:  if INFO = -i, then the i-th argument had an illegal value. */
/* >           = 0:  successful exit; */
/* >           > 0:  CGEJSV  did not converge in the maximal allowed number */
/* >                 of sweeps. The computed values may be inaccurate. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup complexGEsing */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* >  CGEJSV implements a preconditioned Jacobi SVD algorithm. It uses CGEQP3, */
/* >  CGEQRF, and CGELQF as preprocessors and preconditioners. Optionally, an */
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
/* >  & LAPACK routines called by CGEJSV are implemented to work in that range. */
/* >  If that is not the case, then the restriction for safe computation with */
/* >  the singular values in the range of normalized IEEE numbers is that the */
/* >  spectral condition number kappa(A)=sigma_max(A)/sigma_min(A) does not */
/* >  overflow. This code (CGEJSV) is best used in this restricted range, */
/* >  meaning that singular values of magnitude below ||A||_2 / SLAMCH('O') are */
/* >  returned as zeros. See JOBR for details on this. */
/* >     Further, this implementation is somewhat slower than the one described */
/* >  in [1,2] due to replacement of some non-LAPACK components, and because */
/* >  the choice of some tuning parameters in the iterative part (CGESVJ) is */
/* >  left to the implementer on a particular machine. */
/* >     The rank revealing QR factorization (in this code: CGEQP3) should be */
/* >  implemented as in [3]. We have a new version of CGEQP3 under development */
/* >  that is more robust than the current one in LAPACK, with a cleaner cut in */
/* >  rank deficient cases. It will be available in the SIGMA library [4]. */
/* >  If M is much larger than N, it is obvious that the initial QRF with */
/* >  column pivoting can be preprocessed by the QRF without pivoting. That */
/* >  well known trick is not used in CGEJSV because in some cases heavy row */
/* >  weighting can be treated with complete pivoting. The overhead in cases */
/* >  M much larger than N is then only due to pivoting, but the benefits in */
/* >  terms of accuracy have prevailed. The implementer/user can incorporate */
/* >  this extra QRF step easily. The implementer can also improve data movement */
/* >  (matrix transpose, matrix copy, matrix transposed copy) - this */
/* >  implementation of CGEJSV uses only the simplest, naive data movement. */
/* > \endverbatim */

/* > \par Contributor: */
/*  ================== */
/* > */
/* >  Zlatko Drmac (Zagreb, Croatia) */

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
/* >     Department of Mathematics, University of Zagreb, 2008, 2016. */
/* > \endverbatim */

/* >  \par Bugs, examples and comments: */
/*   ================================= */
/* > */
/* >  Please report all bugs and send interesting examples and/or comments to */
/* >  drmac@math.hr. Thank you. */
/* > */
/*  ===================================================================== */
/* Subroutine */ void cgejsv_(char *joba, char *jobu, char *jobv, char *jobr, 
	char *jobt, char *jobp, integer *m, integer *n, complex *a, integer *
	lda, real *sva, complex *u, integer *ldu, complex *v, integer *ldv, 
	complex *cwork, integer *lwork, real *rwork, integer *lrwork, integer 
	*iwork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, u_dim1, u_offset, v_dim1, v_offset, i__1, i__2, 
	    i__3, i__4, i__5, i__6, i__7, i__8, i__9, i__10, i__11;
    real r__1, r__2, r__3;
    complex q__1;

    /* Local variables */
    integer lwrk_cunmqr__;
    logical defr;
    real aapp, aaqq;
    logical kill;
    integer ierr, lwrk_cgeqp3n__;
    real temp1;
    integer lwunmqrm, lwrk_cgesvju__, lwrk_cgesvjv__, lwqp3, lwrk_cunmqrm__, 
	    p, q;
    logical jracc;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void sscal_(integer *, real *, real *, integer *);
    complex ctemp;
    real entra, small;
    integer iwoff;
    real sfmin;
    logical lsvec;
    extern /* Subroutine */ void ccopy_(integer *, complex *, integer *, 
	    complex *, integer *), cswap_(integer *, complex *, integer *, 
	    complex *, integer *);
    real epsln;
    logical rsvec;
    integer lwcon, lwlqf;
    extern /* Subroutine */ void ctrsm_(char *, char *, char *, char *, 
	    integer *, integer *, complex *, complex *, integer *, complex *, 
	    integer *);
    integer lwqrf, n1;
    logical l2aber;
    extern /* Subroutine */ void cgeqp3_(integer *, integer *, complex *, 
	    integer *, integer *, complex *, complex *, integer *, real *, 
	    integer *);
    real condr1, condr2, uscal1, uscal2;
    logical l2kill, l2rank, l2tran;
    extern real scnrm2_(integer *, complex *, integer *);
    logical l2pert;
    integer lrwqp3;
    extern /* Subroutine */ void clacgv_(integer *, complex *, integer *);
    integer nr;
    extern /* Subroutine */ void cgelqf_(integer *, integer *, complex *, 
	    integer *, complex *, complex *, integer *, integer *);
    extern integer icamax_(integer *, complex *, integer *);
    extern /* Subroutine */ void clascl_(char *, integer *, integer *, real *, 
	    real *, integer *, integer *, complex *, integer *, integer *);
    real scalem, sconda;
    logical goscal;
    real aatmin;
    extern real slamch_(char *);
    real aatmax;
    extern /* Subroutine */ void cgeqrf_(integer *, integer *, complex *, 
	    integer *, complex *, complex *, integer *, integer *), clacpy_(
	    char *, integer *, integer *, complex *, integer *, complex *, 
	    integer *), clapmr_(logical *, integer *, integer *, 
	    complex *, integer *, integer *);
    logical noscal;
    extern /* Subroutine */ void claset_(char *, integer *, integer *, complex 
	    *, complex *, complex *, integer *);
    extern integer isamax_(integer *, real *, integer *);
    extern /* Subroutine */ void slascl_(char *, integer *, integer *, real *, 
	    real *, integer *, integer *, real *, integer *, integer *), cpocon_(char *, integer *, complex *, integer *, real *, 
	    real *, complex *, real *, integer *), csscal_(integer *, 
	    real *, complex *, integer *), classq_(integer *, complex *, 
	    integer *, real *, real *);
    extern int xerbla_(char *, integer *, ftnlen); 
    extern void cgesvj_(char *, char *, char *, integer *, integer *, complex *, 
	    integer *, real *, integer *, complex *, integer *, complex *, 
	    integer *, real *, integer *, integer *); 
    extern int claswp_(integer *, complex *, integer *, integer *, integer *, 
	    integer *, integer *);
    real entrat;
    logical almort;
    complex cdummy[1];
    extern /* Subroutine */ void cungqr_(integer *, integer *, integer *, 
	    complex *, integer *, complex *, complex *, integer *, integer *);
    real maxprj;
    extern /* Subroutine */ void cunmlq_(char *, char *, integer *, integer *, 
	    integer *, complex *, integer *, complex *, complex *, integer *, 
	    complex *, integer *, integer *);
    logical errest;
    integer lrwcon;
    extern /* Subroutine */ void slassq_(integer *, real *, integer *, real *, 
	    real *);
    logical transp;
    integer minwrk, lwsvdj;
    extern /* Subroutine */ void cunmqr_(char *, char *, integer *, integer *, 
	    integer *, complex *, integer *, complex *, complex *, integer *, 
	    complex *, integer *, integer *);
    real rdummy[1];
    logical lquery, rowpiv;
    integer optwrk;
    real big;
    integer lwrk_cgeqp3__;
    real cond_ok__, xsc, big1;
    integer warning, numrank, lwrk_cgelqf__, miniwrk, lwrk_cgeqrf__, minrwrk, 
	    lrwsvdj, lwunmlq, lwsvdjv, lwrk_cgesvj__, lwunmqr, lwrk_cunmlq__;


/*  -- LAPACK computational routine (version 3.7.1) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2017 */


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
    --cwork;
    --rwork;
    --iwork;

    /* Function Body */
    lsvec = lsame_(jobu, "U") || lsame_(jobu, "F");
    jracc = lsame_(jobv, "J");
    rsvec = lsame_(jobv, "V") || jracc;
    rowpiv = lsame_(joba, "F") || lsame_(joba, "G");
    l2rank = lsame_(joba, "R");
    l2aber = lsame_(joba, "A");
    errest = lsame_(joba, "E") || lsame_(joba, "G");
    l2tran = lsame_(jobt, "T") && *m == *n;
    l2kill = lsame_(jobr, "R");
    defr = lsame_(jobr, "N");
    l2pert = lsame_(jobp, "P");

    lquery = *lwork == -1 || *lrwork == -1;

    if (! (rowpiv || l2rank || l2aber || errest || lsame_(joba, "C"))) {
	*info = -1;
    } else if (! (lsvec || lsame_(jobu, "N") || lsame_(
	    jobu, "W") && rsvec && l2tran)) {
	*info = -2;
    } else if (! (rsvec || lsame_(jobv, "N") || lsame_(
	    jobv, "W") && lsvec && l2tran)) {
	*info = -3;
    } else if (! (l2kill || defr)) {
	*info = -4;
    } else if (! (lsame_(jobt, "T") || lsame_(jobt, 
	    "N"))) {
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
    } else {
/*        #:) */
	*info = 0;
    }

    if (*info == 0) {
/*         [[The expressions for computing the minimal and the optimal */
/*         values of LCWORK, LRWORK are written with a lot of redundancy and */
/*         can be simplified. However, this verbose form is useful for */
/*         maintenance and modifications of the code.]] */

/*         CGEQRF of an N x N matrix, CGELQF of an N x N matrix, */
/*         CUNMLQ for computing N x N matrix, CUNMQR for computing N x N */
/*         matrix, CUNMQR for computing M x N matrix, respectively. */
	lwqp3 = *n + 1;
	lwqrf = f2cmax(1,*n);
	lwlqf = f2cmax(1,*n);
	lwunmlq = f2cmax(1,*n);
	lwunmqr = f2cmax(1,*n);
	lwunmqrm = f2cmax(1,*m);
	lwcon = *n << 1;
/*         without and with explicit accumulation of Jacobi rotations */
/* Computing MAX */
	i__1 = *n << 1;
	lwsvdj = f2cmax(i__1,1);
/* Computing MAX */
	i__1 = *n << 1;
	lwsvdjv = f2cmax(i__1,1);
	lrwqp3 = *n << 1;
	lrwcon = *n;
	lrwsvdj = *n;
	if (lquery) {
	    cgeqp3_(m, n, &a[a_offset], lda, &iwork[1], cdummy, cdummy, &c_n1,
		     rdummy, &ierr);
	    lwrk_cgeqp3__ = cdummy[0].r;
	    cgeqrf_(n, n, &a[a_offset], lda, cdummy, cdummy, &c_n1, &ierr);
	    lwrk_cgeqrf__ = cdummy[0].r;
	    cgelqf_(n, n, &a[a_offset], lda, cdummy, cdummy, &c_n1, &ierr);
	    lwrk_cgelqf__ = cdummy[0].r;
	}
	minwrk = 2;
	optwrk = 2;
	miniwrk = *n;
	if (! (lsvec || rsvec)) {
/*             only the singular values are requested */
	    if (errest) {
/* Computing MAX */
/* Computing 2nd power */
		i__3 = *n;
		i__1 = *n + lwqp3, i__2 = i__3 * i__3 + lwcon, i__1 = f2cmax(
			i__1,i__2), i__2 = *n + lwqrf, i__1 = f2cmax(i__1,i__2);
		minwrk = f2cmax(i__1,lwsvdj);
	    } else {
/* Computing MAX */
		i__1 = *n + lwqp3, i__2 = *n + lwqrf, i__1 = f2cmax(i__1,i__2);
		minwrk = f2cmax(i__1,lwsvdj);
	    }
	    if (lquery) {
		cgesvj_("L", "N", "N", n, n, &a[a_offset], lda, &sva[1], n, &
			v[v_offset], ldv, cdummy, &c_n1, rdummy, &c_n1, &ierr);
		lwrk_cgesvj__ = cdummy[0].r;
		if (errest) {
/* Computing MAX */
/* Computing 2nd power */
		    i__3 = *n;
		    i__1 = *n + lwrk_cgeqp3__, i__2 = i__3 * i__3 + lwcon, 
			    i__1 = f2cmax(i__1,i__2), i__2 = *n + lwrk_cgeqrf__, 
			    i__1 = f2cmax(i__1,i__2);
		    optwrk = f2cmax(i__1,lwrk_cgesvj__);
		} else {
/* Computing MAX */
		    i__1 = *n + lwrk_cgeqp3__, i__2 = *n + lwrk_cgeqrf__, 
			    i__1 = f2cmax(i__1,i__2);
		    optwrk = f2cmax(i__1,lwrk_cgesvj__);
		}
	    }
	    if (l2tran || rowpiv) {
		if (errest) {
/* Computing MAX */
		    i__1 = 7, i__2 = *m << 1, i__1 = f2cmax(i__1,i__2), i__1 = 
			    f2cmax(i__1,lrwqp3), i__1 = f2cmax(i__1,lrwcon);
		    minrwrk = f2cmax(i__1,lrwsvdj);
		} else {
/* Computing MAX */
		    i__1 = 7, i__2 = *m << 1, i__1 = f2cmax(i__1,i__2), i__1 = 
			    f2cmax(i__1,lrwqp3);
		    minrwrk = f2cmax(i__1,lrwsvdj);
		}
	    } else {
		if (errest) {
/* Computing MAX */
		    i__1 = f2cmax(7,lrwqp3), i__1 = f2cmax(i__1,lrwcon);
		    minrwrk = f2cmax(i__1,lrwsvdj);
		} else {
/* Computing MAX */
		    i__1 = f2cmax(7,lrwqp3);
		    minrwrk = f2cmax(i__1,lrwsvdj);
		}
	    }
	    if (rowpiv || l2tran) {
		miniwrk += *m;
	    }
	} else if (rsvec && ! lsvec) {
/*            singular values and the right singular vectors are requested */
	    if (errest) {
/* Computing MAX */
		i__1 = *n + lwqp3, i__1 = f2cmax(i__1,lwcon), i__1 = f2cmax(i__1,
			lwsvdj), i__2 = *n + lwlqf, i__1 = f2cmax(i__1,i__2), 
			i__2 = (*n << 1) + lwqrf, i__1 = f2cmax(i__1,i__2), i__2 
			= *n + lwsvdj, i__1 = f2cmax(i__1,i__2), i__2 = *n + 
			lwunmlq;
		minwrk = f2cmax(i__1,i__2);
	    } else {
/* Computing MAX */
		i__1 = *n + lwqp3, i__1 = f2cmax(i__1,lwsvdj), i__2 = *n + lwlqf,
			 i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) + lwqrf, 
			i__1 = f2cmax(i__1,i__2), i__2 = *n + lwsvdj, i__1 = f2cmax(
			i__1,i__2), i__2 = *n + lwunmlq;
		minwrk = f2cmax(i__1,i__2);
	    }
	    if (lquery) {
		cgesvj_("L", "U", "N", n, n, &u[u_offset], ldu, &sva[1], n, &
			a[a_offset], lda, cdummy, &c_n1, rdummy, &c_n1, &ierr);
		lwrk_cgesvj__ = cdummy[0].r;
		cunmlq_("L", "C", n, n, n, &a[a_offset], lda, cdummy, &v[
			v_offset], ldv, cdummy, &c_n1, &ierr);
		lwrk_cunmlq__ = cdummy[0].r;
		if (errest) {
/* Computing MAX */
		    i__1 = *n + lwrk_cgeqp3__, i__1 = f2cmax(i__1,lwcon), i__1 = 
			    f2cmax(i__1,lwrk_cgesvj__), i__2 = *n + 
			    lwrk_cgelqf__, i__1 = f2cmax(i__1,i__2), i__2 = (*n 
			    << 1) + lwrk_cgeqrf__, i__1 = f2cmax(i__1,i__2), 
			    i__2 = *n + lwrk_cgesvj__, i__1 = f2cmax(i__1,i__2), 
			    i__2 = *n + lwrk_cunmlq__;
		    optwrk = f2cmax(i__1,i__2);
		} else {
/* Computing MAX */
		    i__1 = *n + lwrk_cgeqp3__, i__1 = f2cmax(i__1,lwrk_cgesvj__),
			     i__2 = *n + lwrk_cgelqf__, i__1 = f2cmax(i__1,i__2),
			     i__2 = (*n << 1) + lwrk_cgeqrf__, i__1 = f2cmax(
			    i__1,i__2), i__2 = *n + lwrk_cgesvj__, i__1 = f2cmax(
			    i__1,i__2), i__2 = *n + lwrk_cunmlq__;
		    optwrk = f2cmax(i__1,i__2);
		}
	    }
	    if (l2tran || rowpiv) {
		if (errest) {
/* Computing MAX */
		    i__1 = 7, i__2 = *m << 1, i__1 = f2cmax(i__1,i__2), i__1 = 
			    f2cmax(i__1,lrwqp3), i__1 = f2cmax(i__1,lrwsvdj);
		    minrwrk = f2cmax(i__1,lrwcon);
		} else {
/* Computing MAX */
		    i__1 = 7, i__2 = *m << 1, i__1 = f2cmax(i__1,i__2), i__1 = 
			    f2cmax(i__1,lrwqp3);
		    minrwrk = f2cmax(i__1,lrwsvdj);
		}
	    } else {
		if (errest) {
/* Computing MAX */
		    i__1 = f2cmax(7,lrwqp3), i__1 = f2cmax(i__1,lrwsvdj);
		    minrwrk = f2cmax(i__1,lrwcon);
		} else {
/* Computing MAX */
		    i__1 = f2cmax(7,lrwqp3);
		    minrwrk = f2cmax(i__1,lrwsvdj);
		}
	    }
	    if (rowpiv || l2tran) {
		miniwrk += *m;
	    }
	} else if (lsvec && ! rsvec) {
/*            singular values and the left singular vectors are requested */
	    if (errest) {
/* Computing MAX */
		i__1 = f2cmax(lwqp3,lwcon), i__2 = *n + lwqrf, i__1 = f2cmax(i__1,
			i__2), i__1 = f2cmax(i__1,lwsvdj);
		minwrk = *n + f2cmax(i__1,lwunmqrm);
	    } else {
/* Computing MAX */
		i__1 = lwqp3, i__2 = *n + lwqrf, i__1 = f2cmax(i__1,i__2), i__1 =
			 f2cmax(i__1,lwsvdj);
		minwrk = *n + f2cmax(i__1,lwunmqrm);
	    }
	    if (lquery) {
		cgesvj_("L", "U", "N", n, n, &u[u_offset], ldu, &sva[1], n, &
			a[a_offset], lda, cdummy, &c_n1, rdummy, &c_n1, &ierr);
		lwrk_cgesvj__ = cdummy[0].r;
		cunmqr_("L", "N", m, n, n, &a[a_offset], lda, cdummy, &u[
			u_offset], ldu, cdummy, &c_n1, &ierr);
		lwrk_cunmqrm__ = cdummy[0].r;
		if (errest) {
/* Computing MAX */
		    i__1 = f2cmax(lwrk_cgeqp3__,lwcon), i__2 = *n + 
			    lwrk_cgeqrf__, i__1 = f2cmax(i__1,i__2), i__1 = f2cmax(
			    i__1,lwrk_cgesvj__);
		    optwrk = *n + f2cmax(i__1,lwrk_cunmqrm__);
		} else {
/* Computing MAX */
		    i__1 = lwrk_cgeqp3__, i__2 = *n + lwrk_cgeqrf__, i__1 = 
			    f2cmax(i__1,i__2), i__1 = f2cmax(i__1,lwrk_cgesvj__);
		    optwrk = *n + f2cmax(i__1,lwrk_cunmqrm__);
		}
	    }
	    if (l2tran || rowpiv) {
		if (errest) {
/* Computing MAX */
		    i__1 = 7, i__2 = *m << 1, i__1 = f2cmax(i__1,i__2), i__1 = 
			    f2cmax(i__1,lrwqp3), i__1 = f2cmax(i__1,lrwsvdj);
		    minrwrk = f2cmax(i__1,lrwcon);
		} else {
/* Computing MAX */
		    i__1 = 7, i__2 = *m << 1, i__1 = f2cmax(i__1,i__2), i__1 = 
			    f2cmax(i__1,lrwqp3);
		    minrwrk = f2cmax(i__1,lrwsvdj);
		}
	    } else {
		if (errest) {
/* Computing MAX */
		    i__1 = f2cmax(7,lrwqp3), i__1 = f2cmax(i__1,lrwsvdj);
		    minrwrk = f2cmax(i__1,lrwcon);
		} else {
/* Computing MAX */
		    i__1 = f2cmax(7,lrwqp3);
		    minrwrk = f2cmax(i__1,lrwsvdj);
		}
	    }
	    if (rowpiv || l2tran) {
		miniwrk += *m;
	    }
	} else {
/*            full SVD is requested */
	    if (! jracc) {
		if (errest) {
/* Computing MAX */
/* Computing 2nd power */
		    i__3 = *n;
/* Computing 2nd power */
		    i__4 = *n;
/* Computing 2nd power */
		    i__5 = *n;
/* Computing 2nd power */
		    i__6 = *n;
/* Computing 2nd power */
		    i__7 = *n;
/* Computing 2nd power */
		    i__8 = *n;
/* Computing 2nd power */
		    i__9 = *n;
/* Computing 2nd power */
		    i__10 = *n;
/* Computing 2nd power */
		    i__11 = *n;
		    i__1 = *n + lwqp3, i__2 = *n + lwcon, i__1 = f2cmax(i__1,
			    i__2), i__2 = (*n << 1) + i__3 * i__3 + lwcon, 
			    i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) + lwqrf, 
			    i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) + lwqp3, 
			    i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) + i__4 * 
			    i__4 + *n + lwlqf, i__1 = f2cmax(i__1,i__2), i__2 = (
			    *n << 1) + i__5 * i__5 + *n + i__6 * i__6 + lwcon,
			     i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) + i__7 * 
			    i__7 + *n + lwsvdj, i__1 = f2cmax(i__1,i__2), i__2 = 
			    (*n << 1) + i__8 * i__8 + *n + lwsvdjv, i__1 = 
			    f2cmax(i__1,i__2), i__2 = (*n << 1) + i__9 * i__9 + *
			    n + lwunmqr, i__1 = f2cmax(i__1,i__2), i__2 = (*n << 
			    1) + i__10 * i__10 + *n + lwunmlq, i__1 = f2cmax(
			    i__1,i__2), i__2 = *n + i__11 * i__11 + lwsvdj, 
			    i__1 = f2cmax(i__1,i__2), i__2 = *n + lwunmqrm;
		    minwrk = f2cmax(i__1,i__2);
		} else {
/* Computing MAX */
/* Computing 2nd power */
		    i__3 = *n;
/* Computing 2nd power */
		    i__4 = *n;
/* Computing 2nd power */
		    i__5 = *n;
/* Computing 2nd power */
		    i__6 = *n;
/* Computing 2nd power */
		    i__7 = *n;
/* Computing 2nd power */
		    i__8 = *n;
/* Computing 2nd power */
		    i__9 = *n;
/* Computing 2nd power */
		    i__10 = *n;
/* Computing 2nd power */
		    i__11 = *n;
		    i__1 = *n + lwqp3, i__2 = (*n << 1) + i__3 * i__3 + lwcon,
			     i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) + lwqrf, 
			    i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) + lwqp3, 
			    i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) + i__4 * 
			    i__4 + *n + lwlqf, i__1 = f2cmax(i__1,i__2), i__2 = (
			    *n << 1) + i__5 * i__5 + *n + i__6 * i__6 + lwcon,
			     i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) + i__7 * 
			    i__7 + *n + lwsvdj, i__1 = f2cmax(i__1,i__2), i__2 = 
			    (*n << 1) + i__8 * i__8 + *n + lwsvdjv, i__1 = 
			    f2cmax(i__1,i__2), i__2 = (*n << 1) + i__9 * i__9 + *
			    n + lwunmqr, i__1 = f2cmax(i__1,i__2), i__2 = (*n << 
			    1) + i__10 * i__10 + *n + lwunmlq, i__1 = f2cmax(
			    i__1,i__2), i__2 = *n + i__11 * i__11 + lwsvdj, 
			    i__1 = f2cmax(i__1,i__2), i__2 = *n + lwunmqrm;
		    minwrk = f2cmax(i__1,i__2);
		}
		miniwrk += *n;
		if (rowpiv || l2tran) {
		    miniwrk += *m;
		}
	    } else {
		if (errest) {
/* Computing MAX */
/* Computing 2nd power */
		    i__3 = *n;
/* Computing 2nd power */
		    i__4 = *n;
		    i__1 = *n + lwqp3, i__2 = *n + lwcon, i__1 = f2cmax(i__1,
			    i__2), i__2 = (*n << 1) + lwqrf, i__1 = f2cmax(i__1,
			    i__2), i__2 = (*n << 1) + i__3 * i__3 + lwsvdjv, 
			    i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) + i__4 * 
			    i__4 + *n + lwunmqr, i__1 = f2cmax(i__1,i__2), i__2 =
			     *n + lwunmqrm;
		    minwrk = f2cmax(i__1,i__2);
		} else {
/* Computing MAX */
/* Computing 2nd power */
		    i__3 = *n;
/* Computing 2nd power */
		    i__4 = *n;
		    i__1 = *n + lwqp3, i__2 = (*n << 1) + lwqrf, i__1 = f2cmax(
			    i__1,i__2), i__2 = (*n << 1) + i__3 * i__3 + 
			    lwsvdjv, i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) 
			    + i__4 * i__4 + *n + lwunmqr, i__1 = f2cmax(i__1,
			    i__2), i__2 = *n + lwunmqrm;
		    minwrk = f2cmax(i__1,i__2);
		}
		if (rowpiv || l2tran) {
		    miniwrk += *m;
		}
	    }
	    if (lquery) {
		cunmqr_("L", "N", m, n, n, &a[a_offset], lda, cdummy, &u[
			u_offset], ldu, cdummy, &c_n1, &ierr);
		lwrk_cunmqrm__ = cdummy[0].r;
		cunmqr_("L", "N", n, n, n, &a[a_offset], lda, cdummy, &u[
			u_offset], ldu, cdummy, &c_n1, &ierr);
		lwrk_cunmqr__ = cdummy[0].r;
		if (! jracc) {
		    cgeqp3_(n, n, &a[a_offset], lda, &iwork[1], cdummy, 
			    cdummy, &c_n1, rdummy, &ierr);
		    lwrk_cgeqp3n__ = cdummy[0].r;
		    cgesvj_("L", "U", "N", n, n, &u[u_offset], ldu, &sva[1], 
			    n, &v[v_offset], ldv, cdummy, &c_n1, rdummy, &
			    c_n1, &ierr);
		    lwrk_cgesvj__ = cdummy[0].r;
		    cgesvj_("U", "U", "N", n, n, &u[u_offset], ldu, &sva[1], 
			    n, &v[v_offset], ldv, cdummy, &c_n1, rdummy, &
			    c_n1, &ierr);
		    lwrk_cgesvju__ = cdummy[0].r;
		    cgesvj_("L", "U", "V", n, n, &u[u_offset], ldu, &sva[1], 
			    n, &v[v_offset], ldv, cdummy, &c_n1, rdummy, &
			    c_n1, &ierr);
		    lwrk_cgesvjv__ = cdummy[0].r;
		    cunmlq_("L", "C", n, n, n, &a[a_offset], lda, cdummy, &v[
			    v_offset], ldv, cdummy, &c_n1, &ierr);
		    lwrk_cunmlq__ = cdummy[0].r;
		    if (errest) {
/* Computing MAX */
/* Computing 2nd power */
			i__3 = *n;
/* Computing 2nd power */
			i__4 = *n;
/* Computing 2nd power */
			i__5 = *n;
/* Computing 2nd power */
			i__6 = *n;
/* Computing 2nd power */
			i__7 = *n;
/* Computing 2nd power */
			i__8 = *n;
/* Computing 2nd power */
			i__9 = *n;
/* Computing 2nd power */
			i__10 = *n;
/* Computing 2nd power */
			i__11 = *n;
			i__1 = *n + lwrk_cgeqp3__, i__2 = *n + lwcon, i__1 = 
				f2cmax(i__1,i__2), i__2 = (*n << 1) + i__3 * 
				i__3 + lwcon, i__1 = f2cmax(i__1,i__2), i__2 = (*
				n << 1) + lwrk_cgeqrf__, i__1 = f2cmax(i__1,i__2)
				, i__2 = (*n << 1) + lwrk_cgeqp3n__, i__1 = 
				f2cmax(i__1,i__2), i__2 = (*n << 1) + i__4 * 
				i__4 + *n + lwrk_cgelqf__, i__1 = f2cmax(i__1,
				i__2), i__2 = (*n << 1) + i__5 * i__5 + *n + 
				i__6 * i__6 + lwcon, i__1 = f2cmax(i__1,i__2), 
				i__2 = (*n << 1) + i__7 * i__7 + *n + 
				lwrk_cgesvj__, i__1 = f2cmax(i__1,i__2), i__2 = (
				*n << 1) + i__8 * i__8 + *n + lwrk_cgesvjv__, 
				i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) + 
				i__9 * i__9 + *n + lwrk_cunmqr__, i__1 = f2cmax(
				i__1,i__2), i__2 = (*n << 1) + i__10 * i__10 
				+ *n + lwrk_cunmlq__, i__1 = f2cmax(i__1,i__2), 
				i__2 = *n + i__11 * i__11 + lwrk_cgesvju__, 
				i__1 = f2cmax(i__1,i__2), i__2 = *n + 
				lwrk_cunmqrm__;
			optwrk = f2cmax(i__1,i__2);
		    } else {
/* Computing MAX */
/* Computing 2nd power */
			i__3 = *n;
/* Computing 2nd power */
			i__4 = *n;
/* Computing 2nd power */
			i__5 = *n;
/* Computing 2nd power */
			i__6 = *n;
/* Computing 2nd power */
			i__7 = *n;
/* Computing 2nd power */
			i__8 = *n;
/* Computing 2nd power */
			i__9 = *n;
/* Computing 2nd power */
			i__10 = *n;
/* Computing 2nd power */
			i__11 = *n;
			i__1 = *n + lwrk_cgeqp3__, i__2 = (*n << 1) + i__3 * 
				i__3 + lwcon, i__1 = f2cmax(i__1,i__2), i__2 = (*
				n << 1) + lwrk_cgeqrf__, i__1 = f2cmax(i__1,i__2)
				, i__2 = (*n << 1) + lwrk_cgeqp3n__, i__1 = 
				f2cmax(i__1,i__2), i__2 = (*n << 1) + i__4 * 
				i__4 + *n + lwrk_cgelqf__, i__1 = f2cmax(i__1,
				i__2), i__2 = (*n << 1) + i__5 * i__5 + *n + 
				i__6 * i__6 + lwcon, i__1 = f2cmax(i__1,i__2), 
				i__2 = (*n << 1) + i__7 * i__7 + *n + 
				lwrk_cgesvj__, i__1 = f2cmax(i__1,i__2), i__2 = (
				*n << 1) + i__8 * i__8 + *n + lwrk_cgesvjv__, 
				i__1 = f2cmax(i__1,i__2), i__2 = (*n << 1) + 
				i__9 * i__9 + *n + lwrk_cunmqr__, i__1 = f2cmax(
				i__1,i__2), i__2 = (*n << 1) + i__10 * i__10 
				+ *n + lwrk_cunmlq__, i__1 = f2cmax(i__1,i__2), 
				i__2 = *n + i__11 * i__11 + lwrk_cgesvju__, 
				i__1 = f2cmax(i__1,i__2), i__2 = *n + 
				lwrk_cunmqrm__;
			optwrk = f2cmax(i__1,i__2);
		    }
		} else {
		    cgesvj_("L", "U", "V", n, n, &u[u_offset], ldu, &sva[1], 
			    n, &v[v_offset], ldv, cdummy, &c_n1, rdummy, &
			    c_n1, &ierr);
		    lwrk_cgesvjv__ = cdummy[0].r;
		    cunmqr_("L", "N", n, n, n, cdummy, n, cdummy, &v[v_offset]
			    , ldv, cdummy, &c_n1, &ierr)
			    ;
		    lwrk_cunmqr__ = cdummy[0].r;
		    cunmqr_("L", "N", m, n, n, &a[a_offset], lda, cdummy, &u[
			    u_offset], ldu, cdummy, &c_n1, &ierr);
		    lwrk_cunmqrm__ = cdummy[0].r;
		    if (errest) {
/* Computing MAX */
/* Computing 2nd power */
			i__3 = *n;
/* Computing 2nd power */
			i__4 = *n;
/* Computing 2nd power */
			i__5 = *n;
			i__1 = *n + lwrk_cgeqp3__, i__2 = *n + lwcon, i__1 = 
				f2cmax(i__1,i__2), i__2 = (*n << 1) + 
				lwrk_cgeqrf__, i__1 = f2cmax(i__1,i__2), i__2 = (
				*n << 1) + i__3 * i__3, i__1 = f2cmax(i__1,i__2),
				 i__2 = (*n << 1) + i__4 * i__4 + 
				lwrk_cgesvjv__, i__1 = f2cmax(i__1,i__2), i__2 = 
				(*n << 1) + i__5 * i__5 + *n + lwrk_cunmqr__, 
				i__1 = f2cmax(i__1,i__2), i__2 = *n + 
				lwrk_cunmqrm__;
			optwrk = f2cmax(i__1,i__2);
		    } else {
/* Computing MAX */
/* Computing 2nd power */
			i__3 = *n;
/* Computing 2nd power */
			i__4 = *n;
/* Computing 2nd power */
			i__5 = *n;
			i__1 = *n + lwrk_cgeqp3__, i__2 = (*n << 1) + 
				lwrk_cgeqrf__, i__1 = f2cmax(i__1,i__2), i__2 = (
				*n << 1) + i__3 * i__3, i__1 = f2cmax(i__1,i__2),
				 i__2 = (*n << 1) + i__4 * i__4 + 
				lwrk_cgesvjv__, i__1 = f2cmax(i__1,i__2), i__2 = 
				(*n << 1) + i__5 * i__5 + *n + lwrk_cunmqr__, 
				i__1 = f2cmax(i__1,i__2), i__2 = *n + 
				lwrk_cunmqrm__;
			optwrk = f2cmax(i__1,i__2);
		    }
		}
	    }
	    if (l2tran || rowpiv) {
/* Computing MAX */
		i__1 = 7, i__2 = *m << 1, i__1 = f2cmax(i__1,i__2), i__1 = f2cmax(
			i__1,lrwqp3), i__1 = f2cmax(i__1,lrwsvdj);
		minrwrk = f2cmax(i__1,lrwcon);
	    } else {
/* Computing MAX */
		i__1 = f2cmax(7,lrwqp3), i__1 = f2cmax(i__1,lrwsvdj);
		minrwrk = f2cmax(i__1,lrwcon);
	    }
	}
	minwrk = f2cmax(2,minwrk);
	optwrk = f2cmax(optwrk,minwrk);
	if (*lwork < minwrk && ! lquery) {
	    *info = -17;
	}
	if (*lrwork < minrwrk && ! lquery) {
	    *info = -19;
	}
    }

    if (*info != 0) {
/*       #:( */
	i__1 = -(*info);
	xerbla_("CGEJSV", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	cwork[1].r = (real) optwrk, cwork[1].i = 0.f;
	cwork[2].r = (real) minwrk, cwork[2].i = 0.f;
	rwork[1] = (real) minrwrk;
	iwork[1] = f2cmax(4,miniwrk);
	return;
    }

/*     Quick return for void matrix (Y3K safe) */
/* #:) */
    if (*m == 0 || *n == 0) {
	iwork[1] = 0;
	iwork[2] = 0;
	iwork[3] = 0;
	iwork[4] = 0;
	rwork[1] = 0.f;
	rwork[2] = 0.f;
	rwork[3] = 0.f;
	rwork[4] = 0.f;
	rwork[5] = 0.f;
	rwork[6] = 0.f;
	rwork[7] = 0.f;
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
	classq_(m, &a[p * a_dim1 + 1], &c__1, &aapp, &aaqq);
	if (aapp > big) {
	    *info = -9;
	    i__2 = -(*info);
	    xerbla_("CGEJSV", &i__2, (ftnlen)6);
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
	    claset_("G", m, &n1, &c_b1, &c_b2, &u[u_offset], ldu);
	}
	if (rsvec) {
	    claset_("G", n, n, &c_b1, &c_b2, &v[v_offset], ldv);
	}
	rwork[1] = 1.f;
	rwork[2] = 1.f;
	if (errest) {
	    rwork[3] = 1.f;
	}
	if (lsvec && rsvec) {
	    rwork[4] = 1.f;
	    rwork[5] = 1.f;
	}
	if (l2tran) {
	    rwork[6] = 0.f;
	    rwork[7] = 0.f;
	}
	iwork[1] = 0;
	iwork[2] = 0;
	iwork[3] = 0;
	iwork[4] = -1;
	return;
    }

/*     Issue warning if denormalized column norms detected. Override the */
/*     high relative accuracy request. Issue licence to kill nonzero columns */
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
	    clascl_("G", &c__0, &c__0, &sva[1], &scalem, m, &c__1, &a[a_dim1 
		    + 1], lda, &ierr);
	    clacpy_("A", m, &c__1, &a[a_offset], lda, &u[u_offset], ldu);
/*           computing all M left singular vectors of the M x 1 matrix */
	    if (n1 != *n) {
		i__1 = *lwork - *n;
		cgeqrf_(m, n, &u[u_offset], ldu, &cwork[1], &cwork[*n + 1], &
			i__1, &ierr);
		i__1 = *lwork - *n;
		cungqr_(m, &n1, &c__1, &u[u_offset], ldu, &cwork[1], &cwork[*
			n + 1], &i__1, &ierr);
		ccopy_(m, &a[a_dim1 + 1], &c__1, &u[u_dim1 + 1], &c__1);
	    }
	}
	if (rsvec) {
	    i__1 = v_dim1 + 1;
	    v[i__1].r = 1.f, v[i__1].i = 0.f;
	}
	if (sva[1] < big * scalem) {
	    sva[1] /= scalem;
	    scalem = 1.f;
	}
	rwork[1] = 1.f / scalem;
	rwork[2] = 1.f;
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
	iwork[4] = -1;
	if (errest) {
	    rwork[3] = 1.f;
	}
	if (lsvec && rsvec) {
	    rwork[4] = 1.f;
	    rwork[5] = 1.f;
	}
	if (l2tran) {
	    rwork[6] = 0.f;
	    rwork[7] = 0.f;
	}
	return;

    }

    transp = FALSE_;

    aatmax = -1.f;
    aatmin = big;
    if (rowpiv || l2tran) {

/*     Compute the row norms, needed to determine row pivoting sequence */
/*     (in the case of heavily row weighted A, row pivoting is strongly */
/*     advised) and to collect information needed to compare the */
/*     structures of A * A^* and A^* * A (in the case L2TRAN.EQ..TRUE.). */

	if (l2tran) {
	    i__1 = *m;
	    for (p = 1; p <= i__1; ++p) {
		xsc = 0.f;
		temp1 = 1.f;
		classq_(n, &a[p + a_dim1], lda, &xsc, &temp1);
/*              CLASSQ gets both the ell_2 and the ell_infinity norm */
/*              in one pass through the vector */
		rwork[*m + p] = xsc * scalem;
		rwork[p] = xsc * (scalem * sqrt(temp1));
/* Computing MAX */
		r__1 = aatmax, r__2 = rwork[p];
		aatmax = f2cmax(r__1,r__2);
		if (rwork[p] != 0.f) {
/* Computing MIN */
		    r__1 = aatmin, r__2 = rwork[p];
		    aatmin = f2cmin(r__1,r__2);
		}
/* L1950: */
	    }
	} else {
	    i__1 = *m;
	    for (p = 1; p <= i__1; ++p) {
		rwork[*m + p] = scalem * c_abs(&a[p + icamax_(n, &a[p + 
			a_dim1], lda) * a_dim1]);
/* Computing MAX */
		r__1 = aatmax, r__2 = rwork[*m + p];
		aatmax = f2cmax(r__1,r__2);
/* Computing MIN */
		r__1 = aatmin, r__2 = rwork[*m + p];
		aatmin = f2cmin(r__1,r__2);
/* L1904: */
	    }
	}

    }

/*     For square matrix A try to determine whether A^*  would be better */
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

/*        Now, SVA().^2/Trace(A^* * A) is a point in the probability simplex. */
/*        It is derived from the diagonal of  A^* * A.  Do the same with the */
/*        diagonal of A * A^*, compute the entropy of the corresponding */
/*        probability distribution. Note that A * A^* and A^* * A have the */
/*        same trace. */

	entrat = 0.f;
	i__1 = *m;
	for (p = 1; p <= i__1; ++p) {
/* Computing 2nd power */
	    r__1 = rwork[p] / xsc;
	    big1 = r__1 * r__1 * temp1;
	    if (big1 != 0.f) {
		entrat += big1 * log(big1);
	    }
/* L1114: */
	}
	entrat = -entrat / log((real) (*m));

/*        Analyze the entropies and decide A or A^*. Smaller entropy */
/*        usually means better input for the algorithm. */

	transp = entrat < entra;

/*        If A^* is better than A, take the adjoint of A. This is allowed */
/*        only for square matrices, M=N. */
	if (transp) {
/*           In an optimal implementation, this trivial transpose */
/*           should be replaced with faster transpose. */
	    i__1 = *n - 1;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = p + p * a_dim1;
		r_cnjg(&q__1, &a[p + p * a_dim1]);
		a[i__2].r = q__1.r, a[i__2].i = q__1.i;
		i__2 = *n;
		for (q = p + 1; q <= i__2; ++q) {
		    r_cnjg(&q__1, &a[q + p * a_dim1]);
		    ctemp.r = q__1.r, ctemp.i = q__1.i;
		    i__3 = q + p * a_dim1;
		    r_cnjg(&q__1, &a[p + q * a_dim1]);
		    a[i__3].r = q__1.r, a[i__3].i = q__1.i;
		    i__3 = p + q * a_dim1;
		    a[i__3].r = ctemp.r, a[i__3].i = ctemp.i;
/* L1116: */
		}
/* L1115: */
	    }
	    i__1 = *n + *n * a_dim1;
	    r_cnjg(&q__1, &a[*n + *n * a_dim1]);
	    a[i__1].r = q__1.r, a[i__1].i = q__1.i;
	    i__1 = *n;
	    for (p = 1; p <= i__1; ++p) {
		rwork[*m + p] = sva[p];
		sva[p] = rwork[p];
/*              previously computed row 2-norms are now column 2-norms */
/*              of the transposed matrix */
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
/*     SQRT(BIG) instead of BIG is the fact that CGEJSV uses LAPACK and */
/*     BLAS routines that, in some implementations, are not capable of */
/*     working in the full interval [SFMIN,BIG] and that they may provoke */
/*     overflows in the intermediate results. If the singular values spread */
/*     from SFMIN to BIG, then CGESVJ will compute them. So, in that case, */
/*     one should use CGESVJ instead of CGEJSV. */
    big1 = sqrt(big);
    temp1 = sqrt(big / (real) (*n));
/*     >> for future updates: allow bigger range, i.e. the largest column */
/*     will be allowed up to BIG/N and CGESVJ will do the rest. However, for */
/*     this all other (LAPACK) components must allow such a range. */
/*     TEMP1  = BIG/REAL(N) */
/*     TEMP1  = BIG * EPSLN  this should 'almost' work with current LAPACK components */
    slascl_("G", &c__0, &c__0, &aapp, &temp1, n, &c__1, &sva[1], n, &ierr);
    if (aaqq > aapp * sfmin) {
	aaqq = aaqq / aapp * temp1;
    } else {
	aaqq = aaqq * temp1 / aapp;
    }
    temp1 *= scalem;
    clascl_("G", &c__0, &c__0, &aapp, &temp1, m, n, &a[a_offset], lda, &ierr);

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
/*        as a precaution measure, the full SVD is computed using CGESVJ */
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
		claset_("A", m, &c__1, &c_b1, &c_b1, &a[p * a_dim1 + 1], lda);
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
	if (lsvec && rsvec && ! jracc) {
	    iwoff = *n << 1;
	} else {
	    iwoff = *n;
	}
	i__1 = *m - 1;
	for (p = 1; p <= i__1; ++p) {
	    i__2 = *m - p + 1;
	    q = isamax_(&i__2, &rwork[*m + p], &c__1) + p - 1;
	    iwork[iwoff + p] = q;
	    if (p != q) {
		temp1 = rwork[*m + p];
		rwork[*m + p] = rwork[*m + q];
		rwork[*m + q] = temp1;
	    }
/* L1952: */
	}
	i__1 = *m - 1;
	claswp_(n, &a[a_offset], lda, &c__1, &i__1, &iwork[iwoff + 1], &c__1);
    }

/*     End of the preparation phase (scaling, optional sorting and */
/*     transposing, optional flushing of small columns). */

/*     Preconditioning */

/*     If the full SVD is needed, the right singular vectors are computed */
/*     from a matrix equation, and for that we need theoretical analysis */
/*     of the Businger-Golub pivoting. So we use CGEQP3 as the first RR QRF. */
/*     In all other cases the first RR QRF can be chosen by other criteria */
/*     (eg speed by replacing global with restricted window pivoting, such */
/*     as in xGEQPX from TOMS # 782). Good results will be obtained using */
/*     xGEQPX with properly (!) chosen numerical parameters. */
/*     Any improvement of CGEQP3 improves overal performance of CGEJSV. */

/*     A * P1 = Q1 * [ R1^* 0]^*: */
    i__1 = *n;
    for (p = 1; p <= i__1; ++p) {
	iwork[p] = 0;
/* L1963: */
    }
    i__1 = *lwork - *n;
    cgeqp3_(m, n, &a[a_offset], lda, &iwork[1], &cwork[1], &cwork[*n + 1], &
	    i__1, &rwork[1], &ierr);

/*     The upper triangular matrix R1 from the first QRF is inspected for */
/*     rank deficiency and possibilities for deflation, or possible */
/*     ill-conditioning. Depending on the user specified flag L2RANK, */
/*     the procedure explores possibilities to reduce the numerical */
/*     rank by inspecting the computed upper triangular factor. If */
/*     L2RANK or L2ABER are up, then CGEJSV will compute the SVD of */
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
	    if (c_abs(&a[p + p * a_dim1]) >= temp1 * c_abs(&a[a_dim1 + 1])) {
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
	    if (c_abs(&a[p + p * a_dim1]) < epsln * c_abs(&a[p - 1 + (p - 1) *
		     a_dim1]) || c_abs(&a[p + p * a_dim1]) < small || l2kill 
		    && c_abs(&a[p + p * a_dim1]) < temp1) {
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
	    if (c_abs(&a[p + p * a_dim1]) < small || l2kill && c_abs(&a[p + p 
		    * a_dim1]) < temp1) {
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
	    temp1 = c_abs(&a[p + p * a_dim1]) / sva[iwork[p]];
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
		clacpy_("U", n, n, &a[a_offset], lda, &v[v_offset], ldv);
		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    temp1 = sva[iwork[p]];
		    r__1 = 1.f / temp1;
		    csscal_(&p, &r__1, &v[p * v_dim1 + 1], &c__1);
/* L3053: */
		}
		if (lsvec) {
		    cpocon_("U", n, &v[v_offset], ldv, &c_b141, &temp1, &
			    cwork[*n + 1], &rwork[1], &ierr);
		} else {
		    cpocon_("U", n, &v[v_offset], ldv, &c_b141, &temp1, &
			    cwork[1], &rwork[1], &ierr);
		}

	    } else if (lsvec) {
		clacpy_("U", n, n, &a[a_offset], lda, &u[u_offset], ldu);
		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    temp1 = sva[iwork[p]];
		    r__1 = 1.f / temp1;
		    csscal_(&p, &r__1, &u[p * u_dim1 + 1], &c__1);
/* L3054: */
		}
		cpocon_("U", n, &u[u_offset], ldu, &c_b141, &temp1, &cwork[*n 
			+ 1], &rwork[1], &ierr);
	    } else {
		clacpy_("U", n, n, &a[a_offset], lda, &cwork[1], n)
			;
/* []            CALL CLACPY( 'U', N, N, A, LDA, CWORK(N+1), N ) */
/*              Change: here index shifted by N to the left, CWORK(1:N) */
/*              not needed for SIGMA only computation */
		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    temp1 = sva[iwork[p]];
/* []               CALL CSSCAL( p, ONE/TEMP1, CWORK(N+(p-1)*N+1), 1 ) */
		    r__1 = 1.f / temp1;
		    csscal_(&p, &r__1, &cwork[(p - 1) * *n + 1], &c__1);
/* L3052: */
		}
/* []               CALL CPOCON( 'U', N, CWORK(N+1), N, ONE, TEMP1, */
/* []     $              CWORK(N+N*N+1), RWORK, IERR ) */
		cpocon_("U", n, &cwork[1], n, &c_b141, &temp1, &cwork[*n * *n 
			+ 1], &rwork[1], &ierr);

	    }
	    if (temp1 != 0.f) {
		sconda = 1.f / sqrt(temp1);
	    } else {
		sconda = -1.f;
	    }
/*           SCONDA is an estimate of SQRT(||(R^* * R)^(-1)||_1). */
/*           N^(-1/4) * SCONDA <= ||R^(-1)||_2 <= N^(1/4) * SCONDA */
	} else {
	    sconda = -1.f;
	}
    }

    c_div(&q__1, &a[a_dim1 + 1], &a[nr + nr * a_dim1]);
    l2pert = l2pert && c_abs(&q__1) > sqrt(big1);
/*     If there is no violent scaling, artificial perturbation is not needed. */

/*     Phase 3: */

    if (! (rsvec || lsvec)) {

/*         Singular Values only */

/* Computing MIN */
	i__2 = *n - 1;
	i__1 = f2cmin(i__2,nr);
	for (p = 1; p <= i__1; ++p) {
	    i__2 = *n - p;
	    ccopy_(&i__2, &a[p + (p + 1) * a_dim1], lda, &a[p + 1 + p * 
		    a_dim1], &c__1);
	    i__2 = *n - p + 1;
	    clacgv_(&i__2, &a[p + p * a_dim1], &c__1);
/* L1946: */
	}
	if (nr == *n) {
	    i__1 = *n + *n * a_dim1;
	    r_cnjg(&q__1, &a[*n + *n * a_dim1]);
	    a[i__1].r = q__1.r, a[i__1].i = q__1.i;
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
		    r__1 = xsc * c_abs(&a[q + q * a_dim1]);
		    q__1.r = r__1, q__1.i = 0.f;
		    ctemp.r = q__1.r, ctemp.i = q__1.i;
		    i__2 = *n;
		    for (p = 1; p <= i__2; ++p) {
			if (p > q && c_abs(&a[p + q * a_dim1]) <= temp1 || p <
				 q) {
			    i__3 = p + q * a_dim1;
			    a[i__3].r = ctemp.r, a[i__3].i = ctemp.i;
			}
/*     $                     A(p,q) = TEMP1 * ( A(p,q) / ABS(A(p,q)) ) */
/* L4949: */
		    }
/* L4947: */
		}
	    } else {
		i__1 = nr - 1;
		i__2 = nr - 1;
		claset_("U", &i__1, &i__2, &c_b1, &c_b1, &a[(a_dim1 << 1) + 1]
			, lda);
	    }


	    i__1 = *lwork - *n;
	    cgeqrf_(n, &nr, &a[a_offset], lda, &cwork[1], &cwork[*n + 1], &
		    i__1, &ierr);

	    i__1 = nr - 1;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = nr - p;
		ccopy_(&i__2, &a[p + (p + 1) * a_dim1], lda, &a[p + 1 + p * 
			a_dim1], &c__1);
		i__2 = nr - p + 1;
		clacgv_(&i__2, &a[p + p * a_dim1], &c__1);
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
		r__1 = xsc * c_abs(&a[q + q * a_dim1]);
		q__1.r = r__1, q__1.i = 0.f;
		ctemp.r = q__1.r, ctemp.i = q__1.i;
		i__2 = nr;
		for (p = 1; p <= i__2; ++p) {
		    if (p > q && c_abs(&a[p + q * a_dim1]) <= temp1 || p < q) 
			    {
			i__3 = p + q * a_dim1;
			a[i__3].r = ctemp.r, a[i__3].i = ctemp.i;
		    }
/*     $                   A(p,q) = TEMP1 * ( A(p,q) / ABS(A(p,q)) ) */
/* L1949: */
		}
/* L1947: */
	    }
	} else {
	    i__1 = nr - 1;
	    i__2 = nr - 1;
	    claset_("U", &i__1, &i__2, &c_b1, &c_b1, &a[(a_dim1 << 1) + 1], 
		    lda);
	}

/*           triangular matrix (plus perturbation which is ignored in */
/*           the part which destroys triangular form (confusing?!)) */

	cgesvj_("L", "N", "N", &nr, &nr, &a[a_offset], lda, &sva[1], n, &v[
		v_offset], ldv, &cwork[1], lwork, &rwork[1], lrwork, info);

	scalem = rwork[1];
	numrank = i_nint(&rwork[2]);


    } else if (rsvec && ! lsvec && ! jracc || jracc && ! lsvec && nr != *n) {

/*        -> Singular Values and Right Singular Vectors <- */

	if (almort) {

	    i__1 = nr;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = *n - p + 1;
		ccopy_(&i__2, &a[p + p * a_dim1], lda, &v[p + p * v_dim1], &
			c__1);
		i__2 = *n - p + 1;
		clacgv_(&i__2, &v[p + p * v_dim1], &c__1);
/* L1998: */
	    }
	    i__1 = nr - 1;
	    i__2 = nr - 1;
	    claset_("U", &i__1, &i__2, &c_b1, &c_b1, &v[(v_dim1 << 1) + 1], 
		    ldv);

	    cgesvj_("L", "U", "N", n, &nr, &v[v_offset], ldv, &sva[1], &nr, &
		    a[a_offset], lda, &cwork[1], lwork, &rwork[1], lrwork, 
		    info);
	    scalem = rwork[1];
	    numrank = i_nint(&rwork[2]);
	} else {

/*        accumulated product of Jacobi rotations, three are perfect ) */

	    i__1 = nr - 1;
	    i__2 = nr - 1;
	    claset_("L", &i__1, &i__2, &c_b1, &c_b1, &a[a_dim1 + 2], lda);
	    i__1 = *lwork - *n;
	    cgelqf_(&nr, n, &a[a_offset], lda, &cwork[1], &cwork[*n + 1], &
		    i__1, &ierr);
	    clacpy_("L", &nr, &nr, &a[a_offset], lda, &v[v_offset], ldv);
	    i__1 = nr - 1;
	    i__2 = nr - 1;
	    claset_("U", &i__1, &i__2, &c_b1, &c_b1, &v[(v_dim1 << 1) + 1], 
		    ldv);
	    i__1 = *lwork - (*n << 1);
	    cgeqrf_(&nr, &nr, &v[v_offset], ldv, &cwork[*n + 1], &cwork[(*n <<
		     1) + 1], &i__1, &ierr);
	    i__1 = nr;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = nr - p + 1;
		ccopy_(&i__2, &v[p + p * v_dim1], ldv, &v[p + p * v_dim1], &
			c__1);
		i__2 = nr - p + 1;
		clacgv_(&i__2, &v[p + p * v_dim1], &c__1);
/* L8998: */
	    }
	    i__1 = nr - 1;
	    i__2 = nr - 1;
	    claset_("U", &i__1, &i__2, &c_b1, &c_b1, &v[(v_dim1 << 1) + 1], 
		    ldv);

	    i__1 = *lwork - *n;
	    cgesvj_("L", "U", "N", &nr, &nr, &v[v_offset], ldv, &sva[1], &nr, 
		    &u[u_offset], ldu, &cwork[*n + 1], &i__1, &rwork[1], 
		    lrwork, info);
	    scalem = rwork[1];
	    numrank = i_nint(&rwork[2]);
	    if (nr < *n) {
		i__1 = *n - nr;
		claset_("A", &i__1, &nr, &c_b1, &c_b1, &v[nr + 1 + v_dim1], 
			ldv);
		i__1 = *n - nr;
		claset_("A", &nr, &i__1, &c_b1, &c_b1, &v[(nr + 1) * v_dim1 + 
			1], ldv);
		i__1 = *n - nr;
		i__2 = *n - nr;
		claset_("A", &i__1, &i__2, &c_b1, &c_b2, &v[nr + 1 + (nr + 1) 
			* v_dim1], ldv);
	    }

	    i__1 = *lwork - *n;
	    cunmlq_("L", "C", n, n, &nr, &a[a_offset], lda, &cwork[1], &v[
		    v_offset], ldv, &cwork[*n + 1], &i__1, &ierr);

	}
/*         DO 8991 p = 1, N */
/*            CALL CCOPY( N, V(p,1), LDV, A(IWORK(p),1), LDA ) */
/* 8991    CONTINUE */
/*         CALL CLACPY( 'All', N, N, A, LDA, V, LDV ) */
	clapmr_(&c_false, n, n, &v[v_offset], ldv, &iwork[1]);

	if (transp) {
	    clacpy_("A", n, n, &v[v_offset], ldv, &u[u_offset], ldu);
	}

    } else if (jracc && ! lsvec && nr == *n) {

	i__1 = *n - 1;
	i__2 = *n - 1;
	claset_("L", &i__1, &i__2, &c_b1, &c_b1, &a[a_dim1 + 2], lda);

	cgesvj_("U", "N", "V", n, n, &a[a_offset], lda, &sva[1], n, &v[
		v_offset], ldv, &cwork[1], lwork, &rwork[1], lrwork, info);
	scalem = rwork[1];
	numrank = i_nint(&rwork[2]);
	clapmr_(&c_false, n, n, &v[v_offset], ldv, &iwork[1]);

    } else if (lsvec && ! rsvec) {


/*        Jacobi rotations in the Jacobi iterations. */
	i__1 = nr;
	for (p = 1; p <= i__1; ++p) {
	    i__2 = *n - p + 1;
	    ccopy_(&i__2, &a[p + p * a_dim1], lda, &u[p + p * u_dim1], &c__1);
	    i__2 = *n - p + 1;
	    clacgv_(&i__2, &u[p + p * u_dim1], &c__1);
/* L1965: */
	}
	i__1 = nr - 1;
	i__2 = nr - 1;
	claset_("U", &i__1, &i__2, &c_b1, &c_b1, &u[(u_dim1 << 1) + 1], ldu);

	i__1 = *lwork - (*n << 1);
	cgeqrf_(n, &nr, &u[u_offset], ldu, &cwork[*n + 1], &cwork[(*n << 1) + 
		1], &i__1, &ierr);

	i__1 = nr - 1;
	for (p = 1; p <= i__1; ++p) {
	    i__2 = nr - p;
	    ccopy_(&i__2, &u[p + (p + 1) * u_dim1], ldu, &u[p + 1 + p * 
		    u_dim1], &c__1);
	    i__2 = *n - p + 1;
	    clacgv_(&i__2, &u[p + p * u_dim1], &c__1);
/* L1967: */
	}
	i__1 = nr - 1;
	i__2 = nr - 1;
	claset_("U", &i__1, &i__2, &c_b1, &c_b1, &u[(u_dim1 << 1) + 1], ldu);

	i__1 = *lwork - *n;
	cgesvj_("L", "U", "N", &nr, &nr, &u[u_offset], ldu, &sva[1], &nr, &a[
		a_offset], lda, &cwork[*n + 1], &i__1, &rwork[1], lrwork, 
		info);
	scalem = rwork[1];
	numrank = i_nint(&rwork[2]);

	if (nr < *m) {
	    i__1 = *m - nr;
	    claset_("A", &i__1, &nr, &c_b1, &c_b1, &u[nr + 1 + u_dim1], ldu);
	    if (nr < n1) {
		i__1 = n1 - nr;
		claset_("A", &nr, &i__1, &c_b1, &c_b1, &u[(nr + 1) * u_dim1 + 
			1], ldu);
		i__1 = *m - nr;
		i__2 = n1 - nr;
		claset_("A", &i__1, &i__2, &c_b1, &c_b2, &u[nr + 1 + (nr + 1) 
			* u_dim1], ldu);
	    }
	}

	i__1 = *lwork - *n;
	cunmqr_("L", "N", m, &n1, n, &a[a_offset], lda, &cwork[1], &u[
		u_offset], ldu, &cwork[*n + 1], &i__1, &ierr);

	if (rowpiv) {
	    i__1 = *m - 1;
	    claswp_(&n1, &u[u_offset], ldu, &c__1, &i__1, &iwork[iwoff + 1], &
		    c_n1);
	}

	i__1 = n1;
	for (p = 1; p <= i__1; ++p) {
	    xsc = 1.f / scnrm2_(m, &u[p * u_dim1 + 1], &c__1);
	    csscal_(m, &xsc, &u[p * u_dim1 + 1], &c__1);
/* L1974: */
	}

	if (transp) {
	    clacpy_("A", n, n, &u[u_offset], ldu, &v[v_offset], ldv);
	}

    } else {


	if (! jracc) {

	    if (! almort) {

/*           Second Preconditioning Step (QRF [with pivoting]) */
/*           Note that the composition of TRANSPOSE, QRF and TRANSPOSE is */
/*           equivalent to an LQF CALL. Since in many libraries the QRF */
/*           seems to be better optimized than the LQF, we do explicit */
/*           transpose and use the QRF. This is subject to changes in an */
/*           optimized implementation of CGEJSV. */

		i__1 = nr;
		for (p = 1; p <= i__1; ++p) {
		    i__2 = *n - p + 1;
		    ccopy_(&i__2, &a[p + p * a_dim1], lda, &v[p + p * v_dim1],
			     &c__1);
		    i__2 = *n - p + 1;
		    clacgv_(&i__2, &v[p + p * v_dim1], &c__1);
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
			r__1 = xsc * c_abs(&v[q + q * v_dim1]);
			q__1.r = r__1, q__1.i = 0.f;
			ctemp.r = q__1.r, ctemp.i = q__1.i;
			i__2 = *n;
			for (p = 1; p <= i__2; ++p) {
			    if (p > q && c_abs(&v[p + q * v_dim1]) <= temp1 ||
				     p < q) {
				i__3 = p + q * v_dim1;
				v[i__3].r = ctemp.r, v[i__3].i = ctemp.i;
			    }
/*     $                   V(p,q) = TEMP1 * ( V(p,q) / ABS(V(p,q)) ) */
			    if (p < q) {
				i__3 = p + q * v_dim1;
				i__4 = p + q * v_dim1;
				q__1.r = -v[i__4].r, q__1.i = -v[i__4].i;
				v[i__3].r = q__1.r, v[i__3].i = q__1.i;
			    }
/* L2968: */
			}
/* L2969: */
		    }
		} else {
		    i__1 = nr - 1;
		    i__2 = nr - 1;
		    claset_("U", &i__1, &i__2, &c_b1, &c_b1, &v[(v_dim1 << 1) 
			    + 1], ldv);
		}

/*           Estimate the row scaled condition number of R1 */
/*           (If R1 is rectangular, N > NR, then the condition number */
/*           of the leading NR x NR submatrix is estimated.) */

		clacpy_("L", &nr, &nr, &v[v_offset], ldv, &cwork[(*n << 1) + 
			1], &nr);
		i__1 = nr;
		for (p = 1; p <= i__1; ++p) {
		    i__2 = nr - p + 1;
		    temp1 = scnrm2_(&i__2, &cwork[(*n << 1) + (p - 1) * nr + 
			    p], &c__1);
		    i__2 = nr - p + 1;
		    r__1 = 1.f / temp1;
		    csscal_(&i__2, &r__1, &cwork[(*n << 1) + (p - 1) * nr + p]
			    , &c__1);
/* L3950: */
		}
		cpocon_("L", &nr, &cwork[(*n << 1) + 1], &nr, &c_b141, &temp1,
			 &cwork[(*n << 1) + nr * nr + 1], &rwork[1], &ierr);
		condr1 = 1.f / sqrt(temp1);
/*           R1 is OK for inverse <=> CONDR1 .LT. REAL(N) */
/*           more conservative    <=> CONDR1 .LT. SQRT(REAL(N)) */

		cond_ok__ = sqrt(sqrt((real) nr));
/* [TP]       COND_OK is a tuning parameter. */

		if (condr1 < cond_ok__) {
/*              implementation, this QRF should be implemented as the QRF */
/*              of a lower triangular matrix. */
/*              R1^* = Q2 * R2 */
		    i__1 = *lwork - (*n << 1);
		    cgeqrf_(n, &nr, &v[v_offset], ldv, &cwork[*n + 1], &cwork[
			    (*n << 1) + 1], &i__1, &ierr);

		    if (l2pert) {
			xsc = sqrt(small) / epsln;
			i__1 = nr;
			for (p = 2; p <= i__1; ++p) {
			    i__2 = p - 1;
			    for (q = 1; q <= i__2; ++q) {
/* Computing MIN */
				r__2 = c_abs(&v[p + p * v_dim1]), r__3 = 
					c_abs(&v[q + q * v_dim1]);
				r__1 = xsc * f2cmin(r__2,r__3);
				q__1.r = r__1, q__1.i = 0.f;
				ctemp.r = q__1.r, ctemp.i = q__1.i;
				if (c_abs(&v[q + p * v_dim1]) <= temp1) {
				    i__3 = q + p * v_dim1;
				    v[i__3].r = ctemp.r, v[i__3].i = ctemp.i;
				}
/*     $                     V(q,p) = TEMP1 * ( V(q,p) / ABS(V(q,p)) ) */
/* L3958: */
			    }
/* L3959: */
			}
		    }

		    if (nr != *n) {
			clacpy_("A", n, &nr, &v[v_offset], ldv, &cwork[(*n << 
				1) + 1], n);
		    }

		    i__1 = nr - 1;
		    for (p = 1; p <= i__1; ++p) {
			i__2 = nr - p;
			ccopy_(&i__2, &v[p + (p + 1) * v_dim1], ldv, &v[p + 1 
				+ p * v_dim1], &c__1);
			i__2 = nr - p + 1;
			clacgv_(&i__2, &v[p + p * v_dim1], &c__1);
/* L1969: */
		    }
		    i__1 = nr + nr * v_dim1;
		    r_cnjg(&q__1, &v[nr + nr * v_dim1]);
		    v[i__1].r = q__1.r, v[i__1].i = q__1.i;

		    condr2 = condr1;

		} else {

/*              Note that windowed pivoting would be equally good */
/*              numerically, and more run-time efficient. So, in */
/*              an optimal implementation, the next call to CGEQP3 */
/*              should be replaced with eg. CALL CGEQPX (ACM TOMS #782) */
/*              with properly (carefully) chosen parameters. */

/*              R1^* * P2 = Q2 * R2 */
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			iwork[*n + p] = 0;
/* L3003: */
		    }
		    i__1 = *lwork - (*n << 1);
		    cgeqp3_(n, &nr, &v[v_offset], ldv, &iwork[*n + 1], &cwork[
			    *n + 1], &cwork[(*n << 1) + 1], &i__1, &rwork[1], 
			    &ierr);
/* *               CALL CGEQRF( N, NR, V, LDV, CWORK(N+1), CWORK(2*N+1), */
/* *     $              LWORK-2*N, IERR ) */
		    if (l2pert) {
			xsc = sqrt(small);
			i__1 = nr;
			for (p = 2; p <= i__1; ++p) {
			    i__2 = p - 1;
			    for (q = 1; q <= i__2; ++q) {
/* Computing MIN */
				r__2 = c_abs(&v[p + p * v_dim1]), r__3 = 
					c_abs(&v[q + q * v_dim1]);
				r__1 = xsc * f2cmin(r__2,r__3);
				q__1.r = r__1, q__1.i = 0.f;
				ctemp.r = q__1.r, ctemp.i = q__1.i;
				if (c_abs(&v[q + p * v_dim1]) <= temp1) {
				    i__3 = q + p * v_dim1;
				    v[i__3].r = ctemp.r, v[i__3].i = ctemp.i;
				}
/*     $                     V(q,p) = TEMP1 * ( V(q,p) / ABS(V(q,p)) ) */
/* L3968: */
			    }
/* L3969: */
			}
		    }

		    clacpy_("A", n, &nr, &v[v_offset], ldv, &cwork[(*n << 1) 
			    + 1], n);

		    if (l2pert) {
			xsc = sqrt(small);
			i__1 = nr;
			for (p = 2; p <= i__1; ++p) {
			    i__2 = p - 1;
			    for (q = 1; q <= i__2; ++q) {
/* Computing MIN */
				r__2 = c_abs(&v[p + p * v_dim1]), r__3 = 
					c_abs(&v[q + q * v_dim1]);
				r__1 = xsc * f2cmin(r__2,r__3);
				q__1.r = r__1, q__1.i = 0.f;
				ctemp.r = q__1.r, ctemp.i = q__1.i;
/*                        V(p,q) = - TEMP1*( V(q,p) / ABS(V(q,p)) ) */
				i__3 = p + q * v_dim1;
				q__1.r = -ctemp.r, q__1.i = -ctemp.i;
				v[i__3].r = q__1.r, v[i__3].i = q__1.i;
/* L8971: */
			    }
/* L8970: */
			}
		    } else {
			i__1 = nr - 1;
			i__2 = nr - 1;
			claset_("L", &i__1, &i__2, &c_b1, &c_b1, &v[v_dim1 + 
				2], ldv);
		    }
/*              Now, compute R2 = L3 * Q3, the LQ factorization. */
		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    cgelqf_(&nr, &nr, &v[v_offset], ldv, &cwork[(*n << 1) + *
			    n * nr + 1], &cwork[(*n << 1) + *n * nr + nr + 1],
			     &i__1, &ierr);
		    clacpy_("L", &nr, &nr, &v[v_offset], ldv, &cwork[(*n << 1)
			     + *n * nr + nr + 1], &nr);
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			temp1 = scnrm2_(&p, &cwork[(*n << 1) + *n * nr + nr + 
				p], &nr);
			r__1 = 1.f / temp1;
			csscal_(&p, &r__1, &cwork[(*n << 1) + *n * nr + nr + 
				p], &nr);
/* L4950: */
		    }
		    cpocon_("L", &nr, &cwork[(*n << 1) + *n * nr + nr + 1], &
			    nr, &c_b141, &temp1, &cwork[(*n << 1) + *n * nr + 
			    nr + nr * nr + 1], &rwork[1], &ierr);
		    condr2 = 1.f / sqrt(temp1);


		    if (condr2 >= cond_ok__) {
/*                 (this overwrites the copy of R2, as it will not be */
/*                 needed in this branch, but it does not overwritte the */
/*                 Huseholder vectors of Q2.). */
			clacpy_("U", &nr, &nr, &v[v_offset], ldv, &cwork[(*n 
				<< 1) + 1], n);
/*                 WORK(2*N+N*NR+1:2*N+N*NR+N) */
		    }

		}

		if (l2pert) {
		    xsc = sqrt(small);
		    i__1 = nr;
		    for (q = 2; q <= i__1; ++q) {
			i__2 = q + q * v_dim1;
			q__1.r = xsc * v[i__2].r, q__1.i = xsc * v[i__2].i;
			ctemp.r = q__1.r, ctemp.i = q__1.i;
			i__2 = q - 1;
			for (p = 1; p <= i__2; ++p) {
/*                     V(p,q) = - TEMP1*( V(p,q) / ABS(V(p,q)) ) */
			    i__3 = p + q * v_dim1;
			    q__1.r = -ctemp.r, q__1.i = -ctemp.i;
			    v[i__3].r = q__1.r, v[i__3].i = q__1.i;
/* L4969: */
			}
/* L4968: */
		    }
		} else {
		    i__1 = nr - 1;
		    i__2 = nr - 1;
		    claset_("U", &i__1, &i__2, &c_b1, &c_b1, &v[(v_dim1 << 1) 
			    + 1], ldv);
		}

/*        Second preconditioning finished; continue with Jacobi SVD */
/*        The input matrix is lower trinagular. */

/*        Recover the right singular vectors as solution of a well */
/*        conditioned triangular matrix equation. */

		if (condr1 < cond_ok__) {

		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    cgesvj_("L", "U", "N", &nr, &nr, &v[v_offset], ldv, &sva[
			    1], &nr, &u[u_offset], ldu, &cwork[(*n << 1) + *n 
			    * nr + nr + 1], &i__1, &rwork[1], lrwork, info);
		    scalem = rwork[1];
		    numrank = i_nint(&rwork[2]);
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			ccopy_(&nr, &v[p * v_dim1 + 1], &c__1, &u[p * u_dim1 
				+ 1], &c__1);
			csscal_(&nr, &sva[p], &v[p * v_dim1 + 1], &c__1);
/* L3970: */
		    }

		    if (nr == *n) {
/* :))             .. best case, R1 is inverted. The solution of this matrix */
/*                 equation is Q2*V2 = the product of the Jacobi rotations */
/*                 used in CGESVJ, premultiplied with the orthogonal matrix */
/*                 from the second QR factorization. */
			ctrsm_("L", "U", "N", "N", &nr, &nr, &c_b2, &a[
				a_offset], lda, &v[v_offset], ldv);
		    } else {
/*                 is inverted to get the product of the Jacobi rotations */
/*                 used in CGESVJ. The Q-factor from the second QR */
/*                 factorization is then built in explicitly. */
			ctrsm_("L", "U", "C", "N", &nr, &nr, &c_b2, &cwork[(*
				n << 1) + 1], n, &v[v_offset], ldv);
			if (nr < *n) {
			    i__1 = *n - nr;
			    claset_("A", &i__1, &nr, &c_b1, &c_b1, &v[nr + 1 
				    + v_dim1], ldv);
			    i__1 = *n - nr;
			    claset_("A", &nr, &i__1, &c_b1, &c_b1, &v[(nr + 1)
				     * v_dim1 + 1], ldv);
			    i__1 = *n - nr;
			    i__2 = *n - nr;
			    claset_("A", &i__1, &i__2, &c_b1, &c_b2, &v[nr + 
				    1 + (nr + 1) * v_dim1], ldv);
			}
			i__1 = *lwork - (*n << 1) - *n * nr - nr;
			cunmqr_("L", "N", n, n, &nr, &cwork[(*n << 1) + 1], n,
				 &cwork[*n + 1], &v[v_offset], ldv, &cwork[(*
				n << 1) + *n * nr + nr + 1], &i__1, &ierr);
		    }

		} else if (condr2 < cond_ok__) {

/*              The matrix R2 is inverted. The solution of the matrix equation */
/*              is Q3^* * V3 = the product of the Jacobi rotations (appplied to */
/*              the lower triangular L3 from the LQ factorization of */
/*              R2=L3*Q3), pre-multiplied with the transposed Q3. */
		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    cgesvj_("L", "U", "N", &nr, &nr, &v[v_offset], ldv, &sva[
			    1], &nr, &u[u_offset], ldu, &cwork[(*n << 1) + *n 
			    * nr + nr + 1], &i__1, &rwork[1], lrwork, info);
		    scalem = rwork[1];
		    numrank = i_nint(&rwork[2]);
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			ccopy_(&nr, &v[p * v_dim1 + 1], &c__1, &u[p * u_dim1 
				+ 1], &c__1);
			csscal_(&nr, &sva[p], &u[p * u_dim1 + 1], &c__1);
/* L3870: */
		    }
		    ctrsm_("L", "U", "N", "N", &nr, &nr, &c_b2, &cwork[(*n << 
			    1) + 1], n, &u[u_offset], ldu);
		    i__1 = nr;
		    for (q = 1; q <= i__1; ++q) {
			i__2 = nr;
			for (p = 1; p <= i__2; ++p) {
			    i__3 = (*n << 1) + *n * nr + nr + iwork[*n + p];
			    i__4 = p + q * u_dim1;
			    cwork[i__3].r = u[i__4].r, cwork[i__3].i = u[i__4]
				    .i;
/* L872: */
			}
			i__2 = nr;
			for (p = 1; p <= i__2; ++p) {
			    i__3 = p + q * u_dim1;
			    i__4 = (*n << 1) + *n * nr + nr + p;
			    u[i__3].r = cwork[i__4].r, u[i__3].i = cwork[i__4]
				    .i;
/* L874: */
			}
/* L873: */
		    }
		    if (nr < *n) {
			i__1 = *n - nr;
			claset_("A", &i__1, &nr, &c_b1, &c_b1, &v[nr + 1 + 
				v_dim1], ldv);
			i__1 = *n - nr;
			claset_("A", &nr, &i__1, &c_b1, &c_b1, &v[(nr + 1) * 
				v_dim1 + 1], ldv);
			i__1 = *n - nr;
			i__2 = *n - nr;
			claset_("A", &i__1, &i__2, &c_b1, &c_b2, &v[nr + 1 + (
				nr + 1) * v_dim1], ldv);
		    }
		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    cunmqr_("L", "N", n, n, &nr, &cwork[(*n << 1) + 1], n, &
			    cwork[*n + 1], &v[v_offset], ldv, &cwork[(*n << 1)
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
/*              defense ensures that CGEJSV completes the task. */
/*              Compute the full SVD of L3 using CGESVJ with explicit */
/*              accumulation of Jacobi rotations. */
		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    cgesvj_("L", "U", "V", &nr, &nr, &v[v_offset], ldv, &sva[
			    1], &nr, &u[u_offset], ldu, &cwork[(*n << 1) + *n 
			    * nr + nr + 1], &i__1, &rwork[1], lrwork, info);
		    scalem = rwork[1];
		    numrank = i_nint(&rwork[2]);
		    if (nr < *n) {
			i__1 = *n - nr;
			claset_("A", &i__1, &nr, &c_b1, &c_b1, &v[nr + 1 + 
				v_dim1], ldv);
			i__1 = *n - nr;
			claset_("A", &nr, &i__1, &c_b1, &c_b1, &v[(nr + 1) * 
				v_dim1 + 1], ldv);
			i__1 = *n - nr;
			i__2 = *n - nr;
			claset_("A", &i__1, &i__2, &c_b1, &c_b2, &v[nr + 1 + (
				nr + 1) * v_dim1], ldv);
		    }
		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    cunmqr_("L", "N", n, n, &nr, &cwork[(*n << 1) + 1], n, &
			    cwork[*n + 1], &v[v_offset], ldv, &cwork[(*n << 1)
			     + *n * nr + nr + 1], &i__1, &ierr);

		    i__1 = *lwork - (*n << 1) - *n * nr - nr;
		    cunmlq_("L", "C", &nr, &nr, &nr, &cwork[(*n << 1) + 1], n,
			     &cwork[(*n << 1) + *n * nr + 1], &u[u_offset], 
			    ldu, &cwork[(*n << 1) + *n * nr + nr + 1], &i__1, 
			    &ierr);
		    i__1 = nr;
		    for (q = 1; q <= i__1; ++q) {
			i__2 = nr;
			for (p = 1; p <= i__2; ++p) {
			    i__3 = (*n << 1) + *n * nr + nr + iwork[*n + p];
			    i__4 = p + q * u_dim1;
			    cwork[i__3].r = u[i__4].r, cwork[i__3].i = u[i__4]
				    .i;
/* L772: */
			}
			i__2 = nr;
			for (p = 1; p <= i__2; ++p) {
			    i__3 = p + q * u_dim1;
			    i__4 = (*n << 1) + *n * nr + nr + p;
			    u[i__3].r = cwork[i__4].r, u[i__3].i = cwork[i__4]
				    .i;
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
			i__3 = (*n << 1) + *n * nr + nr + iwork[p];
			i__4 = p + q * v_dim1;
			cwork[i__3].r = v[i__4].r, cwork[i__3].i = v[i__4].i;
/* L972: */
		    }
		    i__2 = *n;
		    for (p = 1; p <= i__2; ++p) {
			i__3 = p + q * v_dim1;
			i__4 = (*n << 1) + *n * nr + nr + p;
			v[i__3].r = cwork[i__4].r, v[i__3].i = cwork[i__4].i;
/* L973: */
		    }
		    xsc = 1.f / scnrm2_(n, &v[q * v_dim1 + 1], &c__1);
		    if (xsc < 1.f - temp1 || xsc > temp1 + 1.f) {
			csscal_(n, &xsc, &v[q * v_dim1 + 1], &c__1);
		    }
/* L1972: */
		}
/*           At this moment, V contains the right singular vectors of A. */
/*           Next, assemble the left singular vector matrix U (M x N). */
		if (nr < *m) {
		    i__1 = *m - nr;
		    claset_("A", &i__1, &nr, &c_b1, &c_b1, &u[nr + 1 + u_dim1]
			    , ldu);
		    if (nr < n1) {
			i__1 = n1 - nr;
			claset_("A", &nr, &i__1, &c_b1, &c_b1, &u[(nr + 1) * 
				u_dim1 + 1], ldu);
			i__1 = *m - nr;
			i__2 = n1 - nr;
			claset_("A", &i__1, &i__2, &c_b1, &c_b2, &u[nr + 1 + (
				nr + 1) * u_dim1], ldu);
		    }
		}

/*           The Q matrix from the first QRF is built into the left singular */
/*           matrix U. This applies to all cases. */

		i__1 = *lwork - *n;
		cunmqr_("L", "N", m, &n1, n, &a[a_offset], lda, &cwork[1], &u[
			u_offset], ldu, &cwork[*n + 1], &i__1, &ierr);
/*           The columns of U are normalized. The cost is O(M*N) flops. */
		temp1 = sqrt((real) (*m)) * epsln;
		i__1 = nr;
		for (p = 1; p <= i__1; ++p) {
		    xsc = 1.f / scnrm2_(m, &u[p * u_dim1 + 1], &c__1);
		    if (xsc < 1.f - temp1 || xsc > temp1 + 1.f) {
			csscal_(m, &xsc, &u[p * u_dim1 + 1], &c__1);
		    }
/* L1973: */
		}

/*           If the initial QRF is computed with row pivoting, the left */
/*           singular vectors must be adjusted. */

		if (rowpiv) {
		    i__1 = *m - 1;
		    claswp_(&n1, &u[u_offset], ldu, &c__1, &i__1, &iwork[
			    iwoff + 1], &c_n1);
		}

	    } else {

/*        the second QRF is not needed */

		clacpy_("U", n, n, &a[a_offset], lda, &cwork[*n + 1], n);
		if (l2pert) {
		    xsc = sqrt(small);
		    i__1 = *n;
		    for (p = 2; p <= i__1; ++p) {
			i__2 = *n + (p - 1) * *n + p;
			q__1.r = xsc * cwork[i__2].r, q__1.i = xsc * cwork[
				i__2].i;
			ctemp.r = q__1.r, ctemp.i = q__1.i;
			i__2 = p - 1;
			for (q = 1; q <= i__2; ++q) {
/*                     CWORK(N+(q-1)*N+p)=-TEMP1 * ( CWORK(N+(p-1)*N+q) / */
/*     $                                        ABS(CWORK(N+(p-1)*N+q)) ) */
			    i__3 = *n + (q - 1) * *n + p;
			    q__1.r = -ctemp.r, q__1.i = -ctemp.i;
			    cwork[i__3].r = q__1.r, cwork[i__3].i = q__1.i;
/* L5971: */
			}
/* L5970: */
		    }
		} else {
		    i__1 = *n - 1;
		    i__2 = *n - 1;
		    claset_("L", &i__1, &i__2, &c_b1, &c_b1, &cwork[*n + 2], 
			    n);
		}

		i__1 = *lwork - *n - *n * *n;
		cgesvj_("U", "U", "N", n, n, &cwork[*n + 1], n, &sva[1], n, &
			u[u_offset], ldu, &cwork[*n + *n * *n + 1], &i__1, &
			rwork[1], lrwork, info);

		scalem = rwork[1];
		numrank = i_nint(&rwork[2]);
		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    ccopy_(n, &cwork[*n + (p - 1) * *n + 1], &c__1, &u[p * 
			    u_dim1 + 1], &c__1);
		    csscal_(n, &sva[p], &cwork[*n + (p - 1) * *n + 1], &c__1);
/* L6970: */
		}

		ctrsm_("L", "U", "N", "N", n, n, &c_b2, &a[a_offset], lda, &
			cwork[*n + 1], n);
		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    ccopy_(n, &cwork[*n + p], n, &v[iwork[p] + v_dim1], ldv);
/* L6972: */
		}
		temp1 = sqrt((real) (*n)) * epsln;
		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    xsc = 1.f / scnrm2_(n, &v[p * v_dim1 + 1], &c__1);
		    if (xsc < 1.f - temp1 || xsc > temp1 + 1.f) {
			csscal_(n, &xsc, &v[p * v_dim1 + 1], &c__1);
		    }
/* L6971: */
		}

/*           Assemble the left singular vector matrix U (M x N). */

		if (*n < *m) {
		    i__1 = *m - *n;
		    claset_("A", &i__1, n, &c_b1, &c_b1, &u[*n + 1 + u_dim1], 
			    ldu);
		    if (*n < n1) {
			i__1 = n1 - *n;
			claset_("A", n, &i__1, &c_b1, &c_b1, &u[(*n + 1) * 
				u_dim1 + 1], ldu);
			i__1 = *m - *n;
			i__2 = n1 - *n;
			claset_("A", &i__1, &i__2, &c_b1, &c_b2, &u[*n + 1 + (
				*n + 1) * u_dim1], ldu);
		    }
		}
		i__1 = *lwork - *n;
		cunmqr_("L", "N", m, &n1, n, &a[a_offset], lda, &cwork[1], &u[
			u_offset], ldu, &cwork[*n + 1], &i__1, &ierr);
		temp1 = sqrt((real) (*m)) * epsln;
		i__1 = n1;
		for (p = 1; p <= i__1; ++p) {
		    xsc = 1.f / scnrm2_(m, &u[p * u_dim1 + 1], &c__1);
		    if (xsc < 1.f - temp1 || xsc > temp1 + 1.f) {
			csscal_(m, &xsc, &u[p * u_dim1 + 1], &c__1);
		    }
/* L6973: */
		}

		if (rowpiv) {
		    i__1 = *m - 1;
		    claswp_(&n1, &u[u_offset], ldu, &c__1, &i__1, &iwork[
			    iwoff + 1], &c_n1);
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
/*        in presence of extreme values, e.g. when the singular values spread from */
/*        the underflow to the overflow threshold. */

	    i__1 = nr;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = *n - p + 1;
		ccopy_(&i__2, &a[p + p * a_dim1], lda, &v[p + p * v_dim1], &
			c__1);
		i__2 = *n - p + 1;
		clacgv_(&i__2, &v[p + p * v_dim1], &c__1);
/* L7968: */
	    }

	    if (l2pert) {
		xsc = sqrt(small / epsln);
		i__1 = nr;
		for (q = 1; q <= i__1; ++q) {
		    r__1 = xsc * c_abs(&v[q + q * v_dim1]);
		    q__1.r = r__1, q__1.i = 0.f;
		    ctemp.r = q__1.r, ctemp.i = q__1.i;
		    i__2 = *n;
		    for (p = 1; p <= i__2; ++p) {
			if (p > q && c_abs(&v[p + q * v_dim1]) <= temp1 || p <
				 q) {
			    i__3 = p + q * v_dim1;
			    v[i__3].r = ctemp.r, v[i__3].i = ctemp.i;
			}
/*     $                V(p,q) = TEMP1 * ( V(p,q) / ABS(V(p,q)) ) */
			if (p < q) {
			    i__3 = p + q * v_dim1;
			    i__4 = p + q * v_dim1;
			    q__1.r = -v[i__4].r, q__1.i = -v[i__4].i;
			    v[i__3].r = q__1.r, v[i__3].i = q__1.i;
			}
/* L5968: */
		    }
/* L5969: */
		}
	    } else {
		i__1 = nr - 1;
		i__2 = nr - 1;
		claset_("U", &i__1, &i__2, &c_b1, &c_b1, &v[(v_dim1 << 1) + 1]
			, ldv);
	    }
	    i__1 = *lwork - (*n << 1);
	    cgeqrf_(n, &nr, &v[v_offset], ldv, &cwork[*n + 1], &cwork[(*n << 
		    1) + 1], &i__1, &ierr);
	    clacpy_("L", n, &nr, &v[v_offset], ldv, &cwork[(*n << 1) + 1], n);

	    i__1 = nr;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = nr - p + 1;
		ccopy_(&i__2, &v[p + p * v_dim1], ldv, &u[p + p * u_dim1], &
			c__1);
		i__2 = nr - p + 1;
		clacgv_(&i__2, &u[p + p * u_dim1], &c__1);
/* L7969: */
	    }
	    if (l2pert) {
		xsc = sqrt(small / epsln);
		i__1 = nr;
		for (q = 2; q <= i__1; ++q) {
		    i__2 = q - 1;
		    for (p = 1; p <= i__2; ++p) {
/* Computing MIN */
			r__2 = c_abs(&u[p + p * u_dim1]), r__3 = c_abs(&u[q + 
				q * u_dim1]);
			r__1 = xsc * f2cmin(r__2,r__3);
			q__1.r = r__1, q__1.i = 0.f;
			ctemp.r = q__1.r, ctemp.i = q__1.i;
/*                  U(p,q) = - TEMP1 * ( U(q,p) / ABS(U(q,p)) ) */
			i__3 = p + q * u_dim1;
			q__1.r = -ctemp.r, q__1.i = -ctemp.i;
			u[i__3].r = q__1.r, u[i__3].i = q__1.i;
/* L9971: */
		    }
/* L9970: */
		}
	    } else {
		i__1 = nr - 1;
		i__2 = nr - 1;
		claset_("U", &i__1, &i__2, &c_b1, &c_b1, &u[(u_dim1 << 1) + 1]
			, ldu);
	    }
	    i__1 = *lwork - (*n << 1) - *n * nr;
	    cgesvj_("L", "U", "V", &nr, &nr, &u[u_offset], ldu, &sva[1], n, &
		    v[v_offset], ldv, &cwork[(*n << 1) + *n * nr + 1], &i__1, 
		    &rwork[1], lrwork, info);
	    scalem = rwork[1];
	    numrank = i_nint(&rwork[2]);
	    if (nr < *n) {
		i__1 = *n - nr;
		claset_("A", &i__1, &nr, &c_b1, &c_b1, &v[nr + 1 + v_dim1], 
			ldv);
		i__1 = *n - nr;
		claset_("A", &nr, &i__1, &c_b1, &c_b1, &v[(nr + 1) * v_dim1 + 
			1], ldv);
		i__1 = *n - nr;
		i__2 = *n - nr;
		claset_("A", &i__1, &i__2, &c_b1, &c_b2, &v[nr + 1 + (nr + 1) 
			* v_dim1], ldv);
	    }
	    i__1 = *lwork - (*n << 1) - *n * nr - nr;
	    cunmqr_("L", "N", n, n, &nr, &cwork[(*n << 1) + 1], n, &cwork[*n 
		    + 1], &v[v_offset], ldv, &cwork[(*n << 1) + *n * nr + nr 
		    + 1], &i__1, &ierr);

/*           Permute the rows of V using the (column) permutation from the */
/*           first QRF. Also, scale the columns to make them unit in */
/*           Euclidean norm. This applies to all cases. */

	    temp1 = sqrt((real) (*n)) * epsln;
	    i__1 = *n;
	    for (q = 1; q <= i__1; ++q) {
		i__2 = *n;
		for (p = 1; p <= i__2; ++p) {
		    i__3 = (*n << 1) + *n * nr + nr + iwork[p];
		    i__4 = p + q * v_dim1;
		    cwork[i__3].r = v[i__4].r, cwork[i__3].i = v[i__4].i;
/* L8972: */
		}
		i__2 = *n;
		for (p = 1; p <= i__2; ++p) {
		    i__3 = p + q * v_dim1;
		    i__4 = (*n << 1) + *n * nr + nr + p;
		    v[i__3].r = cwork[i__4].r, v[i__3].i = cwork[i__4].i;
/* L8973: */
		}
		xsc = 1.f / scnrm2_(n, &v[q * v_dim1 + 1], &c__1);
		if (xsc < 1.f - temp1 || xsc > temp1 + 1.f) {
		    csscal_(n, &xsc, &v[q * v_dim1 + 1], &c__1);
		}
/* L7972: */
	    }

/*           At this moment, V contains the right singular vectors of A. */
/*           Next, assemble the left singular vector matrix U (M x N). */

	    if (nr < *m) {
		i__1 = *m - nr;
		claset_("A", &i__1, &nr, &c_b1, &c_b1, &u[nr + 1 + u_dim1], 
			ldu);
		if (nr < n1) {
		    i__1 = n1 - nr;
		    claset_("A", &nr, &i__1, &c_b1, &c_b1, &u[(nr + 1) * 
			    u_dim1 + 1], ldu);
		    i__1 = *m - nr;
		    i__2 = n1 - nr;
		    claset_("A", &i__1, &i__2, &c_b1, &c_b2, &u[nr + 1 + (nr 
			    + 1) * u_dim1], ldu);
		}
	    }

	    i__1 = *lwork - *n;
	    cunmqr_("L", "N", m, &n1, n, &a[a_offset], lda, &cwork[1], &u[
		    u_offset], ldu, &cwork[*n + 1], &i__1, &ierr);

	    if (rowpiv) {
		i__1 = *m - 1;
		claswp_(&n1, &u[u_offset], ldu, &c__1, &i__1, &iwork[iwoff + 
			1], &c_n1);
	    }


	}
	if (transp) {
	    i__1 = *n;
	    for (p = 1; p <= i__1; ++p) {
		cswap_(n, &u[p * u_dim1 + 1], &c__1, &v[p * v_dim1 + 1], &
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

    rwork[1] = uscal2 * scalem;
    rwork[2] = uscal1;
    if (errest) {
	rwork[3] = sconda;
    }
    if (lsvec && rsvec) {
	rwork[4] = condr1;
	rwork[5] = condr2;
    }
    if (l2tran) {
	rwork[6] = entra;
	rwork[7] = entrat;
    }

    iwork[1] = nr;
    iwork[2] = numrank;
    iwork[3] = warning;
    if (transp) {
	iwork[4] = 1;
    } else {
	iwork[4] = -1;
    }

    return;
} /* cgejsv_ */

