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
static doublecomplex c_b2 = {1.,0.};
static integer c_n1 = -1;
static integer c__1 = 1;
static doublereal c_b74 = 0.;
static integer c__0 = 0;
static doublereal c_b87 = 1.;
static logical c_false = FALSE_;

/* > \brief <b> ZGESVDQ computes the singular value decomposition (SVD) with a QR-Preconditioned QR SVD Method
 for GE matrices</b> */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZGESVDQ + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgesvdq
.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgesvdq
.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgesvdq
.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*      SUBROUTINE ZGESVDQ( JOBA, JOBP, JOBR, JOBU, JOBV, M, N, A, LDA, */
/*                          S, U, LDU, V, LDV, NUMRANK, IWORK, LIWORK, */
/*                          CWORK, LCWORK, RWORK, LRWORK, INFO ) */

/*      IMPLICIT    NONE */
/*      CHARACTER   JOBA, JOBP, JOBR, JOBU, JOBV */
/*      INTEGER     M, N, LDA, LDU, LDV, NUMRANK, LIWORK, LCWORK, LRWORK, */
/*                  INFO */
/*      COMPLEX*16       A( LDA, * ), U( LDU, * ), V( LDV, * ), CWORK( * ) */
/*      DOUBLE PRECISION S( * ), RWORK( * ) */
/*      INTEGER          IWORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* ZCGESVDQ computes the singular value decomposition (SVD) of a complex */
/* > M-by-N matrix A, where M >= N. The SVD of A is written as */
/* >                                    [++]   [xx]   [x0]   [xx] */
/* >              A = U * SIGMA * V^*,  [++] = [xx] * [ox] * [xx] */
/* >                                    [++]   [xx] */
/* > where SIGMA is an N-by-N diagonal matrix, U is an M-by-N orthonormal */
/* > matrix, and V is an N-by-N unitary matrix. The diagonal elements */
/* > of SIGMA are the singular values of A. The columns of U and V are the */
/* > left and the right singular vectors of A, respectively. */
/* > \endverbatim */

/*  Arguments */
/*  ========= */

/* > \param[in] JOBA */
/* > \verbatim */
/* >  JOBA is CHARACTER*1 */
/* >  Specifies the level of accuracy in the computed SVD */
/* >  = 'A' The requested accuracy corresponds to having the backward */
/* >        error bounded by || delta A ||_F <= f(m,n) * EPS * || A ||_F, */
/* >        where EPS = DLAMCH('Epsilon'). This authorises ZGESVDQ to */
/* >        truncate the computed triangular factor in a rank revealing */
/* >        QR factorization whenever the truncated part is below the */
/* >        threshold of the order of EPS * ||A||_F. This is aggressive */
/* >        truncation level. */
/* >  = 'M' Similarly as with 'A', but the truncation is more gentle: it */
/* >        is allowed only when there is a drop on the diagonal of the */
/* >        triangular factor in the QR factorization. This is medium */
/* >        truncation level. */
/* >  = 'H' High accuracy requested. No numerical rank determination based */
/* >        on the rank revealing QR factorization is attempted. */
/* >  = 'E' Same as 'H', and in addition the condition number of column */
/* >        scaled A is estimated and returned in  RWORK(1). */
/* >        N^(-1/4)*RWORK(1) <= ||pinv(A_scaled)||_2 <= N^(1/4)*RWORK(1) */
/* > \endverbatim */
/* > */
/* > \param[in] JOBP */
/* > \verbatim */
/* >  JOBP is CHARACTER*1 */
/* >  = 'P' The rows of A are ordered in decreasing order with respect to */
/* >        ||A(i,:)||_\infty. This enhances numerical accuracy at the cost */
/* >        of extra data movement. Recommended for numerical robustness. */
/* >  = 'N' No row pivoting. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBR */
/* > \verbatim */
/* >          JOBR is CHARACTER*1 */
/* >          = 'T' After the initial pivoted QR factorization, ZGESVD is applied to */
/* >          the adjoint R**H of the computed triangular factor R. This involves */
/* >          some extra data movement (matrix transpositions). Useful for */
/* >          experiments, research and development. */
/* >          = 'N' The triangular factor R is given as input to CGESVD. This may be */
/* >          preferred as it involves less data movement. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBU */
/* > \verbatim */
/* >          JOBU is CHARACTER*1 */
/* >          = 'A' All M left singular vectors are computed and returned in the */
/* >          matrix U. See the description of U. */
/* >          = 'S' or 'U' N = f2cmin(M,N) left singular vectors are computed and returned */
/* >          in the matrix U. See the description of U. */
/* >          = 'R' Numerical rank NUMRANK is determined and only NUMRANK left singular */
/* >          vectors are computed and returned in the matrix U. */
/* >          = 'F' The N left singular vectors are returned in factored form as the */
/* >          product of the Q factor from the initial QR factorization and the */
/* >          N left singular vectors of (R**H , 0)**H. If row pivoting is used, */
/* >          then the necessary information on the row pivoting is stored in */
/* >          IWORK(N+1:N+M-1). */
/* >          = 'N' The left singular vectors are not computed. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBV */
/* > \verbatim */
/* >          JOBV is CHARACTER*1 */
/* >          = 'A', 'V' All N right singular vectors are computed and returned in */
/* >          the matrix V. */
/* >          = 'R' Numerical rank NUMRANK is determined and only NUMRANK right singular */
/* >          vectors are computed and returned in the matrix V. This option is */
/* >          allowed only if JOBU = 'R' or JOBU = 'N'; otherwise it is illegal. */
/* >          = 'N' The right singular vectors are not computed. */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the input matrix A.  M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the input matrix A.  M >= N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX*16 array of dimensions LDA x N */
/* >          On entry, the input matrix A. */
/* >          On exit, if JOBU .NE. 'N' or JOBV .NE. 'N', the lower triangle of A contains */
/* >          the Householder vectors as stored by ZGEQP3. If JOBU = 'F', these Householder */
/* >          vectors together with CWORK(1:N) can be used to restore the Q factors from */
/* >          the initial pivoted QR factorization of A. See the description of U. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER. */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[out] S */
/* > \verbatim */
/* >          S is DOUBLE PRECISION array of dimension N. */
/* >          The singular values of A, ordered so that S(i) >= S(i+1). */
/* > \endverbatim */
/* > */
/* > \param[out] U */
/* > \verbatim */
/* >          U is COMPLEX*16 array, dimension */
/* >          LDU x M if JOBU = 'A'; see the description of LDU. In this case, */
/* >          on exit, U contains the M left singular vectors. */
/* >          LDU x N if JOBU = 'S', 'U', 'R' ; see the description of LDU. In this */
/* >          case, U contains the leading N or the leading NUMRANK left singular vectors. */
/* >          LDU x N if JOBU = 'F' ; see the description of LDU. In this case U */
/* >          contains N x N unitary matrix that can be used to form the left */
/* >          singular vectors. */
/* >          If JOBU = 'N', U is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU */
/* > \verbatim */
/* >          LDU is INTEGER. */
/* >          The leading dimension of the array U. */
/* >          If JOBU = 'A', 'S', 'U', 'R',  LDU >= f2cmax(1,M). */
/* >          If JOBU = 'F',                 LDU >= f2cmax(1,N). */
/* >          Otherwise,                     LDU >= 1. */
/* > \endverbatim */
/* > */
/* > \param[out] V */
/* > \verbatim */
/* >          V is COMPLEX*16 array, dimension */
/* >          LDV x N if JOBV = 'A', 'V', 'R' or if JOBA = 'E' . */
/* >          If JOBV = 'A', or 'V',  V contains the N-by-N unitary matrix  V**H; */
/* >          If JOBV = 'R', V contains the first NUMRANK rows of V**H (the right */
/* >          singular vectors, stored rowwise, of the NUMRANK largest singular values). */
/* >          If JOBV = 'N' and JOBA = 'E', V is used as a workspace. */
/* >          If JOBV = 'N', and JOBA.NE.'E', V is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDV */
/* > \verbatim */
/* >          LDV is INTEGER */
/* >          The leading dimension of the array V. */
/* >          If JOBV = 'A', 'V', 'R',  or JOBA = 'E', LDV >= f2cmax(1,N). */
/* >          Otherwise,                               LDV >= 1. */
/* > \endverbatim */
/* > */
/* > \param[out] NUMRANK */
/* > \verbatim */
/* >          NUMRANK is INTEGER */
/* >          NUMRANK is the numerical rank first determined after the rank */
/* >          revealing QR factorization, following the strategy specified by the */
/* >          value of JOBA. If JOBV = 'R' and JOBU = 'R', only NUMRANK */
/* >          leading singular values and vectors are then requested in the call */
/* >          of CGESVD. The final value of NUMRANK might be further reduced if */
/* >          some singular values are computed as zeros. */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (f2cmax(1, LIWORK)). */
/* >          On exit, IWORK(1:N) contains column pivoting permutation of the */
/* >          rank revealing QR factorization. */
/* >          If JOBP = 'P', IWORK(N+1:N+M-1) contains the indices of the sequence */
/* >          of row swaps used in row pivoting. These can be used to restore the */
/* >          left singular vectors in the case JOBU = 'F'. */

/* >          If LIWORK, LCWORK, or LRWORK = -1, then on exit, if INFO = 0, */
/* >          LIWORK(1) returns the minimal LIWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LIWORK */
/* > \verbatim */
/* >          LIWORK is INTEGER */
/* >          The dimension of the array IWORK. */
/* >          LIWORK >= N + M - 1,  if JOBP = 'P'; */
/* >          LIWORK >= N           if JOBP = 'N'. */
/* > */
/* >          If LIWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates and returns the optimal and minimal sizes */
/* >          for the CWORK, IWORK, and RWORK arrays, and no error */
/* >          message related to LCWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] CWORK */
/* > \verbatim */
/* >          CWORK is COMPLEX*12 array, dimension (f2cmax(2, LCWORK)), used as a workspace. */
/* >          On exit, if, on entry, LCWORK.NE.-1, CWORK(1:N) contains parameters */
/* >          needed to recover the Q factor from the QR factorization computed by */
/* >          ZGEQP3. */

/* >          If LIWORK, LCWORK, or LRWORK = -1, then on exit, if INFO = 0, */
/* >          CWORK(1) returns the optimal LCWORK, and */
/* >          CWORK(2) returns the minimal LCWORK. */
/* > \endverbatim */
/* > */
/* > \param[in,out] LCWORK */
/* > \verbatim */
/* >          LCWORK is INTEGER */
/* >          The dimension of the array CWORK. It is determined as follows: */
/* >          Let  LWQP3 = N+1,  LWCON = 2*N, and let */
/* >          LWUNQ = { MAX( N, 1 ),  if JOBU = 'R', 'S', or 'U' */
/* >          { MAX( M, 1 ),  if JOBU = 'A' */
/* >          LWSVD = MAX( 3*N, 1 ) */
/* >          LWLQF = MAX( N/2, 1 ), LWSVD2 = MAX( 3*(N/2), 1 ), LWUNLQ = MAX( N, 1 ), */
/* >          LWQRF = MAX( N/2, 1 ), LWUNQ2 = MAX( N, 1 ) */
/* >          Then the minimal value of LCWORK is: */
/* >          = MAX( N + LWQP3, LWSVD )        if only the singular values are needed; */
/* >          = MAX( N + LWQP3, LWCON, LWSVD ) if only the singular values are needed, */
/* >                                   and a scaled condition estimate requested; */
/* > */
/* >          = N + MAX( LWQP3, LWSVD, LWUNQ ) if the singular values and the left */
/* >                                   singular vectors are requested; */
/* >          = N + MAX( LWQP3, LWCON, LWSVD, LWUNQ ) if the singular values and the left */
/* >                                   singular vectors are requested, and also */
/* >                                   a scaled condition estimate requested; */
/* > */
/* >          = N + MAX( LWQP3, LWSVD )        if the singular values and the right */
/* >                                   singular vectors are requested; */
/* >          = N + MAX( LWQP3, LWCON, LWSVD ) if the singular values and the right */
/* >                                   singular vectors are requested, and also */
/* >                                   a scaled condition etimate requested; */
/* > */
/* >          = N + MAX( LWQP3, LWSVD, LWUNQ ) if the full SVD is requested with JOBV = 'R'; */
/* >                                   independent of JOBR; */
/* >          = N + MAX( LWQP3, LWCON, LWSVD, LWUNQ ) if the full SVD is requested, */
/* >                                   JOBV = 'R' and, also a scaled condition */
/* >                                   estimate requested; independent of JOBR; */
/* >          = MAX( N + MAX( LWQP3, LWSVD, LWUNQ ), */
/* >         N + MAX( LWQP3, N/2+LWLQF, N/2+LWSVD2, N/2+LWUNLQ, LWUNQ) ) if the */
/* >                         full SVD is requested with JOBV = 'A' or 'V', and */
/* >                         JOBR ='N' */
/* >          = MAX( N + MAX( LWQP3, LWCON, LWSVD, LWUNQ ), */
/* >         N + MAX( LWQP3, LWCON, N/2+LWLQF, N/2+LWSVD2, N/2+LWUNLQ, LWUNQ ) ) */
/* >                         if the full SVD is requested with JOBV = 'A' or 'V', and */
/* >                         JOBR ='N', and also a scaled condition number estimate */
/* >                         requested. */
/* >          = MAX( N + MAX( LWQP3, LWSVD, LWUNQ ), */
/* >         N + MAX( LWQP3, N/2+LWQRF, N/2+LWSVD2, N/2+LWUNQ2, LWUNQ ) ) if the */
/* >                         full SVD is requested with JOBV = 'A', 'V', and JOBR ='T' */
/* >          = MAX( N + MAX( LWQP3, LWCON, LWSVD, LWUNQ ), */
/* >         N + MAX( LWQP3, LWCON, N/2+LWQRF, N/2+LWSVD2, N/2+LWUNQ2, LWUNQ ) ) */
/* >                         if the full SVD is requested with JOBV = 'A', 'V' and */
/* >                         JOBR ='T', and also a scaled condition number estimate */
/* >                         requested. */
/* >          Finally, LCWORK must be at least two: LCWORK = MAX( 2, LCWORK ). */
/* > */
/* >          If LCWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates and returns the optimal and minimal sizes */
/* >          for the CWORK, IWORK, and RWORK arrays, and no error */
/* >          message related to LCWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is DOUBLE PRECISION array, dimension (f2cmax(1, LRWORK)). */
/* >          On exit, */
/* >          1. If JOBA = 'E', RWORK(1) contains an estimate of the condition */
/* >          number of column scaled A. If A = C * D where D is diagonal and C */
/* >          has unit columns in the Euclidean norm, then, assuming full column rank, */
/* >          N^(-1/4) * RWORK(1) <= ||pinv(C)||_2 <= N^(1/4) * RWORK(1). */
/* >          Otherwise, RWORK(1) = -1. */
/* >          2. RWORK(2) contains the number of singular values computed as */
/* >          exact zeros in ZGESVD applied to the upper triangular or trapeziodal */
/* >          R (from the initial QR factorization). In case of early exit (no call to */
/* >          ZGESVD, such as in the case of zero matrix) RWORK(2) = -1. */

/* >          If LIWORK, LCWORK, or LRWORK = -1, then on exit, if INFO = 0, */
/* >          RWORK(1) returns the minimal LRWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LRWORK */
/* > \verbatim */
/* >          LRWORK is INTEGER. */
/* >          The dimension of the array RWORK. */
/* >          If JOBP ='P', then LRWORK >= MAX(2, M, 5*N); */
/* >          Otherwise, LRWORK >= MAX(2, 5*N). */

/* >          If LRWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates and returns the optimal and minimal sizes */
/* >          for the CWORK, IWORK, and RWORK arrays, and no error */
/* >          message related to LCWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          > 0:  if ZBDSQR did not converge, INFO specifies how many superdiagonals */
/* >          of an intermediate bidiagonal form B (computed in ZGESVD) did not */
/* >          converge to zero. */
/* > \endverbatim */

/* > \par Further Details: */
/*  ======================== */
/* > */
/* > \verbatim */
/* > */
/* >   1. The data movement (matrix transpose) is coded using simple nested */
/* >   DO-loops because BLAS and LAPACK do not provide corresponding subroutines. */
/* >   Those DO-loops are easily identified in this source code - by the CONTINUE */
/* >   statements labeled with 11**. In an optimized version of this code, the */
/* >   nested DO loops should be replaced with calls to an optimized subroutine. */
/* >   2. This code scales A by 1/SQRT(M) if the largest ABS(A(i,j)) could cause */
/* >   column norm overflow. This is the minial precaution and it is left to the */
/* >   SVD routine (CGESVD) to do its own preemptive scaling if potential over- */
/* >   or underflows are detected. To avoid repeated scanning of the array A, */
/* >   an optimal implementation would do all necessary scaling before calling */
/* >   CGESVD and the scaling in CGESVD can be switched off. */
/* >   3. Other comments related to code optimization are given in comments in the */
/* >   code, enlosed in [[double brackets]]. */
/* > \endverbatim */

/* > \par Bugs, examples and comments */
/*  =========================== */

/* > \verbatim */
/* >  Please report all bugs and send interesting examples and/or comments to */
/* >  drmac@math.hr. Thank you. */
/* > \endverbatim */

/* > \par References */
/*  =============== */

/* > \verbatim */
/* >  [1] Zlatko Drmac, Algorithm 977: A QR-Preconditioned QR SVD Method for */
/* >      Computing the SVD with High Accuracy. ACM Trans. Math. Softw. */
/* >      44(1): 11:1-11:30 (2017) */
/* > */
/* >  SIGMA library, xGESVDQ section updated February 2016. */
/* >  Developed and coded by Zlatko Drmac, Department of Mathematics */
/* >  University of Zagreb, Croatia, drmac@math.hr */
/* > \endverbatim */


/* > \par Contributors: */
/*  ================== */
/* > */
/* > \verbatim */
/* > Developed and coded by Zlatko Drmac, Department of Mathematics */
/* >  University of Zagreb, Croatia, drmac@math.hr */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date November 2018 */

/* > \ingroup complex16GEsing */

/*  ===================================================================== */
/* Subroutine */ void zgesvdq_(char *joba, char *jobp, char *jobr, char *jobu, 
	char *jobv, integer *m, integer *n, doublecomplex *a, integer *lda, 
	doublereal *s, doublecomplex *u, integer *ldu, doublecomplex *v, 
	integer *ldv, integer *numrank, integer *iwork, integer *liwork, 
	doublecomplex *cwork, integer *lcwork, doublereal *rwork, integer *
	lrwork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, u_dim1, u_offset, v_dim1, v_offset, i__1, i__2, 
	    i__3;
    doublereal d__1;
    doublecomplex z__1;

    /* Local variables */
    integer lwrk_zunmlq__, lwrk_zunmqr__, ierr;
    doublecomplex ctmp;
    integer lwrk_zgesvd2__;
    doublereal rtmp;
    integer lwrk_zunmqr2__, optratio;
    logical lsvc0, accla;
    integer lwqp3;
    logical acclh, acclm;
    integer p, q;
    logical conda;
    extern logical lsame_(char *, char *);
    logical lsvec;
    doublereal sfmin, epsln;
    integer lwcon;
    logical rsvec;
    integer lwlqf, lwqrf;
    logical wntua;
    integer n1, lwsvd;
    logical dntwu, dntwv, wntuf, wntva;
    integer lwunq;
    logical wntur, wntus, wntvr;
    extern /* Subroutine */ void zgeqp3_(integer *, integer *, doublecomplex *,
	     integer *, integer *, doublecomplex *, doublecomplex *, integer *
	    , doublereal *, integer *);
    extern doublereal dznrm2_(integer *, doublecomplex *, integer *);
    integer lwsvd2, lwunq2;
    extern doublereal dlamch_(char *);
    integer nr;
    extern /* Subroutine */ void dlascl_(char *, integer *, integer *, 
	    doublereal *, doublereal *, integer *, integer *, doublereal *, 
	    integer *, integer *);
    extern integer idamax_(integer *, doublereal *, integer *);
    doublereal sconda;
    extern /* Subroutine */ void dlaset_(char *, integer *, integer *, 
	    doublereal *, doublereal *, doublereal *, integer *); 
    extern int xerbla_(char *, integer *, ftnlen);
    extern void zdscal_(integer *, doublereal 
	    *, doublecomplex *, integer *);
    extern doublereal zlange_(char *, integer *, integer *, doublecomplex *, 
	    integer *, doublereal *);
    extern /* Subroutine */ void zgelqf_(integer *, integer *, doublecomplex *,
	     integer *, doublecomplex *, doublecomplex *, integer *, integer *
	    ), zlascl_(char *, integer *, integer *, doublereal *, doublereal 
	    *, integer *, integer *, doublecomplex *, integer *, integer *);
    doublecomplex cdummy[1];
    extern /* Subroutine */ void zgeqrf_(integer *, integer *, doublecomplex *,
	     integer *, doublecomplex *, doublecomplex *, integer *, integer *
	    ), zgesvd_(char *, char *, integer *, integer *, doublecomplex *, 
	    integer *, doublereal *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublereal *, integer *), zlacpy_(char *, integer 
	    *, integer *, doublecomplex *, integer *, doublecomplex *, 
	    integer *), zlaset_(char *, integer *, integer *, 
	    doublecomplex *, doublecomplex *, doublecomplex *, integer *);
    integer minwrk;
    logical rtrans;
    extern /* Subroutine */ void zlapmt_(logical *, integer *, integer *, 
	    doublecomplex *, integer *, integer *), zpocon_(char *, integer *,
	     doublecomplex *, integer *, doublereal *, doublereal *, 
	    doublecomplex *, doublereal *, integer *);
    doublereal rdummy[1];
    logical lquery;
    integer lwunlq;
    extern /* Subroutine */ int zlaswp_(integer *, doublecomplex *, integer *,
	     integer *, integer *, integer *, integer *);
    integer optwrk;
    logical rowprm;
    extern /* Subroutine */ void zunmlq_(char *, char *, integer *, integer *, 
	    integer *, doublecomplex *, integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, integer *), zunmqr_(char *, char *, integer *, integer *, 
	    integer *, doublecomplex *, integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, integer *);
    doublereal big;
    integer minwrk2;
    logical ascaled;
    integer optwrk2, lwrk_zgeqp3__, iminwrk, rminwrk, lwrk_zgelqf__, 
	    lwrk_zgeqrf__, lwrk_zgesvd__;


/*  ===================================================================== */


/*     Test the input arguments */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --s;
    u_dim1 = *ldu;
    u_offset = 1 + u_dim1 * 1;
    u -= u_offset;
    v_dim1 = *ldv;
    v_offset = 1 + v_dim1 * 1;
    v -= v_offset;
    --iwork;
    --cwork;
    --rwork;

    /* Function Body */
    wntus = lsame_(jobu, "S") || lsame_(jobu, "U");
    wntur = lsame_(jobu, "R");
    wntua = lsame_(jobu, "A");
    wntuf = lsame_(jobu, "F");
    lsvc0 = wntus || wntur || wntua;
    lsvec = lsvc0 || wntuf;
    dntwu = lsame_(jobu, "N");

    wntvr = lsame_(jobv, "R");
    wntva = lsame_(jobv, "A") || lsame_(jobv, "V");
    rsvec = wntvr || wntva;
    dntwv = lsame_(jobv, "N");

    accla = lsame_(joba, "A");
    acclm = lsame_(joba, "M");
    conda = lsame_(joba, "E");
    acclh = lsame_(joba, "H") || conda;

    rowprm = lsame_(jobp, "P");
    rtrans = lsame_(jobr, "T");

    if (rowprm) {
/* Computing MAX */
	i__1 = 1, i__2 = *n + *m - 1;
	iminwrk = f2cmax(i__1,i__2);
/* Computing MAX */
	i__1 = f2cmax(2,*m), i__2 = *n * 5;
	rminwrk = f2cmax(i__1,i__2);
    } else {
	iminwrk = f2cmax(1,*n);
/* Computing MAX */
	i__1 = 2, i__2 = *n * 5;
	rminwrk = f2cmax(i__1,i__2);
    }
    lquery = *liwork == -1 || *lcwork == -1 || *lrwork == -1;
    *info = 0;
    if (! (accla || acclm || acclh)) {
	*info = -1;
    } else if (! (rowprm || lsame_(jobp, "N"))) {
	*info = -2;
    } else if (! (rtrans || lsame_(jobr, "N"))) {
	*info = -3;
    } else if (! (lsvec || dntwu)) {
	*info = -4;
    } else if (wntur && wntva) {
	*info = -5;
    } else if (! (rsvec || dntwv)) {
	*info = -5;
    } else if (*m < 0) {
	*info = -6;
    } else if (*n < 0 || *n > *m) {
	*info = -7;
    } else if (*lda < f2cmax(1,*m)) {
	*info = -9;
    } else if (*ldu < 1 || lsvc0 && *ldu < *m || wntuf && *ldu < *n) {
	*info = -12;
    } else if (*ldv < 1 || rsvec && *ldv < *n || conda && *ldv < *n) {
	*info = -14;
    } else if (*liwork < iminwrk && ! lquery) {
	*info = -17;
    }


    if (*info == 0) {
/*        [[The expressions for computing the minimal and the optimal */
/*        values of LCWORK are written with a lot of redundancy and */
/*        can be simplified. However, this detailed form is easier for */
/*        maintenance and modifications of the code.]] */

	lwqp3 = *n + 1;
	if (wntus || wntur) {
	    lwunq = f2cmax(*n,1);
	} else if (wntua) {
	    lwunq = f2cmax(*m,1);
	}
	lwcon = *n << 1;
/* Computing MAX */
	i__1 = *n * 3;
	lwsvd = f2cmax(i__1,1);
	if (lquery) {
	    zgeqp3_(m, n, &a[a_offset], lda, &iwork[1], cdummy, cdummy, &c_n1,
		     rdummy, &ierr);
	    lwrk_zgeqp3__ = (integer) cdummy[0].r;
	    if (wntus || wntur) {
		zunmqr_("L", "N", m, n, n, &a[a_offset], lda, cdummy, &u[
			u_offset], ldu, cdummy, &c_n1, &ierr);
		lwrk_zunmqr__ = (integer) cdummy[0].r;
	    } else if (wntua) {
		zunmqr_("L", "N", m, m, n, &a[a_offset], lda, cdummy, &u[
			u_offset], ldu, cdummy, &c_n1, &ierr);
		lwrk_zunmqr__ = (integer) cdummy[0].r;
	    } else {
		lwrk_zunmqr__ = 0;
	    }
	}
	minwrk = 2;
	optwrk = 2;
	if (! (lsvec || rsvec)) {
/*            only the singular values are requested */
	    if (conda) {
/* Computing MAX */
		i__1 = *n + lwqp3, i__1 = f2cmax(i__1,lwcon);
		minwrk = f2cmax(i__1,lwsvd);
	    } else {
/* Computing MAX */
		i__1 = *n + lwqp3;
		minwrk = f2cmax(i__1,lwsvd);
	    }
	    if (lquery) {
		zgesvd_("N", "N", n, n, &a[a_offset], lda, &s[1], &u[u_offset]
			, ldu, &v[v_offset], ldv, cdummy, &c_n1, rdummy, &
			ierr);
		lwrk_zgesvd__ = (integer) cdummy[0].r;
		if (conda) {
/* Computing MAX */
		    i__1 = *n + lwrk_zgeqp3__, i__2 = *n + lwcon, i__1 = f2cmax(
			    i__1,i__2);
		    optwrk = f2cmax(i__1,lwrk_zgesvd__);
		} else {
/* Computing MAX */
		    i__1 = *n + lwrk_zgeqp3__;
		    optwrk = f2cmax(i__1,lwrk_zgesvd__);
		}
	    }
	} else if (lsvec && ! rsvec) {
/*            singular values and the left singular vectors are requested */
	    if (conda) {
/* Computing MAX */
		i__1 = f2cmax(lwqp3,lwcon), i__1 = f2cmax(i__1,lwsvd);
		minwrk = *n + f2cmax(i__1,lwunq);
	    } else {
/* Computing MAX */
		i__1 = f2cmax(lwqp3,lwsvd);
		minwrk = *n + f2cmax(i__1,lwunq);
	    }
	    if (lquery) {
		if (rtrans) {
		    zgesvd_("N", "O", n, n, &a[a_offset], lda, &s[1], &u[
			    u_offset], ldu, &v[v_offset], ldv, cdummy, &c_n1, 
			    rdummy, &ierr);
		} else {
		    zgesvd_("O", "N", n, n, &a[a_offset], lda, &s[1], &u[
			    u_offset], ldu, &v[v_offset], ldv, cdummy, &c_n1, 
			    rdummy, &ierr);
		}
		lwrk_zgesvd__ = (integer) cdummy[0].r;
		if (conda) {
/* Computing MAX */
		    i__1 = f2cmax(lwrk_zgeqp3__,lwcon), i__1 = f2cmax(i__1,
			    lwrk_zgesvd__);
		    optwrk = *n + f2cmax(i__1,lwrk_zunmqr__);
		} else {
/* Computing MAX */
		    i__1 = f2cmax(lwrk_zgeqp3__,lwrk_zgesvd__);
		    optwrk = *n + f2cmax(i__1,lwrk_zunmqr__);
		}
	    }
	} else if (rsvec && ! lsvec) {
/*            singular values and the right singular vectors are requested */
	    if (conda) {
/* Computing MAX */
		i__1 = f2cmax(lwqp3,lwcon);
		minwrk = *n + f2cmax(i__1,lwsvd);
	    } else {
		minwrk = *n + f2cmax(lwqp3,lwsvd);
	    }
	    if (lquery) {
		if (rtrans) {
		    zgesvd_("O", "N", n, n, &a[a_offset], lda, &s[1], &u[
			    u_offset], ldu, &v[v_offset], ldv, cdummy, &c_n1, 
			    rdummy, &ierr);
		} else {
		    zgesvd_("N", "O", n, n, &a[a_offset], lda, &s[1], &u[
			    u_offset], ldu, &v[v_offset], ldv, cdummy, &c_n1, 
			    rdummy, &ierr);
		}
		lwrk_zgesvd__ = (integer) cdummy[0].r;
		if (conda) {
/* Computing MAX */
		    i__1 = f2cmax(lwrk_zgeqp3__,lwcon);
		    optwrk = *n + f2cmax(i__1,lwrk_zgesvd__);
		} else {
		    optwrk = *n + f2cmax(lwrk_zgeqp3__,lwrk_zgesvd__);
		}
	    }
	} else {
/*            full SVD is requested */
	    if (rtrans) {
/* Computing MAX */
		i__1 = f2cmax(lwqp3,lwsvd);
		minwrk = f2cmax(i__1,lwunq);
		if (conda) {
		    minwrk = f2cmax(minwrk,lwcon);
		}
		minwrk += *n;
		if (wntva) {
/* Computing MAX */
		    i__1 = *n / 2;
		    lwqrf = f2cmax(i__1,1);
/* Computing MAX */
		    i__1 = *n / 2 * 3;
		    lwsvd2 = f2cmax(i__1,1);
		    lwunq2 = f2cmax(*n,1);
/* Computing MAX */
		    i__1 = lwqp3, i__2 = *n / 2 + lwqrf, i__1 = f2cmax(i__1,i__2)
			    , i__2 = *n / 2 + lwsvd2, i__1 = f2cmax(i__1,i__2), 
			    i__2 = *n / 2 + lwunq2, i__1 = f2cmax(i__1,i__2);
		    minwrk2 = f2cmax(i__1,lwunq);
		    if (conda) {
			minwrk2 = f2cmax(minwrk2,lwcon);
		    }
		    minwrk2 = *n + minwrk2;
		    minwrk = f2cmax(minwrk,minwrk2);
		}
	    } else {
/* Computing MAX */
		i__1 = f2cmax(lwqp3,lwsvd);
		minwrk = f2cmax(i__1,lwunq);
		if (conda) {
		    minwrk = f2cmax(minwrk,lwcon);
		}
		minwrk += *n;
		if (wntva) {
/* Computing MAX */
		    i__1 = *n / 2;
		    lwlqf = f2cmax(i__1,1);
/* Computing MAX */
		    i__1 = *n / 2 * 3;
		    lwsvd2 = f2cmax(i__1,1);
		    lwunlq = f2cmax(*n,1);
/* Computing MAX */
		    i__1 = lwqp3, i__2 = *n / 2 + lwlqf, i__1 = f2cmax(i__1,i__2)
			    , i__2 = *n / 2 + lwsvd2, i__1 = f2cmax(i__1,i__2), 
			    i__2 = *n / 2 + lwunlq, i__1 = f2cmax(i__1,i__2);
		    minwrk2 = f2cmax(i__1,lwunq);
		    if (conda) {
			minwrk2 = f2cmax(minwrk2,lwcon);
		    }
		    minwrk2 = *n + minwrk2;
		    minwrk = f2cmax(minwrk,minwrk2);
		}
	    }
	    if (lquery) {
		if (rtrans) {
		    zgesvd_("O", "A", n, n, &a[a_offset], lda, &s[1], &u[
			    u_offset], ldu, &v[v_offset], ldv, cdummy, &c_n1, 
			    rdummy, &ierr);
		    lwrk_zgesvd__ = (integer) cdummy[0].r;
/* Computing MAX */
		    i__1 = f2cmax(lwrk_zgeqp3__,lwrk_zgesvd__);
		    optwrk = f2cmax(i__1,lwrk_zunmqr__);
		    if (conda) {
			optwrk = f2cmax(optwrk,lwcon);
		    }
		    optwrk = *n + optwrk;
		    if (wntva) {
			i__1 = *n / 2;
			zgeqrf_(n, &i__1, &u[u_offset], ldu, cdummy, cdummy, &
				c_n1, &ierr);
			lwrk_zgeqrf__ = (integer) cdummy[0].r;
			i__1 = *n / 2;
			i__2 = *n / 2;
			zgesvd_("S", "O", &i__1, &i__2, &v[v_offset], ldv, &s[
				1], &u[u_offset], ldu, &v[v_offset], ldv, 
				cdummy, &c_n1, rdummy, &ierr);
			lwrk_zgesvd2__ = (integer) cdummy[0].r;
			i__1 = *n / 2;
			zunmqr_("R", "C", n, n, &i__1, &u[u_offset], ldu, 
				cdummy, &v[v_offset], ldv, cdummy, &c_n1, &
				ierr);
			lwrk_zunmqr2__ = (integer) cdummy[0].r;
/* Computing MAX */
			i__1 = lwrk_zgeqp3__, i__2 = *n / 2 + lwrk_zgeqrf__, 
				i__1 = f2cmax(i__1,i__2), i__2 = *n / 2 + 
				lwrk_zgesvd2__, i__1 = f2cmax(i__1,i__2), i__2 = 
				*n / 2 + lwrk_zunmqr2__;
			optwrk2 = f2cmax(i__1,i__2);
			if (conda) {
			    optwrk2 = f2cmax(optwrk2,lwcon);
			}
			optwrk2 = *n + optwrk2;
			optwrk = f2cmax(optwrk,optwrk2);
		    }
		} else {
		    zgesvd_("S", "O", n, n, &a[a_offset], lda, &s[1], &u[
			    u_offset], ldu, &v[v_offset], ldv, cdummy, &c_n1, 
			    rdummy, &ierr);
		    lwrk_zgesvd__ = (integer) cdummy[0].r;
/* Computing MAX */
		    i__1 = f2cmax(lwrk_zgeqp3__,lwrk_zgesvd__);
		    optwrk = f2cmax(i__1,lwrk_zunmqr__);
		    if (conda) {
			optwrk = f2cmax(optwrk,lwcon);
		    }
		    optwrk = *n + optwrk;
		    if (wntva) {
			i__1 = *n / 2;
			zgelqf_(&i__1, n, &u[u_offset], ldu, cdummy, cdummy, &
				c_n1, &ierr);
			lwrk_zgelqf__ = (integer) cdummy[0].r;
			i__1 = *n / 2;
			i__2 = *n / 2;
			zgesvd_("S", "O", &i__1, &i__2, &v[v_offset], ldv, &s[
				1], &u[u_offset], ldu, &v[v_offset], ldv, 
				cdummy, &c_n1, rdummy, &ierr);
			lwrk_zgesvd2__ = (integer) cdummy[0].r;
			i__1 = *n / 2;
			zunmlq_("R", "N", n, n, &i__1, &u[u_offset], ldu, 
				cdummy, &v[v_offset], ldv, cdummy, &c_n1, &
				ierr);
			lwrk_zunmlq__ = (integer) cdummy[0].r;
/* Computing MAX */
			i__1 = lwrk_zgeqp3__, i__2 = *n / 2 + lwrk_zgelqf__, 
				i__1 = f2cmax(i__1,i__2), i__2 = *n / 2 + 
				lwrk_zgesvd2__, i__1 = f2cmax(i__1,i__2), i__2 = 
				*n / 2 + lwrk_zunmlq__;
			optwrk2 = f2cmax(i__1,i__2);
			if (conda) {
			    optwrk2 = f2cmax(optwrk2,lwcon);
			}
			optwrk2 = *n + optwrk2;
			optwrk = f2cmax(optwrk,optwrk2);
		    }
		}
	    }
	}

	minwrk = f2cmax(2,minwrk);
	optwrk = f2cmax(2,optwrk);
	if (*lcwork < minwrk && ! lquery) {
	    *info = -19;
	}

    }

    if (*info == 0 && *lrwork < rminwrk && ! lquery) {
	*info = -21;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZGESVDQ", &i__1, (ftnlen)7);
	return;
    } else if (lquery) {

/*     Return optimal workspace */

	iwork[1] = iminwrk;
	cwork[1].r = (doublereal) optwrk, cwork[1].i = 0.;
	cwork[2].r = (doublereal) minwrk, cwork[2].i = 0.;
	rwork[1] = (doublereal) rminwrk;
	return;
    }

/*     Quick return if the matrix is void. */

    if (*m == 0 || *n == 0) {
	return;
    }

    big = dlamch_("O");
    ascaled = FALSE_;
    if (rowprm) {
/*           ell-infinity norm - this enhances numerical robustness in */
/*           the case of differently scaled rows. */
	i__1 = *m;
	for (p = 1; p <= i__1; ++p) {
/*               RWORK(p) = ABS( A(p,IZAMAX(N,A(p,1),LDA)) ) */
/*               [[ZLANGE will return NaN if an entry of the p-th row is Nan]] */
	    rwork[p] = zlange_("M", &c__1, n, &a[p + a_dim1], lda, rdummy);
	    if (rwork[p] != rwork[p] || rwork[p] * 0. != 0.) {
		*info = -8;
		i__2 = -(*info);
		xerbla_("ZGESVDQ", &i__2, (ftnlen)7);
		return;
	    }
/* L1904: */
	}
	i__1 = *m - 1;
	for (p = 1; p <= i__1; ++p) {
	    i__2 = *m - p + 1;
	    q = idamax_(&i__2, &rwork[p], &c__1) + p - 1;
	    iwork[*n + p] = q;
	    if (p != q) {
		rtmp = rwork[p];
		rwork[p] = rwork[q];
		rwork[q] = rtmp;
	    }
/* L1952: */
	}

	if (rwork[1] == 0.) {
/*              Quick return: A is the M x N zero matrix. */
	    *numrank = 0;
	    dlaset_("G", n, &c__1, &c_b74, &c_b74, &s[1], n);
	    if (wntus) {
		zlaset_("G", m, n, &c_b1, &c_b2, &u[u_offset], ldu)
			;
	    }
	    if (wntua) {
		zlaset_("G", m, m, &c_b1, &c_b2, &u[u_offset], ldu)
			;
	    }
	    if (wntva) {
		zlaset_("G", n, n, &c_b1, &c_b2, &v[v_offset], ldv)
			;
	    }
	    if (wntuf) {
		zlaset_("G", n, &c__1, &c_b1, &c_b1, &cwork[1], n);
		zlaset_("G", m, n, &c_b1, &c_b2, &u[u_offset], ldu)
			;
	    }
	    i__1 = *n;
	    for (p = 1; p <= i__1; ++p) {
		iwork[p] = p;
/* L5001: */
	    }
	    if (rowprm) {
		i__1 = *n + *m - 1;
		for (p = *n + 1; p <= i__1; ++p) {
		    iwork[p] = p - *n;
/* L5002: */
		}
	    }
	    if (conda) {
		rwork[1] = -1.;
	    }
	    rwork[2] = -1.;
	    return;
	}

	if (rwork[1] > big / sqrt((doublereal) (*m))) {
/*               matrix by 1/sqrt(M) if too large entry detected */
	    d__1 = sqrt((doublereal) (*m));
	    zlascl_("G", &c__0, &c__0, &d__1, &c_b87, m, n, &a[a_offset], lda,
		     &ierr);
	    ascaled = TRUE_;
	}
	i__1 = *m - 1;
	zlaswp_(n, &a[a_offset], lda, &c__1, &i__1, &iwork[*n + 1], &c__1);
    }

/*    norms overflows during the QR factorization. The SVD procedure should */
/*    have its own scaling to save the singular values from overflows and */
/*    underflows. That depends on the SVD procedure. */

    if (! rowprm) {
	rtmp = zlange_("M", m, n, &a[a_offset], lda, &rwork[1]);
	if (rtmp != rtmp || rtmp * 0. != 0.) {
	    *info = -8;
	    i__1 = -(*info);
	    xerbla_("ZGESVDQ", &i__1, (ftnlen)7);
	    return;
	}
	if (rtmp > big / sqrt((doublereal) (*m))) {
/*             matrix by 1/sqrt(M) if too large entry detected */
	    d__1 = sqrt((doublereal) (*m));
	    zlascl_("G", &c__0, &c__0, &d__1, &c_b87, m, n, &a[a_offset], lda,
		     &ierr);
	    ascaled = TRUE_;
	}
    }


/*     A * P = Q * [ R ] */
/*                 [ 0 ] */

    i__1 = *n;
    for (p = 1; p <= i__1; ++p) {
	iwork[p] = 0;
/* L1963: */
    }
    i__1 = *lcwork - *n;
    zgeqp3_(m, n, &a[a_offset], lda, &iwork[1], &cwork[1], &cwork[*n + 1], &
	    i__1, &rwork[1], &ierr);

/*    If the user requested accuracy level allows truncation in the */
/*    computed upper triangular factor, the matrix R is examined and, */
/*    if possible, replaced with its leading upper trapezoidal part. */

    epsln = dlamch_("E");
    sfmin = dlamch_("S");
/*     SMALL = SFMIN / EPSLN */
    nr = *n;

    if (accla) {

/*        Standard absolute error bound suffices. All sigma_i with */
/*        sigma_i < N*EPS*||A||_F are flushed to zero. This is an */
/*        aggressive enforcement of lower numerical rank by introducing a */
/*        backward error of the order of N*EPS*||A||_F. */
	nr = 1;
	rtmp = sqrt((doublereal) (*n)) * epsln;
	i__1 = *n;
	for (p = 2; p <= i__1; ++p) {
	    if (z_abs(&a[p + p * a_dim1]) < rtmp * z_abs(&a[a_dim1 + 1])) {
		goto L3002;
	    }
	    ++nr;
/* L3001: */
	}
L3002:

	;
    } else if (acclm) {
/*        Sudden drop on the diagonal of R is used as the criterion for being */
/*        close-to-rank-deficient. The threshold is set to EPSLN=DLAMCH('E'). */
/*        [[This can be made more flexible by replacing this hard-coded value */
/*        with a user specified threshold.]] Also, the values that underflow */
/*        will be truncated. */
	nr = 1;
	i__1 = *n;
	for (p = 2; p <= i__1; ++p) {
	    if (z_abs(&a[p + p * a_dim1]) < epsln * z_abs(&a[p - 1 + (p - 1) *
		     a_dim1]) || z_abs(&a[p + p * a_dim1]) < sfmin) {
		goto L3402;
	    }
	    ++nr;
/* L3401: */
	}
L3402:

	;
    } else {
/*        obvious case of zero pivots. */
/*        R(i,i)=0 => R(i:N,i:N)=0. */
	nr = 1;
	i__1 = *n;
	for (p = 2; p <= i__1; ++p) {
	    if (z_abs(&a[p + p * a_dim1]) == 0.) {
		goto L3502;
	    }
	    ++nr;
/* L3501: */
	}
L3502:

	if (conda) {
/*           Estimate the scaled condition number of A. Use the fact that it is */
/*           the same as the scaled condition number of R. */
	    zlacpy_("U", n, n, &a[a_offset], lda, &v[v_offset], ldv);
/*              Only the leading NR x NR submatrix of the triangular factor */
/*              is considered. Only if NR=N will this give a reliable error */
/*              bound. However, even for NR < N, this can be used on an */
/*              expert level and obtain useful information in the sense of */
/*              perturbation theory. */
	    i__1 = nr;
	    for (p = 1; p <= i__1; ++p) {
		rtmp = dznrm2_(&p, &v[p * v_dim1 + 1], &c__1);
		d__1 = 1. / rtmp;
		zdscal_(&p, &d__1, &v[p * v_dim1 + 1], &c__1);
/* L3053: */
	    }
	    if (! (lsvec || rsvec)) {
		zpocon_("U", &nr, &v[v_offset], ldv, &c_b87, &rtmp, &cwork[1],
			 &rwork[1], &ierr);
	    } else {
		zpocon_("U", &nr, &v[v_offset], ldv, &c_b87, &rtmp, &cwork[*n 
			+ 1], &rwork[1], &ierr);
	    }
	    sconda = 1. / sqrt(rtmp);
/*           For NR=N, SCONDA is an estimate of SQRT(||(R^* * R)^(-1)||_1), */
/*           N^(-1/4) * SCONDA <= ||R^(-1)||_2 <= N^(1/4) * SCONDA */
/*           See the reference [1] for more details. */
	}

    }

    if (wntur) {
	n1 = nr;
    } else if (wntus || wntuf) {
	n1 = *n;
    } else if (wntua) {
	n1 = *m;
    }

    if (! (rsvec || lsvec)) {
/* ....................................................................... */
/* ....................................................................... */
	if (rtrans) {

/*           the upper triangle of [A] to zero. */
	    i__1 = f2cmin(*n,nr);
	    for (p = 1; p <= i__1; ++p) {
		i__2 = p + p * a_dim1;
		d_cnjg(&z__1, &a[p + p * a_dim1]);
		a[i__2].r = z__1.r, a[i__2].i = z__1.i;
		i__2 = *n;
		for (q = p + 1; q <= i__2; ++q) {
		    i__3 = q + p * a_dim1;
		    d_cnjg(&z__1, &a[p + q * a_dim1]);
		    a[i__3].r = z__1.r, a[i__3].i = z__1.i;
		    if (q <= nr) {
			i__3 = p + q * a_dim1;
			a[i__3].r = 0., a[i__3].i = 0.;
		    }
/* L1147: */
		}
/* L1146: */
	    }

	    zgesvd_("N", "N", n, &nr, &a[a_offset], lda, &s[1], &u[u_offset], 
		    ldu, &v[v_offset], ldv, &cwork[1], lcwork, &rwork[1], 
		    info);

	} else {


	    if (nr > 1) {
		i__1 = nr - 1;
		i__2 = nr - 1;
		zlaset_("L", &i__1, &i__2, &c_b1, &c_b1, &a[a_dim1 + 2], lda);
	    }
	    zgesvd_("N", "N", &nr, n, &a[a_offset], lda, &s[1], &u[u_offset], 
		    ldu, &v[v_offset], ldv, &cwork[1], lcwork, &rwork[1], 
		    info);

	}

    } else if (lsvec && ! rsvec) {
/* ....................................................................... */
/* ......................................................................."""""""" */
	if (rtrans) {
/*            vectors of R */
	    i__1 = nr;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = *n;
		for (q = p; q <= i__2; ++q) {
		    i__3 = q + p * u_dim1;
		    d_cnjg(&z__1, &a[p + q * a_dim1]);
		    u[i__3].r = z__1.r, u[i__3].i = z__1.i;
/* L1193: */
		}
/* L1192: */
	    }
	    if (nr > 1) {
		i__1 = nr - 1;
		i__2 = nr - 1;
		zlaset_("U", &i__1, &i__2, &c_b1, &c_b1, &u[(u_dim1 << 1) + 1]
			, ldu);
	    }
/*           vectors overwrite [U](1:NR,1:NR) as conjugate transposed. These */
/*           will be pre-multiplied by Q to build the left singular vectors of A. */
	    i__1 = *lcwork - *n;
	    zgesvd_("N", "O", n, &nr, &u[u_offset], ldu, &s[1], &u[u_offset], 
		    ldu, &u[u_offset], ldu, &cwork[*n + 1], &i__1, &rwork[1], 
		    info);

	    i__1 = nr;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = p + p * u_dim1;
		d_cnjg(&z__1, &u[p + p * u_dim1]);
		u[i__2].r = z__1.r, u[i__2].i = z__1.i;
		i__2 = nr;
		for (q = p + 1; q <= i__2; ++q) {
		    d_cnjg(&z__1, &u[q + p * u_dim1]);
		    ctmp.r = z__1.r, ctmp.i = z__1.i;
		    i__3 = q + p * u_dim1;
		    d_cnjg(&z__1, &u[p + q * u_dim1]);
		    u[i__3].r = z__1.r, u[i__3].i = z__1.i;
		    i__3 = p + q * u_dim1;
		    u[i__3].r = ctmp.r, u[i__3].i = ctmp.i;
/* L1120: */
		}
/* L1119: */
	    }

	} else {
	    zlacpy_("U", &nr, n, &a[a_offset], lda, &u[u_offset], ldu);
	    if (nr > 1) {
		i__1 = nr - 1;
		i__2 = nr - 1;
		zlaset_("L", &i__1, &i__2, &c_b1, &c_b1, &u[u_dim1 + 2], ldu);
	    }
/*            vectors overwrite [U](1:NR,1:NR) */
	    i__1 = *lcwork - *n;
	    zgesvd_("O", "N", &nr, n, &u[u_offset], ldu, &s[1], &u[u_offset], 
		    ldu, &v[v_offset], ldv, &cwork[*n + 1], &i__1, &rwork[1], 
		    info);
/*               R. These will be pre-multiplied by Q to build the left singular */
/*               vectors of A. */
	}

/*              (M x NR) or (M x N) or (M x M). */
	if (nr < *m && ! wntuf) {
	    i__1 = *m - nr;
	    zlaset_("A", &i__1, &nr, &c_b1, &c_b1, &u[nr + 1 + u_dim1], ldu);
	    if (nr < n1) {
		i__1 = n1 - nr;
		zlaset_("A", &nr, &i__1, &c_b1, &c_b1, &u[(nr + 1) * u_dim1 + 
			1], ldu);
		i__1 = *m - nr;
		i__2 = n1 - nr;
		zlaset_("A", &i__1, &i__2, &c_b1, &c_b2, &u[nr + 1 + (nr + 1) 
			* u_dim1], ldu);
	    }
	}

/*           The Q matrix from the first QRF is built into the left singular */
/*           vectors matrix U. */

	if (! wntuf) {
	    i__1 = *lcwork - *n;
	    zunmqr_("L", "N", m, &n1, n, &a[a_offset], lda, &cwork[1], &u[
		    u_offset], ldu, &cwork[*n + 1], &i__1, &ierr);
	}
	if (rowprm && ! wntuf) {
	    i__1 = *m - 1;
	    zlaswp_(&n1, &u[u_offset], ldu, &c__1, &i__1, &iwork[*n + 1], &
		    c_n1);
	}

    } else if (rsvec && ! lsvec) {
/* ....................................................................... */
/* ....................................................................... */
	if (rtrans) {
	    i__1 = nr;
	    for (p = 1; p <= i__1; ++p) {
		i__2 = *n;
		for (q = p; q <= i__2; ++q) {
		    i__3 = q + p * v_dim1;
		    d_cnjg(&z__1, &a[p + q * a_dim1]);
		    v[i__3].r = z__1.r, v[i__3].i = z__1.i;
/* L1166: */
		}
/* L1165: */
	    }
	    if (nr > 1) {
		i__1 = nr - 1;
		i__2 = nr - 1;
		zlaset_("U", &i__1, &i__2, &c_b1, &c_b1, &v[(v_dim1 << 1) + 1]
			, ldv);
	    }
/*           vectors not computed */
	    if (wntvr || nr == *n) {
		i__1 = *lcwork - *n;
		zgesvd_("O", "N", n, &nr, &v[v_offset], ldv, &s[1], &u[
			u_offset], ldu, &u[u_offset], ldu, &cwork[*n + 1], &
			i__1, &rwork[1], info);

		i__1 = nr;
		for (p = 1; p <= i__1; ++p) {
		    i__2 = p + p * v_dim1;
		    d_cnjg(&z__1, &v[p + p * v_dim1]);
		    v[i__2].r = z__1.r, v[i__2].i = z__1.i;
		    i__2 = nr;
		    for (q = p + 1; q <= i__2; ++q) {
			d_cnjg(&z__1, &v[q + p * v_dim1]);
			ctmp.r = z__1.r, ctmp.i = z__1.i;
			i__3 = q + p * v_dim1;
			d_cnjg(&z__1, &v[p + q * v_dim1]);
			v[i__3].r = z__1.r, v[i__3].i = z__1.i;
			i__3 = p + q * v_dim1;
			v[i__3].r = ctmp.r, v[i__3].i = ctmp.i;
/* L1122: */
		    }
/* L1121: */
		}

		if (nr < *n) {
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			i__2 = *n;
			for (q = nr + 1; q <= i__2; ++q) {
			    i__3 = p + q * v_dim1;
			    d_cnjg(&z__1, &v[q + p * v_dim1]);
			    v[i__3].r = z__1.r, v[i__3].i = z__1.i;
/* L1104: */
			}
/* L1103: */
		    }
		}
		zlapmt_(&c_false, &nr, n, &v[v_offset], ldv, &iwork[1]);
	    } else {
/*               [!] This is simple implementation that augments [V](1:N,1:NR) */
/*               by padding a zero block. In the case NR << N, a more efficient */
/*               way is to first use the QR factorization. For more details */
/*               how to implement this, see the " FULL SVD " branch. */
		i__1 = *n - nr;
		zlaset_("G", n, &i__1, &c_b1, &c_b1, &v[(nr + 1) * v_dim1 + 1]
			, ldv);
		i__1 = *lcwork - *n;
		zgesvd_("O", "N", n, n, &v[v_offset], ldv, &s[1], &u[u_offset]
			, ldu, &u[u_offset], ldu, &cwork[*n + 1], &i__1, &
			rwork[1], info);

		i__1 = *n;
		for (p = 1; p <= i__1; ++p) {
		    i__2 = p + p * v_dim1;
		    d_cnjg(&z__1, &v[p + p * v_dim1]);
		    v[i__2].r = z__1.r, v[i__2].i = z__1.i;
		    i__2 = *n;
		    for (q = p + 1; q <= i__2; ++q) {
			d_cnjg(&z__1, &v[q + p * v_dim1]);
			ctmp.r = z__1.r, ctmp.i = z__1.i;
			i__3 = q + p * v_dim1;
			d_cnjg(&z__1, &v[p + q * v_dim1]);
			v[i__3].r = z__1.r, v[i__3].i = z__1.i;
			i__3 = p + q * v_dim1;
			v[i__3].r = ctmp.r, v[i__3].i = ctmp.i;
/* L1124: */
		    }
/* L1123: */
		}
		zlapmt_(&c_false, n, n, &v[v_offset], ldv, &iwork[1]);
	    }

	} else {
	    zlacpy_("U", &nr, n, &a[a_offset], lda, &v[v_offset], ldv);
	    if (nr > 1) {
		i__1 = nr - 1;
		i__2 = nr - 1;
		zlaset_("L", &i__1, &i__2, &c_b1, &c_b1, &v[v_dim1 + 2], ldv);
	    }
/*            vectors stored in U(1:NR,1:NR) */
	    if (wntvr || nr == *n) {
		i__1 = *lcwork - *n;
		zgesvd_("N", "O", &nr, n, &v[v_offset], ldv, &s[1], &u[
			u_offset], ldu, &v[v_offset], ldv, &cwork[*n + 1], &
			i__1, &rwork[1], info);
		zlapmt_(&c_false, &nr, n, &v[v_offset], ldv, &iwork[1]);
	    } else {
/*               [!] This is simple implementation that augments [V](1:NR,1:N) */
/*               by padding a zero block. In the case NR << N, a more efficient */
/*               way is to first use the LQ factorization. For more details */
/*               how to implement this, see the " FULL SVD " branch. */
		i__1 = *n - nr;
		zlaset_("G", &i__1, n, &c_b1, &c_b1, &v[nr + 1 + v_dim1], ldv);
		i__1 = *lcwork - *n;
		zgesvd_("N", "O", n, n, &v[v_offset], ldv, &s[1], &u[u_offset]
			, ldu, &v[v_offset], ldv, &cwork[*n + 1], &i__1, &
			rwork[1], info);
		zlapmt_(&c_false, n, n, &v[v_offset], ldv, &iwork[1]);
	    }
/*            vectors of A. */
	}

    } else {
/* ....................................................................... */
/* ....................................................................... */
	if (rtrans) {


	    if (wntvr || nr == *n) {
/*            vectors of R**H */
		i__1 = nr;
		for (p = 1; p <= i__1; ++p) {
		    i__2 = *n;
		    for (q = p; q <= i__2; ++q) {
			i__3 = q + p * v_dim1;
			d_cnjg(&z__1, &a[p + q * a_dim1]);
			v[i__3].r = z__1.r, v[i__3].i = z__1.i;
/* L1169: */
		    }
/* L1168: */
		}
		if (nr > 1) {
		    i__1 = nr - 1;
		    i__2 = nr - 1;
		    zlaset_("U", &i__1, &i__2, &c_b1, &c_b1, &v[(v_dim1 << 1) 
			    + 1], ldv);
		}

/*           singular vectors of R**H stored in [U](1:NR,1:NR) as conjugate */
/*           transposed */
		i__1 = *lcwork - *n;
		zgesvd_("O", "A", n, &nr, &v[v_offset], ldv, &s[1], &v[
			v_offset], ldv, &u[u_offset], ldu, &cwork[*n + 1], &
			i__1, &rwork[1], info);
		i__1 = nr;
		for (p = 1; p <= i__1; ++p) {
		    i__2 = p + p * v_dim1;
		    d_cnjg(&z__1, &v[p + p * v_dim1]);
		    v[i__2].r = z__1.r, v[i__2].i = z__1.i;
		    i__2 = nr;
		    for (q = p + 1; q <= i__2; ++q) {
			d_cnjg(&z__1, &v[q + p * v_dim1]);
			ctmp.r = z__1.r, ctmp.i = z__1.i;
			i__3 = q + p * v_dim1;
			d_cnjg(&z__1, &v[p + q * v_dim1]);
			v[i__3].r = z__1.r, v[i__3].i = z__1.i;
			i__3 = p + q * v_dim1;
			v[i__3].r = ctmp.r, v[i__3].i = ctmp.i;
/* L1116: */
		    }
/* L1115: */
		}
		if (nr < *n) {
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			i__2 = *n;
			for (q = nr + 1; q <= i__2; ++q) {
			    i__3 = p + q * v_dim1;
			    d_cnjg(&z__1, &v[q + p * v_dim1]);
			    v[i__3].r = z__1.r, v[i__3].i = z__1.i;
/* L1102: */
			}
/* L1101: */
		    }
		}
		zlapmt_(&c_false, &nr, n, &v[v_offset], ldv, &iwork[1]);

		i__1 = nr;
		for (p = 1; p <= i__1; ++p) {
		    i__2 = p + p * u_dim1;
		    d_cnjg(&z__1, &u[p + p * u_dim1]);
		    u[i__2].r = z__1.r, u[i__2].i = z__1.i;
		    i__2 = nr;
		    for (q = p + 1; q <= i__2; ++q) {
			d_cnjg(&z__1, &u[q + p * u_dim1]);
			ctmp.r = z__1.r, ctmp.i = z__1.i;
			i__3 = q + p * u_dim1;
			d_cnjg(&z__1, &u[p + q * u_dim1]);
			u[i__3].r = z__1.r, u[i__3].i = z__1.i;
			i__3 = p + q * u_dim1;
			u[i__3].r = ctmp.r, u[i__3].i = ctmp.i;
/* L1118: */
		    }
/* L1117: */
		}

		if (nr < *m && ! wntuf) {
		    i__1 = *m - nr;
		    zlaset_("A", &i__1, &nr, &c_b1, &c_b1, &u[nr + 1 + u_dim1]
			    , ldu);
		    if (nr < n1) {
			i__1 = n1 - nr;
			zlaset_("A", &nr, &i__1, &c_b1, &c_b1, &u[(nr + 1) * 
				u_dim1 + 1], ldu);
			i__1 = *m - nr;
			i__2 = n1 - nr;
			zlaset_("A", &i__1, &i__2, &c_b1, &c_b2, &u[nr + 1 + (
				nr + 1) * u_dim1], ldu);
		    }
		}

	    } else {
/*            vectors of R**H */
/*               [[The optimal ratio N/NR for using QRF instead of padding */
/*                 with zeros. Here hard coded to 2; it must be at least */
/*                 two due to work space constraints.]] */
/*               OPTRATIO = ILAENV(6, 'ZGESVD', 'S' // 'O', NR,N,0,0) */
/*               OPTRATIO = MAX( OPTRATIO, 2 ) */
		optratio = 2;
		if (optratio * nr > *n) {
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			i__2 = *n;
			for (q = p; q <= i__2; ++q) {
			    i__3 = q + p * v_dim1;
			    d_cnjg(&z__1, &a[p + q * a_dim1]);
			    v[i__3].r = z__1.r, v[i__3].i = z__1.i;
/* L1199: */
			}
/* L1198: */
		    }
		    if (nr > 1) {
			i__1 = nr - 1;
			i__2 = nr - 1;
			zlaset_("U", &i__1, &i__2, &c_b1, &c_b1, &v[(v_dim1 <<
				 1) + 1], ldv);
		    }

		    i__1 = *n - nr;
		    zlaset_("A", n, &i__1, &c_b1, &c_b1, &v[(nr + 1) * v_dim1 
			    + 1], ldv);
		    i__1 = *lcwork - *n;
		    zgesvd_("O", "A", n, n, &v[v_offset], ldv, &s[1], &v[
			    v_offset], ldv, &u[u_offset], ldu, &cwork[*n + 1],
			     &i__1, &rwork[1], info);

		    i__1 = *n;
		    for (p = 1; p <= i__1; ++p) {
			i__2 = p + p * v_dim1;
			d_cnjg(&z__1, &v[p + p * v_dim1]);
			v[i__2].r = z__1.r, v[i__2].i = z__1.i;
			i__2 = *n;
			for (q = p + 1; q <= i__2; ++q) {
			    d_cnjg(&z__1, &v[q + p * v_dim1]);
			    ctmp.r = z__1.r, ctmp.i = z__1.i;
			    i__3 = q + p * v_dim1;
			    d_cnjg(&z__1, &v[p + q * v_dim1]);
			    v[i__3].r = z__1.r, v[i__3].i = z__1.i;
			    i__3 = p + q * v_dim1;
			    v[i__3].r = ctmp.r, v[i__3].i = ctmp.i;
/* L1114: */
			}
/* L1113: */
		    }
		    zlapmt_(&c_false, n, n, &v[v_offset], ldv, &iwork[1]);
/*              (M x N1), i.e. (M x N) or (M x M). */

		    i__1 = *n;
		    for (p = 1; p <= i__1; ++p) {
			i__2 = p + p * u_dim1;
			d_cnjg(&z__1, &u[p + p * u_dim1]);
			u[i__2].r = z__1.r, u[i__2].i = z__1.i;
			i__2 = *n;
			for (q = p + 1; q <= i__2; ++q) {
			    d_cnjg(&z__1, &u[q + p * u_dim1]);
			    ctmp.r = z__1.r, ctmp.i = z__1.i;
			    i__3 = q + p * u_dim1;
			    d_cnjg(&z__1, &u[p + q * u_dim1]);
			    u[i__3].r = z__1.r, u[i__3].i = z__1.i;
			    i__3 = p + q * u_dim1;
			    u[i__3].r = ctmp.r, u[i__3].i = ctmp.i;
/* L1112: */
			}
/* L1111: */
		    }

		    if (*n < *m && ! wntuf) {
			i__1 = *m - *n;
			zlaset_("A", &i__1, n, &c_b1, &c_b1, &u[*n + 1 + 
				u_dim1], ldu);
			if (*n < n1) {
			    i__1 = n1 - *n;
			    zlaset_("A", n, &i__1, &c_b1, &c_b1, &u[(*n + 1) *
				     u_dim1 + 1], ldu);
			    i__1 = *m - *n;
			    i__2 = n1 - *n;
			    zlaset_("A", &i__1, &i__2, &c_b1, &c_b2, &u[*n + 
				    1 + (*n + 1) * u_dim1], ldu);
			}
		    }
		} else {
/*                  singular vectors of R */
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			i__2 = *n;
			for (q = p; q <= i__2; ++q) {
			    i__3 = q + (nr + p) * u_dim1;
			    d_cnjg(&z__1, &a[p + q * a_dim1]);
			    u[i__3].r = z__1.r, u[i__3].i = z__1.i;
/* L1197: */
			}
/* L1196: */
		    }
		    if (nr > 1) {
			i__1 = nr - 1;
			i__2 = nr - 1;
			zlaset_("U", &i__1, &i__2, &c_b1, &c_b1, &u[(nr + 2) *
				 u_dim1 + 1], ldu);
		    }
		    i__1 = *lcwork - *n - nr;
		    zgeqrf_(n, &nr, &u[(nr + 1) * u_dim1 + 1], ldu, &cwork[*n 
			    + 1], &cwork[*n + nr + 1], &i__1, &ierr);
		    i__1 = nr;
		    for (p = 1; p <= i__1; ++p) {
			i__2 = *n;
			for (q = 1; q <= i__2; ++q) {
			    i__3 = q + p * v_dim1;
			    d_cnjg(&z__1, &u[p + (nr + q) * u_dim1]);
			    v[i__3].r = z__1.r, v[i__3].i = z__1.i;
/* L1144: */
			}
/* L1143: */
		    }
		    i__1 = nr - 1;
		    i__2 = nr - 1;
		    zlaset_("U", &i__1, &i__2, &c_b1, &c_b1, &v[(v_dim1 << 1) 
			    + 1], ldv);
		    i__1 = *lcwork - *n - nr;
		    zgesvd_("S", "O", &nr, &nr, &v[v_offset], ldv, &s[1], &u[
			    u_offset], ldu, &v[v_offset], ldv, &cwork[*n + nr 
			    + 1], &i__1, &rwork[1], info);
		    i__1 = *n - nr;
		    zlaset_("A", &i__1, &nr, &c_b1, &c_b1, &v[nr + 1 + v_dim1]
			    , ldv);
		    i__1 = *n - nr;
		    zlaset_("A", &nr, &i__1, &c_b1, &c_b1, &v[(nr + 1) * 
			    v_dim1 + 1], ldv);
		    i__1 = *n - nr;
		    i__2 = *n - nr;
		    zlaset_("A", &i__1, &i__2, &c_b1, &c_b2, &v[nr + 1 + (nr 
			    + 1) * v_dim1], ldv);
		    i__1 = *lcwork - *n - nr;
		    zunmqr_("R", "C", n, n, &nr, &u[(nr + 1) * u_dim1 + 1], 
			    ldu, &cwork[*n + 1], &v[v_offset], ldv, &cwork[*n 
			    + nr + 1], &i__1, &ierr);
		    zlapmt_(&c_false, n, n, &v[v_offset], ldv, &iwork[1]);
/*                 (M x NR) or (M x N) or (M x M). */
		    if (nr < *m && ! wntuf) {
			i__1 = *m - nr;
			zlaset_("A", &i__1, &nr, &c_b1, &c_b1, &u[nr + 1 + 
				u_dim1], ldu);
			if (nr < n1) {
			    i__1 = n1 - nr;
			    zlaset_("A", &nr, &i__1, &c_b1, &c_b1, &u[(nr + 1)
				     * u_dim1 + 1], ldu);
			    i__1 = *m - nr;
			    i__2 = n1 - nr;
			    zlaset_("A", &i__1, &i__2, &c_b1, &c_b2, &u[nr + 
				    1 + (nr + 1) * u_dim1], ldu);
			}
		    }
		}
	    }

	} else {


	    if (wntvr || nr == *n) {
		zlacpy_("U", &nr, n, &a[a_offset], lda, &v[v_offset], ldv);
		if (nr > 1) {
		    i__1 = nr - 1;
		    i__2 = nr - 1;
		    zlaset_("L", &i__1, &i__2, &c_b1, &c_b1, &v[v_dim1 + 2], 
			    ldv);
		}
/*               singular vectors of R stored in [U](1:NR,1:NR) */
		i__1 = *lcwork - *n;
		zgesvd_("S", "O", &nr, n, &v[v_offset], ldv, &s[1], &u[
			u_offset], ldu, &v[v_offset], ldv, &cwork[*n + 1], &
			i__1, &rwork[1], info);
		zlapmt_(&c_false, &nr, n, &v[v_offset], ldv, &iwork[1]);
/*              (M x NR) or (M x N) or (M x M). */
		if (nr < *m && ! wntuf) {
		    i__1 = *m - nr;
		    zlaset_("A", &i__1, &nr, &c_b1, &c_b1, &u[nr + 1 + u_dim1]
			    , ldu);
		    if (nr < n1) {
			i__1 = n1 - nr;
			zlaset_("A", &nr, &i__1, &c_b1, &c_b1, &u[(nr + 1) * 
				u_dim1 + 1], ldu);
			i__1 = *m - nr;
			i__2 = n1 - nr;
			zlaset_("A", &i__1, &i__2, &c_b1, &c_b2, &u[nr + 1 + (
				nr + 1) * u_dim1], ldu);
		    }
		}

	    } else {
/*               is then N1 (N or M) */
/*               [[The optimal ratio N/NR for using LQ instead of padding */
/*                 with zeros. Here hard coded to 2; it must be at least */
/*                 two due to work space constraints.]] */
/*               OPTRATIO = ILAENV(6, 'ZGESVD', 'S' // 'O', NR,N,0,0) */
/*               OPTRATIO = MAX( OPTRATIO, 2 ) */
		optratio = 2;
		if (optratio * nr > *n) {
		    zlacpy_("U", &nr, n, &a[a_offset], lda, &v[v_offset], ldv);
		    if (nr > 1) {
			i__1 = nr - 1;
			i__2 = nr - 1;
			zlaset_("L", &i__1, &i__2, &c_b1, &c_b1, &v[v_dim1 + 
				2], ldv);
		    }
/*                 singular vectors of R stored in [U](1:NR,1:NR) */
		    i__1 = *n - nr;
		    zlaset_("A", &i__1, n, &c_b1, &c_b1, &v[nr + 1 + v_dim1], 
			    ldv);
		    i__1 = *lcwork - *n;
		    zgesvd_("S", "O", n, n, &v[v_offset], ldv, &s[1], &u[
			    u_offset], ldu, &v[v_offset], ldv, &cwork[*n + 1],
			     &i__1, &rwork[1], info);
		    zlapmt_(&c_false, n, n, &v[v_offset], ldv, &iwork[1]);
/*                 singular vectors of A. The leading N left singular vectors */
/*                 are in [U](1:N,1:N) */
/*                 (M x N1), i.e. (M x N) or (M x M). */
		    if (*n < *m && ! wntuf) {
			i__1 = *m - *n;
			zlaset_("A", &i__1, n, &c_b1, &c_b1, &u[*n + 1 + 
				u_dim1], ldu);
			if (*n < n1) {
			    i__1 = n1 - *n;
			    zlaset_("A", n, &i__1, &c_b1, &c_b1, &u[(*n + 1) *
				     u_dim1 + 1], ldu);
			    i__1 = *m - *n;
			    i__2 = n1 - *n;
			    zlaset_("A", &i__1, &i__2, &c_b1, &c_b2, &u[*n + 
				    1 + (*n + 1) * u_dim1], ldu);
			}
		    }
		} else {
		    zlacpy_("U", &nr, n, &a[a_offset], lda, &u[nr + 1 + 
			    u_dim1], ldu);
		    if (nr > 1) {
			i__1 = nr - 1;
			i__2 = nr - 1;
			zlaset_("L", &i__1, &i__2, &c_b1, &c_b1, &u[nr + 2 + 
				u_dim1], ldu);
		    }
		    i__1 = *lcwork - *n - nr;
		    zgelqf_(&nr, n, &u[nr + 1 + u_dim1], ldu, &cwork[*n + 1], 
			    &cwork[*n + nr + 1], &i__1, &ierr);
		    zlacpy_("L", &nr, &nr, &u[nr + 1 + u_dim1], ldu, &v[
			    v_offset], ldv);
		    if (nr > 1) {
			i__1 = nr - 1;
			i__2 = nr - 1;
			zlaset_("U", &i__1, &i__2, &c_b1, &c_b1, &v[(v_dim1 <<
				 1) + 1], ldv);
		    }
		    i__1 = *lcwork - *n - nr;
		    zgesvd_("S", "O", &nr, &nr, &v[v_offset], ldv, &s[1], &u[
			    u_offset], ldu, &v[v_offset], ldv, &cwork[*n + nr 
			    + 1], &i__1, &rwork[1], info);
		    i__1 = *n - nr;
		    zlaset_("A", &i__1, &nr, &c_b1, &c_b1, &v[nr + 1 + v_dim1]
			    , ldv);
		    i__1 = *n - nr;
		    zlaset_("A", &nr, &i__1, &c_b1, &c_b1, &v[(nr + 1) * 
			    v_dim1 + 1], ldv);
		    i__1 = *n - nr;
		    i__2 = *n - nr;
		    zlaset_("A", &i__1, &i__2, &c_b1, &c_b2, &v[nr + 1 + (nr 
			    + 1) * v_dim1], ldv);
		    i__1 = *lcwork - *n - nr;
		    zunmlq_("R", "N", n, n, &nr, &u[nr + 1 + u_dim1], ldu, &
			    cwork[*n + 1], &v[v_offset], ldv, &cwork[*n + nr 
			    + 1], &i__1, &ierr);
		    zlapmt_(&c_false, n, n, &v[v_offset], ldv, &iwork[1]);
/*              (M x NR) or (M x N) or (M x M). */
		    if (nr < *m && ! wntuf) {
			i__1 = *m - nr;
			zlaset_("A", &i__1, &nr, &c_b1, &c_b1, &u[nr + 1 + 
				u_dim1], ldu);
			if (nr < n1) {
			    i__1 = n1 - nr;
			    zlaset_("A", &nr, &i__1, &c_b1, &c_b1, &u[(nr + 1)
				     * u_dim1 + 1], ldu);
			    i__1 = *m - nr;
			    i__2 = n1 - nr;
			    zlaset_("A", &i__1, &i__2, &c_b1, &c_b2, &u[nr + 
				    1 + (nr + 1) * u_dim1], ldu);
			}
		    }
		}
	    }
	}

/*           The Q matrix from the first QRF is built into the left singular */
/*           vectors matrix U. */

	if (! wntuf) {
	    i__1 = *lcwork - *n;
	    zunmqr_("L", "N", m, &n1, n, &a[a_offset], lda, &cwork[1], &u[
		    u_offset], ldu, &cwork[*n + 1], &i__1, &ierr);
	}
	if (rowprm && ! wntuf) {
	    i__1 = *m - 1;
	    zlaswp_(&n1, &u[u_offset], ldu, &c__1, &i__1, &iwork[*n + 1], &
		    c_n1);
	}

/*     ... end of the "full SVD" branch */
    }

/*     Check whether some singular values are returned as zeros, e.g. */
/*     due to underflow, and update the numerical rank. */
    p = nr;
    for (q = p; q >= 1; --q) {
	if (s[q] > 0.) {
	    goto L4002;
	}
	--nr;
/* L4001: */
    }
L4002:

/*     singular values are set to zero. */
    if (nr < *n) {
	i__1 = *n - nr;
	dlaset_("G", &i__1, &c__1, &c_b74, &c_b74, &s[nr + 1], n);
    }
/*     values. */
    if (ascaled) {
	d__1 = sqrt((doublereal) (*m));
	dlascl_("G", &c__0, &c__0, &c_b87, &d__1, &nr, &c__1, &s[1], n, &ierr);
    }
    if (conda) {
	rwork[1] = sconda;
    }
    rwork[2] = (doublereal) (p - nr);
/*     exact zeros in ZGESVD() applied to the (possibly truncated) */
/*     full row rank triangular (trapezoidal) factor of A. */
    *numrank = nr;

    return;

/*     End of ZGESVDQ */

} /* zgesvdq_ */

