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
static integer c__6 = 6;
static integer c__0 = 0;
static integer c__2 = 2;
static integer c__1 = 1;
static integer c_n1 = -1;

/* > \brief <b> ZGESVDX computes the singular value decomposition (SVD) for GE matrices</b> */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZGESVDX + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgesvdx
.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgesvdx
.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgesvdx
.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*     SUBROUTINE ZGESVDX( JOBU, JOBVT, RANGE, M, N, A, LDA, VL, VU, */
/*    $                    IL, IU, NS, S, U, LDU, VT, LDVT, WORK, */
/*    $                    LWORK, RWORK, IWORK, INFO ) */


/*      CHARACTER          JOBU, JOBVT, RANGE */
/*      INTEGER            IL, INFO, IU, LDA, LDU, LDVT, LWORK, M, N, NS */
/*      DOUBLE PRECISION   VL, VU */
/*      INTEGER            IWORK( * ) */
/*      DOUBLE PRECISION   S( * ), RWORK( * ) */
/*      COMPLEX*16         A( LDA, * ), U( LDU, * ), VT( LDVT, * ), */
/*     $                   WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >  ZGESVDX computes the singular value decomposition (SVD) of a complex */
/* >  M-by-N matrix A, optionally computing the left and/or right singular */
/* >  vectors. The SVD is written */
/* > */
/* >      A = U * SIGMA * transpose(V) */
/* > */
/* >  where SIGMA is an M-by-N matrix which is zero except for its */
/* >  f2cmin(m,n) diagonal elements, U is an M-by-M unitary matrix, and */
/* >  V is an N-by-N unitary matrix.  The diagonal elements of SIGMA */
/* >  are the singular values of A; they are real and non-negative, and */
/* >  are returned in descending order.  The first f2cmin(m,n) columns of */
/* >  U and V are the left and right singular vectors of A. */
/* > */
/* >  ZGESVDX uses an eigenvalue problem for obtaining the SVD, which */
/* >  allows for the computation of a subset of singular values and */
/* >  vectors. See DBDSVDX for details. */
/* > */
/* >  Note that the routine returns V**T, not V. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOBU */
/* > \verbatim */
/* >          JOBU is CHARACTER*1 */
/* >          Specifies options for computing all or part of the matrix U: */
/* >          = 'V':  the first f2cmin(m,n) columns of U (the left singular */
/* >                  vectors) or as specified by RANGE are returned in */
/* >                  the array U; */
/* >          = 'N':  no columns of U (no left singular vectors) are */
/* >                  computed. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBVT */
/* > \verbatim */
/* >          JOBVT is CHARACTER*1 */
/* >           Specifies options for computing all or part of the matrix */
/* >           V**T: */
/* >           = 'V':  the first f2cmin(m,n) rows of V**T (the right singular */
/* >                   vectors) or as specified by RANGE are returned in */
/* >                   the array VT; */
/* >           = 'N':  no rows of V**T (no right singular vectors) are */
/* >                   computed. */
/* > \endverbatim */
/* > */
/* > \param[in] RANGE */
/* > \verbatim */
/* >          RANGE is CHARACTER*1 */
/* >          = 'A': all singular values will be found. */
/* >          = 'V': all singular values in the half-open interval (VL,VU] */
/* >                 will be found. */
/* >          = 'I': the IL-th through IU-th singular values will be found. */
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
/* >          The number of columns of the input matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX*16 array, dimension (LDA,N) */
/* >          On entry, the M-by-N matrix A. */
/* >          On exit, the contents of A are destroyed. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[in] VL */
/* > \verbatim */
/* >          VL is DOUBLE PRECISION */
/* >          If RANGE='V', the lower bound of the interval to */
/* >          be searched for singular values. VU > VL. */
/* >          Not referenced if RANGE = 'A' or 'I'. */
/* > \endverbatim */
/* > */
/* > \param[in] VU */
/* > \verbatim */
/* >          VU is DOUBLE PRECISION */
/* >          If RANGE='V', the upper bound of the interval to */
/* >          be searched for singular values. VU > VL. */
/* >          Not referenced if RANGE = 'A' or 'I'. */
/* > \endverbatim */
/* > */
/* > \param[in] IL */
/* > \verbatim */
/* >          IL is INTEGER */
/* >          If RANGE='I', the index of the */
/* >          smallest singular value to be returned. */
/* >          1 <= IL <= IU <= f2cmin(M,N), if f2cmin(M,N) > 0. */
/* >          Not referenced if RANGE = 'A' or 'V'. */
/* > \endverbatim */
/* > */
/* > \param[in] IU */
/* > \verbatim */
/* >          IU is INTEGER */
/* >          If RANGE='I', the index of the */
/* >          largest singular value to be returned. */
/* >          1 <= IL <= IU <= f2cmin(M,N), if f2cmin(M,N) > 0. */
/* >          Not referenced if RANGE = 'A' or 'V'. */
/* > \endverbatim */
/* > */
/* > \param[out] NS */
/* > \verbatim */
/* >          NS is INTEGER */
/* >          The total number of singular values found, */
/* >          0 <= NS <= f2cmin(M,N). */
/* >          If RANGE = 'A', NS = f2cmin(M,N); if RANGE = 'I', NS = IU-IL+1. */
/* > \endverbatim */
/* > */
/* > \param[out] S */
/* > \verbatim */
/* >          S is DOUBLE PRECISION array, dimension (f2cmin(M,N)) */
/* >          The singular values of A, sorted so that S(i) >= S(i+1). */
/* > \endverbatim */
/* > */
/* > \param[out] U */
/* > \verbatim */
/* >          U is COMPLEX*16 array, dimension (LDU,UCOL) */
/* >          If JOBU = 'V', U contains columns of U (the left singular */
/* >          vectors, stored columnwise) as specified by RANGE; if */
/* >          JOBU = 'N', U is not referenced. */
/* >          Note: The user must ensure that UCOL >= NS; if RANGE = 'V', */
/* >          the exact value of NS is not known in advance and an upper */
/* >          bound must be used. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU */
/* > \verbatim */
/* >          LDU is INTEGER */
/* >          The leading dimension of the array U.  LDU >= 1; if */
/* >          JOBU = 'V', LDU >= M. */
/* > \endverbatim */
/* > */
/* > \param[out] VT */
/* > \verbatim */
/* >          VT is COMPLEX*16 array, dimension (LDVT,N) */
/* >          If JOBVT = 'V', VT contains the rows of V**T (the right singular */
/* >          vectors, stored rowwise) as specified by RANGE; if JOBVT = 'N', */
/* >          VT is not referenced. */
/* >          Note: The user must ensure that LDVT >= NS; if RANGE = 'V', */
/* >          the exact value of NS is not known in advance and an upper */
/* >          bound must be used. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVT */
/* > \verbatim */
/* >          LDVT is INTEGER */
/* >          The leading dimension of the array VT.  LDVT >= 1; if */
/* >          JOBVT = 'V', LDVT >= NS (see above). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK; */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. */
/* >          LWORK >= MAX(1,MIN(M,N)*(MIN(M,N)+4)) for the paths (see */
/* >          comments inside the code): */
/* >             - PATH 1  (M much larger than N) */
/* >             - PATH 1t (N much larger than M) */
/* >          LWORK >= MAX(1,MIN(M,N)*2+MAX(M,N)) for the other paths. */
/* >          For good performance, LWORK should generally be larger. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is DOUBLE PRECISION array, dimension (MAX(1,LRWORK)) */
/* >          LRWORK >= MIN(M,N)*(MIN(M,N)*2+15*MIN(M,N)). */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (12*MIN(M,N)) */
/* >          If INFO = 0, the first NS elements of IWORK are zero. If INFO > 0, */
/* >          then IWORK contains the indices of the eigenvectors that failed */
/* >          to converge in DBDSVDX/DSTEVX. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >     INFO is INTEGER */
/* >           = 0:  successful exit */
/* >           < 0:  if INFO = -i, the i-th argument had an illegal value */
/* >           > 0:  if INFO = i, then i eigenvectors failed to converge */
/* >                 in DBDSVDX/DSTEVX. */
/* >                 if INFO = N*2 + 1, an internal error occurred in */
/* >                 DBDSVDX */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup complex16GEsing */

/*  ===================================================================== */
/* Subroutine */ void zgesvdx_(char *jobu, char *jobvt, char *range, integer *
	m, integer *n, doublecomplex *a, integer *lda, doublereal *vl, 
	doublereal *vu, integer *il, integer *iu, integer *ns, doublereal *s, 
	doublecomplex *u, integer *ldu, doublecomplex *vt, integer *ldvt, 
	doublecomplex *work, integer *lwork, doublereal *rwork, integer *
	iwork, integer *info)
{
    /* System generated locals */
    address a__1[2];
    integer a_dim1, a_offset, u_dim1, u_offset, vt_dim1, vt_offset, i__1[2], 
	    i__2, i__3, i__4, i__5;
    doublereal d__1;
    doublecomplex z__1;
    char ch__1[2];

    /* Local variables */
    integer iscl;
    logical alls, inds;
    integer ilqf;
    doublereal anrm;
    integer ierr, iqrf, itau;
    char jobz[1];
    logical vals;
    integer i__, j, k;
    extern logical lsame_(char *, char *);
    integer iltgk, itemp, minmn, itaup, itauq, iutgk, itgkz, mnthr;
    logical wantu;
    integer id, ie;
    extern doublereal dlamch_(char *);
    extern /* Subroutine */ void dlascl_(char *, integer *, integer *, 
	    doublereal *, doublereal *, integer *, integer *, doublereal *, 
	    integer *, integer *), zgebrd_(integer *, integer *, 
	    doublecomplex *, integer *, doublereal *, doublereal *, 
	    doublecomplex *, doublecomplex *, doublecomplex *, integer *, 
	    integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    extern doublereal zlange_(char *, integer *, integer *, doublecomplex *, 
	    integer *, doublereal *);
    doublereal bignum, abstol;
    extern /* Subroutine */ void zgelqf_(integer *, integer *, doublecomplex *,
	     integer *, doublecomplex *, doublecomplex *, integer *, integer *
	    ), zlascl_(char *, integer *, integer *, doublereal *, doublereal 
	    *, integer *, integer *, doublecomplex *, integer *, integer *);
    char rngtgk[1];
    extern /* Subroutine */ void zgeqrf_(integer *, integer *, doublecomplex *,
	     integer *, doublecomplex *, doublecomplex *, integer *, integer *
	    );
    integer itempr;
    extern /* Subroutine */ void zlaset_(char *, integer *, integer *, 
	    doublecomplex *, doublecomplex *, doublecomplex *, integer *), zlacpy_(char *, integer *, integer *, doublecomplex *, 
	    integer *, doublecomplex *, integer *);
    integer minwrk, maxwrk;
    doublereal smlnum;
    extern /* Subroutine */ void zunmbr_(char *, char *, char *, integer *, 
	    integer *, integer *, doublecomplex *, integer *, doublecomplex *,
	     doublecomplex *, integer *, doublecomplex *, integer *, integer *
	    );
    logical lquery, wantvt;
    extern /* Subroutine */ void zunmlq_(char *, char *, integer *, integer *, 
	    integer *, doublecomplex *, integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, integer *), zunmqr_(char *, char *, integer *, integer *, 
	    integer *, doublecomplex *, integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, integer *);
    doublereal dum[1], eps;
    extern /* Subroutine */ void dbdsvdx_(char *, char *, char *, integer *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, integer *,
	     integer *, integer *, doublereal *, doublereal *, integer *, 
	    doublereal *, integer *, integer *);


/*  -- LAPACK driver routine (version 3.8.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */


/*  ===================================================================== */


/*     Test the input arguments. */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --s;
    u_dim1 = *ldu;
    u_offset = 1 + u_dim1 * 1;
    u -= u_offset;
    vt_dim1 = *ldvt;
    vt_offset = 1 + vt_dim1 * 1;
    vt -= vt_offset;
    --work;
    --rwork;
    --iwork;

    /* Function Body */
    *ns = 0;
    *info = 0;
    abstol = dlamch_("S") * 2;
    lquery = *lwork == -1;
    minmn = f2cmin(*m,*n);
    wantu = lsame_(jobu, "V");
    wantvt = lsame_(jobvt, "V");
    if (wantu || wantvt) {
	*(unsigned char *)jobz = 'V';
    } else {
	*(unsigned char *)jobz = 'N';
    }
    alls = lsame_(range, "A");
    vals = lsame_(range, "V");
    inds = lsame_(range, "I");

    *info = 0;
    if (! lsame_(jobu, "V") && ! lsame_(jobu, "N")) {
	*info = -1;
    } else if (! lsame_(jobvt, "V") && ! lsame_(jobvt, 
	    "N")) {
	*info = -2;
    } else if (! (alls || vals || inds)) {
	*info = -3;
    } else if (*m < 0) {
	*info = -4;
    } else if (*n < 0) {
	*info = -5;
    } else if (*m > *lda) {
	*info = -7;
    } else if (minmn > 0) {
	if (vals) {
	    if (*vl < 0.) {
		*info = -8;
	    } else if (*vu <= *vl) {
		*info = -9;
	    }
	} else if (inds) {
	    if (*il < 1 || *il > f2cmax(1,minmn)) {
		*info = -10;
	    } else if (*iu < f2cmin(minmn,*il) || *iu > minmn) {
		*info = -11;
	    }
	}
	if (*info == 0) {
	    if (wantu && *ldu < *m) {
		*info = -15;
	    } else if (wantvt) {
		if (inds) {
		    if (*ldvt < *iu - *il + 1) {
			*info = -17;
		    }
		} else if (*ldvt < minmn) {
		    *info = -17;
		}
	    }
	}
    }

/*     Compute workspace */
/*     (Note: Comments in the code beginning "Workspace:" describe the */
/*     minimal amount of workspace needed at that point in the code, */
/*     as well as the preferred amount for good performance. */
/*     NB refers to the optimal block size for the immediately */
/*     following subroutine, as returned by ILAENV.) */

    if (*info == 0) {
	minwrk = 1;
	maxwrk = 1;
	if (minmn > 0) {
	    if (*m >= *n) {
/* Writing concatenation */
		i__1[0] = 1, a__1[0] = jobu;
		i__1[1] = 1, a__1[1] = jobvt;
		s_cat(ch__1, a__1, i__1, &c__2, (ftnlen)2);
		mnthr = ilaenv_(&c__6, "ZGESVD", ch__1, m, n, &c__0, &c__0, (
			ftnlen)6, (ftnlen)2);
		if (*m >= mnthr) {

/*                 Path 1 (M much larger than N) */

		    minwrk = *n * (*n + 5);
		    maxwrk = *n + *n * ilaenv_(&c__1, "ZGEQRF", " ", m, n, &
			    c_n1, &c_n1, (ftnlen)6, (ftnlen)1);
/* Computing MAX */
		    i__2 = maxwrk, i__3 = *n * *n + (*n << 1) + (*n << 1) * 
			    ilaenv_(&c__1, "ZGEBRD", " ", n, n, &c_n1, &c_n1, 
			    (ftnlen)6, (ftnlen)1);
		    maxwrk = f2cmax(i__2,i__3);
		    if (wantu || wantvt) {
/* Computing MAX */
			i__2 = maxwrk, i__3 = *n * *n + (*n << 1) + *n * 
				ilaenv_(&c__1, "ZUNMQR", "LN", n, n, n, &c_n1,
				 (ftnlen)6, (ftnlen)2);
			maxwrk = f2cmax(i__2,i__3);
		    }
		} else {

/*                 Path 2 (M at least N, but not much larger) */

		    minwrk = *n * 3 + *m;
		    maxwrk = (*n << 1) + (*m + *n) * ilaenv_(&c__1, "ZGEBRD", 
			    " ", m, n, &c_n1, &c_n1, (ftnlen)6, (ftnlen)1);
		    if (wantu || wantvt) {
/* Computing MAX */
			i__2 = maxwrk, i__3 = (*n << 1) + *n * ilaenv_(&c__1, 
				"ZUNMQR", "LN", n, n, n, &c_n1, (ftnlen)6, (
				ftnlen)2);
			maxwrk = f2cmax(i__2,i__3);
		    }
		}
	    } else {
/* Writing concatenation */
		i__1[0] = 1, a__1[0] = jobu;
		i__1[1] = 1, a__1[1] = jobvt;
		s_cat(ch__1, a__1, i__1, &c__2, (ftnlen)2);
		mnthr = ilaenv_(&c__6, "ZGESVD", ch__1, m, n, &c__0, &c__0, (
			ftnlen)6, (ftnlen)2);
		if (*n >= mnthr) {

/*                 Path 1t (N much larger than M) */

		    minwrk = *m * (*m + 5);
		    maxwrk = *m + *m * ilaenv_(&c__1, "ZGELQF", " ", m, n, &
			    c_n1, &c_n1, (ftnlen)6, (ftnlen)1);
/* Computing MAX */
		    i__2 = maxwrk, i__3 = *m * *m + (*m << 1) + (*m << 1) * 
			    ilaenv_(&c__1, "ZGEBRD", " ", m, m, &c_n1, &c_n1, 
			    (ftnlen)6, (ftnlen)1);
		    maxwrk = f2cmax(i__2,i__3);
		    if (wantu || wantvt) {
/* Computing MAX */
			i__2 = maxwrk, i__3 = *m * *m + (*m << 1) + *m * 
				ilaenv_(&c__1, "ZUNMQR", "LN", m, m, m, &c_n1,
				 (ftnlen)6, (ftnlen)2);
			maxwrk = f2cmax(i__2,i__3);
		    }
		} else {

/*                 Path 2t (N greater than M, but not much larger) */


		    minwrk = *m * 3 + *n;
		    maxwrk = (*m << 1) + (*m + *n) * ilaenv_(&c__1, "ZGEBRD", 
			    " ", m, n, &c_n1, &c_n1, (ftnlen)6, (ftnlen)1);
		    if (wantu || wantvt) {
/* Computing MAX */
			i__2 = maxwrk, i__3 = (*m << 1) + *m * ilaenv_(&c__1, 
				"ZUNMQR", "LN", m, m, m, &c_n1, (ftnlen)6, (
				ftnlen)2);
			maxwrk = f2cmax(i__2,i__3);
		    }
		}
	    }
	}
	maxwrk = f2cmax(maxwrk,minwrk);
	d__1 = (doublereal) maxwrk;
	z__1.r = d__1, z__1.i = 0.;
	work[1].r = z__1.r, work[1].i = z__1.i;

	if (*lwork < minwrk && ! lquery) {
	    *info = -19;
	}
    }

    if (*info != 0) {
	i__2 = -(*info);
	xerbla_("ZGESVDX", &i__2, (ftnlen)7);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*m == 0 || *n == 0) {
	return;
    }

/*     Set singular values indices accord to RANGE='A'. */

    if (alls) {
	*(unsigned char *)rngtgk = 'I';
	iltgk = 1;
	iutgk = f2cmin(*m,*n);
    } else if (inds) {
	*(unsigned char *)rngtgk = 'I';
	iltgk = *il;
	iutgk = *iu;
    } else {
	*(unsigned char *)rngtgk = 'V';
	iltgk = 0;
	iutgk = 0;
    }

/*     Get machine constants */

    eps = dlamch_("P");
    smlnum = sqrt(dlamch_("S")) / eps;
    bignum = 1. / smlnum;

/*     Scale A if f2cmax element outside range [SMLNUM,BIGNUM] */

    anrm = zlange_("M", m, n, &a[a_offset], lda, dum);
    iscl = 0;
    if (anrm > 0. && anrm < smlnum) {
	iscl = 1;
	zlascl_("G", &c__0, &c__0, &anrm, &smlnum, m, n, &a[a_offset], lda, 
		info);
    } else if (anrm > bignum) {
	iscl = 1;
	zlascl_("G", &c__0, &c__0, &anrm, &bignum, m, n, &a[a_offset], lda, 
		info);
    }

    if (*m >= *n) {

/*        A has at least as many rows as columns. If A has sufficiently */
/*        more rows than columns, first reduce A using the QR */
/*        decomposition. */

	if (*m >= mnthr) {

/*           Path 1 (M much larger than N): */
/*           A = Q * R = Q * ( QB * B * PB**T ) */
/*                     = Q * ( QB * ( UB * S * VB**T ) * PB**T ) */
/*           U = Q * QB * UB; V**T = VB**T * PB**T */

/*           Compute A=Q*R */
/*           (Workspace: need 2*N, prefer N+N*NB) */

	    itau = 1;
	    itemp = itau + *n;
	    i__2 = *lwork - itemp + 1;
	    zgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[itemp], &i__2,
		     info);

/*           Copy R into WORK and bidiagonalize it: */
/*           (Workspace: need N*N+3*N, prefer N*N+N+2*N*NB) */

	    iqrf = itemp;
	    itauq = itemp + *n * *n;
	    itaup = itauq + *n;
	    itemp = itaup + *n;
	    id = 1;
	    ie = id + *n;
	    itgkz = ie + *n;
	    zlacpy_("U", n, n, &a[a_offset], lda, &work[iqrf], n);
	    i__2 = *n - 1;
	    i__3 = *n - 1;
	    zlaset_("L", &i__2, &i__3, &c_b1, &c_b1, &work[iqrf + 1], n);
	    i__2 = *lwork - itemp + 1;
	    zgebrd_(n, n, &work[iqrf], n, &rwork[id], &rwork[ie], &work[itauq]
		    , &work[itaup], &work[itemp], &i__2, info);
	    itempr = itgkz + *n * ((*n << 1) + 1);

/*           Solve eigenvalue problem TGK*Z=Z*S. */
/*           (Workspace: need 2*N*N+14*N) */

	    i__2 = *n << 1;
	    dbdsvdx_("U", jobz, rngtgk, n, &rwork[id], &rwork[ie], vl, vu, &
		    iltgk, &iutgk, ns, &s[1], &rwork[itgkz], &i__2, &rwork[
		    itempr], &iwork[1], info)
		    ;

/*           If needed, compute left singular vectors. */

	    if (wantu) {
		k = itgkz;
		i__2 = *ns;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    i__3 = *n;
		    for (j = 1; j <= i__3; ++j) {
			i__4 = j + i__ * u_dim1;
			i__5 = k;
			z__1.r = rwork[i__5], z__1.i = 0.;
			u[i__4].r = z__1.r, u[i__4].i = z__1.i;
			++k;
		    }
		    k += *n;
		}
		i__2 = *m - *n;
		zlaset_("A", &i__2, ns, &c_b1, &c_b1, &u[*n + 1 + u_dim1], 
			ldu);

/*              Call ZUNMBR to compute QB*UB. */
/*              (Workspace in WORK( ITEMP ): need N, prefer N*NB) */

		i__2 = *lwork - itemp + 1;
		zunmbr_("Q", "L", "N", n, ns, n, &work[iqrf], n, &work[itauq],
			 &u[u_offset], ldu, &work[itemp], &i__2, info);

/*              Call ZUNMQR to compute Q*(QB*UB). */
/*              (Workspace in WORK( ITEMP ): need N, prefer N*NB) */

		i__2 = *lwork - itemp + 1;
		zunmqr_("L", "N", m, ns, n, &a[a_offset], lda, &work[itau], &
			u[u_offset], ldu, &work[itemp], &i__2, info);
	    }

/*           If needed, compute right singular vectors. */

	    if (wantvt) {
		k = itgkz + *n;
		i__2 = *ns;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    i__3 = *n;
		    for (j = 1; j <= i__3; ++j) {
			i__4 = i__ + j * vt_dim1;
			i__5 = k;
			z__1.r = rwork[i__5], z__1.i = 0.;
			vt[i__4].r = z__1.r, vt[i__4].i = z__1.i;
			++k;
		    }
		    k += *n;
		}

/*              Call ZUNMBR to compute VB**T * PB**T */
/*              (Workspace in WORK( ITEMP ): need N, prefer N*NB) */

		i__2 = *lwork - itemp + 1;
		zunmbr_("P", "R", "C", ns, n, n, &work[iqrf], n, &work[itaup],
			 &vt[vt_offset], ldvt, &work[itemp], &i__2, info);
	    }
	} else {

/*           Path 2 (M at least N, but not much larger) */
/*           Reduce A to bidiagonal form without QR decomposition */
/*           A = QB * B * PB**T = QB * ( UB * S * VB**T ) * PB**T */
/*           U = QB * UB; V**T = VB**T * PB**T */

/*           Bidiagonalize A */
/*           (Workspace: need 2*N+M, prefer 2*N+(M+N)*NB) */

	    itauq = 1;
	    itaup = itauq + *n;
	    itemp = itaup + *n;
	    id = 1;
	    ie = id + *n;
	    itgkz = ie + *n;
	    i__2 = *lwork - itemp + 1;
	    zgebrd_(m, n, &a[a_offset], lda, &rwork[id], &rwork[ie], &work[
		    itauq], &work[itaup], &work[itemp], &i__2, info);
	    itempr = itgkz + *n * ((*n << 1) + 1);

/*           Solve eigenvalue problem TGK*Z=Z*S. */
/*           (Workspace: need 2*N*N+14*N) */

	    i__2 = *n << 1;
	    dbdsvdx_("U", jobz, rngtgk, n, &rwork[id], &rwork[ie], vl, vu, &
		    iltgk, &iutgk, ns, &s[1], &rwork[itgkz], &i__2, &rwork[
		    itempr], &iwork[1], info)
		    ;

/*           If needed, compute left singular vectors. */

	    if (wantu) {
		k = itgkz;
		i__2 = *ns;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    i__3 = *n;
		    for (j = 1; j <= i__3; ++j) {
			i__4 = j + i__ * u_dim1;
			i__5 = k;
			z__1.r = rwork[i__5], z__1.i = 0.;
			u[i__4].r = z__1.r, u[i__4].i = z__1.i;
			++k;
		    }
		    k += *n;
		}
		i__2 = *m - *n;
		zlaset_("A", &i__2, ns, &c_b1, &c_b1, &u[*n + 1 + u_dim1], 
			ldu);

/*              Call ZUNMBR to compute QB*UB. */
/*              (Workspace in WORK( ITEMP ): need N, prefer N*NB) */

		i__2 = *lwork - itemp + 1;
		zunmbr_("Q", "L", "N", m, ns, n, &a[a_offset], lda, &work[
			itauq], &u[u_offset], ldu, &work[itemp], &i__2, &ierr);
	    }

/*           If needed, compute right singular vectors. */

	    if (wantvt) {
		k = itgkz + *n;
		i__2 = *ns;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    i__3 = *n;
		    for (j = 1; j <= i__3; ++j) {
			i__4 = i__ + j * vt_dim1;
			i__5 = k;
			z__1.r = rwork[i__5], z__1.i = 0.;
			vt[i__4].r = z__1.r, vt[i__4].i = z__1.i;
			++k;
		    }
		    k += *n;
		}

/*              Call ZUNMBR to compute VB**T * PB**T */
/*              (Workspace in WORK( ITEMP ): need N, prefer N*NB) */

		i__2 = *lwork - itemp + 1;
		zunmbr_("P", "R", "C", ns, n, n, &a[a_offset], lda, &work[
			itaup], &vt[vt_offset], ldvt, &work[itemp], &i__2, &
			ierr);
	    }
	}
    } else {

/*        A has more columns than rows. If A has sufficiently more */
/*        columns than rows, first reduce A using the LQ decomposition. */

	if (*n >= mnthr) {

/*           Path 1t (N much larger than M): */
/*           A = L * Q = ( QB * B * PB**T ) * Q */
/*                     = ( QB * ( UB * S * VB**T ) * PB**T ) * Q */
/*           U = QB * UB ; V**T = VB**T * PB**T * Q */

/*           Compute A=L*Q */
/*           (Workspace: need 2*M, prefer M+M*NB) */

	    itau = 1;
	    itemp = itau + *m;
	    i__2 = *lwork - itemp + 1;
	    zgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[itemp], &i__2,
		     info);
/*           Copy L into WORK and bidiagonalize it: */
/*           (Workspace in WORK( ITEMP ): need M*M+3*M, prefer M*M+M+2*M*NB) */

	    ilqf = itemp;
	    itauq = ilqf + *m * *m;
	    itaup = itauq + *m;
	    itemp = itaup + *m;
	    id = 1;
	    ie = id + *m;
	    itgkz = ie + *m;
	    zlacpy_("L", m, m, &a[a_offset], lda, &work[ilqf], m);
	    i__2 = *m - 1;
	    i__3 = *m - 1;
	    zlaset_("U", &i__2, &i__3, &c_b1, &c_b1, &work[ilqf + *m], m);
	    i__2 = *lwork - itemp + 1;
	    zgebrd_(m, m, &work[ilqf], m, &rwork[id], &rwork[ie], &work[itauq]
		    , &work[itaup], &work[itemp], &i__2, info);
	    itempr = itgkz + *m * ((*m << 1) + 1);

/*           Solve eigenvalue problem TGK*Z=Z*S. */
/*           (Workspace: need 2*M*M+14*M) */

	    i__2 = *m << 1;
	    dbdsvdx_("U", jobz, rngtgk, m, &rwork[id], &rwork[ie], vl, vu, &
		    iltgk, &iutgk, ns, &s[1], &rwork[itgkz], &i__2, &rwork[
		    itempr], &iwork[1], info)
		    ;

/*           If needed, compute left singular vectors. */

	    if (wantu) {
		k = itgkz;
		i__2 = *ns;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    i__3 = *m;
		    for (j = 1; j <= i__3; ++j) {
			i__4 = j + i__ * u_dim1;
			i__5 = k;
			z__1.r = rwork[i__5], z__1.i = 0.;
			u[i__4].r = z__1.r, u[i__4].i = z__1.i;
			++k;
		    }
		    k += *m;
		}

/*              Call ZUNMBR to compute QB*UB. */
/*              (Workspace in WORK( ITEMP ): need M, prefer M*NB) */

		i__2 = *lwork - itemp + 1;
		zunmbr_("Q", "L", "N", m, ns, m, &work[ilqf], m, &work[itauq],
			 &u[u_offset], ldu, &work[itemp], &i__2, info);
	    }

/*           If needed, compute right singular vectors. */

	    if (wantvt) {
		k = itgkz + *m;
		i__2 = *ns;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    i__3 = *m;
		    for (j = 1; j <= i__3; ++j) {
			i__4 = i__ + j * vt_dim1;
			i__5 = k;
			z__1.r = rwork[i__5], z__1.i = 0.;
			vt[i__4].r = z__1.r, vt[i__4].i = z__1.i;
			++k;
		    }
		    k += *m;
		}
		i__2 = *n - *m;
		zlaset_("A", ns, &i__2, &c_b1, &c_b1, &vt[(*m + 1) * vt_dim1 
			+ 1], ldvt);

/*              Call ZUNMBR to compute (VB**T)*(PB**T) */
/*              (Workspace in WORK( ITEMP ): need M, prefer M*NB) */

		i__2 = *lwork - itemp + 1;
		zunmbr_("P", "R", "C", ns, m, m, &work[ilqf], m, &work[itaup],
			 &vt[vt_offset], ldvt, &work[itemp], &i__2, info);

/*              Call ZUNMLQ to compute ((VB**T)*(PB**T))*Q. */
/*              (Workspace in WORK( ITEMP ): need M, prefer M*NB) */

		i__2 = *lwork - itemp + 1;
		zunmlq_("R", "N", ns, n, m, &a[a_offset], lda, &work[itau], &
			vt[vt_offset], ldvt, &work[itemp], &i__2, info);
	    }
	} else {

/*           Path 2t (N greater than M, but not much larger) */
/*           Reduce to bidiagonal form without LQ decomposition */
/*           A = QB * B * PB**T = QB * ( UB * S * VB**T ) * PB**T */
/*           U = QB * UB; V**T = VB**T * PB**T */

/*           Bidiagonalize A */
/*           (Workspace: need 2*M+N, prefer 2*M+(M+N)*NB) */

	    itauq = 1;
	    itaup = itauq + *m;
	    itemp = itaup + *m;
	    id = 1;
	    ie = id + *m;
	    itgkz = ie + *m;
	    i__2 = *lwork - itemp + 1;
	    zgebrd_(m, n, &a[a_offset], lda, &rwork[id], &rwork[ie], &work[
		    itauq], &work[itaup], &work[itemp], &i__2, info);
	    itempr = itgkz + *m * ((*m << 1) + 1);

/*           Solve eigenvalue problem TGK*Z=Z*S. */
/*           (Workspace: need 2*M*M+14*M) */

	    i__2 = *m << 1;
	    dbdsvdx_("L", jobz, rngtgk, m, &rwork[id], &rwork[ie], vl, vu, &
		    iltgk, &iutgk, ns, &s[1], &rwork[itgkz], &i__2, &rwork[
		    itempr], &iwork[1], info)
		    ;

/*           If needed, compute left singular vectors. */

	    if (wantu) {
		k = itgkz;
		i__2 = *ns;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    i__3 = *m;
		    for (j = 1; j <= i__3; ++j) {
			i__4 = j + i__ * u_dim1;
			i__5 = k;
			z__1.r = rwork[i__5], z__1.i = 0.;
			u[i__4].r = z__1.r, u[i__4].i = z__1.i;
			++k;
		    }
		    k += *m;
		}

/*              Call ZUNMBR to compute QB*UB. */
/*              (Workspace in WORK( ITEMP ): need M, prefer M*NB) */

		i__2 = *lwork - itemp + 1;
		zunmbr_("Q", "L", "N", m, ns, n, &a[a_offset], lda, &work[
			itauq], &u[u_offset], ldu, &work[itemp], &i__2, info);
	    }

/*           If needed, compute right singular vectors. */

	    if (wantvt) {
		k = itgkz + *m;
		i__2 = *ns;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    i__3 = *m;
		    for (j = 1; j <= i__3; ++j) {
			i__4 = i__ + j * vt_dim1;
			i__5 = k;
			z__1.r = rwork[i__5], z__1.i = 0.;
			vt[i__4].r = z__1.r, vt[i__4].i = z__1.i;
			++k;
		    }
		    k += *m;
		}
		i__2 = *n - *m;
		zlaset_("A", ns, &i__2, &c_b1, &c_b1, &vt[(*m + 1) * vt_dim1 
			+ 1], ldvt);

/*              Call ZUNMBR to compute VB**T * PB**T */
/*              (Workspace in WORK( ITEMP ): need M, prefer M*NB) */

		i__2 = *lwork - itemp + 1;
		zunmbr_("P", "R", "C", ns, n, m, &a[a_offset], lda, &work[
			itaup], &vt[vt_offset], ldvt, &work[itemp], &i__2, 
			info);
	    }
	}
    }

/*     Undo scaling if necessary */

    if (iscl == 1) {
	if (anrm > bignum) {
	    dlascl_("G", &c__0, &c__0, &bignum, &anrm, &minmn, &c__1, &s[1], &
		    minmn, info);
	}
	if (anrm < smlnum) {
	    dlascl_("G", &c__0, &c__0, &smlnum, &anrm, &minmn, &c__1, &s[1], &
		    minmn, info);
	}
    }

/*     Return optimal workspace in WORK(1) */

    d__1 = (doublereal) maxwrk;
    z__1.r = d__1, z__1.i = 0.;
    work[1].r = z__1.r, work[1].i = z__1.i;

    return;

/*     End of ZGESVDX */

} /* zgesvdx_ */

