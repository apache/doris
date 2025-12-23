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

static integer c__4 = 4;
static doublereal c_b5 = 0.;
static integer c__1 = 1;
static integer c__2 = 2;
static doublereal c_b42 = 1.;
static doublereal c_b48 = -1.;
static integer c__0 = 0;

/* > \brief \b DTGEX2 swaps adjacent diagonal blocks in an upper (quasi) triangular matrix pair by an orthogon
al equivalence transformation. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DTGEX2 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtgex2.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtgex2.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtgex2.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DTGEX2( WANTQ, WANTZ, N, A, LDA, B, LDB, Q, LDQ, Z, */
/*                          LDZ, J1, N1, N2, WORK, LWORK, INFO ) */

/*       LOGICAL            WANTQ, WANTZ */
/*       INTEGER            INFO, J1, LDA, LDB, LDQ, LDZ, LWORK, N, N1, N2 */
/*       DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), Q( LDQ, * ), */
/*      $                   WORK( * ), Z( LDZ, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DTGEX2 swaps adjacent diagonal blocks (A11, B11) and (A22, B22) */
/* > of size 1-by-1 or 2-by-2 in an upper (quasi) triangular matrix pair */
/* > (A, B) by an orthogonal equivalence transformation. */
/* > */
/* > (A, B) must be in generalized real Schur canonical form (as returned */
/* > by DGGES), i.e. A is block upper triangular with 1-by-1 and 2-by-2 */
/* > diagonal blocks. B is upper triangular. */
/* > */
/* > Optionally, the matrices Q and Z of generalized Schur vectors are */
/* > updated. */
/* > */
/* >        Q(in) * A(in) * Z(in)**T = Q(out) * A(out) * Z(out)**T */
/* >        Q(in) * B(in) * Z(in)**T = Q(out) * B(out) * Z(out)**T */
/* > */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] WANTQ */
/* > \verbatim */
/* >          WANTQ is LOGICAL */
/* >          .TRUE. : update the left transformation matrix Q; */
/* >          .FALSE.: do not update Q. */
/* > \endverbatim */
/* > */
/* > \param[in] WANTZ */
/* > \verbatim */
/* >          WANTZ is LOGICAL */
/* >          .TRUE. : update the right transformation matrix Z; */
/* >          .FALSE.: do not update Z. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices A and B. N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is DOUBLE PRECISION array, dimensions (LDA,N) */
/* >          On entry, the matrix A in the pair (A, B). */
/* >          On exit, the updated matrix A. */
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
/* >          B is DOUBLE PRECISION array, dimensions (LDB,N) */
/* >          On entry, the matrix B in the pair (A, B). */
/* >          On exit, the updated matrix B. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B. LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] Q */
/* > \verbatim */
/* >          Q is DOUBLE PRECISION array, dimension (LDQ,N) */
/* >          On entry, if WANTQ = .TRUE., the orthogonal matrix Q. */
/* >          On exit, the updated matrix Q. */
/* >          Not referenced if WANTQ = .FALSE.. */
/* > \endverbatim */
/* > */
/* > \param[in] LDQ */
/* > \verbatim */
/* >          LDQ is INTEGER */
/* >          The leading dimension of the array Q. LDQ >= 1. */
/* >          If WANTQ = .TRUE., LDQ >= N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Z */
/* > \verbatim */
/* >          Z is DOUBLE PRECISION array, dimension (LDZ,N) */
/* >          On entry, if WANTZ =.TRUE., the orthogonal matrix Z. */
/* >          On exit, the updated matrix Z. */
/* >          Not referenced if WANTZ = .FALSE.. */
/* > \endverbatim */
/* > */
/* > \param[in] LDZ */
/* > \verbatim */
/* >          LDZ is INTEGER */
/* >          The leading dimension of the array Z. LDZ >= 1. */
/* >          If WANTZ = .TRUE., LDZ >= N. */
/* > \endverbatim */
/* > */
/* > \param[in] J1 */
/* > \verbatim */
/* >          J1 is INTEGER */
/* >          The index to the first block (A11, B11). 1 <= J1 <= N. */
/* > \endverbatim */
/* > */
/* > \param[in] N1 */
/* > \verbatim */
/* >          N1 is INTEGER */
/* >          The order of the first block (A11, B11). N1 = 0, 1 or 2. */
/* > \endverbatim */
/* > */
/* > \param[in] N2 */
/* > \verbatim */
/* >          N2 is INTEGER */
/* >          The order of the second block (A22, B22). N2 = 0, 1 or 2. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension (MAX(1,LWORK)). */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. */
/* >          LWORK >=  MAX( 1, N*(N2+N1), (N2+N1)*(N2+N1)*2 ) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >            =0: Successful exit */
/* >            >0: If INFO = 1, the transformed matrix (A, B) would be */
/* >                too far from generalized Schur form; the blocks are */
/* >                not swapped and (A, B) and (Q, Z) are unchanged. */
/* >                The problem of swapping is too ill-conditioned. */
/* >            <0: If INFO = -16: LWORK is too small. Appropriate value */
/* >                for LWORK is returned in WORK(1). */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleGEauxiliary */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* >  In the current code both weak and strong stability tests are */
/* >  performed. The user can omit the strong stability test by changing */
/* >  the internal logical parameter WANDS to .FALSE.. See ref. [2] for */
/* >  details. */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >     Bo Kagstrom and Peter Poromaa, Department of Computing Science, */
/* >     Umea University, S-901 87 Umea, Sweden. */

/* > \par References: */
/*  ================ */
/* > */
/* > \verbatim */
/* > */
/* >  [1] B. Kagstrom; A Direct Method for Reordering Eigenvalues in the */
/* >      Generalized Real Schur Form of a Regular Matrix Pair (A, B), in */
/* >      M.S. Moonen et al (eds), Linear Algebra for Large Scale and */
/* >      Real-Time Applications, Kluwer Academic Publ. 1993, pp 195-218. */
/* > */
/* >  [2] B. Kagstrom and P. Poromaa; Computing Eigenspaces with Specified */
/* >      Eigenvalues of a Regular Matrix Pair (A, B) and Condition */
/* >      Estimation: Theory, Algorithms and Software, */
/* >      Report UMINF - 94.04, Department of Computing Science, Umea */
/* >      University, S-901 87 Umea, Sweden, 1994. Also as LAPACK Working */
/* >      Note 87. To appear in Numerical Algorithms, 1996. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void dtgex2_(logical *wantq, logical *wantz, integer *n, 
	doublereal *a, integer *lda, doublereal *b, integer *ldb, doublereal *
	q, integer *ldq, doublereal *z__, integer *ldz, integer *j1, integer *
	n1, integer *n2, doublereal *work, integer *lwork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, q_dim1, q_offset, z_dim1, 
	    z_offset, i__1, i__2;
    doublereal d__1;

    /* Local variables */
    logical weak;
    doublereal ddum;
    integer idum;
    doublereal taul[4], dsum;
    extern /* Subroutine */ void drot_(integer *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *);
    doublereal taur[4], scpy[16]	/* was [4][4] */, tcpy[16]	/* 
	    was [4][4] */, f, g;
    integer i__, m;
    doublereal s[16]	/* was [4][4] */, t[16]	/* was [4][4] */;
    extern /* Subroutine */ void dscal_(integer *, doublereal *, doublereal *, 
	    integer *);
    doublereal scale, bqra21, brqa21;
    extern /* Subroutine */ void dgemm_(char *, char *, integer *, integer *, 
	    integer *, doublereal *, doublereal *, integer *, doublereal *, 
	    integer *, doublereal *, doublereal *, integer *);
    doublereal licop[16]	/* was [4][4] */;
    integer linfo;
    doublereal ircop[16]	/* was [4][4] */, dnorm;
    integer iwork[4];
    extern /* Subroutine */ void dlagv2_(doublereal *, integer *, doublereal *,
	     integer *, doublereal *, doublereal *, doublereal *, doublereal *
	    , doublereal *, doublereal *, doublereal *), dgeqr2_(integer *, 
	    integer *, doublereal *, integer *, doublereal *, doublereal *, 
	    integer *), dgerq2_(integer *, integer *, doublereal *, integer *,
	     doublereal *, doublereal *, integer *), dorg2r_(integer *, 
	    integer *, integer *, doublereal *, integer *, doublereal *, 
	    doublereal *, integer *), dorgr2_(integer *, integer *, integer *,
	     doublereal *, integer *, doublereal *, doublereal *, integer *), 
	    dorm2r_(char *, char *, integer *, integer *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *, integer *, 
	    doublereal *, integer *), dormr2_(char *, char *, 
	    integer *, integer *, integer *, doublereal *, integer *, 
	    doublereal *, doublereal *, integer *, doublereal *, integer *);
    doublereal be[2], ai[2];
    extern /* Subroutine */ void dtgsy2_(char *, integer *, integer *, integer 
	    *, doublereal *, integer *, doublereal *, integer *, doublereal *,
	     integer *, doublereal *, integer *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *, doublereal *,
	     integer *, integer *, integer *);
    doublereal ar[2], sa, sb, li[16]	/* was [4][4] */;
    extern doublereal dlamch_(char *);
    doublereal dscale, ir[16]	/* was [4][4] */, ss, ws;
    extern /* Subroutine */ void dlacpy_(char *, integer *, integer *, 
	    doublereal *, integer *, doublereal *, integer *), 
	    dlartg_(doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *), dlaset_(char *, integer *, integer *, doublereal *,
	     doublereal *, doublereal *, integer *), dlassq_(integer *
	    , doublereal *, integer *, doublereal *, doublereal *);
    logical dtrong;
    doublereal thresh, smlnum, eps;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */
/*  Replaced various illegal calls to DCOPY by calls to DLASET, or by DO */
/*  loops. Sven Hammarling, 1/5/02. */


    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    q_dim1 = *ldq;
    q_offset = 1 + q_dim1 * 1;
    q -= q_offset;
    z_dim1 = *ldz;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;
    --work;

    /* Function Body */
    *info = 0;

/*     Quick return if possible */

    if (*n <= 1 || *n1 <= 0 || *n2 <= 0) {
	return;
    }
    if (*n1 > *n || *j1 + *n1 > *n) {
	return;
    }
    m = *n1 + *n2;
/* Computing MAX */
    i__1 = 1, i__2 = *n * m, i__1 = f2cmax(i__1,i__2), i__2 = m * m << 1;
    if (*lwork < f2cmax(i__1,i__2)) {
	*info = -16;
/* Computing MAX */
	i__1 = 1, i__2 = *n * m, i__1 = f2cmax(i__1,i__2), i__2 = m * m << 1;
	work[1] = (doublereal) f2cmax(i__1,i__2);
	return;
    }

    weak = FALSE_;
    dtrong = FALSE_;

/*     Make a local copy of selected block */

    dlaset_("Full", &c__4, &c__4, &c_b5, &c_b5, li, &c__4);
    dlaset_("Full", &c__4, &c__4, &c_b5, &c_b5, ir, &c__4);
    dlacpy_("Full", &m, &m, &a[*j1 + *j1 * a_dim1], lda, s, &c__4);
    dlacpy_("Full", &m, &m, &b[*j1 + *j1 * b_dim1], ldb, t, &c__4);

/*     Compute threshold for testing acceptance of swapping. */

    eps = dlamch_("P");
    smlnum = dlamch_("S") / eps;
    dscale = 0.;
    dsum = 1.;
    dlacpy_("Full", &m, &m, s, &c__4, &work[1], &m);
    i__1 = m * m;
    dlassq_(&i__1, &work[1], &c__1, &dscale, &dsum);
    dlacpy_("Full", &m, &m, t, &c__4, &work[1], &m);
    i__1 = m * m;
    dlassq_(&i__1, &work[1], &c__1, &dscale, &dsum);
    dnorm = dscale * sqrt(dsum);

/*     THRES has been changed from */
/*        THRESH = MAX( TEN*EPS*SA, SMLNUM ) */
/*     to */
/*        THRESH = MAX( TWENTY*EPS*SA, SMLNUM ) */
/*     on 04/01/10. */
/*     "Bug" reported by Ondra Kamenik, confirmed by Julie Langou, fixed by */
/*     Jim Demmel and Guillaume Revy. See forum post 1783. */

/* Computing MAX */
    d__1 = eps * 20. * dnorm;
    thresh = f2cmax(d__1,smlnum);

    if (m == 2) {

/*        CASE 1: Swap 1-by-1 and 1-by-1 blocks. */

/*        Compute orthogonal QL and RQ that swap 1-by-1 and 1-by-1 blocks */
/*        using Givens rotations and perform the swap tentatively. */

	f = s[5] * t[0] - t[5] * s[0];
	g = s[5] * t[4] - t[5] * s[4];
	sb = abs(t[5]);
	sa = abs(s[5]);
	dlartg_(&f, &g, &ir[4], ir, &ddum);
	ir[1] = -ir[4];
	ir[5] = ir[0];
	drot_(&c__2, s, &c__1, &s[4], &c__1, ir, &ir[1]);
	drot_(&c__2, t, &c__1, &t[4], &c__1, ir, &ir[1]);
	if (sa >= sb) {
	    dlartg_(s, &s[1], li, &li[1], &ddum);
	} else {
	    dlartg_(t, &t[1], li, &li[1], &ddum);
	}
	drot_(&c__2, s, &c__4, &s[1], &c__4, li, &li[1]);
	drot_(&c__2, t, &c__4, &t[1], &c__4, li, &li[1]);
	li[5] = li[0];
	li[4] = -li[1];

/*        Weak stability test: */
/*           |S21| + |T21| <= O(EPS * F-norm((S, T))) */

	ws = abs(s[1]) + abs(t[1]);
	weak = ws <= thresh;
	if (! weak) {
	    goto L70;
	}

	if (TRUE_) {

/*           Strong stability test: */
/*             F-norm((A-QL**T*S*QR, B-QL**T*T*QR)) <= O(EPS*F-norm((A,B))) */

	    dlacpy_("Full", &m, &m, &a[*j1 + *j1 * a_dim1], lda, &work[m * m 
		    + 1], &m);
	    dgemm_("N", "N", &m, &m, &m, &c_b42, li, &c__4, s, &c__4, &c_b5, &
		    work[1], &m);
	    dgemm_("N", "T", &m, &m, &m, &c_b48, &work[1], &m, ir, &c__4, &
		    c_b42, &work[m * m + 1], &m);
	    dscale = 0.;
	    dsum = 1.;
	    i__1 = m * m;
	    dlassq_(&i__1, &work[m * m + 1], &c__1, &dscale, &dsum);

	    dlacpy_("Full", &m, &m, &b[*j1 + *j1 * b_dim1], ldb, &work[m * m 
		    + 1], &m);
	    dgemm_("N", "N", &m, &m, &m, &c_b42, li, &c__4, t, &c__4, &c_b5, &
		    work[1], &m);
	    dgemm_("N", "T", &m, &m, &m, &c_b48, &work[1], &m, ir, &c__4, &
		    c_b42, &work[m * m + 1], &m);
	    i__1 = m * m;
	    dlassq_(&i__1, &work[m * m + 1], &c__1, &dscale, &dsum);
	    ss = dscale * sqrt(dsum);
	    dtrong = ss <= thresh;
	    if (! dtrong) {
		goto L70;
	    }
	}

/*        Update (A(J1:J1+M-1, M+J1:N), B(J1:J1+M-1, M+J1:N)) and */
/*               (A(1:J1-1, J1:J1+M), B(1:J1-1, J1:J1+M)). */

	i__1 = *j1 + 1;
	drot_(&i__1, &a[*j1 * a_dim1 + 1], &c__1, &a[(*j1 + 1) * a_dim1 + 1], 
		&c__1, ir, &ir[1]);
	i__1 = *j1 + 1;
	drot_(&i__1, &b[*j1 * b_dim1 + 1], &c__1, &b[(*j1 + 1) * b_dim1 + 1], 
		&c__1, ir, &ir[1]);
	i__1 = *n - *j1 + 1;
	drot_(&i__1, &a[*j1 + *j1 * a_dim1], lda, &a[*j1 + 1 + *j1 * a_dim1], 
		lda, li, &li[1]);
	i__1 = *n - *j1 + 1;
	drot_(&i__1, &b[*j1 + *j1 * b_dim1], ldb, &b[*j1 + 1 + *j1 * b_dim1], 
		ldb, li, &li[1]);

/*        Set  N1-by-N2 (2,1) - blocks to ZERO. */

	a[*j1 + 1 + *j1 * a_dim1] = 0.;
	b[*j1 + 1 + *j1 * b_dim1] = 0.;

/*        Accumulate transformations into Q and Z if requested. */

	if (*wantz) {
	    drot_(n, &z__[*j1 * z_dim1 + 1], &c__1, &z__[(*j1 + 1) * z_dim1 + 
		    1], &c__1, ir, &ir[1]);
	}
	if (*wantq) {
	    drot_(n, &q[*j1 * q_dim1 + 1], &c__1, &q[(*j1 + 1) * q_dim1 + 1], 
		    &c__1, li, &li[1]);
	}

/*        Exit with INFO = 0 if swap was successfully performed. */

	return;

    } else {

/*        CASE 2: Swap 1-by-1 and 2-by-2 blocks, or 2-by-2 */
/*                and 2-by-2 blocks. */

/*        Solve the generalized Sylvester equation */
/*                 S11 * R - L * S22 = SCALE * S12 */
/*                 T11 * R - L * T22 = SCALE * T12 */
/*        for R and L. Solutions in LI and IR. */

	dlacpy_("Full", n1, n2, &t[(*n1 + 1 << 2) - 4], &c__4, li, &c__4);
	dlacpy_("Full", n1, n2, &s[(*n1 + 1 << 2) - 4], &c__4, &ir[*n2 + 1 + (
		*n1 + 1 << 2) - 5], &c__4);
	dtgsy2_("N", &c__0, n1, n2, s, &c__4, &s[*n1 + 1 + (*n1 + 1 << 2) - 5]
		, &c__4, &ir[*n2 + 1 + (*n1 + 1 << 2) - 5], &c__4, t, &c__4, &
		t[*n1 + 1 + (*n1 + 1 << 2) - 5], &c__4, li, &c__4, &scale, &
		dsum, &dscale, iwork, &idum, &linfo);

/*        Compute orthogonal matrix QL: */

/*                    QL**T * LI = [ TL ] */
/*                                 [ 0  ] */
/*        where */
/*                    LI =  [      -L              ] */
/*                          [ SCALE * identity(N2) ] */

	i__1 = *n2;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    dscal_(n1, &c_b48, &li[(i__ << 2) - 4], &c__1);
	    li[*n1 + i__ + (i__ << 2) - 5] = scale;
/* L10: */
	}
	dgeqr2_(&m, n2, li, &c__4, taul, &work[1], &linfo);
	if (linfo != 0) {
	    goto L70;
	}
	dorg2r_(&m, &m, n2, li, &c__4, taul, &work[1], &linfo);
	if (linfo != 0) {
	    goto L70;
	}

/*        Compute orthogonal matrix RQ: */

/*                    IR * RQ**T =   [ 0  TR], */

/*         where IR = [ SCALE * identity(N1), R ] */

	i__1 = *n1;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    ir[*n2 + i__ + (i__ << 2) - 5] = scale;
/* L20: */
	}
	dgerq2_(n1, &m, &ir[*n2], &c__4, taur, &work[1], &linfo);
	if (linfo != 0) {
	    goto L70;
	}
	dorgr2_(&m, &m, n1, ir, &c__4, taur, &work[1], &linfo);
	if (linfo != 0) {
	    goto L70;
	}

/*        Perform the swapping tentatively: */

	dgemm_("T", "N", &m, &m, &m, &c_b42, li, &c__4, s, &c__4, &c_b5, &
		work[1], &m);
	dgemm_("N", "T", &m, &m, &m, &c_b42, &work[1], &m, ir, &c__4, &c_b5, 
		s, &c__4);
	dgemm_("T", "N", &m, &m, &m, &c_b42, li, &c__4, t, &c__4, &c_b5, &
		work[1], &m);
	dgemm_("N", "T", &m, &m, &m, &c_b42, &work[1], &m, ir, &c__4, &c_b5, 
		t, &c__4);
	dlacpy_("F", &m, &m, s, &c__4, scpy, &c__4);
	dlacpy_("F", &m, &m, t, &c__4, tcpy, &c__4);
	dlacpy_("F", &m, &m, ir, &c__4, ircop, &c__4);
	dlacpy_("F", &m, &m, li, &c__4, licop, &c__4);

/*        Triangularize the B-part by an RQ factorization. */
/*        Apply transformation (from left) to A-part, giving S. */

	dgerq2_(&m, &m, t, &c__4, taur, &work[1], &linfo);
	if (linfo != 0) {
	    goto L70;
	}
	dormr2_("R", "T", &m, &m, &m, t, &c__4, taur, s, &c__4, &work[1], &
		linfo);
	if (linfo != 0) {
	    goto L70;
	}
	dormr2_("L", "N", &m, &m, &m, t, &c__4, taur, ir, &c__4, &work[1], &
		linfo);
	if (linfo != 0) {
	    goto L70;
	}

/*        Compute F-norm(S21) in BRQA21. (T21 is 0.) */

	dscale = 0.;
	dsum = 1.;
	i__1 = *n2;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    dlassq_(n1, &s[*n2 + 1 + (i__ << 2) - 5], &c__1, &dscale, &dsum);
/* L30: */
	}
	brqa21 = dscale * sqrt(dsum);

/*        Triangularize the B-part by a QR factorization. */
/*        Apply transformation (from right) to A-part, giving S. */

	dgeqr2_(&m, &m, tcpy, &c__4, taul, &work[1], &linfo);
	if (linfo != 0) {
	    goto L70;
	}
	dorm2r_("L", "T", &m, &m, &m, tcpy, &c__4, taul, scpy, &c__4, &work[1]
		, info);
	dorm2r_("R", "N", &m, &m, &m, tcpy, &c__4, taul, licop, &c__4, &work[
		1], info);
	if (linfo != 0) {
	    goto L70;
	}

/*        Compute F-norm(S21) in BQRA21. (T21 is 0.) */

	dscale = 0.;
	dsum = 1.;
	i__1 = *n2;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    dlassq_(n1, &scpy[*n2 + 1 + (i__ << 2) - 5], &c__1, &dscale, &
		    dsum);
/* L40: */
	}
	bqra21 = dscale * sqrt(dsum);

/*        Decide which method to use. */
/*          Weak stability test: */
/*             F-norm(S21) <= O(EPS * F-norm((S, T))) */

	if (bqra21 <= brqa21 && bqra21 <= thresh) {
	    dlacpy_("F", &m, &m, scpy, &c__4, s, &c__4);
	    dlacpy_("F", &m, &m, tcpy, &c__4, t, &c__4);
	    dlacpy_("F", &m, &m, ircop, &c__4, ir, &c__4);
	    dlacpy_("F", &m, &m, licop, &c__4, li, &c__4);
	} else if (brqa21 >= thresh) {
	    goto L70;
	}

/*        Set lower triangle of B-part to zero */

	i__1 = m - 1;
	i__2 = m - 1;
	dlaset_("Lower", &i__1, &i__2, &c_b5, &c_b5, &t[1], &c__4);

	if (TRUE_) {

/*           Strong stability test: */
/*              F-norm((A-QL*S*QR**T, B-QL*T*QR**T)) <= O(EPS*F-norm((A,B))) */

	    dlacpy_("Full", &m, &m, &a[*j1 + *j1 * a_dim1], lda, &work[m * m 
		    + 1], &m);
	    dgemm_("N", "N", &m, &m, &m, &c_b42, li, &c__4, s, &c__4, &c_b5, &
		    work[1], &m);
	    dgemm_("N", "N", &m, &m, &m, &c_b48, &work[1], &m, ir, &c__4, &
		    c_b42, &work[m * m + 1], &m);
	    dscale = 0.;
	    dsum = 1.;
	    i__1 = m * m;
	    dlassq_(&i__1, &work[m * m + 1], &c__1, &dscale, &dsum);

	    dlacpy_("Full", &m, &m, &b[*j1 + *j1 * b_dim1], ldb, &work[m * m 
		    + 1], &m);
	    dgemm_("N", "N", &m, &m, &m, &c_b42, li, &c__4, t, &c__4, &c_b5, &
		    work[1], &m);
	    dgemm_("N", "N", &m, &m, &m, &c_b48, &work[1], &m, ir, &c__4, &
		    c_b42, &work[m * m + 1], &m);
	    i__1 = m * m;
	    dlassq_(&i__1, &work[m * m + 1], &c__1, &dscale, &dsum);
	    ss = dscale * sqrt(dsum);
	    dtrong = ss <= thresh;
	    if (! dtrong) {
		goto L70;
	    }

	}

/*        If the swap is accepted ("weakly" and "strongly"), apply the */
/*        transformations and set N1-by-N2 (2,1)-block to zero. */

	dlaset_("Full", n1, n2, &c_b5, &c_b5, &s[*n2], &c__4);

/*        copy back M-by-M diagonal block starting at index J1 of (A, B) */

	dlacpy_("F", &m, &m, s, &c__4, &a[*j1 + *j1 * a_dim1], lda)
		;
	dlacpy_("F", &m, &m, t, &c__4, &b[*j1 + *j1 * b_dim1], ldb)
		;
	dlaset_("Full", &c__4, &c__4, &c_b5, &c_b5, t, &c__4);

/*        Standardize existing 2-by-2 blocks. */

	dlaset_("Full", &m, &m, &c_b5, &c_b5, &work[1], &m);
	work[1] = 1.;
	t[0] = 1.;
	idum = *lwork - m * m - 2;
	if (*n2 > 1) {
	    dlagv2_(&a[*j1 + *j1 * a_dim1], lda, &b[*j1 + *j1 * b_dim1], ldb, 
		    ar, ai, be, &work[1], &work[2], t, &t[1]);
	    work[m + 1] = -work[2];
	    work[m + 2] = work[1];
	    t[*n2 + (*n2 << 2) - 5] = t[0];
	    t[4] = -t[1];
	}
	work[m * m] = 1.;
	t[m + (m << 2) - 5] = 1.;

	if (*n1 > 1) {
	    dlagv2_(&a[*j1 + *n2 + (*j1 + *n2) * a_dim1], lda, &b[*j1 + *n2 + 
		    (*j1 + *n2) * b_dim1], ldb, taur, taul, &work[m * m + 1], 
		    &work[*n2 * m + *n2 + 1], &work[*n2 * m + *n2 + 2], &t[*
		    n2 + 1 + (*n2 + 1 << 2) - 5], &t[m + (m - 1 << 2) - 5]);
	    work[m * m] = work[*n2 * m + *n2 + 1];
	    work[m * m - 1] = -work[*n2 * m + *n2 + 2];
	    t[m + (m << 2) - 5] = t[*n2 + 1 + (*n2 + 1 << 2) - 5];
	    t[m - 1 + (m << 2) - 5] = -t[m + (m - 1 << 2) - 5];
	}
	dgemm_("T", "N", n2, n1, n2, &c_b42, &work[1], &m, &a[*j1 + (*j1 + *
		n2) * a_dim1], lda, &c_b5, &work[m * m + 1], n2);
	dlacpy_("Full", n2, n1, &work[m * m + 1], n2, &a[*j1 + (*j1 + *n2) * 
		a_dim1], lda);
	dgemm_("T", "N", n2, n1, n2, &c_b42, &work[1], &m, &b[*j1 + (*j1 + *
		n2) * b_dim1], ldb, &c_b5, &work[m * m + 1], n2);
	dlacpy_("Full", n2, n1, &work[m * m + 1], n2, &b[*j1 + (*j1 + *n2) * 
		b_dim1], ldb);
	dgemm_("N", "N", &m, &m, &m, &c_b42, li, &c__4, &work[1], &m, &c_b5, &
		work[m * m + 1], &m);
	dlacpy_("Full", &m, &m, &work[m * m + 1], &m, li, &c__4);
	dgemm_("N", "N", n2, n1, n1, &c_b42, &a[*j1 + (*j1 + *n2) * a_dim1], 
		lda, &t[*n2 + 1 + (*n2 + 1 << 2) - 5], &c__4, &c_b5, &work[1],
		 n2);
	dlacpy_("Full", n2, n1, &work[1], n2, &a[*j1 + (*j1 + *n2) * a_dim1], 
		lda);
	dgemm_("N", "N", n2, n1, n1, &c_b42, &b[*j1 + (*j1 + *n2) * b_dim1], 
		ldb, &t[*n2 + 1 + (*n2 + 1 << 2) - 5], &c__4, &c_b5, &work[1],
		 n2);
	dlacpy_("Full", n2, n1, &work[1], n2, &b[*j1 + (*j1 + *n2) * b_dim1], 
		ldb);
	dgemm_("T", "N", &m, &m, &m, &c_b42, ir, &c__4, t, &c__4, &c_b5, &
		work[1], &m);
	dlacpy_("Full", &m, &m, &work[1], &m, ir, &c__4);

/*        Accumulate transformations into Q and Z if requested. */

	if (*wantq) {
	    dgemm_("N", "N", n, &m, &m, &c_b42, &q[*j1 * q_dim1 + 1], ldq, li,
		     &c__4, &c_b5, &work[1], n);
	    dlacpy_("Full", n, &m, &work[1], n, &q[*j1 * q_dim1 + 1], ldq);

	}

	if (*wantz) {
	    dgemm_("N", "N", n, &m, &m, &c_b42, &z__[*j1 * z_dim1 + 1], ldz, 
		    ir, &c__4, &c_b5, &work[1], n);
	    dlacpy_("Full", n, &m, &work[1], n, &z__[*j1 * z_dim1 + 1], ldz);

	}

/*        Update (A(J1:J1+M-1, M+J1:N), B(J1:J1+M-1, M+J1:N)) and */
/*                (A(1:J1-1, J1:J1+M), B(1:J1-1, J1:J1+M)). */

	i__ = *j1 + m;
	if (i__ <= *n) {
	    i__1 = *n - i__ + 1;
	    dgemm_("T", "N", &m, &i__1, &m, &c_b42, li, &c__4, &a[*j1 + i__ * 
		    a_dim1], lda, &c_b5, &work[1], &m);
	    i__1 = *n - i__ + 1;
	    dlacpy_("Full", &m, &i__1, &work[1], &m, &a[*j1 + i__ * a_dim1], 
		    lda);
	    i__1 = *n - i__ + 1;
	    dgemm_("T", "N", &m, &i__1, &m, &c_b42, li, &c__4, &b[*j1 + i__ * 
		    b_dim1], ldb, &c_b5, &work[1], &m);
	    i__1 = *n - i__ + 1;
	    dlacpy_("Full", &m, &i__1, &work[1], &m, &b[*j1 + i__ * b_dim1], 
		    ldb);
	}
	i__ = *j1 - 1;
	if (i__ > 0) {
	    dgemm_("N", "N", &i__, &m, &m, &c_b42, &a[*j1 * a_dim1 + 1], lda, 
		    ir, &c__4, &c_b5, &work[1], &i__);
	    dlacpy_("Full", &i__, &m, &work[1], &i__, &a[*j1 * a_dim1 + 1], 
		    lda);
	    dgemm_("N", "N", &i__, &m, &m, &c_b42, &b[*j1 * b_dim1 + 1], ldb, 
		    ir, &c__4, &c_b5, &work[1], &i__);
	    dlacpy_("Full", &i__, &m, &work[1], &i__, &b[*j1 * b_dim1 + 1], 
		    ldb);
	}

/*        Exit with INFO = 0 if swap was successfully performed. */

	return;

    }

/*     Exit with INFO = 1 if swap was rejected. */

L70:

    *info = 1;
    return;

/*     End of DTGEX2 */

} /* dtgex2_ */

