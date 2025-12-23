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
static complex c_b6 = {-1.f,0.f};
static complex c_b8 = {1.f,0.f};
static real c_b31 = 1.f;

/* > \brief \b CLA_GBRFSX_EXTENDED improves the computed solution to a system of linear equations for general 
banded matrices by performing extra-precise iterative refinement and provides error bounds and backwar
d error estimates for the solution. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CLA_GBRFSX_EXTENDED + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cla_gbr
fsx_extended.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cla_gbr
fsx_extended.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cla_gbr
fsx_extended.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CLA_GBRFSX_EXTENDED ( PREC_TYPE, TRANS_TYPE, N, KL, KU, */
/*                                       NRHS, AB, LDAB, AFB, LDAFB, IPIV, */
/*                                       COLEQU, C, B, LDB, Y, LDY, */
/*                                       BERR_OUT, N_NORMS, ERR_BNDS_NORM, */
/*                                       ERR_BNDS_COMP, RES, AYB, DY, */
/*                                       Y_TAIL, RCOND, ITHRESH, RTHRESH, */
/*                                       DZ_UB, IGNORE_CWISE, INFO ) */

/*       INTEGER            INFO, LDAB, LDAFB, LDB, LDY, N, KL, KU, NRHS, */
/*      $                   PREC_TYPE, TRANS_TYPE, N_NORMS, ITHRESH */
/*       LOGICAL            COLEQU, IGNORE_CWISE */
/*       REAL               RTHRESH, DZ_UB */
/*       INTEGER            IPIV( * ) */
/*       COMPLEX            AB( LDAB, * ), AFB( LDAFB, * ), B( LDB, * ), */
/*      $                   Y( LDY, * ), RES( * ), DY( * ), Y_TAIL( * ) */
/*       REAL               C( * ), AYB(*), RCOND, BERR_OUT( * ), */
/*      $                   ERR_BNDS_NORM( NRHS, * ), */
/*      $                   ERR_BNDS_COMP( NRHS, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CLA_GBRFSX_EXTENDED improves the computed solution to a system of */
/* > linear equations by performing extra-precise iterative refinement */
/* > and provides error bounds and backward error estimates for the solution. */
/* > This subroutine is called by CGBRFSX to perform iterative refinement. */
/* > In addition to normwise error bound, the code provides maximum */
/* > componentwise error bound if possible. See comments for ERR_BNDS_NORM */
/* > and ERR_BNDS_COMP for details of the error bounds. Note that this */
/* > subroutine is only resonsible for setting the second fields of */
/* > ERR_BNDS_NORM and ERR_BNDS_COMP. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] PREC_TYPE */
/* > \verbatim */
/* >          PREC_TYPE is INTEGER */
/* >     Specifies the intermediate precision to be used in refinement. */
/* >     The value is defined by ILAPREC(P) where P is a CHARACTER and P */
/* >          = 'S':  Single */
/* >          = 'D':  Double */
/* >          = 'I':  Indigenous */
/* >          = 'X' or 'E':  Extra */
/* > \endverbatim */
/* > */
/* > \param[in] TRANS_TYPE */
/* > \verbatim */
/* >          TRANS_TYPE is INTEGER */
/* >     Specifies the transposition operation on A. */
/* >     The value is defined by ILATRANS(T) where T is a CHARACTER and T */
/* >          = 'N':  No transpose */
/* >          = 'T':  Transpose */
/* >          = 'C':  Conjugate transpose */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >     The number of linear equations, i.e., the order of the */
/* >     matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] KL */
/* > \verbatim */
/* >          KL is INTEGER */
/* >     The number of subdiagonals within the band of A.  KL >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] KU */
/* > \verbatim */
/* >          KU is INTEGER */
/* >     The number of superdiagonals within the band of A.  KU >= 0 */
/* > \endverbatim */
/* > */
/* > \param[in] NRHS */
/* > \verbatim */
/* >          NRHS is INTEGER */
/* >     The number of right-hand-sides, i.e., the number of columns of the */
/* >     matrix B. */
/* > \endverbatim */
/* > */
/* > \param[in] AB */
/* > \verbatim */
/* >          AB is COMPLEX array, dimension (LDAB,N) */
/* >     On entry, the N-by-N matrix AB. */
/* > \endverbatim */
/* > */
/* > \param[in] LDAB */
/* > \verbatim */
/* >          LDAB is INTEGER */
/* >     The leading dimension of the array AB.  LDAB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] AFB */
/* > \verbatim */
/* >          AFB is COMPLEX array, dimension (LDAF,N) */
/* >     The factors L and U from the factorization */
/* >     A = P*L*U as computed by CGBTRF. */
/* > \endverbatim */
/* > */
/* > \param[in] LDAFB */
/* > \verbatim */
/* >          LDAFB is INTEGER */
/* >     The leading dimension of the array AF.  LDAF >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] IPIV */
/* > \verbatim */
/* >          IPIV is INTEGER array, dimension (N) */
/* >     The pivot indices from the factorization A = P*L*U */
/* >     as computed by CGBTRF; row i of the matrix was interchanged */
/* >     with row IPIV(i). */
/* > \endverbatim */
/* > */
/* > \param[in] COLEQU */
/* > \verbatim */
/* >          COLEQU is LOGICAL */
/* >     If .TRUE. then column equilibration was done to A before calling */
/* >     this routine. This is needed to compute the solution and error */
/* >     bounds correctly. */
/* > \endverbatim */
/* > */
/* > \param[in] C */
/* > \verbatim */
/* >          C is REAL array, dimension (N) */
/* >     The column scale factors for A. If COLEQU = .FALSE., C */
/* >     is not accessed. If C is input, each element of C should be a power */
/* >     of the radix to ensure a reliable solution and error estimates. */
/* >     Scaling by powers of the radix does not cause rounding errors unless */
/* >     the result underflows or overflows. Rounding errors during scaling */
/* >     lead to refining with a matrix that is not equivalent to the */
/* >     input matrix, producing error estimates that may not be */
/* >     reliable. */
/* > \endverbatim */
/* > */
/* > \param[in] B */
/* > \verbatim */
/* >          B is COMPLEX array, dimension (LDB,NRHS) */
/* >     The right-hand-side matrix B. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >     The leading dimension of the array B.  LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] Y */
/* > \verbatim */
/* >          Y is COMPLEX array, dimension (LDY,NRHS) */
/* >     On entry, the solution matrix X, as computed by CGBTRS. */
/* >     On exit, the improved solution matrix Y. */
/* > \endverbatim */
/* > */
/* > \param[in] LDY */
/* > \verbatim */
/* >          LDY is INTEGER */
/* >     The leading dimension of the array Y.  LDY >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] BERR_OUT */
/* > \verbatim */
/* >          BERR_OUT is REAL array, dimension (NRHS) */
/* >     On exit, BERR_OUT(j) contains the componentwise relative backward */
/* >     error for right-hand-side j from the formula */
/* >         f2cmax(i) ( abs(RES(i)) / ( abs(op(A_s))*abs(Y) + abs(B_s) )(i) ) */
/* >     where abs(Z) is the componentwise absolute value of the matrix */
/* >     or vector Z. This is computed by CLA_LIN_BERR. */
/* > \endverbatim */
/* > */
/* > \param[in] N_NORMS */
/* > \verbatim */
/* >          N_NORMS is INTEGER */
/* >     Determines which error bounds to return (see ERR_BNDS_NORM */
/* >     and ERR_BNDS_COMP). */
/* >     If N_NORMS >= 1 return normwise error bounds. */
/* >     If N_NORMS >= 2 return componentwise error bounds. */
/* > \endverbatim */
/* > */
/* > \param[in,out] ERR_BNDS_NORM */
/* > \verbatim */
/* >          ERR_BNDS_NORM is REAL array, dimension (NRHS, N_ERR_BNDS) */
/* >     For each right-hand side, this array contains information about */
/* >     various error bounds and condition numbers corresponding to the */
/* >     normwise relative error, which is defined as follows: */
/* > */
/* >     Normwise relative error in the ith solution vector: */
/* >             max_j (abs(XTRUE(j,i) - X(j,i))) */
/* >            ------------------------------ */
/* >                  max_j abs(X(j,i)) */
/* > */
/* >     The array is indexed by the type of error information as described */
/* >     below. There currently are up to three pieces of information */
/* >     returned. */
/* > */
/* >     The first index in ERR_BNDS_NORM(i,:) corresponds to the ith */
/* >     right-hand side. */
/* > */
/* >     The second index in ERR_BNDS_NORM(:,err) contains the following */
/* >     three fields: */
/* >     err = 1 "Trust/don't trust" boolean. Trust the answer if the */
/* >              reciprocal condition number is less than the threshold */
/* >              sqrt(n) * slamch('Epsilon'). */
/* > */
/* >     err = 2 "Guaranteed" error bound: The estimated forward error, */
/* >              almost certainly within a factor of 10 of the true error */
/* >              so long as the next entry is greater than the threshold */
/* >              sqrt(n) * slamch('Epsilon'). This error bound should only */
/* >              be trusted if the previous boolean is true. */
/* > */
/* >     err = 3  Reciprocal condition number: Estimated normwise */
/* >              reciprocal condition number.  Compared with the threshold */
/* >              sqrt(n) * slamch('Epsilon') to determine if the error */
/* >              estimate is "guaranteed". These reciprocal condition */
/* >              numbers are 1 / (norm(Z^{-1},inf) * norm(Z,inf)) for some */
/* >              appropriately scaled matrix Z. */
/* >              Let Z = S*A, where S scales each row by a power of the */
/* >              radix so all absolute row sums of Z are approximately 1. */
/* > */
/* >     This subroutine is only responsible for setting the second field */
/* >     above. */
/* >     See Lapack Working Note 165 for further details and extra */
/* >     cautions. */
/* > \endverbatim */
/* > */
/* > \param[in,out] ERR_BNDS_COMP */
/* > \verbatim */
/* >          ERR_BNDS_COMP is REAL array, dimension (NRHS, N_ERR_BNDS) */
/* >     For each right-hand side, this array contains information about */
/* >     various error bounds and condition numbers corresponding to the */
/* >     componentwise relative error, which is defined as follows: */
/* > */
/* >     Componentwise relative error in the ith solution vector: */
/* >                    abs(XTRUE(j,i) - X(j,i)) */
/* >             max_j ---------------------- */
/* >                         abs(X(j,i)) */
/* > */
/* >     The array is indexed by the right-hand side i (on which the */
/* >     componentwise relative error depends), and the type of error */
/* >     information as described below. There currently are up to three */
/* >     pieces of information returned for each right-hand side. If */
/* >     componentwise accuracy is not requested (PARAMS(3) = 0.0), then */
/* >     ERR_BNDS_COMP is not accessed.  If N_ERR_BNDS < 3, then at most */
/* >     the first (:,N_ERR_BNDS) entries are returned. */
/* > */
/* >     The first index in ERR_BNDS_COMP(i,:) corresponds to the ith */
/* >     right-hand side. */
/* > */
/* >     The second index in ERR_BNDS_COMP(:,err) contains the following */
/* >     three fields: */
/* >     err = 1 "Trust/don't trust" boolean. Trust the answer if the */
/* >              reciprocal condition number is less than the threshold */
/* >              sqrt(n) * slamch('Epsilon'). */
/* > */
/* >     err = 2 "Guaranteed" error bound: The estimated forward error, */
/* >              almost certainly within a factor of 10 of the true error */
/* >              so long as the next entry is greater than the threshold */
/* >              sqrt(n) * slamch('Epsilon'). This error bound should only */
/* >              be trusted if the previous boolean is true. */
/* > */
/* >     err = 3  Reciprocal condition number: Estimated componentwise */
/* >              reciprocal condition number.  Compared with the threshold */
/* >              sqrt(n) * slamch('Epsilon') to determine if the error */
/* >              estimate is "guaranteed". These reciprocal condition */
/* >              numbers are 1 / (norm(Z^{-1},inf) * norm(Z,inf)) for some */
/* >              appropriately scaled matrix Z. */
/* >              Let Z = S*(A*diag(x)), where x is the solution for the */
/* >              current right-hand side and S scales each row of */
/* >              A*diag(x) by a power of the radix so all absolute row */
/* >              sums of Z are approximately 1. */
/* > */
/* >     This subroutine is only responsible for setting the second field */
/* >     above. */
/* >     See Lapack Working Note 165 for further details and extra */
/* >     cautions. */
/* > \endverbatim */
/* > */
/* > \param[in] RES */
/* > \verbatim */
/* >          RES is COMPLEX array, dimension (N) */
/* >     Workspace to hold the intermediate residual. */
/* > \endverbatim */
/* > */
/* > \param[in] AYB */
/* > \verbatim */
/* >          AYB is REAL array, dimension (N) */
/* >     Workspace. */
/* > \endverbatim */
/* > */
/* > \param[in] DY */
/* > \verbatim */
/* >          DY is COMPLEX array, dimension (N) */
/* >     Workspace to hold the intermediate solution. */
/* > \endverbatim */
/* > */
/* > \param[in] Y_TAIL */
/* > \verbatim */
/* >          Y_TAIL is COMPLEX array, dimension (N) */
/* >     Workspace to hold the trailing bits of the intermediate solution. */
/* > \endverbatim */
/* > */
/* > \param[in] RCOND */
/* > \verbatim */
/* >          RCOND is REAL */
/* >     Reciprocal scaled condition number.  This is an estimate of the */
/* >     reciprocal Skeel condition number of the matrix A after */
/* >     equilibration (if done).  If this is less than the machine */
/* >     precision (in particular, if it is zero), the matrix is singular */
/* >     to working precision.  Note that the error may still be small even */
/* >     if this number is very small and the matrix appears ill- */
/* >     conditioned. */
/* > \endverbatim */
/* > */
/* > \param[in] ITHRESH */
/* > \verbatim */
/* >          ITHRESH is INTEGER */
/* >     The maximum number of residual computations allowed for */
/* >     refinement. The default is 10. For 'aggressive' set to 100 to */
/* >     permit convergence using approximate factorizations or */
/* >     factorizations other than LU. If the factorization uses a */
/* >     technique other than Gaussian elimination, the guarantees in */
/* >     ERR_BNDS_NORM and ERR_BNDS_COMP may no longer be trustworthy. */
/* > \endverbatim */
/* > */
/* > \param[in] RTHRESH */
/* > \verbatim */
/* >          RTHRESH is REAL */
/* >     Determines when to stop refinement if the error estimate stops */
/* >     decreasing. Refinement will stop when the next solution no longer */
/* >     satisfies norm(dx_{i+1}) < RTHRESH * norm(dx_i) where norm(Z) is */
/* >     the infinity norm of Z. RTHRESH satisfies 0 < RTHRESH <= 1. The */
/* >     default value is 0.5. For 'aggressive' set to 0.9 to permit */
/* >     convergence on extremely ill-conditioned matrices. See LAWN 165 */
/* >     for more details. */
/* > \endverbatim */
/* > */
/* > \param[in] DZ_UB */
/* > \verbatim */
/* >          DZ_UB is REAL */
/* >     Determines when to start considering componentwise convergence. */
/* >     Componentwise convergence is only considered after each component */
/* >     of the solution Y is stable, which we definte as the relative */
/* >     change in each component being less than DZ_UB. The default value */
/* >     is 0.25, requiring the first bit to be stable. See LAWN 165 for */
/* >     more details. */
/* > \endverbatim */
/* > */
/* > \param[in] IGNORE_CWISE */
/* > \verbatim */
/* >          IGNORE_CWISE is LOGICAL */
/* >     If .TRUE. then ignore componentwise convergence. Default value */
/* >     is .FALSE.. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >       = 0:  Successful exit. */
/* >       < 0:  if INFO = -i, the ith argument to CGBTRS had an illegal */
/* >             value */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2017 */

/* > \ingroup complexGBcomputational */

/*  ===================================================================== */
/* Subroutine */ void cla_gbrfsx_extended_(integer *prec_type__, integer *
	trans_type__, integer *n, integer *kl, integer *ku, integer *nrhs, 
	complex *ab, integer *ldab, complex *afb, integer *ldafb, integer *
	ipiv, logical *colequ, real *c__, complex *b, integer *ldb, complex *
	y, integer *ldy, real *berr_out__, integer *n_norms__, real *
	err_bnds_norm__, real *err_bnds_comp__, complex *res, real *ayb, 
	complex *dy, complex *y_tail__, real *rcond, integer *ithresh, real *
	rthresh, real *dz_ub__, logical *ignore_cwise__, integer *info)
{
    /* System generated locals */
    integer ab_dim1, ab_offset, afb_dim1, afb_offset, b_dim1, b_offset, 
	    y_dim1, y_offset, err_bnds_norm_dim1, err_bnds_norm_offset, 
	    err_bnds_comp_dim1, err_bnds_comp_offset, i__1, i__2, i__3, i__4;
    real r__1, r__2;
    char ch__1[1];

    /* Local variables */
    real dx_x__, dz_z__;
    extern /* Subroutine */ void cla_lin_berr_(integer *, integer *, integer *
	    , complex *, real *, real *);
    real ymin;
    extern /* Subroutine */ void blas_cgbmv_x_(integer *, integer *, integer *
	    , integer *, integer *, complex *, complex *, integer *, complex *
	    , integer *, complex *, complex *, integer *, integer *);
    real dxratmax, dzratmax;
    integer y_prec_state__;
    extern /* Subroutine */ void blas_cgbmv2_x_(integer *, integer *, integer 
	    *, integer *, integer *, complex *, complex *, integer *, complex 
	    *, complex *, integer *, complex *, complex *, integer *, integer 
	    *);
    integer i__, j, m;
    extern /* Subroutine */ void cla_gbamv_(integer *, integer *, integer *, 
	    integer *, integer *, real *, complex *, integer *, complex *, 
	    integer *, real *, real *, integer *), cgbmv_(char *, integer *, 
	    integer *, integer *, integer *, complex *, complex *, integer *, 
	    complex *, integer *, complex *, complex *, integer *), 
	    ccopy_(integer *, complex *, integer *, complex *, integer *);
    real dxrat;
    logical incr_prec__;
    real dzrat;
    extern /* Subroutine */ void caxpy_(integer *, complex *, complex *, 
	    integer *, complex *, integer *);
    char trans[1];
    real normx, normy, myhugeval, prev_dz_z__, yk;
    extern real slamch_(char *);
    extern /* Subroutine */ void cgbtrs_(char *, integer *, integer *, integer 
	    *, integer *, complex *, integer *, integer *, complex *, integer 
	    *, integer *);
    real final_dx_x__;
    extern /* Subroutine */ void cla_wwaddw_(integer *, complex *, complex *, 
	    complex *);
    real final_dz_z__, normdx;
    extern /* Character */ VOID chla_transtype_(char *, integer *);
    real prevnormdx;
    integer cnt;
    real dyk, eps;
    integer x_state__, z_state__;
    real incr_thresh__;


/*  -- LAPACK computational routine (version 3.7.1) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2017 */


/*  ===================================================================== */


    /* Parameter adjustments */
    err_bnds_comp_dim1 = *nrhs;
    err_bnds_comp_offset = 1 + err_bnds_comp_dim1 * 1;
    err_bnds_comp__ -= err_bnds_comp_offset;
    err_bnds_norm_dim1 = *nrhs;
    err_bnds_norm_offset = 1 + err_bnds_norm_dim1 * 1;
    err_bnds_norm__ -= err_bnds_norm_offset;
    ab_dim1 = *ldab;
    ab_offset = 1 + ab_dim1 * 1;
    ab -= ab_offset;
    afb_dim1 = *ldafb;
    afb_offset = 1 + afb_dim1 * 1;
    afb -= afb_offset;
    --ipiv;
    --c__;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    y_dim1 = *ldy;
    y_offset = 1 + y_dim1 * 1;
    y -= y_offset;
    --berr_out__;
    --res;
    --ayb;
    --dy;
    --y_tail__;

    /* Function Body */
    if (*info != 0) {
	return;
    }
    chla_transtype_(ch__1, trans_type__);
    *(unsigned char *)trans = *(unsigned char *)&ch__1[0];
    eps = slamch_("Epsilon");
    myhugeval = slamch_("Overflow");
/*     Force MYHUGEVAL to Inf */
    myhugeval *= myhugeval;
/*     Using MYHUGEVAL may lead to spurious underflows. */
    incr_thresh__ = (real) (*n) * eps;
    m = *kl + *ku + 1;
    i__1 = *nrhs;
    for (j = 1; j <= i__1; ++j) {
	y_prec_state__ = 1;
	if (y_prec_state__ == 2) {
	    i__2 = *n;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__;
		y_tail__[i__3].r = 0.f, y_tail__[i__3].i = 0.f;
	    }
	}
	dxrat = 0.f;
	dxratmax = 0.f;
	dzrat = 0.f;
	dzratmax = 0.f;
	final_dx_x__ = myhugeval;
	final_dz_z__ = myhugeval;
	prevnormdx = myhugeval;
	prev_dz_z__ = myhugeval;
	dz_z__ = myhugeval;
	dx_x__ = myhugeval;
	x_state__ = 1;
	z_state__ = 0;
	incr_prec__ = FALSE_;
	i__2 = *ithresh;
	for (cnt = 1; cnt <= i__2; ++cnt) {

/*        Compute residual RES = B_s - op(A_s) * Y, */
/*            op(A) = A, A**T, or A**H depending on TRANS (and type). */

	    ccopy_(n, &b[j * b_dim1 + 1], &c__1, &res[1], &c__1);
	    if (y_prec_state__ == 0) {
		cgbmv_(trans, &m, n, kl, ku, &c_b6, &ab[ab_offset], ldab, &y[
			j * y_dim1 + 1], &c__1, &c_b8, &res[1], &c__1);
	    } else if (y_prec_state__ == 1) {
		blas_cgbmv_x__(trans_type__, n, n, kl, ku, &c_b6, &ab[
			ab_offset], ldab, &y[j * y_dim1 + 1], &c__1, &c_b8, &
			res[1], &c__1, prec_type__);
	    } else {
		blas_cgbmv2_x__(trans_type__, n, n, kl, ku, &c_b6, &ab[
			ab_offset], ldab, &y[j * y_dim1 + 1], &y_tail__[1], &
			c__1, &c_b8, &res[1], &c__1, prec_type__);
	    }
/*        XXX: RES is no longer needed. */
	    ccopy_(n, &res[1], &c__1, &dy[1], &c__1);
	    cgbtrs_(trans, n, kl, ku, &c__1, &afb[afb_offset], ldafb, &ipiv[1]
		    , &dy[1], n, info);

/*         Calculate relative changes DX_X, DZ_Z and ratios DXRAT, DZRAT. */

	    normx = 0.f;
	    normy = 0.f;
	    normdx = 0.f;
	    dz_z__ = 0.f;
	    ymin = myhugeval;
	    i__3 = *n;
	    for (i__ = 1; i__ <= i__3; ++i__) {
		i__4 = i__ + j * y_dim1;
		yk = (r__1 = y[i__4].r, abs(r__1)) + (r__2 = r_imag(&y[i__ + 
			j * y_dim1]), abs(r__2));
		i__4 = i__;
		dyk = (r__1 = dy[i__4].r, abs(r__1)) + (r__2 = r_imag(&dy[i__]
			), abs(r__2));
		if (yk != 0.f) {
/* Computing MAX */
		    r__1 = dz_z__, r__2 = dyk / yk;
		    dz_z__ = f2cmax(r__1,r__2);
		} else if (dyk != 0.f) {
		    dz_z__ = myhugeval;
		}
		ymin = f2cmin(ymin,yk);
		normy = f2cmax(normy,yk);
		if (*colequ) {
/* Computing MAX */
		    r__1 = normx, r__2 = yk * c__[i__];
		    normx = f2cmax(r__1,r__2);
/* Computing MAX */
		    r__1 = normdx, r__2 = dyk * c__[i__];
		    normdx = f2cmax(r__1,r__2);
		} else {
		    normx = normy;
		    normdx = f2cmax(normdx,dyk);
		}
	    }
	    if (normx != 0.f) {
		dx_x__ = normdx / normx;
	    } else if (normdx == 0.f) {
		dx_x__ = 0.f;
	    } else {
		dx_x__ = myhugeval;
	    }
	    dxrat = normdx / prevnormdx;
	    dzrat = dz_z__ / prev_dz_z__;

/*         Check termination criteria. */

	    if (! (*ignore_cwise__) && ymin * *rcond < incr_thresh__ * normy 
		    && y_prec_state__ < 2) {
		incr_prec__ = TRUE_;
	    }
	    if (x_state__ == 3 && dxrat <= *rthresh) {
		x_state__ = 1;
	    }
	    if (x_state__ == 1) {
		if (dx_x__ <= eps) {
		    x_state__ = 2;
		} else if (dxrat > *rthresh) {
		    if (y_prec_state__ != 2) {
			incr_prec__ = TRUE_;
		    } else {
			x_state__ = 3;
		    }
		} else {
		    if (dxrat > dxratmax) {
			dxratmax = dxrat;
		    }
		}
		if (x_state__ > 1) {
		    final_dx_x__ = dx_x__;
		}
	    }
	    if (z_state__ == 0 && dz_z__ <= *dz_ub__) {
		z_state__ = 1;
	    }
	    if (z_state__ == 3 && dzrat <= *rthresh) {
		z_state__ = 1;
	    }
	    if (z_state__ == 1) {
		if (dz_z__ <= eps) {
		    z_state__ = 2;
		} else if (dz_z__ > *dz_ub__) {
		    z_state__ = 0;
		    dzratmax = 0.f;
		    final_dz_z__ = myhugeval;
		} else if (dzrat > *rthresh) {
		    if (y_prec_state__ != 2) {
			incr_prec__ = TRUE_;
		    } else {
			z_state__ = 3;
		    }
		} else {
		    if (dzrat > dzratmax) {
			dzratmax = dzrat;
		    }
		}
		if (z_state__ > 1) {
		    final_dz_z__ = dz_z__;
		}
	    }

/*           Exit if both normwise and componentwise stopped working, */
/*           but if componentwise is unstable, let it go at least two */
/*           iterations. */

	    if (x_state__ != 1) {
		if (*ignore_cwise__) {
		    goto L666;
		}
		if (z_state__ == 3 || z_state__ == 2) {
		    goto L666;
		}
		if (z_state__ == 0 && cnt > 1) {
		    goto L666;
		}
	    }
	    if (incr_prec__) {
		incr_prec__ = FALSE_;
		++y_prec_state__;
		i__3 = *n;
		for (i__ = 1; i__ <= i__3; ++i__) {
		    i__4 = i__;
		    y_tail__[i__4].r = 0.f, y_tail__[i__4].i = 0.f;
		}
	    }
	    prevnormdx = normdx;
	    prev_dz_z__ = dz_z__;

/*           Update soluton. */

	    if (y_prec_state__ < 2) {
		caxpy_(n, &c_b8, &dy[1], &c__1, &y[j * y_dim1 + 1], &c__1);
	    } else {
		cla_wwaddw_(n, &y[j * y_dim1 + 1], &y_tail__[1], &dy[1]);
	    }
	}
/*        Target of "IF (Z_STOP .AND. X_STOP)".  Sun's f77 won't CALL MYEXIT. */
L666:

/*     Set final_* when cnt hits ithresh. */

	if (x_state__ == 1) {
	    final_dx_x__ = dx_x__;
	}
	if (z_state__ == 1) {
	    final_dz_z__ = dz_z__;
	}

/*     Compute error bounds. */

	if (*n_norms__ >= 1) {
	    err_bnds_norm__[j + (err_bnds_norm_dim1 << 1)] = final_dx_x__ / (
		    1 - dxratmax);
	}
	if (*n_norms__ >= 2) {
	    err_bnds_comp__[j + (err_bnds_comp_dim1 << 1)] = final_dz_z__ / (
		    1 - dzratmax);
	}

/*     Compute componentwise relative backward error from formula */
/*         f2cmax(i) ( abs(R(i)) / ( abs(op(A_s))*abs(Y) + abs(B_s) )(i) ) */
/*     where abs(Z) is the componentwise absolute value of the matrix */
/*     or vector Z. */

/*        Compute residual RES = B_s - op(A_s) * Y, */
/*            op(A) = A, A**T, or A**H depending on TRANS (and type). */

	ccopy_(n, &b[j * b_dim1 + 1], &c__1, &res[1], &c__1);
	cgbmv_(trans, n, n, kl, ku, &c_b6, &ab[ab_offset], ldab, &y[j * 
		y_dim1 + 1], &c__1, &c_b8, &res[1], &c__1);
	i__2 = *n;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    i__3 = i__ + j * b_dim1;
	    ayb[i__] = (r__1 = b[i__3].r, abs(r__1)) + (r__2 = r_imag(&b[i__ 
		    + j * b_dim1]), abs(r__2));
	}

/*     Compute abs(op(A_s))*abs(Y) + abs(B_s). */

	cla_gbamv_(trans_type__, n, n, kl, ku, &c_b31, &ab[ab_offset], ldab, 
		&y[j * y_dim1 + 1], &c__1, &c_b31, &ayb[1], &c__1);
	cla_lin_berr_(n, n, &c__1, &res[1], &ayb[1], &berr_out__[j]);

/*     End of loop for each RHS. */

    }

    return;
} /* cla_gbrfsx_extended__ */

