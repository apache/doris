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




/* > \brief \b DLAG2 computes the eigenvalues of a 2-by-2 generalized eigenvalue problem, with scaling as nece
ssary to avoid over-/underflow. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DLAG2 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlag2.f
"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlag2.f
"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlag2.f
"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DLAG2( A, LDA, B, LDB, SAFMIN, SCALE1, SCALE2, WR1, */
/*                         WR2, WI ) */

/*       INTEGER            LDA, LDB */
/*       DOUBLE PRECISION   SAFMIN, SCALE1, SCALE2, WI, WR1, WR2 */
/*       DOUBLE PRECISION   A( LDA, * ), B( LDB, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DLAG2 computes the eigenvalues of a 2 x 2 generalized eigenvalue */
/* > problem  A - w B, with scaling as necessary to avoid over-/underflow. */
/* > */
/* > The scaling factor "s" results in a modified eigenvalue equation */
/* > */
/* >     s A - w B */
/* > */
/* > where  s  is a non-negative scaling factor chosen so that  w,  w B, */
/* > and  s A  do not overflow and, if possible, do not underflow, either. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] A */
/* > \verbatim */
/* >          A is DOUBLE PRECISION array, dimension (LDA, 2) */
/* >          On entry, the 2 x 2 matrix A.  It is assumed that its 1-norm */
/* >          is less than 1/SAFMIN.  Entries less than */
/* >          sqrt(SAFMIN)*norm(A) are subject to being treated as zero. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= 2. */
/* > \endverbatim */
/* > */
/* > \param[in] B */
/* > \verbatim */
/* >          B is DOUBLE PRECISION array, dimension (LDB, 2) */
/* >          On entry, the 2 x 2 upper triangular matrix B.  It is */
/* >          assumed that the one-norm of B is less than 1/SAFMIN.  The */
/* >          diagonals should be at least sqrt(SAFMIN) times the largest */
/* >          element of B (in absolute value); if a diagonal is smaller */
/* >          than that, then  +/- sqrt(SAFMIN) will be used instead of */
/* >          that diagonal. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B.  LDB >= 2. */
/* > \endverbatim */
/* > */
/* > \param[in] SAFMIN */
/* > \verbatim */
/* >          SAFMIN is DOUBLE PRECISION */
/* >          The smallest positive number s.t. 1/SAFMIN does not */
/* >          overflow.  (This should always be DLAMCH('S') -- it is an */
/* >          argument in order to avoid having to call DLAMCH frequently.) */
/* > \endverbatim */
/* > */
/* > \param[out] SCALE1 */
/* > \verbatim */
/* >          SCALE1 is DOUBLE PRECISION */
/* >          A scaling factor used to avoid over-/underflow in the */
/* >          eigenvalue equation which defines the first eigenvalue.  If */
/* >          the eigenvalues are complex, then the eigenvalues are */
/* >          ( WR1  +/-  WI i ) / SCALE1  (which may lie outside the */
/* >          exponent range of the machine), SCALE1=SCALE2, and SCALE1 */
/* >          will always be positive.  If the eigenvalues are real, then */
/* >          the first (real) eigenvalue is  WR1 / SCALE1 , but this may */
/* >          overflow or underflow, and in fact, SCALE1 may be zero or */
/* >          less than the underflow threshold if the exact eigenvalue */
/* >          is sufficiently large. */
/* > \endverbatim */
/* > */
/* > \param[out] SCALE2 */
/* > \verbatim */
/* >          SCALE2 is DOUBLE PRECISION */
/* >          A scaling factor used to avoid over-/underflow in the */
/* >          eigenvalue equation which defines the second eigenvalue.  If */
/* >          the eigenvalues are complex, then SCALE2=SCALE1.  If the */
/* >          eigenvalues are real, then the second (real) eigenvalue is */
/* >          WR2 / SCALE2 , but this may overflow or underflow, and in */
/* >          fact, SCALE2 may be zero or less than the underflow */
/* >          threshold if the exact eigenvalue is sufficiently large. */
/* > \endverbatim */
/* > */
/* > \param[out] WR1 */
/* > \verbatim */
/* >          WR1 is DOUBLE PRECISION */
/* >          If the eigenvalue is real, then WR1 is SCALE1 times the */
/* >          eigenvalue closest to the (2,2) element of A B**(-1).  If the */
/* >          eigenvalue is complex, then WR1=WR2 is SCALE1 times the real */
/* >          part of the eigenvalues. */
/* > \endverbatim */
/* > */
/* > \param[out] WR2 */
/* > \verbatim */
/* >          WR2 is DOUBLE PRECISION */
/* >          If the eigenvalue is real, then WR2 is SCALE2 times the */
/* >          other eigenvalue.  If the eigenvalue is complex, then */
/* >          WR1=WR2 is SCALE1 times the real part of the eigenvalues. */
/* > \endverbatim */
/* > */
/* > \param[out] WI */
/* > \verbatim */
/* >          WI is DOUBLE PRECISION */
/* >          If the eigenvalue is real, then WI is zero.  If the */
/* >          eigenvalue is complex, then WI is SCALE1 times the imaginary */
/* >          part of the eigenvalues.  WI will always be non-negative. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup doubleOTHERauxiliary */

/*  ===================================================================== */
/* Subroutine */ void dlag2_(doublereal *a, integer *lda, doublereal *b, 
	integer *ldb, doublereal *safmin, doublereal *scale1, doublereal *
	scale2, doublereal *wr1, doublereal *wr2, doublereal *wi)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset;
    doublereal d__1, d__2, d__3, d__4, d__5, d__6;

    /* Local variables */
    doublereal diff, bmin, wbig, wabs, wdet, r__, binv11, binv22, discr, 
	    anorm, bnorm, bsize, shift, c1, c2, c3, c4, c5, rtmin, rtmax, 
	    wsize, s1, s2, a11, a12, a21, a22, b11, b12, b22, ascale, bscale, 
	    pp, qq, ss, wscale, safmax, wsmall, as11, as12, as22, sum, abi22;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */


/*  ===================================================================== */


    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;

    /* Function Body */
    rtmin = sqrt(*safmin);
    rtmax = 1. / rtmin;
    safmax = 1. / *safmin;

/*     Scale A */

/* Computing MAX */
    d__5 = (d__1 = a[a_dim1 + 1], abs(d__1)) + (d__2 = a[a_dim1 + 2], abs(
	    d__2)), d__6 = (d__3 = a[(a_dim1 << 1) + 1], abs(d__3)) + (d__4 = 
	    a[(a_dim1 << 1) + 2], abs(d__4)), d__5 = f2cmax(d__5,d__6);
    anorm = f2cmax(d__5,*safmin);
    ascale = 1. / anorm;
    a11 = ascale * a[a_dim1 + 1];
    a21 = ascale * a[a_dim1 + 2];
    a12 = ascale * a[(a_dim1 << 1) + 1];
    a22 = ascale * a[(a_dim1 << 1) + 2];

/*     Perturb B if necessary to insure non-singularity */

    b11 = b[b_dim1 + 1];
    b12 = b[(b_dim1 << 1) + 1];
    b22 = b[(b_dim1 << 1) + 2];
/* Computing MAX */
    d__1 = abs(b11), d__2 = abs(b12), d__1 = f2cmax(d__1,d__2), d__2 = abs(b22), 
	    d__1 = f2cmax(d__1,d__2);
    bmin = rtmin * f2cmax(d__1,rtmin);
    if (abs(b11) < bmin) {
	b11 = d_sign(&bmin, &b11);
    }
    if (abs(b22) < bmin) {
	b22 = d_sign(&bmin, &b22);
    }

/*     Scale B */

/* Computing MAX */
    d__1 = abs(b11), d__2 = abs(b12) + abs(b22), d__1 = f2cmax(d__1,d__2);
    bnorm = f2cmax(d__1,*safmin);
/* Computing MAX */
    d__1 = abs(b11), d__2 = abs(b22);
    bsize = f2cmax(d__1,d__2);
    bscale = 1. / bsize;
    b11 *= bscale;
    b12 *= bscale;
    b22 *= bscale;

/*     Compute larger eigenvalue by method described by C. van Loan */

/*     ( AS is A shifted by -SHIFT*B ) */

    binv11 = 1. / b11;
    binv22 = 1. / b22;
    s1 = a11 * binv11;
    s2 = a22 * binv22;
    if (abs(s1) <= abs(s2)) {
	as12 = a12 - s1 * b12;
	as22 = a22 - s1 * b22;
	ss = a21 * (binv11 * binv22);
	abi22 = as22 * binv22 - ss * b12;
	pp = abi22 * .5;
	shift = s1;
    } else {
	as12 = a12 - s2 * b12;
	as11 = a11 - s2 * b11;
	ss = a21 * (binv11 * binv22);
	abi22 = -ss * b12;
	pp = (as11 * binv11 + abi22) * .5;
	shift = s2;
    }
    qq = ss * as12;
    if ((d__1 = pp * rtmin, abs(d__1)) >= 1.) {
/* Computing 2nd power */
	d__1 = rtmin * pp;
	discr = d__1 * d__1 + qq * *safmin;
	r__ = sqrt((abs(discr))) * rtmax;
    } else {
/* Computing 2nd power */
	d__1 = pp;
	if (d__1 * d__1 + abs(qq) <= *safmin) {
/* Computing 2nd power */
	    d__1 = rtmax * pp;
	    discr = d__1 * d__1 + qq * safmax;
	    r__ = sqrt((abs(discr))) * rtmin;
	} else {
/* Computing 2nd power */
	    d__1 = pp;
	    discr = d__1 * d__1 + qq;
	    r__ = sqrt((abs(discr)));
	}
    }

/*     Note: the test of R in the following IF is to cover the case when */
/*           DISCR is small and negative and is flushed to zero during */
/*           the calculation of R.  On machines which have a consistent */
/*           flush-to-zero threshold and handle numbers above that */
/*           threshold correctly, it would not be necessary. */

    if (discr >= 0. || r__ == 0.) {
	sum = pp + d_sign(&r__, &pp);
	diff = pp - d_sign(&r__, &pp);
	wbig = shift + sum;

/*        Compute smaller eigenvalue */

	wsmall = shift + diff;
/* Computing MAX */
	d__1 = abs(wsmall);
	if (abs(wbig) * .5 > f2cmax(d__1,*safmin)) {
	    wdet = (a11 * a22 - a12 * a21) * (binv11 * binv22);
	    wsmall = wdet / wbig;
	}

/*        Choose (real) eigenvalue closest to 2,2 element of A*B**(-1) */
/*        for WR1. */

	if (pp > abi22) {
	    *wr1 = f2cmin(wbig,wsmall);
	    *wr2 = f2cmax(wbig,wsmall);
	} else {
	    *wr1 = f2cmax(wbig,wsmall);
	    *wr2 = f2cmin(wbig,wsmall);
	}
	*wi = 0.;
    } else {

/*        Complex eigenvalues */

	*wr1 = shift + pp;
	*wr2 = *wr1;
	*wi = r__;
    }

/*     Further scaling to avoid underflow and overflow in computing */
/*     SCALE1 and overflow in computing w*B. */

/*     This scale factor (WSCALE) is bounded from above using C1 and C2, */
/*     and from below using C3 and C4. */
/*        C1 implements the condition  s A  must never overflow. */
/*        C2 implements the condition  w B  must never overflow. */
/*        C3, with C2, */
/*           implement the condition that s A - w B must never overflow. */
/*        C4 implements the condition  s    should not underflow. */
/*        C5 implements the condition  f2cmax(s,|w|) should be at least 2. */

    c1 = bsize * (*safmin * f2cmax(1.,ascale));
    c2 = *safmin * f2cmax(1.,bnorm);
    c3 = bsize * *safmin;
    if (ascale <= 1. && bsize <= 1.) {
/* Computing MIN */
	d__1 = 1., d__2 = ascale / *safmin * bsize;
	c4 = f2cmin(d__1,d__2);
    } else {
	c4 = 1.;
    }
    if (ascale <= 1. || bsize <= 1.) {
/* Computing MIN */
	d__1 = 1., d__2 = ascale * bsize;
	c5 = f2cmin(d__1,d__2);
    } else {
	c5 = 1.;
    }

/*     Scale first eigenvalue */

    wabs = abs(*wr1) + abs(*wi);
/* Computing MAX */
/* Computing MIN */
    d__3 = c4, d__4 = f2cmax(wabs,c5) * .5;
    d__1 = f2cmax(*safmin,c1), d__2 = (wabs * c2 + c3) * 1.0000100000000001, 
	    d__1 = f2cmax(d__1,d__2), d__2 = f2cmin(d__3,d__4);
    wsize = f2cmax(d__1,d__2);
    if (wsize != 1.) {
	wscale = 1. / wsize;
	if (wsize > 1.) {
	    *scale1 = f2cmax(ascale,bsize) * wscale * f2cmin(ascale,bsize);
	} else {
	    *scale1 = f2cmin(ascale,bsize) * wscale * f2cmax(ascale,bsize);
	}
	*wr1 *= wscale;
	if (*wi != 0.) {
	    *wi *= wscale;
	    *wr2 = *wr1;
	    *scale2 = *scale1;
	}
    } else {
	*scale1 = ascale * bsize;
	*scale2 = *scale1;
    }

/*     Scale second eigenvalue (if real) */

    if (*wi == 0.) {
/* Computing MAX */
/* Computing MIN */
/* Computing MAX */
	d__5 = abs(*wr2);
	d__3 = c4, d__4 = f2cmax(d__5,c5) * .5;
	d__1 = f2cmax(*safmin,c1), d__2 = (abs(*wr2) * c2 + c3) * 
		1.0000100000000001, d__1 = f2cmax(d__1,d__2), d__2 = f2cmin(d__3,
		d__4);
	wsize = f2cmax(d__1,d__2);
	if (wsize != 1.) {
	    wscale = 1. / wsize;
	    if (wsize > 1.) {
		*scale2 = f2cmax(ascale,bsize) * wscale * f2cmin(ascale,bsize);
	    } else {
		*scale2 = f2cmin(ascale,bsize) * wscale * f2cmax(ascale,bsize);
	    }
	    *wr2 *= wscale;
	} else {
	    *scale2 = ascale * bsize;
	}
    }

/*     End of DLAG2 */

    return;
} /* dlag2_ */

