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




/* > \brief \b SLAEBZ computes the number of eigenvalues of a real symmetric tridiagonal matrix which are less
 than or equal to a given value, and performs other tasks required by the routine sstebz. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SLAEBZ + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaebz.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaebz.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaebz.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SLAEBZ( IJOB, NITMAX, N, MMAX, MINP, NBMIN, ABSTOL, */
/*                          RELTOL, PIVMIN, D, E, E2, NVAL, AB, C, MOUT, */
/*                          NAB, WORK, IWORK, INFO ) */

/*       INTEGER            IJOB, INFO, MINP, MMAX, MOUT, N, NBMIN, NITMAX */
/*       REAL               ABSTOL, PIVMIN, RELTOL */
/*       INTEGER            IWORK( * ), NAB( MMAX, * ), NVAL( * ) */
/*       REAL               AB( MMAX, * ), C( * ), D( * ), E( * ), E2( * ), */
/*      $                   WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SLAEBZ contains the iteration loops which compute and use the */
/* > function N(w), which is the count of eigenvalues of a symmetric */
/* > tridiagonal matrix T less than or equal to its argument  w.  It */
/* > performs a choice of two types of loops: */
/* > */
/* > IJOB=1, followed by */
/* > IJOB=2: It takes as input a list of intervals and returns a list of */
/* >         sufficiently small intervals whose union contains the same */
/* >         eigenvalues as the union of the original intervals. */
/* >         The input intervals are (AB(j,1),AB(j,2)], j=1,...,MINP. */
/* >         The output interval (AB(j,1),AB(j,2)] will contain */
/* >         eigenvalues NAB(j,1)+1,...,NAB(j,2), where 1 <= j <= MOUT. */
/* > */
/* > IJOB=3: It performs a binary search in each input interval */
/* >         (AB(j,1),AB(j,2)] for a point  w(j)  such that */
/* >         N(w(j))=NVAL(j), and uses  C(j)  as the starting point of */
/* >         the search.  If such a w(j) is found, then on output */
/* >         AB(j,1)=AB(j,2)=w.  If no such w(j) is found, then on output */
/* >         (AB(j,1),AB(j,2)] will be a small interval containing the */
/* >         point where N(w) jumps through NVAL(j), unless that point */
/* >         lies outside the initial interval. */
/* > */
/* > Note that the intervals are in all cases half-open intervals, */
/* > i.e., of the form  (a,b] , which includes  b  but not  a . */
/* > */
/* > To avoid underflow, the matrix should be scaled so that its largest */
/* > element is no greater than  overflow**(1/2) * underflow**(1/4) */
/* > in absolute value.  To assure the most accurate computation */
/* > of small eigenvalues, the matrix should be scaled to be */
/* > not much smaller than that, either. */
/* > */
/* > See W. Kahan "Accurate Eigenvalues of a Symmetric Tridiagonal */
/* > Matrix", Report CS41, Computer Science Dept., Stanford */
/* > University, July 21, 1966 */
/* > */
/* > Note: the arguments are, in general, *not* checked for unreasonable */
/* > values. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] IJOB */
/* > \verbatim */
/* >          IJOB is INTEGER */
/* >          Specifies what is to be done: */
/* >          = 1:  Compute NAB for the initial intervals. */
/* >          = 2:  Perform bisection iteration to find eigenvalues of T. */
/* >          = 3:  Perform bisection iteration to invert N(w), i.e., */
/* >                to find a point which has a specified number of */
/* >                eigenvalues of T to its left. */
/* >          Other values will cause SLAEBZ to return with INFO=-1. */
/* > \endverbatim */
/* > */
/* > \param[in] NITMAX */
/* > \verbatim */
/* >          NITMAX is INTEGER */
/* >          The maximum number of "levels" of bisection to be */
/* >          performed, i.e., an interval of width W will not be made */
/* >          smaller than 2^(-NITMAX) * W.  If not all intervals */
/* >          have converged after NITMAX iterations, then INFO is set */
/* >          to the number of non-converged intervals. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The dimension n of the tridiagonal matrix T.  It must be at */
/* >          least 1. */
/* > \endverbatim */
/* > */
/* > \param[in] MMAX */
/* > \verbatim */
/* >          MMAX is INTEGER */
/* >          The maximum number of intervals.  If more than MMAX intervals */
/* >          are generated, then SLAEBZ will quit with INFO=MMAX+1. */
/* > \endverbatim */
/* > */
/* > \param[in] MINP */
/* > \verbatim */
/* >          MINP is INTEGER */
/* >          The initial number of intervals.  It may not be greater than */
/* >          MMAX. */
/* > \endverbatim */
/* > */
/* > \param[in] NBMIN */
/* > \verbatim */
/* >          NBMIN is INTEGER */
/* >          The smallest number of intervals that should be processed */
/* >          using a vector loop.  If zero, then only the scalar loop */
/* >          will be used. */
/* > \endverbatim */
/* > */
/* > \param[in] ABSTOL */
/* > \verbatim */
/* >          ABSTOL is REAL */
/* >          The minimum (absolute) width of an interval.  When an */
/* >          interval is narrower than ABSTOL, or than RELTOL times the */
/* >          larger (in magnitude) endpoint, then it is considered to be */
/* >          sufficiently small, i.e., converged.  This must be at least */
/* >          zero. */
/* > \endverbatim */
/* > */
/* > \param[in] RELTOL */
/* > \verbatim */
/* >          RELTOL is REAL */
/* >          The minimum relative width of an interval.  When an interval */
/* >          is narrower than ABSTOL, or than RELTOL times the larger (in */
/* >          magnitude) endpoint, then it is considered to be */
/* >          sufficiently small, i.e., converged.  Note: this should */
/* >          always be at least radix*machine epsilon. */
/* > \endverbatim */
/* > */
/* > \param[in] PIVMIN */
/* > \verbatim */
/* >          PIVMIN is REAL */
/* >          The minimum absolute value of a "pivot" in the Sturm */
/* >          sequence loop. */
/* >          This must be at least  f2cmax |e(j)**2|*safe_min  and at */
/* >          least safe_min, where safe_min is at least */
/* >          the smallest number that can divide one without overflow. */
/* > \endverbatim */
/* > */
/* > \param[in] D */
/* > \verbatim */
/* >          D is REAL array, dimension (N) */
/* >          The diagonal elements of the tridiagonal matrix T. */
/* > \endverbatim */
/* > */
/* > \param[in] E */
/* > \verbatim */
/* >          E is REAL array, dimension (N) */
/* >          The offdiagonal elements of the tridiagonal matrix T in */
/* >          positions 1 through N-1.  E(N) is arbitrary. */
/* > \endverbatim */
/* > */
/* > \param[in] E2 */
/* > \verbatim */
/* >          E2 is REAL array, dimension (N) */
/* >          The squares of the offdiagonal elements of the tridiagonal */
/* >          matrix T.  E2(N) is ignored. */
/* > \endverbatim */
/* > */
/* > \param[in,out] NVAL */
/* > \verbatim */
/* >          NVAL is INTEGER array, dimension (MINP) */
/* >          If IJOB=1 or 2, not referenced. */
/* >          If IJOB=3, the desired values of N(w).  The elements of NVAL */
/* >          will be reordered to correspond with the intervals in AB. */
/* >          Thus, NVAL(j) on output will not, in general be the same as */
/* >          NVAL(j) on input, but it will correspond with the interval */
/* >          (AB(j,1),AB(j,2)] on output. */
/* > \endverbatim */
/* > */
/* > \param[in,out] AB */
/* > \verbatim */
/* >          AB is REAL array, dimension (MMAX,2) */
/* >          The endpoints of the intervals.  AB(j,1) is  a(j), the left */
/* >          endpoint of the j-th interval, and AB(j,2) is b(j), the */
/* >          right endpoint of the j-th interval.  The input intervals */
/* >          will, in general, be modified, split, and reordered by the */
/* >          calculation. */
/* > \endverbatim */
/* > */
/* > \param[in,out] C */
/* > \verbatim */
/* >          C is REAL array, dimension (MMAX) */
/* >          If IJOB=1, ignored. */
/* >          If IJOB=2, workspace. */
/* >          If IJOB=3, then on input C(j) should be initialized to the */
/* >          first search point in the binary search. */
/* > \endverbatim */
/* > */
/* > \param[out] MOUT */
/* > \verbatim */
/* >          MOUT is INTEGER */
/* >          If IJOB=1, the number of eigenvalues in the intervals. */
/* >          If IJOB=2 or 3, the number of intervals output. */
/* >          If IJOB=3, MOUT will equal MINP. */
/* > \endverbatim */
/* > */
/* > \param[in,out] NAB */
/* > \verbatim */
/* >          NAB is INTEGER array, dimension (MMAX,2) */
/* >          If IJOB=1, then on output NAB(i,j) will be set to N(AB(i,j)). */
/* >          If IJOB=2, then on input, NAB(i,j) should be set.  It must */
/* >             satisfy the condition: */
/* >             N(AB(i,1)) <= NAB(i,1) <= NAB(i,2) <= N(AB(i,2)), */
/* >             which means that in interval i only eigenvalues */
/* >             NAB(i,1)+1,...,NAB(i,2) will be considered.  Usually, */
/* >             NAB(i,j)=N(AB(i,j)), from a previous call to SLAEBZ with */
/* >             IJOB=1. */
/* >             On output, NAB(i,j) will contain */
/* >             f2cmax(na(k),f2cmin(nb(k),N(AB(i,j)))), where k is the index of */
/* >             the input interval that the output interval */
/* >             (AB(j,1),AB(j,2)] came from, and na(k) and nb(k) are the */
/* >             the input values of NAB(k,1) and NAB(k,2). */
/* >          If IJOB=3, then on output, NAB(i,j) contains N(AB(i,j)), */
/* >             unless N(w) > NVAL(i) for all search points  w , in which */
/* >             case NAB(i,1) will not be modified, i.e., the output */
/* >             value will be the same as the input value (modulo */
/* >             reorderings -- see NVAL and AB), or unless N(w) < NVAL(i) */
/* >             for all search points  w , in which case NAB(i,2) will */
/* >             not be modified.  Normally, NAB should be set to some */
/* >             distinctive value(s) before SLAEBZ is called. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (MMAX) */
/* >          Workspace. */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (MMAX) */
/* >          Workspace. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:       All intervals converged. */
/* >          = 1--MMAX: The last INFO intervals did not converge. */
/* >          = MMAX+1:  More than MMAX intervals were generated. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup OTHERauxiliary */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >      This routine is intended to be called only by other LAPACK */
/* >  routines, thus the interface is less user-friendly.  It is intended */
/* >  for two purposes: */
/* > */
/* >  (a) finding eigenvalues.  In this case, SLAEBZ should have one or */
/* >      more initial intervals set up in AB, and SLAEBZ should be called */
/* >      with IJOB=1.  This sets up NAB, and also counts the eigenvalues. */
/* >      Intervals with no eigenvalues would usually be thrown out at */
/* >      this point.  Also, if not all the eigenvalues in an interval i */
/* >      are desired, NAB(i,1) can be increased or NAB(i,2) decreased. */
/* >      For example, set NAB(i,1)=NAB(i,2)-1 to get the largest */
/* >      eigenvalue.  SLAEBZ is then called with IJOB=2 and MMAX */
/* >      no smaller than the value of MOUT returned by the call with */
/* >      IJOB=1.  After this (IJOB=2) call, eigenvalues NAB(i,1)+1 */
/* >      through NAB(i,2) are approximately AB(i,1) (or AB(i,2)) to the */
/* >      tolerance specified by ABSTOL and RELTOL. */
/* > */
/* >  (b) finding an interval (a',b'] containing eigenvalues w(f),...,w(l). */
/* >      In this case, start with a Gershgorin interval  (a,b).  Set up */
/* >      AB to contain 2 search intervals, both initially (a,b).  One */
/* >      NVAL element should contain  f-1  and the other should contain  l */
/* >      , while C should contain a and b, resp.  NAB(i,1) should be -1 */
/* >      and NAB(i,2) should be N+1, to flag an error if the desired */
/* >      interval does not lie in (a,b).  SLAEBZ is then called with */
/* >      IJOB=3.  On exit, if w(f-1) < w(f), then one of the intervals -- */
/* >      j -- will have AB(j,1)=AB(j,2) and NAB(j,1)=NAB(j,2)=f-1, while */
/* >      if, to the specified tolerance, w(f-k)=...=w(f+r), k > 0 and r */
/* >      >= 0, then the interval will have  N(AB(j,1))=NAB(j,1)=f-k and */
/* >      N(AB(j,2))=NAB(j,2)=f+r.  The cases w(l) < w(l+1) and */
/* >      w(l-r)=...=w(l+k) are handled similarly. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void slaebz_(integer *ijob, integer *nitmax, integer *n, 
	integer *mmax, integer *minp, integer *nbmin, real *abstol, real *
	reltol, real *pivmin, real *d__, real *e, real *e2, integer *nval, 
	real *ab, real *c__, integer *mout, integer *nab, real *work, integer 
	*iwork, integer *info)
{
    /* System generated locals */
    integer nab_dim1, nab_offset, ab_dim1, ab_offset, i__1, i__2, i__3, i__4, 
	    i__5, i__6;
    real r__1, r__2, r__3, r__4;

    /* Local variables */
    integer itmp1, itmp2, j, kfnew, klnew, kf, ji, kl, jp, jit;
    real tmp1, tmp2;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Check for Errors */

    /* Parameter adjustments */
    nab_dim1 = *mmax;
    nab_offset = 1 + nab_dim1 * 1;
    nab -= nab_offset;
    ab_dim1 = *mmax;
    ab_offset = 1 + ab_dim1 * 1;
    ab -= ab_offset;
    --d__;
    --e;
    --e2;
    --nval;
    --c__;
    --work;
    --iwork;

    /* Function Body */
    *info = 0;
    if (*ijob < 1 || *ijob > 3) {
	*info = -1;
	return;
    }

/*     Initialize NAB */

    if (*ijob == 1) {

/*        Compute the number of eigenvalues in the initial intervals. */

	*mout = 0;
	i__1 = *minp;
	for (ji = 1; ji <= i__1; ++ji) {
	    for (jp = 1; jp <= 2; ++jp) {
		tmp1 = d__[1] - ab[ji + jp * ab_dim1];
		if (abs(tmp1) < *pivmin) {
		    tmp1 = -(*pivmin);
		}
		nab[ji + jp * nab_dim1] = 0;
		if (tmp1 <= 0.f) {
		    nab[ji + jp * nab_dim1] = 1;
		}

		i__2 = *n;
		for (j = 2; j <= i__2; ++j) {
		    tmp1 = d__[j] - e2[j - 1] / tmp1 - ab[ji + jp * ab_dim1];
		    if (abs(tmp1) < *pivmin) {
			tmp1 = -(*pivmin);
		    }
		    if (tmp1 <= 0.f) {
			++nab[ji + jp * nab_dim1];
		    }
/* L10: */
		}
/* L20: */
	    }
	    *mout = *mout + nab[ji + (nab_dim1 << 1)] - nab[ji + nab_dim1];
/* L30: */
	}
	return;
    }

/*     Initialize for loop */

/*     KF and KL have the following meaning: */
/*        Intervals 1,...,KF-1 have converged. */
/*        Intervals KF,...,KL  still need to be refined. */

    kf = 1;
    kl = *minp;

/*     If IJOB=2, initialize C. */
/*     If IJOB=3, use the user-supplied starting point. */

    if (*ijob == 2) {
	i__1 = *minp;
	for (ji = 1; ji <= i__1; ++ji) {
	    c__[ji] = (ab[ji + ab_dim1] + ab[ji + (ab_dim1 << 1)]) * .5f;
/* L40: */
	}
    }

/*     Iteration loop */

    i__1 = *nitmax;
    for (jit = 1; jit <= i__1; ++jit) {

/*        Loop over intervals */

	if (kl - kf + 1 >= *nbmin && *nbmin > 0) {

/*           Begin of Parallel Version of the loop */

	    i__2 = kl;
	    for (ji = kf; ji <= i__2; ++ji) {

/*              Compute N(c), the number of eigenvalues less than c */

		work[ji] = d__[1] - c__[ji];
		iwork[ji] = 0;
		if (work[ji] <= *pivmin) {
		    iwork[ji] = 1;
/* Computing MIN */
		    r__1 = work[ji], r__2 = -(*pivmin);
		    work[ji] = f2cmin(r__1,r__2);
		}

		i__3 = *n;
		for (j = 2; j <= i__3; ++j) {
		    work[ji] = d__[j] - e2[j - 1] / work[ji] - c__[ji];
		    if (work[ji] <= *pivmin) {
			++iwork[ji];
/* Computing MIN */
			r__1 = work[ji], r__2 = -(*pivmin);
			work[ji] = f2cmin(r__1,r__2);
		    }
/* L50: */
		}
/* L60: */
	    }

	    if (*ijob <= 2) {

/*              IJOB=2: Choose all intervals containing eigenvalues. */

		klnew = kl;
		i__2 = kl;
		for (ji = kf; ji <= i__2; ++ji) {

/*                 Insure that N(w) is monotone */

/* Computing MIN */
/* Computing MAX */
		    i__5 = nab[ji + nab_dim1], i__6 = iwork[ji];
		    i__3 = nab[ji + (nab_dim1 << 1)], i__4 = f2cmax(i__5,i__6);
		    iwork[ji] = f2cmin(i__3,i__4);

/*                 Update the Queue -- add intervals if both halves */
/*                 contain eigenvalues. */

		    if (iwork[ji] == nab[ji + (nab_dim1 << 1)]) {

/*                    No eigenvalue in the upper interval: */
/*                    just use the lower interval. */

			ab[ji + (ab_dim1 << 1)] = c__[ji];

		    } else if (iwork[ji] == nab[ji + nab_dim1]) {

/*                    No eigenvalue in the lower interval: */
/*                    just use the upper interval. */

			ab[ji + ab_dim1] = c__[ji];
		    } else {
			++klnew;
			if (klnew <= *mmax) {

/*                       Eigenvalue in both intervals -- add upper to */
/*                       queue. */

			    ab[klnew + (ab_dim1 << 1)] = ab[ji + (ab_dim1 << 
				    1)];
			    nab[klnew + (nab_dim1 << 1)] = nab[ji + (nab_dim1 
				    << 1)];
			    ab[klnew + ab_dim1] = c__[ji];
			    nab[klnew + nab_dim1] = iwork[ji];
			    ab[ji + (ab_dim1 << 1)] = c__[ji];
			    nab[ji + (nab_dim1 << 1)] = iwork[ji];
			} else {
			    *info = *mmax + 1;
			}
		    }
/* L70: */
		}
		if (*info != 0) {
		    return;
		}
		kl = klnew;
	    } else {

/*              IJOB=3: Binary search.  Keep only the interval containing */
/*                      w   s.t. N(w) = NVAL */

		i__2 = kl;
		for (ji = kf; ji <= i__2; ++ji) {
		    if (iwork[ji] <= nval[ji]) {
			ab[ji + ab_dim1] = c__[ji];
			nab[ji + nab_dim1] = iwork[ji];
		    }
		    if (iwork[ji] >= nval[ji]) {
			ab[ji + (ab_dim1 << 1)] = c__[ji];
			nab[ji + (nab_dim1 << 1)] = iwork[ji];
		    }
/* L80: */
		}
	    }

	} else {

/*           End of Parallel Version of the loop */

/*           Begin of Serial Version of the loop */

	    klnew = kl;
	    i__2 = kl;
	    for (ji = kf; ji <= i__2; ++ji) {

/*              Compute N(w), the number of eigenvalues less than w */

		tmp1 = c__[ji];
		tmp2 = d__[1] - tmp1;
		itmp1 = 0;
		if (tmp2 <= *pivmin) {
		    itmp1 = 1;
/* Computing MIN */
		    r__1 = tmp2, r__2 = -(*pivmin);
		    tmp2 = f2cmin(r__1,r__2);
		}

		i__3 = *n;
		for (j = 2; j <= i__3; ++j) {
		    tmp2 = d__[j] - e2[j - 1] / tmp2 - tmp1;
		    if (tmp2 <= *pivmin) {
			++itmp1;
/* Computing MIN */
			r__1 = tmp2, r__2 = -(*pivmin);
			tmp2 = f2cmin(r__1,r__2);
		    }
/* L90: */
		}

		if (*ijob <= 2) {

/*                 IJOB=2: Choose all intervals containing eigenvalues. */

/*                 Insure that N(w) is monotone */

/* Computing MIN */
/* Computing MAX */
		    i__5 = nab[ji + nab_dim1];
		    i__3 = nab[ji + (nab_dim1 << 1)], i__4 = f2cmax(i__5,itmp1);
		    itmp1 = f2cmin(i__3,i__4);

/*                 Update the Queue -- add intervals if both halves */
/*                 contain eigenvalues. */

		    if (itmp1 == nab[ji + (nab_dim1 << 1)]) {

/*                    No eigenvalue in the upper interval: */
/*                    just use the lower interval. */

			ab[ji + (ab_dim1 << 1)] = tmp1;

		    } else if (itmp1 == nab[ji + nab_dim1]) {

/*                    No eigenvalue in the lower interval: */
/*                    just use the upper interval. */

			ab[ji + ab_dim1] = tmp1;
		    } else if (klnew < *mmax) {

/*                    Eigenvalue in both intervals -- add upper to queue. */

			++klnew;
			ab[klnew + (ab_dim1 << 1)] = ab[ji + (ab_dim1 << 1)];
			nab[klnew + (nab_dim1 << 1)] = nab[ji + (nab_dim1 << 
				1)];
			ab[klnew + ab_dim1] = tmp1;
			nab[klnew + nab_dim1] = itmp1;
			ab[ji + (ab_dim1 << 1)] = tmp1;
			nab[ji + (nab_dim1 << 1)] = itmp1;
		    } else {
			*info = *mmax + 1;
			return;
		    }
		} else {

/*                 IJOB=3: Binary search.  Keep only the interval */
/*                         containing  w  s.t. N(w) = NVAL */

		    if (itmp1 <= nval[ji]) {
			ab[ji + ab_dim1] = tmp1;
			nab[ji + nab_dim1] = itmp1;
		    }
		    if (itmp1 >= nval[ji]) {
			ab[ji + (ab_dim1 << 1)] = tmp1;
			nab[ji + (nab_dim1 << 1)] = itmp1;
		    }
		}
/* L100: */
	    }
	    kl = klnew;

	}

/*        Check for convergence */

	kfnew = kf;
	i__2 = kl;
	for (ji = kf; ji <= i__2; ++ji) {
	    tmp1 = (r__1 = ab[ji + (ab_dim1 << 1)] - ab[ji + ab_dim1], abs(
		    r__1));
/* Computing MAX */
	    r__3 = (r__1 = ab[ji + (ab_dim1 << 1)], abs(r__1)), r__4 = (r__2 =
		     ab[ji + ab_dim1], abs(r__2));
	    tmp2 = f2cmax(r__3,r__4);
/* Computing MAX */
	    r__1 = f2cmax(*abstol,*pivmin), r__2 = *reltol * tmp2;
	    if (tmp1 < f2cmax(r__1,r__2) || nab[ji + nab_dim1] >= nab[ji + (
		    nab_dim1 << 1)]) {

/*              Converged -- Swap with position KFNEW, */
/*                           then increment KFNEW */

		if (ji > kfnew) {
		    tmp1 = ab[ji + ab_dim1];
		    tmp2 = ab[ji + (ab_dim1 << 1)];
		    itmp1 = nab[ji + nab_dim1];
		    itmp2 = nab[ji + (nab_dim1 << 1)];
		    ab[ji + ab_dim1] = ab[kfnew + ab_dim1];
		    ab[ji + (ab_dim1 << 1)] = ab[kfnew + (ab_dim1 << 1)];
		    nab[ji + nab_dim1] = nab[kfnew + nab_dim1];
		    nab[ji + (nab_dim1 << 1)] = nab[kfnew + (nab_dim1 << 1)];
		    ab[kfnew + ab_dim1] = tmp1;
		    ab[kfnew + (ab_dim1 << 1)] = tmp2;
		    nab[kfnew + nab_dim1] = itmp1;
		    nab[kfnew + (nab_dim1 << 1)] = itmp2;
		    if (*ijob == 3) {
			itmp1 = nval[ji];
			nval[ji] = nval[kfnew];
			nval[kfnew] = itmp1;
		    }
		}
		++kfnew;
	    }
/* L110: */
	}
	kf = kfnew;

/*        Choose Midpoints */

	i__2 = kl;
	for (ji = kf; ji <= i__2; ++ji) {
	    c__[ji] = (ab[ji + ab_dim1] + ab[ji + (ab_dim1 << 1)]) * .5f;
/* L120: */
	}

/*        If no more intervals to refine, quit. */

	if (kf > kl) {
	    goto L140;
	}
/* L130: */
    }

/*     Converged */

L140:
/* Computing MAX */
    i__1 = kl + 1 - kf;
    *info = f2cmax(i__1,0);
    *mout = kl;

    return;

/*     End of SLAEBZ */

} /* slaebz_ */

