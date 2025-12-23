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




/* Table of constant values */

static doublereal c_b29 = 1.;
static doublereal c_b30 = 0.;
static doublereal c_b33 = -1.;

/* > \brief \b DLATM5 */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DLATM5( PRTYPE, M, N, A, LDA, B, LDB, C, LDC, D, LDD, */
/*                          E, LDE, F, LDF, R, LDR, L, LDL, ALPHA, QBLCKA, */
/*                          QBLCKB ) */

/*       INTEGER            LDA, LDB, LDC, LDD, LDE, LDF, LDL, LDR, M, N, */
/*      $                   PRTYPE, QBLCKA, QBLCKB */
/*       DOUBLE PRECISION   ALPHA */
/*       DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), C( LDC, * ), */
/*      $                   D( LDD, * ), E( LDE, * ), F( LDF, * ), */
/*      $                   L( LDL, * ), R( LDR, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DLATM5 generates matrices involved in the Generalized Sylvester */
/* > equation: */
/* > */
/* >     A * R - L * B = C */
/* >     D * R - L * E = F */
/* > */
/* > They also satisfy (the diagonalization condition) */
/* > */
/* >  [ I -L ] ( [ A  -C ], [ D -F ] ) [ I  R ] = ( [ A    ], [ D    ] ) */
/* >  [    I ] ( [     B ]  [    E ] ) [    I ]   ( [    B ]  [    E ] ) */
/* > */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] PRTYPE */
/* > \verbatim */
/* >          PRTYPE is INTEGER */
/* >          "Points" to a certain type of the matrices to generate */
/* >          (see further details). */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          Specifies the order of A and D and the number of rows in */
/* >          C, F,  R and L. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          Specifies the order of B and E and the number of columns in */
/* >          C, F, R and L. */
/* > \endverbatim */
/* > */
/* > \param[out] A */
/* > \verbatim */
/* >          A is DOUBLE PRECISION array, dimension (LDA, M). */
/* >          On exit A M-by-M is initialized according to PRTYPE. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of A. */
/* > \endverbatim */
/* > */
/* > \param[out] B */
/* > \verbatim */
/* >          B is DOUBLE PRECISION array, dimension (LDB, N). */
/* >          On exit B N-by-N is initialized according to PRTYPE. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of B. */
/* > \endverbatim */
/* > */
/* > \param[out] C */
/* > \verbatim */
/* >          C is DOUBLE PRECISION array, dimension (LDC, N). */
/* >          On exit C M-by-N is initialized according to PRTYPE. */
/* > \endverbatim */
/* > */
/* > \param[in] LDC */
/* > \verbatim */
/* >          LDC is INTEGER */
/* >          The leading dimension of C. */
/* > \endverbatim */
/* > */
/* > \param[out] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array, dimension (LDD, M). */
/* >          On exit D M-by-M is initialized according to PRTYPE. */
/* > \endverbatim */
/* > */
/* > \param[in] LDD */
/* > \verbatim */
/* >          LDD is INTEGER */
/* >          The leading dimension of D. */
/* > \endverbatim */
/* > */
/* > \param[out] E */
/* > \verbatim */
/* >          E is DOUBLE PRECISION array, dimension (LDE, N). */
/* >          On exit E N-by-N is initialized according to PRTYPE. */
/* > \endverbatim */
/* > */
/* > \param[in] LDE */
/* > \verbatim */
/* >          LDE is INTEGER */
/* >          The leading dimension of E. */
/* > \endverbatim */
/* > */
/* > \param[out] F */
/* > \verbatim */
/* >          F is DOUBLE PRECISION array, dimension (LDF, N). */
/* >          On exit F M-by-N is initialized according to PRTYPE. */
/* > \endverbatim */
/* > */
/* > \param[in] LDF */
/* > \verbatim */
/* >          LDF is INTEGER */
/* >          The leading dimension of F. */
/* > \endverbatim */
/* > */
/* > \param[out] R */
/* > \verbatim */
/* >          R is DOUBLE PRECISION array, dimension (LDR, N). */
/* >          On exit R M-by-N is initialized according to PRTYPE. */
/* > \endverbatim */
/* > */
/* > \param[in] LDR */
/* > \verbatim */
/* >          LDR is INTEGER */
/* >          The leading dimension of R. */
/* > \endverbatim */
/* > */
/* > \param[out] L */
/* > \verbatim */
/* >          L is DOUBLE PRECISION array, dimension (LDL, N). */
/* >          On exit L M-by-N is initialized according to PRTYPE. */
/* > \endverbatim */
/* > */
/* > \param[in] LDL */
/* > \verbatim */
/* >          LDL is INTEGER */
/* >          The leading dimension of L. */
/* > \endverbatim */
/* > */
/* > \param[in] ALPHA */
/* > \verbatim */
/* >          ALPHA is DOUBLE PRECISION */
/* >          Parameter used in generating PRTYPE = 1 and 5 matrices. */
/* > \endverbatim */
/* > */
/* > \param[in] QBLCKA */
/* > \verbatim */
/* >          QBLCKA is INTEGER */
/* >          When PRTYPE = 3, specifies the distance between 2-by-2 */
/* >          blocks on the diagonal in A. Otherwise, QBLCKA is not */
/* >          referenced. QBLCKA > 1. */
/* > \endverbatim */
/* > */
/* > \param[in] QBLCKB */
/* > \verbatim */
/* >          QBLCKB is INTEGER */
/* >          When PRTYPE = 3, specifies the distance between 2-by-2 */
/* >          blocks on the diagonal in B. Otherwise, QBLCKB is not */
/* >          referenced. QBLCKB > 1. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup double_matgen */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  PRTYPE = 1: A and B are Jordan blocks, D and E are identity matrices */
/* > */
/* >             A : if (i == j) then A(i, j) = 1.0 */
/* >                 if (j == i + 1) then A(i, j) = -1.0 */
/* >                 else A(i, j) = 0.0,            i, j = 1...M */
/* > */
/* >             B : if (i == j) then B(i, j) = 1.0 - ALPHA */
/* >                 if (j == i + 1) then B(i, j) = 1.0 */
/* >                 else B(i, j) = 0.0,            i, j = 1...N */
/* > */
/* >             D : if (i == j) then D(i, j) = 1.0 */
/* >                 else D(i, j) = 0.0,            i, j = 1...M */
/* > */
/* >             E : if (i == j) then E(i, j) = 1.0 */
/* >                 else E(i, j) = 0.0,            i, j = 1...N */
/* > */
/* >             L =  R are chosen from [-10...10], */
/* >                  which specifies the right hand sides (C, F). */
/* > */
/* >  PRTYPE = 2 or 3: Triangular and/or quasi- triangular. */
/* > */
/* >             A : if (i <= j) then A(i, j) = [-1...1] */
/* >                 else A(i, j) = 0.0,             i, j = 1...M */
/* > */
/* >                 if (PRTYPE = 3) then */
/* >                    A(k + 1, k + 1) = A(k, k) */
/* >                    A(k + 1, k) = [-1...1] */
/* >                    sign(A(k, k + 1) = -(sin(A(k + 1, k)) */
/* >                        k = 1, M - 1, QBLCKA */
/* > */
/* >             B : if (i <= j) then B(i, j) = [-1...1] */
/* >                 else B(i, j) = 0.0,            i, j = 1...N */
/* > */
/* >                 if (PRTYPE = 3) then */
/* >                    B(k + 1, k + 1) = B(k, k) */
/* >                    B(k + 1, k) = [-1...1] */
/* >                    sign(B(k, k + 1) = -(sign(B(k + 1, k)) */
/* >                        k = 1, N - 1, QBLCKB */
/* > */
/* >             D : if (i <= j) then D(i, j) = [-1...1]. */
/* >                 else D(i, j) = 0.0,            i, j = 1...M */
/* > */
/* > */
/* >             E : if (i <= j) then D(i, j) = [-1...1] */
/* >                 else E(i, j) = 0.0,            i, j = 1...N */
/* > */
/* >                 L, R are chosen from [-10...10], */
/* >                 which specifies the right hand sides (C, F). */
/* > */
/* >  PRTYPE = 4 Full */
/* >             A(i, j) = [-10...10] */
/* >             D(i, j) = [-1...1]    i,j = 1...M */
/* >             B(i, j) = [-10...10] */
/* >             E(i, j) = [-1...1]    i,j = 1...N */
/* >             R(i, j) = [-10...10] */
/* >             L(i, j) = [-1...1]    i = 1..M ,j = 1...N */
/* > */
/* >             L, R specifies the right hand sides (C, F). */
/* > */
/* >  PRTYPE = 5 special case common and/or close eigs. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void dlatm5_(integer *prtype, integer *m, integer *n, 
	doublereal *a, integer *lda, doublereal *b, integer *ldb, doublereal *
	c__, integer *ldc, doublereal *d__, integer *ldd, doublereal *e, 
	integer *lde, doublereal *f, integer *ldf, doublereal *r__, integer *
	ldr, doublereal *l, integer *ldl, doublereal *alpha, integer *qblcka, 
	integer *qblckb)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, d_dim1, 
	    d_offset, e_dim1, e_offset, f_dim1, f_offset, l_dim1, l_offset, 
	    r_dim1, r_offset, i__1, i__2;

    /* Local variables */
    integer i__, j, k;
    extern /* Subroutine */ void dgemm_(char *, char *, integer *, integer *, 
	    integer *, doublereal *, doublereal *, integer *, doublereal *, 
	    integer *, doublereal *, doublereal *, integer *);
    doublereal imeps, reeps;


/*  -- LAPACK computational routine (version 3.7.0) -- */
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
    c_dim1 = *ldc;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    d_dim1 = *ldd;
    d_offset = 1 + d_dim1 * 1;
    d__ -= d_offset;
    e_dim1 = *lde;
    e_offset = 1 + e_dim1 * 1;
    e -= e_offset;
    f_dim1 = *ldf;
    f_offset = 1 + f_dim1 * 1;
    f -= f_offset;
    r_dim1 = *ldr;
    r_offset = 1 + r_dim1 * 1;
    r__ -= r_offset;
    l_dim1 = *ldl;
    l_offset = 1 + l_dim1 * 1;
    l -= l_offset;

    /* Function Body */
    if (*prtype == 1) {
	i__1 = *m;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = *m;
	    for (j = 1; j <= i__2; ++j) {
		if (i__ == j) {
		    a[i__ + j * a_dim1] = 1.;
		    d__[i__ + j * d_dim1] = 1.;
		} else if (i__ == j - 1) {
		    a[i__ + j * a_dim1] = -1.;
		    d__[i__ + j * d_dim1] = 0.;
		} else {
		    a[i__ + j * a_dim1] = 0.;
		    d__[i__ + j * d_dim1] = 0.;
		}
/* L10: */
	    }
/* L20: */
	}

	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = *n;
	    for (j = 1; j <= i__2; ++j) {
		if (i__ == j) {
		    b[i__ + j * b_dim1] = 1. - *alpha;
		    e[i__ + j * e_dim1] = 1.;
		} else if (i__ == j - 1) {
		    b[i__ + j * b_dim1] = 1.;
		    e[i__ + j * e_dim1] = 0.;
		} else {
		    b[i__ + j * b_dim1] = 0.;
		    e[i__ + j * e_dim1] = 0.;
		}
/* L30: */
	    }
/* L40: */
	}

	i__1 = *m;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = *n;
	    for (j = 1; j <= i__2; ++j) {
		r__[i__ + j * r_dim1] = (.5 - sin((doublereal) (i__ / j))) * 
			20.;
		l[i__ + j * l_dim1] = r__[i__ + j * r_dim1];
/* L50: */
	    }
/* L60: */
	}

    } else if (*prtype == 2 || *prtype == 3) {
	i__1 = *m;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = *m;
	    for (j = 1; j <= i__2; ++j) {
		if (i__ <= j) {
		    a[i__ + j * a_dim1] = (.5 - sin((doublereal) i__)) * 2.;
		    d__[i__ + j * d_dim1] = (.5 - sin((doublereal) (i__ * j)))
			     * 2.;
		} else {
		    a[i__ + j * a_dim1] = 0.;
		    d__[i__ + j * d_dim1] = 0.;
		}
/* L70: */
	    }
/* L80: */
	}

	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = *n;
	    for (j = 1; j <= i__2; ++j) {
		if (i__ <= j) {
		    b[i__ + j * b_dim1] = (.5 - sin((doublereal) (i__ + j))) *
			     2.;
		    e[i__ + j * e_dim1] = (.5 - sin((doublereal) j)) * 2.;
		} else {
		    b[i__ + j * b_dim1] = 0.;
		    e[i__ + j * e_dim1] = 0.;
		}
/* L90: */
	    }
/* L100: */
	}

	i__1 = *m;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = *n;
	    for (j = 1; j <= i__2; ++j) {
		r__[i__ + j * r_dim1] = (.5 - sin((doublereal) (i__ * j))) * 
			20.;
		l[i__ + j * l_dim1] = (.5 - sin((doublereal) (i__ + j))) * 
			20.;
/* L110: */
	    }
/* L120: */
	}

	if (*prtype == 3) {
	    if (*qblcka <= 1) {
		*qblcka = 2;
	    }
	    i__1 = *m - 1;
	    i__2 = *qblcka;
	    for (k = 1; i__2 < 0 ? k >= i__1 : k <= i__1; k += i__2) {
		a[k + 1 + (k + 1) * a_dim1] = a[k + k * a_dim1];
		a[k + 1 + k * a_dim1] = -sin(a[k + (k + 1) * a_dim1]);
/* L130: */
	    }

	    if (*qblckb <= 1) {
		*qblckb = 2;
	    }
	    i__2 = *n - 1;
	    i__1 = *qblckb;
	    for (k = 1; i__1 < 0 ? k >= i__2 : k <= i__2; k += i__1) {
		b[k + 1 + (k + 1) * b_dim1] = b[k + k * b_dim1];
		b[k + 1 + k * b_dim1] = -sin(b[k + (k + 1) * b_dim1]);
/* L140: */
	    }
	}

    } else if (*prtype == 4) {
	i__1 = *m;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = *m;
	    for (j = 1; j <= i__2; ++j) {
		a[i__ + j * a_dim1] = (.5 - sin((doublereal) (i__ * j))) * 
			20.;
		d__[i__ + j * d_dim1] = (.5 - sin((doublereal) (i__ + j))) * 
			2.;
/* L150: */
	    }
/* L160: */
	}

	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = *n;
	    for (j = 1; j <= i__2; ++j) {
		b[i__ + j * b_dim1] = (.5 - sin((doublereal) (i__ + j))) * 
			20.;
		e[i__ + j * e_dim1] = (.5 - sin((doublereal) (i__ * j))) * 2.;
/* L170: */
	    }
/* L180: */
	}

	i__1 = *m;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = *n;
	    for (j = 1; j <= i__2; ++j) {
		r__[i__ + j * r_dim1] = (.5 - sin((doublereal) (j / i__))) * 
			20.;
		l[i__ + j * l_dim1] = (.5 - sin((doublereal) (i__ * j))) * 2.;
/* L190: */
	    }
/* L200: */
	}

    } else if (*prtype >= 5) {
	reeps = 20. / *alpha;
	imeps = -1.5 / *alpha;
	i__1 = *m;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = *n;
	    for (j = 1; j <= i__2; ++j) {
		r__[i__ + j * r_dim1] = (.5 - sin((doublereal) (i__ * j))) * *
			alpha / 20.;
		l[i__ + j * l_dim1] = (.5 - sin((doublereal) (i__ + j))) * *
			alpha / 20.;
/* L210: */
	    }
/* L220: */
	}

	i__1 = *m;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    d__[i__ + i__ * d_dim1] = 1.;
/* L230: */
	}

	i__1 = *m;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    if (i__ <= 4) {
		a[i__ + i__ * a_dim1] = 1.;
		if (i__ > 2) {
		    a[i__ + i__ * a_dim1] = reeps + 1.;
		}
		if (i__ % 2 != 0 && i__ < *m) {
		    a[i__ + (i__ + 1) * a_dim1] = imeps;
		} else if (i__ > 1) {
		    a[i__ + (i__ - 1) * a_dim1] = -imeps;
		}
	    } else if (i__ <= 8) {
		if (i__ <= 6) {
		    a[i__ + i__ * a_dim1] = reeps;
		} else {
		    a[i__ + i__ * a_dim1] = -reeps;
		}
		if (i__ % 2 != 0 && i__ < *m) {
		    a[i__ + (i__ + 1) * a_dim1] = 1.;
		} else if (i__ > 1) {
		    a[i__ + (i__ - 1) * a_dim1] = -1.;
		}
	    } else {
		a[i__ + i__ * a_dim1] = 1.;
		if (i__ % 2 != 0 && i__ < *m) {
		    a[i__ + (i__ + 1) * a_dim1] = imeps * 2;
		} else if (i__ > 1) {
		    a[i__ + (i__ - 1) * a_dim1] = -imeps * 2;
		}
	    }
/* L240: */
	}

	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    e[i__ + i__ * e_dim1] = 1.;
	    if (i__ <= 4) {
		b[i__ + i__ * b_dim1] = -1.;
		if (i__ > 2) {
		    b[i__ + i__ * b_dim1] = 1. - reeps;
		}
		if (i__ % 2 != 0 && i__ < *n) {
		    b[i__ + (i__ + 1) * b_dim1] = imeps;
		} else if (i__ > 1) {
		    b[i__ + (i__ - 1) * b_dim1] = -imeps;
		}
	    } else if (i__ <= 8) {
		if (i__ <= 6) {
		    b[i__ + i__ * b_dim1] = reeps;
		} else {
		    b[i__ + i__ * b_dim1] = -reeps;
		}
		if (i__ % 2 != 0 && i__ < *n) {
		    b[i__ + (i__ + 1) * b_dim1] = imeps + 1.;
		} else if (i__ > 1) {
		    b[i__ + (i__ - 1) * b_dim1] = -1. - imeps;
		}
	    } else {
		b[i__ + i__ * b_dim1] = 1. - reeps;
		if (i__ % 2 != 0 && i__ < *n) {
		    b[i__ + (i__ + 1) * b_dim1] = imeps * 2;
		} else if (i__ > 1) {
		    b[i__ + (i__ - 1) * b_dim1] = -imeps * 2;
		}
	    }
/* L250: */
	}
    }

/*     Compute rhs (C, F) */

    dgemm_("N", "N", m, n, m, &c_b29, &a[a_offset], lda, &r__[r_offset], ldr, 
	    &c_b30, &c__[c_offset], ldc);
    dgemm_("N", "N", m, n, n, &c_b33, &l[l_offset], ldl, &b[b_offset], ldb, &
	    c_b29, &c__[c_offset], ldc);
    dgemm_("N", "N", m, n, m, &c_b29, &d__[d_offset], ldd, &r__[r_offset], 
	    ldr, &c_b30, &f[f_offset], ldf);
    dgemm_("N", "N", m, n, n, &c_b33, &l[l_offset], ldl, &e[e_offset], lde, &
	    c_b29, &f[f_offset], ldf);

/*     End of DLATM5 */

    return;
} /* dlatm5_ */

