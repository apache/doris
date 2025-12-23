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

/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/




/* Table of constant values */

static real c_b10 = 0.f;
static real c_b11 = 1.f;
static integer c__1 = 1;

/* > \brief \b SGGHRD */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SGGHRD + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgghrd.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgghrd.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgghrd.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SGGHRD( COMPQ, COMPZ, N, ILO, IHI, A, LDA, B, LDB, Q, */
/*                          LDQ, Z, LDZ, INFO ) */

/*       CHARACTER          COMPQ, COMPZ */
/*       INTEGER            IHI, ILO, INFO, LDA, LDB, LDQ, LDZ, N */
/*       REAL               A( LDA, * ), B( LDB, * ), Q( LDQ, * ), */
/*      $                   Z( LDZ, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SGGHRD reduces a pair of real matrices (A,B) to generalized upper */
/* > Hessenberg form using orthogonal transformations, where A is a */
/* > general matrix and B is upper triangular.  The form of the */
/* > generalized eigenvalue problem is */
/* >    A*x = lambda*B*x, */
/* > and B is typically made upper triangular by computing its QR */
/* > factorization and moving the orthogonal matrix Q to the left side */
/* > of the equation. */
/* > */
/* > This subroutine simultaneously reduces A to a Hessenberg matrix H: */
/* >    Q**T*A*Z = H */
/* > and transforms B to another upper triangular matrix T: */
/* >    Q**T*B*Z = T */
/* > in order to reduce the problem to its standard form */
/* >    H*y = lambda*T*y */
/* > where y = Z**T*x. */
/* > */
/* > The orthogonal matrices Q and Z are determined as products of Givens */
/* > rotations.  They may either be formed explicitly, or they may be */
/* > postmultiplied into input matrices Q1 and Z1, so that */
/* > */
/* >      Q1 * A * Z1**T = (Q1*Q) * H * (Z1*Z)**T */
/* > */
/* >      Q1 * B * Z1**T = (Q1*Q) * T * (Z1*Z)**T */
/* > */
/* > If Q1 is the orthogonal matrix from the QR factorization of B in the */
/* > original equation A*x = lambda*B*x, then SGGHRD reduces the original */
/* > problem to generalized Hessenberg form. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] COMPQ */
/* > \verbatim */
/* >          COMPQ is CHARACTER*1 */
/* >          = 'N': do not compute Q; */
/* >          = 'I': Q is initialized to the unit matrix, and the */
/* >                 orthogonal matrix Q is returned; */
/* >          = 'V': Q must contain an orthogonal matrix Q1 on entry, */
/* >                 and the product Q1*Q is returned. */
/* > \endverbatim */
/* > */
/* > \param[in] COMPZ */
/* > \verbatim */
/* >          COMPZ is CHARACTER*1 */
/* >          = 'N': do not compute Z; */
/* >          = 'I': Z is initialized to the unit matrix, and the */
/* >                 orthogonal matrix Z is returned; */
/* >          = 'V': Z must contain an orthogonal matrix Z1 on entry, */
/* >                 and the product Z1*Z is returned. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices A and B.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] ILO */
/* > \verbatim */
/* >          ILO is INTEGER */
/* > \endverbatim */
/* > */
/* > \param[in] IHI */
/* > \verbatim */
/* >          IHI is INTEGER */
/* > */
/* >          ILO and IHI mark the rows and columns of A which are to be */
/* >          reduced.  It is assumed that A is already upper triangular */
/* >          in rows and columns 1:ILO-1 and IHI+1:N.  ILO and IHI are */
/* >          normally set by a previous call to SGGBAL; otherwise they */
/* >          should be set to 1 and N respectively. */
/* >          1 <= ILO <= IHI <= N, if N > 0; ILO=1 and IHI=0, if N=0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is REAL array, dimension (LDA, N) */
/* >          On entry, the N-by-N general matrix to be reduced. */
/* >          On exit, the upper triangle and the first subdiagonal of A */
/* >          are overwritten with the upper Hessenberg matrix H, and the */
/* >          rest is set to zero. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is REAL array, dimension (LDB, N) */
/* >          On entry, the N-by-N upper triangular matrix B. */
/* >          On exit, the upper triangular matrix T = Q**T B Z.  The */
/* >          elements below the diagonal are set to zero. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B.  LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] Q */
/* > \verbatim */
/* >          Q is REAL array, dimension (LDQ, N) */
/* >          On entry, if COMPQ = 'V', the orthogonal matrix Q1, */
/* >          typically from the QR factorization of B. */
/* >          On exit, if COMPQ='I', the orthogonal matrix Q, and if */
/* >          COMPQ = 'V', the product Q1*Q. */
/* >          Not referenced if COMPQ='N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDQ */
/* > \verbatim */
/* >          LDQ is INTEGER */
/* >          The leading dimension of the array Q. */
/* >          LDQ >= N if COMPQ='V' or 'I'; LDQ >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Z */
/* > \verbatim */
/* >          Z is REAL array, dimension (LDZ, N) */
/* >          On entry, if COMPZ = 'V', the orthogonal matrix Z1. */
/* >          On exit, if COMPZ='I', the orthogonal matrix Z, and if */
/* >          COMPZ = 'V', the product Z1*Z. */
/* >          Not referenced if COMPZ='N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDZ */
/* > \verbatim */
/* >          LDZ is INTEGER */
/* >          The leading dimension of the array Z. */
/* >          LDZ >= N if COMPZ='V' or 'I'; LDZ >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup realOTHERcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  This routine reduces A to Hessenberg and B to triangular form by */
/* >  an unblocked reduction, as described in _Matrix_Computations_, */
/* >  by Golub and Van Loan (Johns Hopkins Press.) */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void sgghrd_(char *compq, char *compz, integer *n, integer *
	ilo, integer *ihi, real *a, integer *lda, real *b, integer *ldb, real 
	*q, integer *ldq, real *z__, integer *ldz, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, q_dim1, q_offset, z_dim1, 
	    z_offset, i__1, i__2, i__3;

    /* Local variables */
    integer jcol;
    real temp;
    integer jrow;
    extern /* Subroutine */ void srot_(integer *, real *, integer *, real *, 
	    integer *, real *, real *);
    real c__, s;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    integer icompq;
    extern /* Subroutine */ void slaset_(char *, integer *, integer *, real *, 
	    real *, real *, integer *), slartg_(real *, real *, real *
	    , real *, real *);
    integer icompz;
    logical ilq, ilz;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Decode COMPQ */

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

    /* Function Body */
    if (lsame_(compq, "N")) {
	ilq = FALSE_;
	icompq = 1;
    } else if (lsame_(compq, "V")) {
	ilq = TRUE_;
	icompq = 2;
    } else if (lsame_(compq, "I")) {
	ilq = TRUE_;
	icompq = 3;
    } else {
	icompq = 0;
    }

/*     Decode COMPZ */

    if (lsame_(compz, "N")) {
	ilz = FALSE_;
	icompz = 1;
    } else if (lsame_(compz, "V")) {
	ilz = TRUE_;
	icompz = 2;
    } else if (lsame_(compz, "I")) {
	ilz = TRUE_;
	icompz = 3;
    } else {
	icompz = 0;
    }

/*     Test the input parameters. */

    *info = 0;
    if (icompq <= 0) {
	*info = -1;
    } else if (icompz <= 0) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (*ilo < 1) {
	*info = -4;
    } else if (*ihi > *n || *ihi < *ilo - 1) {
	*info = -5;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -7;
    } else if (*ldb < f2cmax(1,*n)) {
	*info = -9;
    } else if (ilq && *ldq < *n || *ldq < 1) {
	*info = -11;
    } else if (ilz && *ldz < *n || *ldz < 1) {
	*info = -13;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SGGHRD", &i__1, (ftnlen)6);
	return;
    }

/*     Initialize Q and Z if desired. */

    if (icompq == 3) {
	slaset_("Full", n, n, &c_b10, &c_b11, &q[q_offset], ldq);
    }
    if (icompz == 3) {
	slaset_("Full", n, n, &c_b10, &c_b11, &z__[z_offset], ldz);
    }

/*     Quick return if possible */

    if (*n <= 1) {
	return;
    }

/*     Zero out lower triangle of B */

    i__1 = *n - 1;
    for (jcol = 1; jcol <= i__1; ++jcol) {
	i__2 = *n;
	for (jrow = jcol + 1; jrow <= i__2; ++jrow) {
	    b[jrow + jcol * b_dim1] = 0.f;
/* L10: */
	}
/* L20: */
    }

/*     Reduce A and B */

    i__1 = *ihi - 2;
    for (jcol = *ilo; jcol <= i__1; ++jcol) {

	i__2 = jcol + 2;
	for (jrow = *ihi; jrow >= i__2; --jrow) {

/*           Step 1: rotate rows JROW-1, JROW to kill A(JROW,JCOL) */

	    temp = a[jrow - 1 + jcol * a_dim1];
	    slartg_(&temp, &a[jrow + jcol * a_dim1], &c__, &s, &a[jrow - 1 + 
		    jcol * a_dim1]);
	    a[jrow + jcol * a_dim1] = 0.f;
	    i__3 = *n - jcol;
	    srot_(&i__3, &a[jrow - 1 + (jcol + 1) * a_dim1], lda, &a[jrow + (
		    jcol + 1) * a_dim1], lda, &c__, &s);
	    i__3 = *n + 2 - jrow;
	    srot_(&i__3, &b[jrow - 1 + (jrow - 1) * b_dim1], ldb, &b[jrow + (
		    jrow - 1) * b_dim1], ldb, &c__, &s);
	    if (ilq) {
		srot_(n, &q[(jrow - 1) * q_dim1 + 1], &c__1, &q[jrow * q_dim1 
			+ 1], &c__1, &c__, &s);
	    }

/*           Step 2: rotate columns JROW, JROW-1 to kill B(JROW,JROW-1) */

	    temp = b[jrow + jrow * b_dim1];
	    slartg_(&temp, &b[jrow + (jrow - 1) * b_dim1], &c__, &s, &b[jrow 
		    + jrow * b_dim1]);
	    b[jrow + (jrow - 1) * b_dim1] = 0.f;
	    srot_(ihi, &a[jrow * a_dim1 + 1], &c__1, &a[(jrow - 1) * a_dim1 + 
		    1], &c__1, &c__, &s);
	    i__3 = jrow - 1;
	    srot_(&i__3, &b[jrow * b_dim1 + 1], &c__1, &b[(jrow - 1) * b_dim1 
		    + 1], &c__1, &c__, &s);
	    if (ilz) {
		srot_(n, &z__[jrow * z_dim1 + 1], &c__1, &z__[(jrow - 1) * 
			z_dim1 + 1], &c__1, &c__, &s);
	    }
/* L30: */
	}
/* L40: */
    }

    return;

/*     End of SGGHRD */

} /* sgghrd_ */

