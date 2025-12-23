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


/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/




/* Table of constant values */

static complex c_b1 = {0.f,0.f};
static complex c_b2 = {1.f,0.f};
static integer c__1 = 1;
static integer c_n1 = -1;
static real c_b29 = 1.f;

/* > \brief <b> CGEEVX computes the eigenvalues and, optionally, the left and/or right eigenvectors for GE mat
rices</b> */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CGEGV + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cgegv.f
"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cgegv.f
"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cgegv.f
"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CGEGV( JOBVL, JOBVR, N, A, LDA, B, LDB, ALPHA, BETA, */
/*                         VL, LDVL, VR, LDVR, WORK, LWORK, RWORK, INFO ) */

/*       CHARACTER          JOBVL, JOBVR */
/*       INTEGER            INFO, LDA, LDB, LDVL, LDVR, LWORK, N */
/*       REAL               RWORK( * ) */
/*       COMPLEX            A( LDA, * ), ALPHA( * ), B( LDB, * ), */
/*      $                   BETA( * ), VL( LDVL, * ), VR( LDVR, * ), */
/*      $                   WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > This routine is deprecated and has been replaced by routine CGGEV. */
/* > */
/* > CGEGV computes the eigenvalues and, optionally, the left and/or right */
/* > eigenvectors of a complex matrix pair (A,B). */
/* > Given two square matrices A and B, */
/* > the generalized nonsymmetric eigenvalue problem (GNEP) is to find the */
/* > eigenvalues lambda and corresponding (non-zero) eigenvectors x such */
/* > that */
/* >    A*x = lambda*B*x. */
/* > */
/* > An alternate form is to find the eigenvalues mu and corresponding */
/* > eigenvectors y such that */
/* >    mu*A*y = B*y. */
/* > */
/* > These two forms are equivalent with mu = 1/lambda and x = y if */
/* > neither lambda nor mu is zero.  In order to deal with the case that */
/* > lambda or mu is zero or small, two values alpha and beta are returned */
/* > for each eigenvalue, such that lambda = alpha/beta and */
/* > mu = beta/alpha. */
/* > */
/* > The vectors x and y in the above equations are right eigenvectors of */
/* > the matrix pair (A,B).  Vectors u and v satisfying */
/* >    u**H*A = lambda*u**H*B  or  mu*v**H*A = v**H*B */
/* > are left eigenvectors of (A,B). */
/* > */
/* > Note: this routine performs "full balancing" on A and B */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOBVL */
/* > \verbatim */
/* >          JOBVL is CHARACTER*1 */
/* >          = 'N':  do not compute the left generalized eigenvectors; */
/* >          = 'V':  compute the left generalized eigenvectors (returned */
/* >                  in VL). */
/* > \endverbatim */
/* > */
/* > \param[in] JOBVR */
/* > \verbatim */
/* >          JOBVR is CHARACTER*1 */
/* >          = 'N':  do not compute the right generalized eigenvectors; */
/* >          = 'V':  compute the right generalized eigenvectors (returned */
/* >                  in VR). */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices A, B, VL, and VR.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX array, dimension (LDA, N) */
/* >          On entry, the matrix A. */
/* >          If JOBVL = 'V' or JOBVR = 'V', then on exit A */
/* >          contains the Schur form of A from the generalized Schur */
/* >          factorization of the pair (A,B) after balancing.  If no */
/* >          eigenvectors were computed, then only the diagonal elements */
/* >          of the Schur form will be correct.  See CGGHRD and CHGEQZ */
/* >          for details. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of A.  LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is COMPLEX array, dimension (LDB, N) */
/* >          On entry, the matrix B. */
/* >          If JOBVL = 'V' or JOBVR = 'V', then on exit B contains the */
/* >          upper triangular matrix obtained from B in the generalized */
/* >          Schur factorization of the pair (A,B) after balancing. */
/* >          If no eigenvectors were computed, then only the diagonal */
/* >          elements of B will be correct.  See CGGHRD and CHGEQZ for */
/* >          details. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of B.  LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] ALPHA */
/* > \verbatim */
/* >          ALPHA is COMPLEX array, dimension (N) */
/* >          The complex scalars alpha that define the eigenvalues of */
/* >          GNEP. */
/* > \endverbatim */
/* > */
/* > \param[out] BETA */
/* > \verbatim */
/* >          BETA is COMPLEX array, dimension (N) */
/* >          The complex scalars beta that define the eigenvalues of GNEP. */
/* > */
/* >          Together, the quantities alpha = ALPHA(j) and beta = BETA(j) */
/* >          represent the j-th eigenvalue of the matrix pair (A,B), in */
/* >          one of the forms lambda = alpha/beta or mu = beta/alpha. */
/* >          Since either lambda or mu may overflow, they should not, */
/* >          in general, be computed. */
/* > \endverbatim */
/* > */
/* > \param[out] VL */
/* > \verbatim */
/* >          VL is COMPLEX array, dimension (LDVL,N) */
/* >          If JOBVL = 'V', the left eigenvectors u(j) are stored */
/* >          in the columns of VL, in the same order as their eigenvalues. */
/* >          Each eigenvector is scaled so that its largest component has */
/* >          abs(real part) + abs(imag. part) = 1, except for eigenvectors */
/* >          corresponding to an eigenvalue with alpha = beta = 0, which */
/* >          are set to zero. */
/* >          Not referenced if JOBVL = 'N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVL */
/* > \verbatim */
/* >          LDVL is INTEGER */
/* >          The leading dimension of the matrix VL. LDVL >= 1, and */
/* >          if JOBVL = 'V', LDVL >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] VR */
/* > \verbatim */
/* >          VR is COMPLEX array, dimension (LDVR,N) */
/* >          If JOBVR = 'V', the right eigenvectors x(j) are stored */
/* >          in the columns of VR, in the same order as their eigenvalues. */
/* >          Each eigenvector is scaled so that its largest component has */
/* >          abs(real part) + abs(imag. part) = 1, except for eigenvectors */
/* >          corresponding to an eigenvalue with alpha = beta = 0, which */
/* >          are set to zero. */
/* >          Not referenced if JOBVR = 'N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVR */
/* > \verbatim */
/* >          LDVR is INTEGER */
/* >          The leading dimension of the matrix VR. LDVR >= 1, and */
/* >          if JOBVR = 'V', LDVR >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK.  LWORK >= f2cmax(1,2*N). */
/* >          For good performance, LWORK must generally be larger. */
/* >          To compute the optimal value of LWORK, call ILAENV to get */
/* >          blocksizes (for CGEQRF, CUNMQR, and CUNGQR.)  Then compute: */
/* >          NB  -- MAX of the blocksizes for CGEQRF, CUNMQR, and CUNGQR; */
/* >          The optimal LWORK is  MAX( 2*N, N*(NB+1) ). */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is REAL array, dimension (8*N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          =1,...,N: */
/* >                The QZ iteration failed.  No eigenvectors have been */
/* >                calculated, but ALPHA(j) and BETA(j) should be */
/* >                correct for j=INFO+1,...,N. */
/* >          > N:  errors that usually indicate LAPACK problems: */
/* >                =N+1: error return from CGGBAL */
/* >                =N+2: error return from CGEQRF */
/* >                =N+3: error return from CUNMQR */
/* >                =N+4: error return from CUNGQR */
/* >                =N+5: error return from CGGHRD */
/* >                =N+6: error return from CHGEQZ (other than failed */
/* >                                               iteration) */
/* >                =N+7: error return from CTGEVC */
/* >                =N+8: error return from CGGBAK (computing VL) */
/* >                =N+9: error return from CGGBAK (computing VR) */
/* >                =N+10: error return from CLASCL (various calls) */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complexGEeigen */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  Balancing */
/* >  --------- */
/* > */
/* >  This driver calls CGGBAL to both permute and scale rows and columns */
/* >  of A and B.  The permutations PL and PR are chosen so that PL*A*PR */
/* >  and PL*B*R will be upper triangular except for the diagonal blocks */
/* >  A(i:j,i:j) and B(i:j,i:j), with i and j as close together as */
/* >  possible.  The diagonal scaling matrices DL and DR are chosen so */
/* >  that the pair  DL*PL*A*PR*DR, DL*PL*B*PR*DR have elements close to */
/* >  one (except for the elements that start out zero.) */
/* > */
/* >  After the eigenvalues and eigenvectors of the balanced matrices */
/* >  have been computed, CGGBAK transforms the eigenvectors back to what */
/* >  they would have been (in perfect arithmetic) if they had not been */
/* >  balanced. */
/* > */
/* >  Contents of A and B on Exit */
/* >  -------- -- - --- - -- ---- */
/* > */
/* >  If any eigenvectors are computed (either JOBVL='V' or JOBVR='V' or */
/* >  both), then on exit the arrays A and B will contain the complex Schur */
/* >  form[*] of the "balanced" versions of A and B.  If no eigenvectors */
/* >  are computed, then only the diagonal blocks will be correct. */
/* > */
/* >  [*] In other words, upper triangular form. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void cgegv_(char *jobvl, char *jobvr, integer *n, complex *a, 
	integer *lda, complex *b, integer *ldb, complex *alpha, complex *beta,
	 complex *vl, integer *ldvl, complex *vr, integer *ldvr, complex *
	work, integer *lwork, real *rwork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, vl_dim1, vl_offset, vr_dim1, 
	    vr_offset, i__1, i__2, i__3, i__4;
    real r__1, r__2, r__3, r__4;
    complex q__1, q__2;

    /* Local variables */
    real absb, anrm, bnrm;
    integer itau;
    real temp;
    logical ilvl, ilvr;
    integer lopt;
    real anrm1, anrm2, bnrm1, bnrm2, absai, scale, absar, sbeta;
    extern logical lsame_(char *, char *);
    integer ileft, iinfo, icols, iwork, irows, jc;
    extern /* Subroutine */ void cggbak_(char *, char *, integer *, integer *, 
	    integer *, real *, real *, integer *, complex *, integer *, 
	    integer *), cggbal_(char *, integer *, complex *, 
	    integer *, complex *, integer *, integer *, integer *, real *, 
	    real *, real *, integer *);
    integer nb, in;
    extern real clange_(char *, integer *, integer *, complex *, integer *, 
	    real *);
    integer jr;
    extern /* Subroutine */ void cgghrd_(char *, char *, integer *, integer *, 
	    integer *, complex *, integer *, complex *, integer *, complex *, 
	    integer *, complex *, integer *, integer *);
    real salfai;
    extern /* Subroutine */ void clascl_(char *, integer *, integer *, real *, 
	    real *, integer *, integer *, complex *, integer *, integer *), cgeqrf_(integer *, integer *, complex *, integer *, 
	    complex *, complex *, integer *, integer *);
    real salfar;
    extern real slamch_(char *);
    extern /* Subroutine */ void clacpy_(char *, integer *, integer *, complex 
	    *, integer *, complex *, integer *), claset_(char *, 
	    integer *, integer *, complex *, complex *, complex *, integer *);
    real safmin;
    extern /* Subroutine */ void ctgevc_(char *, char *, logical *, integer *, 
	    complex *, integer *, complex *, integer *, complex *, integer *, 
	    complex *, integer *, integer *, integer *, complex *, real *, 
	    integer *);
    real safmax;
    char chtemp[1];
    logical ldumma[1];
    extern /* Subroutine */ void chgeqz_(char *, char *, char *, integer *, 
	    integer *, integer *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, integer *, real *, integer *); 
    extern int xerbla_(char *, integer *, ftnlen);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    integer ijobvl, iright;
    logical ilimit;
    integer ijobvr;
    extern /* Subroutine */ void cungqr_(integer *, integer *, integer *, 
	    complex *, integer *, complex *, complex *, integer *, integer *);
    integer lwkmin, nb1, nb2, nb3;
    extern /* Subroutine */ void cunmqr_(char *, char *, integer *, integer *, 
	    integer *, complex *, integer *, complex *, complex *, integer *, 
	    complex *, integer *, integer *);
    integer irwork, lwkopt;
    logical lquery;
    integer ihi, ilo;
    real eps;
    logical ilv;


/*  -- LAPACK driver routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Decode the input arguments */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --alpha;
    --beta;
    vl_dim1 = *ldvl;
    vl_offset = 1 + vl_dim1 * 1;
    vl -= vl_offset;
    vr_dim1 = *ldvr;
    vr_offset = 1 + vr_dim1 * 1;
    vr -= vr_offset;
    --work;
    --rwork;

    /* Function Body */
    if (lsame_(jobvl, "N")) {
	ijobvl = 1;
	ilvl = FALSE_;
    } else if (lsame_(jobvl, "V")) {
	ijobvl = 2;
	ilvl = TRUE_;
    } else {
	ijobvl = -1;
	ilvl = FALSE_;
    }

    if (lsame_(jobvr, "N")) {
	ijobvr = 1;
	ilvr = FALSE_;
    } else if (lsame_(jobvr, "V")) {
	ijobvr = 2;
	ilvr = TRUE_;
    } else {
	ijobvr = -1;
	ilvr = FALSE_;
    }
    ilv = ilvl || ilvr;

/*     Test the input arguments */

/* Computing MAX */
    i__1 = *n << 1;
    lwkmin = f2cmax(i__1,1);
    lwkopt = lwkmin;
    work[1].r = (real) lwkopt, work[1].i = 0.f;
    lquery = *lwork == -1;
    *info = 0;
    if (ijobvl <= 0) {
	*info = -1;
    } else if (ijobvr <= 0) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -5;
    } else if (*ldb < f2cmax(1,*n)) {
	*info = -7;
    } else if (*ldvl < 1 || ilvl && *ldvl < *n) {
	*info = -11;
    } else if (*ldvr < 1 || ilvr && *ldvr < *n) {
	*info = -13;
    } else if (*lwork < lwkmin && ! lquery) {
	*info = -15;
    }

    if (*info == 0) {
	nb1 = ilaenv_(&c__1, "CGEQRF", " ", n, n, &c_n1, &c_n1, (ftnlen)6, (
		ftnlen)1);
	nb2 = ilaenv_(&c__1, "CUNMQR", " ", n, n, n, &c_n1, (ftnlen)6, (
		ftnlen)1);
	nb3 = ilaenv_(&c__1, "CUNGQR", " ", n, n, n, &c_n1, (ftnlen)6, (
		ftnlen)1);
/* Computing MAX */
	i__1 = f2cmax(nb1,nb2);
	nb = f2cmax(i__1,nb3);
/* Computing MAX */
	i__1 = *n << 1, i__2 = *n * (nb + 1);
	lopt = f2cmax(i__1,i__2);
	work[1].r = (real) lopt, work[1].i = 0.f;
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("CGEGV ", &i__1, 6);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

/*     Get machine constants */

    eps = slamch_("E") * slamch_("B");
    safmin = slamch_("S");
    safmin += safmin;
    safmax = 1.f / safmin;

/*     Scale A */

    anrm = clange_("M", n, n, &a[a_offset], lda, &rwork[1]);
    anrm1 = anrm;
    anrm2 = 1.f;
    if (anrm < 1.f) {
	if (safmax * anrm < 1.f) {
	    anrm1 = safmin;
	    anrm2 = safmax * anrm;
	}
    }

    if (anrm > 0.f) {
	clascl_("G", &c_n1, &c_n1, &anrm, &c_b29, n, n, &a[a_offset], lda, &
		iinfo);
	if (iinfo != 0) {
	    *info = *n + 10;
	    return;
	}
    }

/*     Scale B */

    bnrm = clange_("M", n, n, &b[b_offset], ldb, &rwork[1]);
    bnrm1 = bnrm;
    bnrm2 = 1.f;
    if (bnrm < 1.f) {
	if (safmax * bnrm < 1.f) {
	    bnrm1 = safmin;
	    bnrm2 = safmax * bnrm;
	}
    }

    if (bnrm > 0.f) {
	clascl_("G", &c_n1, &c_n1, &bnrm, &c_b29, n, n, &b[b_offset], ldb, &
		iinfo);
	if (iinfo != 0) {
	    *info = *n + 10;
	    return;
	}
    }

/*     Permute the matrix to make it more nearly triangular */
/*     Also "balance" the matrix. */

    ileft = 1;
    iright = *n + 1;
    irwork = iright + *n;
    cggbal_("P", n, &a[a_offset], lda, &b[b_offset], ldb, &ilo, &ihi, &rwork[
	    ileft], &rwork[iright], &rwork[irwork], &iinfo);
    if (iinfo != 0) {
	*info = *n + 1;
	goto L80;
    }

/*     Reduce B to triangular form, and initialize VL and/or VR */

    irows = ihi + 1 - ilo;
    if (ilv) {
	icols = *n + 1 - ilo;
    } else {
	icols = irows;
    }
    itau = 1;
    iwork = itau + irows;
    i__1 = *lwork + 1 - iwork;
    cgeqrf_(&irows, &icols, &b[ilo + ilo * b_dim1], ldb, &work[itau], &work[
	    iwork], &i__1, &iinfo);
    if (iinfo >= 0) {
/* Computing MAX */
	i__3 = iwork;
	i__1 = lwkopt, i__2 = (integer) work[i__3].r + iwork - 1;
	lwkopt = f2cmax(i__1,i__2);
    }
    if (iinfo != 0) {
	*info = *n + 2;
	goto L80;
    }

    i__1 = *lwork + 1 - iwork;
    cunmqr_("L", "C", &irows, &icols, &irows, &b[ilo + ilo * b_dim1], ldb, &
	    work[itau], &a[ilo + ilo * a_dim1], lda, &work[iwork], &i__1, &
	    iinfo);
    if (iinfo >= 0) {
/* Computing MAX */
	i__3 = iwork;
	i__1 = lwkopt, i__2 = (integer) work[i__3].r + iwork - 1;
	lwkopt = f2cmax(i__1,i__2);
    }
    if (iinfo != 0) {
	*info = *n + 3;
	goto L80;
    }

    if (ilvl) {
	claset_("Full", n, n, &c_b1, &c_b2, &vl[vl_offset], ldvl);
	i__1 = irows - 1;
	i__2 = irows - 1;
	clacpy_("L", &i__1, &i__2, &b[ilo + 1 + ilo * b_dim1], ldb, &vl[ilo + 
		1 + ilo * vl_dim1], ldvl);
	i__1 = *lwork + 1 - iwork;
	cungqr_(&irows, &irows, &irows, &vl[ilo + ilo * vl_dim1], ldvl, &work[
		itau], &work[iwork], &i__1, &iinfo);
	if (iinfo >= 0) {
/* Computing MAX */
	    i__3 = iwork;
	    i__1 = lwkopt, i__2 = (integer) work[i__3].r + iwork - 1;
	    lwkopt = f2cmax(i__1,i__2);
	}
	if (iinfo != 0) {
	    *info = *n + 4;
	    goto L80;
	}
    }

    if (ilvr) {
	claset_("Full", n, n, &c_b1, &c_b2, &vr[vr_offset], ldvr);
    }

/*     Reduce to generalized Hessenberg form */

    if (ilv) {

/*        Eigenvectors requested -- work on whole matrix. */

	cgghrd_(jobvl, jobvr, n, &ilo, &ihi, &a[a_offset], lda, &b[b_offset], 
		ldb, &vl[vl_offset], ldvl, &vr[vr_offset], ldvr, &iinfo);
    } else {
	cgghrd_("N", "N", &irows, &c__1, &irows, &a[ilo + ilo * a_dim1], lda, 
		&b[ilo + ilo * b_dim1], ldb, &vl[vl_offset], ldvl, &vr[
		vr_offset], ldvr, &iinfo);
    }
    if (iinfo != 0) {
	*info = *n + 5;
	goto L80;
    }

/*     Perform QZ algorithm */

    iwork = itau;
    if (ilv) {
	*(unsigned char *)chtemp = 'S';
    } else {
	*(unsigned char *)chtemp = 'E';
    }
    i__1 = *lwork + 1 - iwork;
    chgeqz_(chtemp, jobvl, jobvr, n, &ilo, &ihi, &a[a_offset], lda, &b[
	    b_offset], ldb, &alpha[1], &beta[1], &vl[vl_offset], ldvl, &vr[
	    vr_offset], ldvr, &work[iwork], &i__1, &rwork[irwork], &iinfo);
    if (iinfo >= 0) {
/* Computing MAX */
	i__3 = iwork;
	i__1 = lwkopt, i__2 = (integer) work[i__3].r + iwork - 1;
	lwkopt = f2cmax(i__1,i__2);
    }
    if (iinfo != 0) {
	if (iinfo > 0 && iinfo <= *n) {
	    *info = iinfo;
	} else if (iinfo > *n && iinfo <= *n << 1) {
	    *info = iinfo - *n;
	} else {
	    *info = *n + 6;
	}
	goto L80;
    }

    if (ilv) {

/*        Compute Eigenvectors */

	if (ilvl) {
	    if (ilvr) {
		*(unsigned char *)chtemp = 'B';
	    } else {
		*(unsigned char *)chtemp = 'L';
	    }
	} else {
	    *(unsigned char *)chtemp = 'R';
	}

	ctgevc_(chtemp, "B", ldumma, n, &a[a_offset], lda, &b[b_offset], ldb, 
		&vl[vl_offset], ldvl, &vr[vr_offset], ldvr, n, &in, &work[
		iwork], &rwork[irwork], &iinfo);
	if (iinfo != 0) {
	    *info = *n + 7;
	    goto L80;
	}

/*        Undo balancing on VL and VR, rescale */

	if (ilvl) {
	    cggbak_("P", "L", n, &ilo, &ihi, &rwork[ileft], &rwork[iright], n,
		     &vl[vl_offset], ldvl, &iinfo);
	    if (iinfo != 0) {
		*info = *n + 8;
		goto L80;
	    }
	    i__1 = *n;
	    for (jc = 1; jc <= i__1; ++jc) {
		temp = 0.f;
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
/* Computing MAX */
		    i__3 = jr + jc * vl_dim1;
		    r__3 = temp, r__4 = (r__1 = vl[i__3].r, abs(r__1)) + (
			    r__2 = r_imag(&vl[jr + jc * vl_dim1]), abs(r__2));
		    temp = f2cmax(r__3,r__4);
/* L10: */
		}
		if (temp < safmin) {
		    goto L30;
		}
		temp = 1.f / temp;
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
		    i__3 = jr + jc * vl_dim1;
		    i__4 = jr + jc * vl_dim1;
		    q__1.r = temp * vl[i__4].r, q__1.i = temp * vl[i__4].i;
		    vl[i__3].r = q__1.r, vl[i__3].i = q__1.i;
/* L20: */
		}
L30:
		;
	    }
	}
	if (ilvr) {
	    cggbak_("P", "R", n, &ilo, &ihi, &rwork[ileft], &rwork[iright], n,
		     &vr[vr_offset], ldvr, &iinfo);
	    if (iinfo != 0) {
		*info = *n + 9;
		goto L80;
	    }
	    i__1 = *n;
	    for (jc = 1; jc <= i__1; ++jc) {
		temp = 0.f;
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
/* Computing MAX */
		    i__3 = jr + jc * vr_dim1;
		    r__3 = temp, r__4 = (r__1 = vr[i__3].r, abs(r__1)) + (
			    r__2 = r_imag(&vr[jr + jc * vr_dim1]), abs(r__2));
		    temp = f2cmax(r__3,r__4);
/* L40: */
		}
		if (temp < safmin) {
		    goto L60;
		}
		temp = 1.f / temp;
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
		    i__3 = jr + jc * vr_dim1;
		    i__4 = jr + jc * vr_dim1;
		    q__1.r = temp * vr[i__4].r, q__1.i = temp * vr[i__4].i;
		    vr[i__3].r = q__1.r, vr[i__3].i = q__1.i;
/* L50: */
		}
L60:
		;
	    }
	}

/*        End of eigenvector calculation */

    }

/*     Undo scaling in alpha, beta */

/*     Note: this does not give the alpha and beta for the unscaled */
/*     problem. */

/*     Un-scaling is limited to avoid underflow in alpha and beta */
/*     if they are significant. */

    i__1 = *n;
    for (jc = 1; jc <= i__1; ++jc) {
	i__2 = jc;
	absar = (r__1 = alpha[i__2].r, abs(r__1));
	absai = (r__1 = r_imag(&alpha[jc]), abs(r__1));
	i__2 = jc;
	absb = (r__1 = beta[i__2].r, abs(r__1));
	i__2 = jc;
	salfar = anrm * alpha[i__2].r;
	salfai = anrm * r_imag(&alpha[jc]);
	i__2 = jc;
	sbeta = bnrm * beta[i__2].r;
	ilimit = FALSE_;
	scale = 1.f;

/*        Check for significant underflow in imaginary part of ALPHA */

/* Computing MAX */
	r__1 = safmin, r__2 = eps * absar, r__1 = f2cmax(r__1,r__2), r__2 = eps *
		 absb;
	if (abs(salfai) < safmin && absai >= f2cmax(r__1,r__2)) {
	    ilimit = TRUE_;
/* Computing MAX */
	    r__1 = safmin, r__2 = anrm2 * absai;
	    scale = safmin / anrm1 / f2cmax(r__1,r__2);
	}

/*        Check for significant underflow in real part of ALPHA */

/* Computing MAX */
	r__1 = safmin, r__2 = eps * absai, r__1 = f2cmax(r__1,r__2), r__2 = eps *
		 absb;
	if (abs(salfar) < safmin && absar >= f2cmax(r__1,r__2)) {
	    ilimit = TRUE_;
/* Computing MAX */
/* Computing MAX */
	    r__3 = safmin, r__4 = anrm2 * absar;
	    r__1 = scale, r__2 = safmin / anrm1 / f2cmax(r__3,r__4);
	    scale = f2cmax(r__1,r__2);
	}

/*        Check for significant underflow in BETA */

/* Computing MAX */
	r__1 = safmin, r__2 = eps * absar, r__1 = f2cmax(r__1,r__2), r__2 = eps *
		 absai;
	if (abs(sbeta) < safmin && absb >= f2cmax(r__1,r__2)) {
	    ilimit = TRUE_;
/* Computing MAX */
/* Computing MAX */
	    r__3 = safmin, r__4 = bnrm2 * absb;
	    r__1 = scale, r__2 = safmin / bnrm1 / f2cmax(r__3,r__4);
	    scale = f2cmax(r__1,r__2);
	}

/*        Check for possible overflow when limiting scaling */

	if (ilimit) {
/* Computing MAX */
	    r__1 = abs(salfar), r__2 = abs(salfai), r__1 = f2cmax(r__1,r__2), 
		    r__2 = abs(sbeta);
	    temp = scale * safmin * f2cmax(r__1,r__2);
	    if (temp > 1.f) {
		scale /= temp;
	    }
	    if (scale < 1.f) {
		ilimit = FALSE_;
	    }
	}

/*        Recompute un-scaled ALPHA, BETA if necessary. */

	if (ilimit) {
	    i__2 = jc;
	    salfar = scale * alpha[i__2].r * anrm;
	    salfai = scale * r_imag(&alpha[jc]) * anrm;
	    i__2 = jc;
	    q__2.r = scale * beta[i__2].r, q__2.i = scale * beta[i__2].i;
	    q__1.r = bnrm * q__2.r, q__1.i = bnrm * q__2.i;
	    sbeta = q__1.r;
	}
	i__2 = jc;
	q__1.r = salfar, q__1.i = salfai;
	alpha[i__2].r = q__1.r, alpha[i__2].i = q__1.i;
	i__2 = jc;
	beta[i__2].r = sbeta, beta[i__2].i = 0.f;
/* L70: */
    }

L80:
    work[1].r = (real) lwkopt, work[1].i = 0.f;

    return;

/*     End of CGEGV */

} /* cgegv_ */

