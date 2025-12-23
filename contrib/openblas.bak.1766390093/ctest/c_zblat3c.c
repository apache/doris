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

#include "common.h"

typedef blasint integer;

typedef unsigned int uinteger;
typedef char *address;
typedef short int shortint;
typedef float real;
typedef double doublereal;
typedef struct { real r, i; } complex;
typedef struct { doublereal r, i; } doublecomplex;
#ifdef _MSC_VER
static inline _Dcomplex Cd(doublecomplex *z) {_Dcomplex zz={z->r , z->i};return zz;}
static inline _Dcomplex * _pCd(doublecomplex *z) {return (_Dcomplex*)z;}
#else
static inline _Complex float Cf(complex *z) {return z->r + z->i*_Complex_I;}
static inline _Complex double Cd(doublecomplex *z) {return z->r + z->i*_Complex_I;}
static inline _Complex double * _pCd(doublecomplex *z) {return (_Complex double*)z;}
#endif
#define pCf(z) (*_pCf(z))
#define pCd(z) (*_pCd(z))
typedef int logical;
typedef short int shortlogical;
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
#define z_div(c, a, b) {Cd(c)._Val[0] = (Cd(a)._Val[0]/Cd(b)._Val[0]); Cd(c)._Val[1]=(Cd(a)._Val[1]/Cd(b)._Val[1]);}
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
#define mycycle_() continue;
#define myceiling_(w) {ceil(w)}
#define myhuge_(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc_(w,s,e,n) dmaxloc_(w,*(s),*(e),n)

/* procedure parameter types for -A and -C++ */

#define F2C_proc_par_types 1


/* Common Block Declarations */

struct {
    integer infot, noutc;
    logical ok, lerr;
} infoc_;

#define infoc_1 infoc_

struct {
    char srnamt[12];
} srnamc_;

#define srnamc_1 srnamc_

/* Table of constant values */

static doublecomplex c_b1 = {0.,0.};
static doublecomplex c_b2 = {1.,0.};
static integer c__1 = 1;
static integer c__65 = 65;
static doublereal c_b92 = 1.;
static integer c__6 = 6;
static logical c_true = TRUE_;
static integer c__0 = 0;
static logical c_false = FALSE_;

/* Main program  MAIN__() */ int main(void)
{
    /* Initialized data */

    static char snames[9][13] = { "cblas_zgemm ", "cblas_zhemm ", "cblas_zsymm ", "cblas_ztrmm ",
     "cblas_ztrsm ", "cblas_zherk ", "cblas_zsyrk ", "cblas_zher2k", "cblas_zsyr2k"};

    /* System generated locals */
    integer i__1, i__2, i__3, i__4, i__5;
    doublereal d__1;

    /* Builtin functions */
    integer s_rsle(void), do_lio(void), e_rsle(void), f_open(void), s_wsfe(void), do_fio(void), 
	    e_wsfe(void), s_wsle(void), e_wsle(void), s_rsfe(void), e_rsfe(void);

    /* Local variables */
    static integer nalf, idim[9];
    static logical same;
    static integer nbet, ntra;
    static logical rewi;
    extern /* Subroutine */ int zchk1_(char*, doublereal*, doublereal*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublereal*, integer*, ftnlen);
    extern /* Subroutine */ int zchk2_(char*, doublereal*, doublereal*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublereal*, integer*, ftnlen);
    extern /* Subroutine */ int zchk3_(char*, doublereal*, doublereal*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublereal*, doublecomplex*, integer*, ftnlen);
    extern /* Subroutine */ int zchk4_(char*, doublereal*, doublereal*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublereal*, integer*, ftnlen);
    extern /* Subroutine */ int zchk5_(char*, doublereal*, doublereal*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublecomplex*, doublereal*, doublecomplex*, integer*, ftnlen);
    static doublecomplex c__[4225]	/* was [65][65] */;
    static doublereal g[65];
    static integer i__, j;
    extern doublereal ddiff_(doublereal*, doublereal*);
    static integer n;
    static logical fatal;
    static doublecomplex w[130];
    static logical trace;
    static integer nidim;
    extern /* Subroutine */ int zmmch_(char*, char*, integer*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, doublereal*, doublecomplex*, integer*, doublereal*, doublereal*, logical*, integer*, logical*, ftnlen, ftnlen);
    static char snaps[32];
    static integer isnum;
    static logical ltest[9];
    static doublecomplex aa[4225], ab[8450]	/* was [65][130] */, bb[4225],
	     cc[4225], as[4225], bs[4225], cs[4225], ct[65];
    static logical sfatal, corder;
    static char snamet[12], transa[1], transb[1];
    static doublereal thresh;
    static logical rorder;
    static integer layout;
    static logical ltestt, tsterr;
    extern /* Subroutine */ int cz3chke_(char*, ftnlen);
    static doublecomplex alf[7], bet[7];
    static doublereal eps, err;
    extern logical lze_(doublecomplex*, doublecomplex*, integer*);
    char tmpchar;
    
/*  Test program for the COMPLEX*16          Level 3 Blas. */

/*  The program must be driven by a short data file. The first 13 records */
/*  of the file are read using list-directed input, the last 9 records */
/*  are read using the format ( A12,L2 ). An annotated example of a data */
/*  file can be obtained by deleting the first 3 characters from the */
/*  following 22 lines: */
/*  'CBLAT3.SNAP'     NAME OF SNAPSHOT OUTPUT FILE */
/*  -1                UNIT NUMBER OF SNAPSHOT FILE (NOT USED IF .LT. 0) */
/*  F        LOGICAL FLAG, T TO REWIND SNAPSHOT FILE AFTER EACH RECORD. */
/*  F        LOGICAL FLAG, T TO STOP ON FAILURES. */
/*  T        LOGICAL FLAG, T TO TEST ERROR EXITS. */
/*  2        0 TO TEST COLUMN-MAJOR, 1 TO TEST ROW-MAJOR, 2 TO TEST BOTH */
/*  16.0     THRESHOLD VALUE OF TEST RATIO */
/*  6                 NUMBER OF VALUES OF N */
/*  0 1 2 3 5 9       VALUES OF N */
/*  3                 NUMBER OF VALUES OF ALPHA */
/*  (0.0,0.0) (1.0,0.0) (0.7,-0.9)       VALUES OF ALPHA */
/*  3                 NUMBER OF VALUES OF BETA */
/*  (0.0,0.0) (1.0,0.0) (1.3,-1.1)       VALUES OF BETA */
/*  ZGEMM  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  ZHEMM  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  ZSYMM  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  ZTRMM  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  ZTRSM  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  ZHERK  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  ZSYRK  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  ZHER2K T PUT F FOR NO TEST. SAME COLUMNS. */
/*  ZSYR2K T PUT F FOR NO TEST. SAME COLUMNS. */

/*  See: */

/*     Dongarra J. J., Du Croz J. J., Duff I. S. and Hammarling S. */
/*     A Set of Level 3 Basic Linear Algebra Subprograms. */

/*     Technical Memorandum No.88 (Revision 1), Mathematics and */
/*     Computer Science Division, Argonne National Laboratory, 9700 */
/*     South Cass Avenue, Argonne, Illinois 60439, US. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Parameters .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
/*     .. Executable Statements .. */

    infoc_1.noutc = 6;

/*     Read name and unit number for snapshot output file and open file. */

    char line[80];
    
    fgets(line,80,stdin);
    sscanf(line,"'%s'",snaps);
    fgets(line,80,stdin);
#ifdef USE64BITINT
    sscanf(line,"%ld",&ntra);
#else
    sscanf(line,"%d",&ntra);
#endif
    trace = ntra >= 0;
    if (trace) {
/*	o__1.oerr = 0;
	o__1.ounit = ntra;
	o__1.ofnmlen = 32;
	o__1.ofnm = snaps;
	o__1.orl = 0;
	o__1.osta = "NEW";
	o__1.oacc = 0;
	o__1.ofm = 0;
	o__1.oblnk = 0;
	f_open(&o__1);*/
    }
/*     Read the flag that directs rewinding of the snapshot file. */
   fgets(line,80,stdin);
   sscanf(line,"%d",&rewi);
   rewi = rewi && trace;
/*     Read the flag that directs stopping on any failure. */
   fgets(line,80,stdin);
   sscanf(line,"%c",&tmpchar);
   sfatal=FALSE_;
   if (tmpchar=='T')sfatal=TRUE_;
/*     Read the flag that indicates whether error exits are to be tested. */
   fgets(line,80,stdin);
   sscanf(line,"%c",&tmpchar);
   tsterr=FALSE_;
   if (tmpchar=='T')tsterr=TRUE_;
/*     Read the flag that indicates whether row-major data layout to be tested. */
   fgets(line,80,stdin);
   sscanf(line,"%d",&layout);
/*     Read the threshold value of the test ratio */
   fgets(line,80,stdin);
   sscanf(line,"%lf",&thresh);

/*     Read and check the parameter values for the tests. */

/*     Values of N */
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%d",&nidim);
#else
   sscanf(line,"%d",&nidim);
#endif
    if (nidim < 1 || nidim > 9) {
        fprintf(stderr,"NUMBER OF VALUES OF N IS LESS THAN 1 OR GREATER THAN 9");
        goto L220;
    }
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld %ld %ld %ld %ld %ld %ld %ld %ld",&idim[0],&idim[1],&idim[2],
    &idim[3],&idim[4],&idim[5],&idim[6],&idim[7],&idim[8]);
#else
   sscanf(line,"%d %d %d %d %d %d %d %d %d",&idim[0],&idim[1],&idim[2],
    &idim[3],&idim[4],&idim[5],&idim[6],&idim[7],&idim[8]);
#endif
    i__1 = nidim;
    for (i__ = 1; i__ <= i__1; ++i__) {
        if (idim[i__ - 1] < 0 || idim[i__ - 1] > 65) {
        fprintf(stderr,"VALUE OF N IS LESS THAN 0 OR GREATER THAN 65\n");
            goto L220;
        }
/* L10: */
    }
/*     Values of ALPHA */
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld",&nalf);
#else
   sscanf(line,"%d",&nalf);
#endif
    if (nalf < 1 || nalf > 7) {
        fprintf(stderr,"VALUE OF ALPHA IS LESS THAN 0 OR GREATER THAN 7\n");
        goto L220;
    }
   fgets(line,80,stdin);
   sscanf(line,"(%lf,%lf) (%lf,%lf) (%lf,%lf) (%lf,%lf) (%lf,%lf) (%lf,%lf) (%lf,%lf)",&alf[0].r,&alf[0].i,&alf[1].r,&alf[1].i,&alf[2].r,&alf[2].i,&alf[3].r,&alf[3].i,
   &alf[4].r,&alf[4].i,&alf[5].r,&alf[5].i,&alf[6].r,&alf[6].i);

/*     Values of BETA */
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld",&nbet);
#else
   sscanf(line,"%d",&nbet);
#endif
    if (nalf < 1 || nbet > 7) {
        fprintf(stderr,"VALUE OF BETA IS LESS THAN 0 OR GREATER THAN 7\n");
        goto L220;
    }
   fgets(line,80,stdin);
   sscanf(line,"(%lf,%lf) (%lf,%lf) (%lf,%lf) (%lf,%lf) (%lf,%lf) (%lf,%lf) (%lf,%lf)",&bet[0].r,&bet[0].i,&bet[1].r,&bet[1].i,&bet[2].r,&bet[2].i,&bet[3].r,&bet[3].i,
   &bet[4].r,&bet[4].i,&bet[5].r,&bet[5].i,&bet[6].r,&bet[6].i);

/*     Report values of parameters. */

    printf("TESTS OF THE DOUBLE PRECISION COMPLEX LEVEL 3 BLAS\nTHE FOLLOWING PARAMETER VALUES WILL BE USED:\n");
    printf(" FOR N");
    for (i__ =1; i__ <=nidim;++i__) printf(" %d",idim[i__-1]);
    printf("\n");    
    printf(" FOR ALPHA");
    for (i__ =1; i__ <=nalf;++i__) printf(" (%lf,%lf)",alf[i__-1].r,alf[i__-1].i);
    printf("\n");    
    printf(" FOR BETA");
    for (i__ =1; i__ <=nbet;++i__) printf(" (%lf,%lf)",bet[i__-1].r,bet[i__-1].i);
    printf("\n");    

    if (! tsterr) {
      printf(" ERROR-EXITS WILL NOT BE TESTED\n"); 
    }

    printf("ROUTINES PASS COMPUTATIONAL TESTS IF TEST RATIO IS LESS THAN %lf\n",thresh);
    rorder = FALSE_;
    corder = FALSE_;
    if (layout == 2) {
	rorder = TRUE_;
	corder = TRUE_;
        printf("COLUMN-MAJOR AND ROW-MAJOR DATA LAYOUTS ARE TESTED\n");
    } else if (layout == 1) {
	rorder = TRUE_;
        printf("ROW-MAJOR DATA LAYOUT IS TESTED\n");
    } else if (layout == 0) {
	corder = TRUE_;
        printf("COLUMN-MAJOR DATA LAYOUT IS TESTED\n");
    }

/*     Read names of subroutines and flags which indicate */
/*     whether they are to be tested. */

    for (i__ = 1; i__ <= 9; ++i__) {
	ltest[i__ - 1] = FALSE_;
/* L20: */
    }
L30:
   if (! fgets(line,80,stdin)) {
        goto L60;
    }
   i__1 = sscanf(line,"%12c %c",snamet,&tmpchar);
   ltestt=FALSE_;
   if (tmpchar=='T')ltestt=TRUE_;
    if (i__1 < 2) {
        goto L60;
    }
    for (i__ = 1; i__ <= 9; ++i__) {
        if (s_cmp(snamet, snames[i__ - 1] , (ftnlen)12, (ftnlen)12) == 
                0) {
            goto L50;
        }
/* L40: */
    }
    printf("SUBPROGRAM NAME %s NOT RECOGNIZED\n****** TESTS ABANDONED ******\n",snamet);
    exit(1);
L50:
    ltest[i__ - 1] = ltestt;
    goto L30;

L60:
/*    cl__1.cerr = 0;
    cl__1.cunit = 5;
    cl__1.csta = 0;
    f_clos(&cl__1);*/

/*     Compute EPS (the machine precision). */

    eps = 1.;
L70:
    d__1 = eps + 1.;
    if (ddiff_(&d__1, &c_b92) == 0.) {
	goto L80;
    }
    eps *= .5;
    goto L70;
L80:
    eps += eps;
    printf("RELATIVE MACHINE PRECISION IS TAKEN TO BE %9.1g\n",eps);

/*     Check the reliability of ZMMCH using exact data. */

    n = 32;
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = n;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    i__3 = i__ + j * 65 - 66;
/* Computing MAX */
	    i__5 = i__ - j + 1;
	    i__4 = f2cmax(i__5,0);
	    ab[i__3].r = (doublereal) i__4, ab[i__3].i = 0.;
/* L90: */
	}
	i__2 = j + 4224;
	ab[i__2].r = (doublereal) j, ab[i__2].i = 0.;
	i__2 = (j + 65) * 65 - 65;
	ab[i__2].r = (doublereal) j, ab[i__2].i = 0.;
	i__2 = j - 1;
	c__[i__2].r = 0., c__[i__2].i = 0.;
/* L100: */
    }
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = j - 1;
	i__3 = j * ((j + 1) * j) / 2 - (j + 1) * j * (j - 1) / 3;
	cc[i__2].r = (doublereal) i__3, cc[i__2].i = 0.;
/* L110: */
    }
/*     CC holds the exact result. On exit from ZMMCH CT holds */
/*     the result computed by ZMMCH. */
    *(unsigned char *)transa = 'N';
    *(unsigned char *)transb = 'N';
    zmmch_(transa, transb, &n, &c__1, &n, &c_b2, ab, &c__65, &ab[4225], &
	    c__65, &c_b1, c__, &c__65, ct, g, cc, &c__65, &eps, &err, &fatal, 
	    &c__6, &c_true, (ftnlen)1, (ftnlen)1);
    same = lze_(cc, ct, &n);
    if (! same || err != 0.) {
      printf("ERROR IN ZMMCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("ZMMCH WAS CALLED WITH TRANSA = %s AND TRANSB = %s\n", transa,transb);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }
    *(unsigned char *)transb = 'C';
    zmmch_(transa, transb, &n, &c__1, &n, &c_b2, ab, &c__65, &ab[4225], &
	    c__65, &c_b1, c__, &c__65, ct, g, cc, &c__65, &eps, &err, &fatal, 
	    &c__6, &c_true, (ftnlen)1, (ftnlen)1);
    same = lze_(cc, ct, &n);
    if (! same || err != 0.) {
      printf("ERROR IN ZMMCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("ZMMCH WAS CALLED WITH TRANSA = %s AND TRANSB = %s\n", transa,transb);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = j + 4224;
	i__3 = n - j + 1;
	ab[i__2].r = (doublereal) i__3, ab[i__2].i = 0.;
	i__2 = (j + 65) * 65 - 65;
	i__3 = n - j + 1;
	ab[i__2].r = (doublereal) i__3, ab[i__2].i = 0.;
/* L120: */
    }
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = n - j;
	i__3 = j * ((j + 1) * j) / 2 - (j + 1) * j * (j - 1) / 3;
	cc[i__2].r = (doublereal) i__3, cc[i__2].i = 0.;
/* L130: */
    }
    *(unsigned char *)transa = 'C';
    *(unsigned char *)transb = 'N';
    zmmch_(transa, transb, &n, &c__1, &n, &c_b2, ab, &c__65, &ab[4225], &
	    c__65, &c_b1, c__, &c__65, ct, g, cc, &c__65, &eps, &err, &fatal, 
	    &c__6, &c_true, (ftnlen)1, (ftnlen)1);
    same = lze_(cc, ct, &n);
    if (! same || err != 0.) {
      printf("ERROR IN ZMMCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("ZMMCH WAS CALLED WITH TRANSA = %s AND TRANSB = %s\n", transa,transb);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }
    *(unsigned char *)transb = 'C';
    zmmch_(transa, transb, &n, &c__1, &n, &c_b2, ab, &c__65, &ab[4225], &
	    c__65, &c_b1, c__, &c__65, ct, g, cc, &c__65, &eps, &err, &fatal, 
	    &c__6, &c_true, (ftnlen)1, (ftnlen)1);
    same = lze_(cc, ct, &n);
    if (! same || err != 0.) {
      printf("ERROR IN ZMMCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("ZMMCH WAS CALLED WITH TRANSA = %s AND TRANSB = %s\n", transa,transb);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }

/*     Test each subroutine in turn. */

    for (isnum = 1; isnum <= 9; ++isnum) {
	if (! ltest[isnum - 1]) {
/*           Subprogram is not to be tested. */
           printf("%12s WAS NOT TESTED\n",snames[isnum-1]);
	} else {
	    s_copy(srnamc_1.srnamt, snames[isnum - 1], (ftnlen)12, (
		    ftnlen)12);
/*           Test error exits. */
	    if (tsterr) {
		cz3chke_(snames[isnum - 1], (ftnlen)12);
	    }
/*           Test computations. */
	    infoc_1.infot = 0;
	    infoc_1.ok = TRUE_;
	    fatal = FALSE_;
	    switch ((int)isnum) {
		case 1:  goto L140;
		case 2:  goto L150;
		case 3:  goto L150;
		case 4:  goto L160;
		case 5:  goto L160;
		case 6:  goto L170;
		case 7:  goto L170;
		case 8:  goto L180;
		case 9:  goto L180;
	    }
/*           Test ZGEMM, 01. */
L140:
	    if (corder) {
		zchk1_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		zchk1_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__1, (ftnlen)12);
	    }
	    goto L190;
/*           Test ZHEMM, 02, ZSYMM, 03. */
L150:
	    if (corder) {
		zchk2_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		zchk2_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__1, (ftnlen)12);
	    }
	    goto L190;
/*           Test ZTRMM, 04, ZTRSM, 05. */
L160:
	    if (corder) {
		zchk3_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			c__65, ab, aa, as, &ab[4225], bb, bs, ct, g, c__, &
			c__0, (ftnlen)12);
	    }
	    if (rorder) {
		zchk3_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			c__65, ab, aa, as, &ab[4225], bb, bs, ct, g, c__, &
			c__1, (ftnlen)12);
	    }
	    goto L190;
/*           Test ZHERK, 06, ZSYRK, 07. */
L170:
	    if (corder) {
		zchk4_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		zchk4_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__1, (ftnlen)12);
	    }
	    goto L190;
/*           Test ZHER2K, 08, ZSYR2K, 09. */
L180:
	    if (corder) {
		zchk5_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, bb, bs, c__, cc, cs, 
			ct, g, w, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		zchk5_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, bb, bs, c__, cc, cs, 
			ct, g, w, &c__1, (ftnlen)12);
	    }
	    goto L190;

L190:
	    if (fatal && sfatal) {
		goto L210;
	    }
	}
/* L200: */
    }
    printf("\nEND OF TESTS\n");
    goto L230;

L210:
    printf("\n****** FATAL ERROR - TESTS ABANDONED ******\n");
    goto L230;

L220:
    printf("AMEND DATA FILE OR INCREASE ARRAY SIZES IN PROGRAM\n");
    printf("****** TESTS ABANDONED ******\n");

L230:
    if (trace) {
/*	cl__1.cerr = 0;
	cl__1.cunit = ntra;
	cl__1.csta = 0;
	f_clos(&cl__1);*/
    }
/*    cl__1.cerr = 0;
    cl__1.cunit = 6;
    cl__1.csta = 0;
    f_clos(&cl__1);*/
    exit(0);

/*     End of ZBLAT3. */

} /* MAIN__ */

/* Subroutine */ int zchk1_(char* sname, doublereal* eps, doublereal* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, doublecomplex* alf, integer* nbet, doublecomplex* bet, integer* nmax, doublecomplex* a, doublecomplex* aa, doublecomplex* as, doublecomplex* b, doublecomplex* bb, doublecomplex* bs, doublecomplex* c__, doublecomplex* cc, doublecomplex* cs, doublecomplex* ct, doublereal* g, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char ich[3+1] = "NTC";

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2, 
	    i__3, i__4, i__5, i__6, i__7, i__8;

    /* Local variables */
    static doublecomplex beta;
    static integer ldas, ldbs, ldcs;
    static logical same, null;
    static integer i__, k, m, n;
    static doublecomplex alpha;
    static logical isame[13], trana, tranb;
    extern /* Subroutine */ int zmake_(char*, char*, char*, integer*, integer*, doublecomplex*, integer*, doublecomplex*, integer*, logical*, doublecomplex*, ftnlen, ftnlen, ftnlen);
    static integer nargs;
    extern /* Subroutine */ int zmmch_(char*, char*, integer*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, doublereal*, doublecomplex*, integer*, doublereal*, doublereal*, logical*, integer*, logical*, ftnlen, ftnlen);
    static logical reset;
    static integer ia, ib;
    extern /* Subroutine */ int zprcn1_(integer*, integer*, char*, integer*, char*, char*, integer*, integer*, integer*, doublecomplex*, integer*, integer*, doublecomplex*, integer*, ftnlen, ftnlen, ftnlen);
    static integer ma, mb, na, nb, nc, ik, im, in, ks, ms, ns;
    extern /* Subroutine */ void czgemm_(integer*, char*, char*, integer*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, integer*, ftnlen, ftnlen);
    static char tranas[1], tranbs[1], transa[1], transb[1];
    static doublereal errmax;
    extern logical lzeres_(char*, char*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, ftnlen, ftnlen);
    static integer ica, icb, laa, lbb, lda, lcc, ldb, ldc;
    static doublecomplex als, bls;
    static doublereal err;
    extern logical lze_(doublecomplex*, doublecomplex*, integer*);

/*  Tests ZGEMM. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
    /* Parameter adjustments */
    --idim;
    --alf;
    --bet;
    --g;
    --ct;
    --cs;
    --cc;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --bs;
    --bb;
    b_dim1 = *nmax;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */
/*     .. Executable Statements .. */

    nargs = 13;
    nc = 0;
    reset = TRUE_;
    errmax = 0.;

    i__1 = *nidim;
    for (im = 1; im <= i__1; ++im) {
	m = idim[im];

	i__2 = *nidim;
	for (in = 1; in <= i__2; ++in) {
	    n = idim[in];
/*           Set LDC to 1 more than minimum value if room. */
	    ldc = m;
	    if (ldc < *nmax) {
		++ldc;
	    }
/*           Skip tests if not enough room. */
	    if (ldc > *nmax) {
		goto L100;
	    }
	    lcc = ldc * n;
	    null = n <= 0 || m <= 0;

	    i__3 = *nidim;
	    for (ik = 1; ik <= i__3; ++ik) {
		k = idim[ik];

		for (ica = 1; ica <= 3; ++ica) {
		    *(unsigned char *)transa = *(unsigned char *)&ich[ica - 1]
			    ;
		    trana = *(unsigned char *)transa == 'T' || *(unsigned 
			    char *)transa == 'C';

		    if (trana) {
			ma = k;
			na = m;
		    } else {
			ma = m;
			na = k;
		    }
/*                 Set LDA to 1 more than minimum value if room. */
		    lda = ma;
		    if (lda < *nmax) {
			++lda;
		    }
/*                 Skip tests if not enough room. */
		    if (lda > *nmax) {
			goto L80;
		    }
		    laa = lda * na;

/*                 Generate the matrix A. */

		    zmake_("ge", " ", " ", &ma, &na, &a[a_offset], nmax, &aa[
			    1], &lda, &reset, &c_b1, (ftnlen)2, (ftnlen)1, (
			    ftnlen)1);

		    for (icb = 1; icb <= 3; ++icb) {
			*(unsigned char *)transb = *(unsigned char *)&ich[icb 
				- 1];
			tranb = *(unsigned char *)transb == 'T' || *(unsigned 
				char *)transb == 'C';

			if (tranb) {
			    mb = n;
			    nb = k;
			} else {
			    mb = k;
			    nb = n;
			}
/*                    Set LDB to 1 more than minimum value if room. */
			ldb = mb;
			if (ldb < *nmax) {
			    ++ldb;
			}
/*                    Skip tests if not enough room. */
			if (ldb > *nmax) {
			    goto L70;
			}
			lbb = ldb * nb;

/*                    Generate the matrix B. */

			zmake_("ge", " ", " ", &mb, &nb, &b[b_offset], nmax, &
				bb[1], &ldb, &reset, &c_b1, (ftnlen)2, (
				ftnlen)1, (ftnlen)1);

			i__4 = *nalf;
			for (ia = 1; ia <= i__4; ++ia) {
			    i__5 = ia;
			    alpha.r = alf[i__5].r, alpha.i = alf[i__5].i;

			    i__5 = *nbet;
			    for (ib = 1; ib <= i__5; ++ib) {
				i__6 = ib;
				beta.r = bet[i__6].r, beta.i = bet[i__6].i;

/*                          Generate the matrix C. */

				zmake_("ge", " ", " ", &m, &n, &c__[c_offset],
					 nmax, &cc[1], &ldc, &reset, &c_b1, (
					ftnlen)2, (ftnlen)1, (ftnlen)1);

				++nc;

/*                          Save every datum before calling the */
/*                          subroutine. */

				*(unsigned char *)tranas = *(unsigned char *)
					transa;
				*(unsigned char *)tranbs = *(unsigned char *)
					transb;
				ms = m;
				ns = n;
				ks = k;
				als.r = alpha.r, als.i = alpha.i;
				i__6 = laa;
				for (i__ = 1; i__ <= i__6; ++i__) {
				    i__7 = i__;
				    i__8 = i__;
				    as[i__7].r = aa[i__8].r, as[i__7].i = aa[
					    i__8].i;
/* L10: */
				}
				ldas = lda;
				i__6 = lbb;
				for (i__ = 1; i__ <= i__6; ++i__) {
				    i__7 = i__;
				    i__8 = i__;
				    bs[i__7].r = bb[i__8].r, bs[i__7].i = bb[
					    i__8].i;
/* L20: */
				}
				ldbs = ldb;
				bls.r = beta.r, bls.i = beta.i;
				i__6 = lcc;
				for (i__ = 1; i__ <= i__6; ++i__) {
				    i__7 = i__;
				    i__8 = i__;
				    cs[i__7].r = cc[i__8].r, cs[i__7].i = cc[
					    i__8].i;
/* L30: */
				}
				ldcs = ldc;

/*                          Call the subroutine. */

				if (*trace) {
				    zprcn1_(ntra, &nc, sname, iorder, transa, 
					    transb, &m, &n, &k, &alpha, &lda, 
					    &ldb, &beta, &ldc, (ftnlen)12, (
					    ftnlen)1, (ftnlen)1);
				}
				if (*rewi) {
/*				    al__1.aerr = 0;
				    al__1.aunit = *ntra;
				    f_rew(&al__1);*/
				}
				czgemm_(iorder, transa, transb, &m, &n, &k, &
					alpha, &aa[1], &lda, &bb[1], &ldb, &
					beta, &cc[1], &ldc, (ftnlen)1, (
					ftnlen)1);

/*                          Check if error-exit was taken incorrectly. */

				if (! infoc_1.ok) {
                                    printf(" *** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				    *fatal = TRUE_;
				    goto L120;
				}

/*                          See what data changed inside subroutines. */

				isame[0] = *(unsigned char *)transa == *(
					unsigned char *)tranas;
				isame[1] = *(unsigned char *)transb == *(
					unsigned char *)tranbs;
				isame[2] = ms == m;
				isame[3] = ns == n;
				isame[4] = ks == k;
				isame[5] = als.r == alpha.r && als.i == 
					alpha.i;
				isame[6] = lze_(&as[1], &aa[1], &laa);
				isame[7] = ldas == lda;
				isame[8] = lze_(&bs[1], &bb[1], &lbb);
				isame[9] = ldbs == ldb;
				isame[10] = bls.r == beta.r && bls.i == 
					beta.i;
				if (null) {
				    isame[11] = lze_(&cs[1], &cc[1], &lcc);
				} else {
				    isame[11] = lzeres_("ge", " ", &m, &n, &
					    cs[1], &cc[1], &ldc, (ftnlen)2, (
					    ftnlen)1);
				}
				isame[12] = ldcs == ldc;

/*                          If data was incorrectly changed, report */
/*                          and return. */

				same = TRUE_;
				i__6 = nargs;
				for (i__ = 1; i__ <= i__6; ++i__) {
				    same = same && isame[i__ - 1];
				    if (! isame[i__ - 1]) {
	                                printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				    }
/* L40: */
				}
				if (! same) {
				    *fatal = TRUE_;
				    goto L120;
				}

				if (! null) {

/*                             Check the result. */

				    zmmch_(transa, transb, &m, &n, &k, &alpha,
					     &a[a_offset], nmax, &b[b_offset],
					     nmax, &beta, &c__[c_offset], 
					    nmax, &ct[1], &g[1], &cc[1], &ldc,
					     eps, &err, fatal, nout, &c_true, 
					    (ftnlen)1, (ftnlen)1);
				    errmax = f2cmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L120;
				    }
				}

/* L50: */
			    }

/* L60: */
			}

L70:
			;
		    }

L80:
		    ;
		}

/* L90: */
	    }

L100:
	    ;
	}

/* L110: */
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L130;

L120:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    zprcn1_(nout, &nc, sname, iorder, transa, transb, &m, &n, &k, &alpha, &
	    lda, &ldb, &beta, &ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1);

L130:
    return 0;

/* 9995 FORMAT( 1X, I6, ': ', A12,'(''', A1, ''',''', A1, ''',', */
/*     $     3( I3, ',' ), '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, */
/*     $     ',(', F4.1, ',', F4.1, '), C,', I3, ').' ) */

/*     End of ZCHK1. */

} /* zchk1_ */


/* Subroutine */ int zprcn1_(integer* nout, integer* nc, char* sname, integer* iorder, char* transa, char* transb, integer* m, integer* n, integer* k, doublecomplex* alpha, integer* lda, integer* ldb, doublecomplex* beta, integer* ldc, ftnlen sname_len, ftnlen transa_len, ftnlen transb_len)
{
    /* Local variables */
    static char crc[14], cta[14], ctb[14];

    if (*(unsigned char *)transa == 'N') {
	s_copy(cta, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(cta, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cta, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transb == 'N') {
	s_copy(ctb, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transb == 'T') {
	s_copy(ctb, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ctb, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cta,ctb);
    printf("%d %d %d (%4.1lf,%4.1lf) , A, %d, B, %d, (%4.1lf,%4.1lf) , C, %d.\n",*m,*n,*k,alpha->r,alpha->i,*lda,*ldb,beta->r,beta->i,*ldc);

return 0;
} /* zprcn1_ */


/* Subroutine */ int zchk2_(char* sname, doublereal* eps, doublereal* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, doublecomplex* alf, integer* nbet, doublecomplex* bet, integer* nmax, doublecomplex* a, doublecomplex* aa, doublecomplex* as, doublecomplex* b, doublecomplex* bb, doublecomplex* bs, doublecomplex* c__, doublecomplex* cc, doublecomplex* cs, doublecomplex* ct, doublereal* g, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char ichs[2+1] = "LR";
    static char ichu[2+1] = "UL";

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2, 
	    i__3, i__4, i__5, i__6, i__7;

    /* Local variables */
    static doublecomplex beta;
    static integer ldas, ldbs, ldcs;
    static logical same;
    static char side[1];
    static logical isconj, left, null;
    static char uplo[1];
    static integer i__, m, n;
    static doublecomplex alpha;
    static logical isame[13];
    static char sides[1];
    extern /* Subroutine */ int zmake_(char*, char*, char*, integer*, integer*, doublecomplex*, integer*, doublecomplex*, integer*, logical*, doublecomplex*, ftnlen, ftnlen, ftnlen);
    static integer nargs;
    extern /* Subroutine */ int zmmch_(char*, char*, integer*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, doublereal*, doublecomplex*, integer*, doublereal*, doublereal*, logical*, integer*, logical*, ftnlen, ftnlen);
    static logical reset;
    static char uplos[1];
    static integer ia, ib;
    extern /* Subroutine */ int zprcn2_(integer*, integer*, char*, integer*, char*, char*, integer*, integer*, doublecomplex*, integer*, integer*, doublecomplex*, integer*, ftnlen, ftnlen, ftnlen);
    static integer na, nc, im, in, ms, ns;
    extern /* Subroutine */ void czhemm_(integer*, char*, char*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, integer*, ftnlen, ftnlen);
    static doublereal errmax;
    extern logical lzeres_(char*, char*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, ftnlen, ftnlen);
    extern /* Subroutine */ void czsymm_(integer*, char*, char*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, integer*, ftnlen, ftnlen);
    static integer laa, lbb, lda, lcc, ldb, ldc, ics;
    static doublecomplex als, bls;
    static integer icu;
    static doublereal err;
    extern logical lze_(doublecomplex*, doublecomplex*, integer*);

/*  Tests ZHEMM and ZSYMM. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
    /* Parameter adjustments */
    --idim;
    --alf;
    --bet;
    --g;
    --ct;
    --cs;
    --cc;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --bs;
    --bb;
    b_dim1 = *nmax;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */
/*     .. Executable Statements .. */
    isconj = s_cmp(sname + 7, "he", (ftnlen)2, (ftnlen)2) == 0;

    nargs = 12;
    nc = 0;
    reset = TRUE_;
    errmax = 0.;

    i__1 = *nidim;
    for (im = 1; im <= i__1; ++im) {
	m = idim[im];

	i__2 = *nidim;
	for (in = 1; in <= i__2; ++in) {
	    n = idim[in];
/*           Set LDC to 1 more than minimum value if room. */
	    ldc = m;
	    if (ldc < *nmax) {
		++ldc;
	    }
/*           Skip tests if not enough room. */
	    if (ldc > *nmax) {
		goto L90;
	    }
	    lcc = ldc * n;
	    null = n <= 0 || m <= 0;
/*           Set LDB to 1 more than minimum value if room. */
	    ldb = m;
	    if (ldb < *nmax) {
		++ldb;
	    }
/*           Skip tests if not enough room. */
	    if (ldb > *nmax) {
		goto L90;
	    }
	    lbb = ldb * n;

/*           Generate the matrix B. */

	    zmake_("ge", " ", " ", &m, &n, &b[b_offset], nmax, &bb[1], &ldb, &
		    reset, &c_b1, (ftnlen)2, (ftnlen)1, (ftnlen)1);

	    for (ics = 1; ics <= 2; ++ics) {
		*(unsigned char *)side = *(unsigned char *)&ichs[ics - 1];
		left = *(unsigned char *)side == 'L';

		if (left) {
		    na = m;
		} else {
		    na = n;
		}
/*              Set LDA to 1 more than minimum value if room. */
		lda = na;
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L80;
		}
		laa = lda * na;

		for (icu = 1; icu <= 2; ++icu) {
		    *(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];

/*                 Generate the hermitian or symmetric matrix A. */

		    zmake_(sname + 7, uplo, " ", &na, &na, &a[a_offset], nmax,
			     &aa[1], &lda, &reset, &c_b1, (ftnlen)2, (ftnlen)
			    1, (ftnlen)1);

		    i__3 = *nalf;
		    for (ia = 1; ia <= i__3; ++ia) {
			i__4 = ia;
			alpha.r = alf[i__4].r, alpha.i = alf[i__4].i;

			i__4 = *nbet;
			for (ib = 1; ib <= i__4; ++ib) {
			    i__5 = ib;
			    beta.r = bet[i__5].r, beta.i = bet[i__5].i;

/*                       Generate the matrix C. */

			    zmake_("ge", " ", " ", &m, &n, &c__[c_offset], 
				    nmax, &cc[1], &ldc, &reset, &c_b1, (
				    ftnlen)2, (ftnlen)1, (ftnlen)1);

			    ++nc;

/*                       Save every datum before calling the */
/*                       subroutine. */

			    *(unsigned char *)sides = *(unsigned char *)side;
			    *(unsigned char *)uplos = *(unsigned char *)uplo;
			    ms = m;
			    ns = n;
			    als.r = alpha.r, als.i = alpha.i;
			    i__5 = laa;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				as[i__6].r = aa[i__7].r, as[i__6].i = aa[i__7]
					.i;
/* L10: */
			    }
			    ldas = lda;
			    i__5 = lbb;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				bs[i__6].r = bb[i__7].r, bs[i__6].i = bb[i__7]
					.i;
/* L20: */
			    }
			    ldbs = ldb;
			    bls.r = beta.r, bls.i = beta.i;
			    i__5 = lcc;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				cs[i__6].r = cc[i__7].r, cs[i__6].i = cc[i__7]
					.i;
/* L30: */
			    }
			    ldcs = ldc;

/*                       Call the subroutine. */

			    if (*trace) {
				zprcn2_(ntra, &nc, sname, iorder, side, uplo, 
					&m, &n, &alpha, &lda, &ldb, &beta, &
					ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1)
					;
			    }
			    if (*rewi) {
/*				al__1.aerr = 0;
				al__1.aunit = *ntra;
				f_rew(&al__1);*/
			    }
			    if (isconj) {
				czhemm_(iorder, side, uplo, &m, &n, &alpha, &
					aa[1], &lda, &bb[1], &ldb, &beta, &cc[
					1], &ldc, (ftnlen)1, (ftnlen)1);
			    } else {
				czsymm_(iorder, side, uplo, &m, &n, &alpha, &
					aa[1], &lda, &bb[1], &ldb, &beta, &cc[
					1], &ldc, (ftnlen)1, (ftnlen)1);
			    }

/*                       Check if error-exit was taken incorrectly. */

			    if (! infoc_1.ok) {
			    	printf("*** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				*fatal = TRUE_;
				goto L110;
			    }

/*                       See what data changed inside subroutines. */

			    isame[0] = *(unsigned char *)sides == *(unsigned 
				    char *)side;
			    isame[1] = *(unsigned char *)uplos == *(unsigned 
				    char *)uplo;
			    isame[2] = ms == m;
			    isame[3] = ns == n;
			    isame[4] = als.r == alpha.r && als.i == alpha.i;
			    isame[5] = lze_(&as[1], &aa[1], &laa);
			    isame[6] = ldas == lda;
			    isame[7] = lze_(&bs[1], &bb[1], &lbb);
			    isame[8] = ldbs == ldb;
			    isame[9] = bls.r == beta.r && bls.i == beta.i;
			    if (null) {
				isame[10] = lze_(&cs[1], &cc[1], &lcc);
			    } else {
				isame[10] = lzeres_("ge", " ", &m, &n, &cs[1],
					 &cc[1], &ldc, (ftnlen)2, (ftnlen)1);
			    }
			    isame[11] = ldcs == ldc;

/*                       If data was incorrectly changed, report and */
/*                       return. */

			    same = TRUE_;
			    i__5 = nargs;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				same = same && isame[i__ - 1];
				if (! isame[i__ - 1]) {
                                printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				}
/* L40: */
			    }
			    if (! same) {
				*fatal = TRUE_;
				goto L110;
			    }

			    if (! null) {

/*                          Check the result. */

				if (left) {
				    zmmch_("N", "N", &m, &n, &m, &alpha, &a[
					    a_offset], nmax, &b[b_offset], 
					    nmax, &beta, &c__[c_offset], nmax,
					     &ct[1], &g[1], &cc[1], &ldc, eps,
					     &err, fatal, nout, &c_true, (
					    ftnlen)1, (ftnlen)1);
				} else {
				    zmmch_("N", "N", &m, &n, &n, &alpha, &b[
					    b_offset], nmax, &a[a_offset], 
					    nmax, &beta, &c__[c_offset], nmax,
					     &ct[1], &g[1], &cc[1], &ldc, eps,
					     &err, fatal, nout, &c_true, (
					    ftnlen)1, (ftnlen)1);
				}
				errmax = f2cmax(errmax,err);
/*                          If got really bad answer, report and */
/*                          return. */
				if (*fatal) {
				    goto L110;
				}
			    }

/* L50: */
			}

/* L60: */
		    }

/* L70: */
		}

L80:
		;
	    }

L90:
	    ;
	}

/* L100: */
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L120;

L110:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    zprcn2_(nout, &nc, sname, iorder, side, uplo, &m, &n, &alpha, &lda, &ldb, 
	    &beta, &ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1);

L120:
    return 0;

/* 9995 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $      '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, ',(', F4.1, */
/*     $      ',', F4.1, '), C,', I3, ')    .' ) */

/*     End of ZCHK2. */

} /* zchk2_ */


/* Subroutine */ int zprcn2_(integer* nout, integer* nc, char* sname, integer* iorder, char* side, char* uplo, integer* m, integer* n, doublecomplex* alpha, integer* lda, integer* ldb, doublecomplex* beta, integer* ldc, ftnlen sname_len, ftnlen side_len, ftnlen uplo_len)
{
    /* Local variables */
    static char cs[14], cu[14], crc[14];

    if (*(unsigned char *)side == 'L') {
	s_copy(cs, "     CblasLeft", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cs, "    CblasRight", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cs,cu);
    printf("%d %d (%4.1lf,%4.1lf) , A, %d, B, %d, (%4.1lf,%4.1lf) , C, %d.\n",*m,*n,alpha->r,alpha->i,*lda,*ldb,beta->r,beta->i,*ldc);

return 0;
} /* zprcn2_ */


/* Subroutine */ int zchk3_(char* sname, doublereal* eps, doublereal* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, doublecomplex* alf, integer* nmax, doublecomplex* a, doublecomplex* aa, doublecomplex* as, doublecomplex* b, doublecomplex* bb, doublecomplex* bs, doublecomplex* ct, doublereal* g, doublecomplex* c__, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char ichu[2+1] = "UL";
    static char icht[3+1] = "NTC";
    static char ichd[2+1] = "UN";
    static char ichs[2+1] = "LR";

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2, 
	    i__3, i__4, i__5, i__6, i__7;
    doublecomplex z__1;

    /* Local variables */
    static char diag[1];
    static integer ldas, ldbs;
    static logical same;
    static char side[1];
    static logical left, null;
    static char uplo[1];
    static integer i__, j, m, n;
    static doublecomplex alpha;
    static char diags[1];
    static logical isame[13];
    static char sides[1];
    extern /* Subroutine */ int zmake_(char*, char*, char*, integer*, integer*, doublecomplex*, integer*, doublecomplex*, integer*, logical*, doublecomplex*, ftnlen, ftnlen, ftnlen);
    static integer nargs;
    extern /* Subroutine */ int zmmch_(char*, char*, integer*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, doublereal*, doublecomplex*, integer*, doublereal*, doublereal*, logical*, integer*, logical*, ftnlen, ftnlen);
    static logical reset;
    static char uplos[1];
    static integer ia, na;
    extern /* Subroutine */ int zprcn3_(integer*, integer*, char*, integer*, char*, char*, char*, char*, integer*, integer*, doublecomplex*, integer*, integer*, ftnlen, ftnlen, ftnlen, ftnlen, ftnlen);
    static integer nc, im, in, ms, ns;
    static char tranas[1], transa[1];
    static doublereal errmax;
    extern logical lzeres_(char*, char*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, ftnlen, ftnlen);
    extern /* Subroutine */ void cztrmm_(integer*, char*, char*, char*, char*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, ftnlen, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ void cztrsm_(integer*, char*, char*, char*, char*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, ftnlen, ftnlen, ftnlen, ftnlen);
    static integer laa, icd, lbb, lda, ldb, ics;
    static doublecomplex als;
    static integer ict, icu;
    static doublereal err;
    extern logical lze_(doublecomplex*, doublecomplex*, integer*);

/*  Tests ZTRMM and ZTRSM. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
    /* Parameter adjustments */
    --idim;
    --alf;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --g;
    --ct;
    --bs;
    --bb;
    b_dim1 = *nmax;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */
/*     .. Executable Statements .. */

    nargs = 11;
    nc = 0;
    reset = TRUE_;
    errmax = 0.;
/*     Set up zero matrix for ZMMCH. */
    i__1 = *nmax;
    for (j = 1; j <= i__1; ++j) {
	i__2 = *nmax;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    i__3 = i__ + j * c_dim1;
	    c__[i__3].r = 0., c__[i__3].i = 0.;
/* L10: */
	}
/* L20: */
    }

    i__1 = *nidim;
    for (im = 1; im <= i__1; ++im) {
	m = idim[im];

	i__2 = *nidim;
	for (in = 1; in <= i__2; ++in) {
	    n = idim[in];
/*           Set LDB to 1 more than minimum value if room. */
	    ldb = m;
	    if (ldb < *nmax) {
		++ldb;
	    }
/*           Skip tests if not enough room. */
	    if (ldb > *nmax) {
		goto L130;
	    }
	    lbb = ldb * n;
	    null = m <= 0 || n <= 0;

	    for (ics = 1; ics <= 2; ++ics) {
		*(unsigned char *)side = *(unsigned char *)&ichs[ics - 1];
		left = *(unsigned char *)side == 'L';
		if (left) {
		    na = m;
		} else {
		    na = n;
		}
/*              Set LDA to 1 more than minimum value if room. */
		lda = na;
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L130;
		}
		laa = lda * na;

		for (icu = 1; icu <= 2; ++icu) {
		    *(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];

		    for (ict = 1; ict <= 3; ++ict) {
			*(unsigned char *)transa = *(unsigned char *)&icht[
				ict - 1];

			for (icd = 1; icd <= 2; ++icd) {
			    *(unsigned char *)diag = *(unsigned char *)&ichd[
				    icd - 1];

			    i__3 = *nalf;
			    for (ia = 1; ia <= i__3; ++ia) {
				i__4 = ia;
				alpha.r = alf[i__4].r, alpha.i = alf[i__4].i;

/*                          Generate the matrix A. */

				zmake_("tr", uplo, diag, &na, &na, &a[
					a_offset], nmax, &aa[1], &lda, &reset,
					 &c_b1, (ftnlen)2, (ftnlen)1, (ftnlen)
					1);

/*                          Generate the matrix B. */

				zmake_("ge", " ", " ", &m, &n, &b[b_offset], 
					nmax, &bb[1], &ldb, &reset, &c_b1, (
					ftnlen)2, (ftnlen)1, (ftnlen)1);

				++nc;

/*                          Save every datum before calling the */
/*                          subroutine. */

				*(unsigned char *)sides = *(unsigned char *)
					side;
				*(unsigned char *)uplos = *(unsigned char *)
					uplo;
				*(unsigned char *)tranas = *(unsigned char *)
					transa;
				*(unsigned char *)diags = *(unsigned char *)
					diag;
				ms = m;
				ns = n;
				als.r = alpha.r, als.i = alpha.i;
				i__4 = laa;
				for (i__ = 1; i__ <= i__4; ++i__) {
				    i__5 = i__;
				    i__6 = i__;
				    as[i__5].r = aa[i__6].r, as[i__5].i = aa[
					    i__6].i;
/* L30: */
				}
				ldas = lda;
				i__4 = lbb;
				for (i__ = 1; i__ <= i__4; ++i__) {
				    i__5 = i__;
				    i__6 = i__;
				    bs[i__5].r = bb[i__6].r, bs[i__5].i = bb[
					    i__6].i;
/* L40: */
				}
				ldbs = ldb;

/*                          Call the subroutine. */

				if (s_cmp(sname + 9, "mm", (ftnlen)2, (ftnlen)
					2) == 0) {
				    if (*trace) {
					zprcn3_(ntra, &nc, sname, iorder, 
						side, uplo, transa, diag, &m, 
						&n, &alpha, &lda, &ldb, (
						ftnlen)12, (ftnlen)1, (ftnlen)
						1, (ftnlen)1, (ftnlen)1);
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    cztrmm_(iorder, side, uplo, transa, diag, 
					    &m, &n, &alpha, &aa[1], &lda, &bb[
					    1], &ldb, (ftnlen)1, (ftnlen)1, (
					    ftnlen)1, (ftnlen)1);
				} else if (s_cmp(sname + 9, "sm", (ftnlen)2, (
					ftnlen)2) == 0) {
				    if (*trace) {
					zprcn3_(ntra, &nc, sname, iorder, 
						side, uplo, transa, diag, &m, 
						&n, &alpha, &lda, &ldb, (
						ftnlen)12, (ftnlen)1, (ftnlen)
						1, (ftnlen)1, (ftnlen)1);
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    cztrsm_(iorder, side, uplo, transa, diag, 
					    &m, &n, &alpha, &aa[1], &lda, &bb[
					    1], &ldb, (ftnlen)1, (ftnlen)1, (
					    ftnlen)1, (ftnlen)1);
				}

/*                          Check if error-exit was taken incorrectly. */

				if (! infoc_1.ok) {
                                    printf("*** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				    *fatal = TRUE_;
				    goto L150;
				}

/*                          See what data changed inside subroutines. */

				isame[0] = *(unsigned char *)sides == *(
					unsigned char *)side;
				isame[1] = *(unsigned char *)uplos == *(
					unsigned char *)uplo;
				isame[2] = *(unsigned char *)tranas == *(
					unsigned char *)transa;
				isame[3] = *(unsigned char *)diags == *(
					unsigned char *)diag;
				isame[4] = ms == m;
				isame[5] = ns == n;
				isame[6] = als.r == alpha.r && als.i == 
					alpha.i;
				isame[7] = lze_(&as[1], &aa[1], &laa);
				isame[8] = ldas == lda;
				if (null) {
				    isame[9] = lze_(&bs[1], &bb[1], &lbb);
				} else {
				    isame[9] = lzeres_("ge", " ", &m, &n, &bs[
					    1], &bb[1], &ldb, (ftnlen)2, (
					    ftnlen)1);
				}
				isame[10] = ldbs == ldb;

/*                          If data was incorrectly changed, report and */
/*                          return. */

				same = TRUE_;
				i__4 = nargs;
				for (i__ = 1; i__ <= i__4; ++i__) {
				    same = same && isame[i__ - 1];
				    if (! isame[i__ - 1]) {
                                        printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				    }
/* L50: */
				}
				if (! same) {
				    *fatal = TRUE_;
				    goto L150;
				}

				if (! null) {
				    if (s_cmp(sname + 9, "mm", (ftnlen)2, (
					    ftnlen)2) == 0) {

/*                                Check the result. */

					if (left) {
					    zmmch_(transa, "N", &m, &n, &m, &
						    alpha, &a[a_offset], nmax,
						     &b[b_offset], nmax, &
						    c_b1, &c__[c_offset], 
						    nmax, &ct[1], &g[1], &bb[
						    1], &ldb, eps, &err, 
						    fatal, nout, &c_true, (
						    ftnlen)1, (ftnlen)1);
					} else {
					    zmmch_("N", transa, &m, &n, &n, &
						    alpha, &b[b_offset], nmax,
						     &a[a_offset], nmax, &
						    c_b1, &c__[c_offset], 
						    nmax, &ct[1], &g[1], &bb[
						    1], &ldb, eps, &err, 
						    fatal, nout, &c_true, (
						    ftnlen)1, (ftnlen)1);
					}
				    } else if (s_cmp(sname + 9, "sm", (ftnlen)
					    2, (ftnlen)2) == 0) {

/*                                Compute approximation to original */
/*                                matrix. */

					i__4 = n;
					for (j = 1; j <= i__4; ++j) {
					    i__5 = m;
					    for (i__ = 1; i__ <= i__5; ++i__) 
						    {
			  i__6 = i__ + j * c_dim1;
			  i__7 = i__ + (j - 1) * ldb;
			  c__[i__6].r = bb[i__7].r, c__[i__6].i = bb[i__7].i;
			  i__6 = i__ + (j - 1) * ldb;
			  i__7 = i__ + j * b_dim1;
			  z__1.r = alpha.r * b[i__7].r - alpha.i * b[i__7].i, 
				  z__1.i = alpha.r * b[i__7].i + alpha.i * b[
				  i__7].r;
			  bb[i__6].r = z__1.r, bb[i__6].i = z__1.i;
/* L60: */
					    }
/* L70: */
					}

					if (left) {
					    zmmch_(transa, "N", &m, &n, &m, &
						    c_b2, &a[a_offset], nmax, 
						    &c__[c_offset], nmax, &
						    c_b1, &b[b_offset], nmax, 
						    &ct[1], &g[1], &bb[1], &
						    ldb, eps, &err, fatal, 
						    nout, &c_false, (ftnlen)1,
						     (ftnlen)1);
					} else {
					    zmmch_("N", transa, &m, &n, &n, &
						    c_b2, &c__[c_offset], 
						    nmax, &a[a_offset], nmax, 
						    &c_b1, &b[b_offset], nmax,
						     &ct[1], &g[1], &bb[1], &
						    ldb, eps, &err, fatal, 
						    nout, &c_false, (ftnlen)1,
						     (ftnlen)1);
					}
				    }
				    errmax = f2cmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L150;
				    }
				}

/* L80: */
			    }

/* L90: */
			}

/* L100: */
		    }

/* L110: */
		}

/* L120: */
	    }

L130:
	    ;
	}

/* L140: */
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L160;

L150:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    if (*trace) {
	zprcn3_(ntra, &nc, sname, iorder, side, uplo, transa, diag, &m, &n, &
		alpha, &lda, &ldb, (ftnlen)12, (ftnlen)1, (ftnlen)1, (ftnlen)
		1, (ftnlen)1);
    }

L160:
    return 0;

/* 9995 FORMAT(1X, I6, ': ', A12,'(', 4( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $     '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, ')         ', */
/*     $      '      .' ) */

/*     End of ZCHK3. */

} /* zchk3_ */


/* Subroutine */ int zprcn3_(integer* nout, integer* nc, char* sname, integer* iorder, char* side, char* uplo, char* transa, char* diag, integer* m, integer* n, doublecomplex* alpha, integer* lda, integer* ldb, ftnlen sname_len, ftnlen side_len, ftnlen uplo_len, ftnlen transa_len, ftnlen diag_len)
{

    /* Local variables */
    static char ca[14], cd[14], cs[14], cu[14], crc[14];

    if (*(unsigned char *)side == 'L') {
	s_copy(cs, "     CblasLeft", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cs, "    CblasRight", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)diag == 'N') {
	s_copy(cd, "  CblasNonUnit", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cd, "     CblasUnit", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cs,cu);
    printf("         %s %s %d %d (%4.1lf,%4.1lf) A %d B %d\n",ca,cd,*m,*n,alpha->r,alpha->i,*lda,*ldb);

return 0;
} /* zprcn3_ */


/* Subroutine */ int zchk4_(char* sname, doublereal* eps, doublereal* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, doublecomplex* alf, integer* nbet, doublecomplex* bet, integer* nmax, doublecomplex* a, doublecomplex* aa, doublecomplex* as, doublecomplex* b, doublecomplex* bb, doublecomplex* bs, doublecomplex* c__, doublecomplex* cc, doublecomplex* cs, doublecomplex* ct, doublereal* g, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char icht[2+1] = "NC";
    static char ichu[2+1] = "UL";

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2, 
	    i__3, i__4, i__5, i__6, i__7;
    doublecomplex z__1;

    /* Local variables */
    static doublecomplex beta;
    static integer ldas, ldcs;
    static logical same, isconj;
    static doublecomplex bets;
    static doublereal rals;
    static logical tran, null;
    static char uplo[1];
    static integer i__, j, k, n;
    static doublecomplex alpha;
    static doublereal rbeta;
    static logical isame[13];
    extern /* Subroutine */ int zmake_(char*, char*, char*, integer*, integer*, doublecomplex*, integer*, doublecomplex*, integer*, logical*, doublecomplex*, ftnlen, ftnlen, ftnlen);
    static integer nargs;
    extern /* Subroutine */ int zmmch_(char*, char*, integer*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, doublereal*, doublecomplex*, integer*, doublereal*, doublereal*, logical*, integer*, logical*, ftnlen, ftnlen);
    static doublereal rbets;
    static logical reset;
    static char trans[1];
    static logical upper;
    static char uplos[1];
    static integer ia, ib, jc, ma, na;
    extern /* Subroutine */ int zprcn4_(integer*, integer*, char*, integer*, char*, char*, integer*, integer*, doublecomplex*, integer*, doublecomplex*, integer*, ftnlen, ftnlen, ftnlen);
    static integer nc;
    extern /* Subroutine */ int zprcn6_(integer*, integer*, char*, integer*, char*, char*, integer*, integer*, doublereal*, integer*, doublereal*, integer*, ftnlen, ftnlen, ftnlen);
    static integer ik, in, jj, lj, ks, ns;
    static doublereal ralpha;
    extern /* Subroutine */ int czherk_(integer*, char*, char*, integer*, integer*, doublereal*, doublecomplex*, integer*, doublereal*, doublecomplex*, integer*, ftnlen, ftnlen);
    static doublereal errmax;
    extern logical lzeres_(char*, char*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, ftnlen, ftnlen);
    static char transs[1], transt[1];
    extern /* Subroutine */ int czsyrk_(integer*, char*, char*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, integer*, ftnlen, ftnlen);
    static integer laa, lda, lcc, ldc;
    static doublecomplex als;
    static integer ict, icu;
    static doublereal err;
    extern logical lze_(doublecomplex*, doublecomplex*, integer*);

/*  Tests ZHERK and ZSYRK. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
    /* Parameter adjustments */
    --idim;
    --alf;
    --bet;
    --g;
    --ct;
    --cs;
    --cc;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --bs;
    --bb;
    b_dim1 = *nmax;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */
/*     .. Executable Statements .. */
    isconj = s_cmp(sname + 7, "he", (ftnlen)2, (ftnlen)2) == 0;

    nargs = 10;
    nc = 0;
    reset = TRUE_;
    errmax = 0.;
    rals = 1.;
    rbets = 1.;

    i__1 = *nidim;
    for (in = 1; in <= i__1; ++in) {
	n = idim[in];
/*        Set LDC to 1 more than minimum value if room. */
	ldc = n;
	if (ldc < *nmax) {
	    ++ldc;
	}
/*        Skip tests if not enough room. */
	if (ldc > *nmax) {
	    goto L100;
	}
	lcc = ldc * n;

	i__2 = *nidim;
	for (ik = 1; ik <= i__2; ++ik) {
	    k = idim[ik];

	    for (ict = 1; ict <= 2; ++ict) {
		*(unsigned char *)trans = *(unsigned char *)&icht[ict - 1];
		tran = *(unsigned char *)trans == 'C';
		if (tran && ! isconj) {
		    *(unsigned char *)trans = 'T';
		}
		if (tran) {
		    ma = k;
		    na = n;
		} else {
		    ma = n;
		    na = k;
		}
/*              Set LDA to 1 more than minimum value if room. */
		lda = ma;
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L80;
		}
		laa = lda * na;

/*              Generate the matrix A. */

		zmake_("ge", " ", " ", &ma, &na, &a[a_offset], nmax, &aa[1], &
			lda, &reset, &c_b1, (ftnlen)2, (ftnlen)1, (ftnlen)1);

		for (icu = 1; icu <= 2; ++icu) {
		    *(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];
		    upper = *(unsigned char *)uplo == 'U';

		    i__3 = *nalf;
		    for (ia = 1; ia <= i__3; ++ia) {
			i__4 = ia;
			alpha.r = alf[i__4].r, alpha.i = alf[i__4].i;
			if (isconj) {
			    ralpha = alpha.r;
			    z__1.r = ralpha, z__1.i = 0.;
			    alpha.r = z__1.r, alpha.i = z__1.i;
			}

			i__4 = *nbet;
			for (ib = 1; ib <= i__4; ++ib) {
			    i__5 = ib;
			    beta.r = bet[i__5].r, beta.i = bet[i__5].i;
			    if (isconj) {
				rbeta = beta.r;
				z__1.r = rbeta, z__1.i = 0.;
				beta.r = z__1.r, beta.i = z__1.i;
			    }
			    null = n <= 0;
			    if (isconj) {
				null = null ||( (k <= 0 || ralpha == 0.) && 
					rbeta == 1.);
			    }

/*                       Generate the matrix C. */

			    zmake_(sname + 7, uplo, " ", &n, &n, &c__[
				    c_offset], nmax, &cc[1], &ldc, &reset, &
				    c_b1, (ftnlen)2, (ftnlen)1, (ftnlen)1);

			    ++nc;

/*                       Save every datum before calling the subroutine. */

			    *(unsigned char *)uplos = *(unsigned char *)uplo;
			    *(unsigned char *)transs = *(unsigned char *)
				    trans;
			    ns = n;
			    ks = k;
			    if (isconj) {
				rals = ralpha;
			    } else {
				als.r = alpha.r, als.i = alpha.i;
			    }
			    i__5 = laa;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				as[i__6].r = aa[i__7].r, as[i__6].i = aa[i__7]
					.i;
/* L10: */
			    }
			    ldas = lda;
			    if (isconj) {
				rbets = rbeta;
			    } else {
				bets.r = beta.r, bets.i = beta.i;
			    }
			    i__5 = lcc;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				cs[i__6].r = cc[i__7].r, cs[i__6].i = cc[i__7]
					.i;
/* L20: */
			    }
			    ldcs = ldc;

/*                       Call the subroutine. */

			    if (isconj) {
				if (*trace) {
				    zprcn6_(ntra, &nc, sname, iorder, uplo, 
					    trans, &n, &k, &ralpha, &lda, &
					    rbeta, &ldc, (ftnlen)12, (ftnlen)
					    1, (ftnlen)1);
				}
				if (*rewi) {
/*				    al__1.aerr = 0;
				    al__1.aunit = *ntra;
				    f_rew(&al__1);*/
				}
				czherk_(iorder, uplo, trans, &n, &k, &ralpha, 
					&aa[1], &lda, &rbeta, &cc[1], &ldc, (
					ftnlen)1, (ftnlen)1);
			    } else {
				if (*trace) {
				    zprcn4_(ntra, &nc, sname, iorder, uplo, 
					    trans, &n, &k, &alpha, &lda, &
					    beta, &ldc, (ftnlen)12, (ftnlen)1,
					     (ftnlen)1);
				}
				if (*rewi) {
/*				    al__1.aerr = 0;
				    al__1.aunit = *ntra;
				    f_rew(&al__1);*/
				}
				czsyrk_(iorder, uplo, trans, &n, &k, &alpha, &
					aa[1], &lda, &beta, &cc[1], &ldc, (
					ftnlen)1, (ftnlen)1);
			    }

/*                       Check if error-exit was taken incorrectly. */

			    if (! infoc_1.ok) {
                                printf("*** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				*fatal = TRUE_;
				goto L120;
			    }

/*                       See what data changed inside subroutines. */

			    isame[0] = *(unsigned char *)uplos == *(unsigned 
				    char *)uplo;
			    isame[1] = *(unsigned char *)transs == *(unsigned 
				    char *)trans;
			    isame[2] = ns == n;
			    isame[3] = ks == k;
			    if (isconj) {
				isame[4] = rals == ralpha;
			    } else {
				isame[4] = als.r == alpha.r && als.i == 
					alpha.i;
			    }
			    isame[5] = lze_(&as[1], &aa[1], &laa);
			    isame[6] = ldas == lda;
			    if (isconj) {
				isame[7] = rbets == rbeta;
			    } else {
				isame[7] = bets.r == beta.r && bets.i == 
					beta.i;
			    }
			    if (null) {
				isame[8] = lze_(&cs[1], &cc[1], &lcc);
			    } else {
				isame[8] = lzeres_(sname + 7, uplo, &n, &n, &
					cs[1], &cc[1], &ldc, (ftnlen)2, (
					ftnlen)1);
			    }
			    isame[9] = ldcs == ldc;

/*                       If data was incorrectly changed, report and */
/*                       return. */

			    same = TRUE_;
			    i__5 = nargs;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				same = same && isame[i__ - 1];
				if (! isame[i__ - 1]) {
                                    printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				}
/* L30: */
			    }
			    if (! same) {
				*fatal = TRUE_;
				goto L120;
			    }

			    if (! null) {

/*                          Check the result column by column. */

				if (isconj) {
				    *(unsigned char *)transt = 'C';
				} else {
				    *(unsigned char *)transt = 'T';
				}
				jc = 1;
				i__5 = n;
				for (j = 1; j <= i__5; ++j) {
				    if (upper) {
					jj = 1;
					lj = j;
				    } else {
					jj = j;
					lj = n - j + 1;
				    }
				    if (tran) {
					zmmch_(transt, "N", &lj, &c__1, &k, &
						alpha, &a[jj * a_dim1 + 1], 
						nmax, &a[j * a_dim1 + 1], 
						nmax, &beta, &c__[jj + j * 
						c_dim1], nmax, &ct[1], &g[1], 
						&cc[jc], &ldc, eps, &err, 
						fatal, nout, &c_true, (ftnlen)
						1, (ftnlen)1);
				    } else {
					zmmch_("N", transt, &lj, &c__1, &k, &
						alpha, &a[jj + a_dim1], nmax, 
						&a[j + a_dim1], nmax, &beta, &
						c__[jj + j * c_dim1], nmax, &
						ct[1], &g[1], &cc[jc], &ldc, 
						eps, &err, fatal, nout, &
						c_true, (ftnlen)1, (ftnlen)1);
				    }
				    if (upper) {
					jc += ldc;
				    } else {
					jc = jc + ldc + 1;
				    }
				    errmax = f2cmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L110;
				    }
/* L40: */
				}
			    }

/* L50: */
			}

/* L60: */
		    }

/* L70: */
		}

L80:
		;
	    }

/* L90: */
	}

L100:
	;
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L130;

L110:
    if (n > 1) {
        printf("      THESE ARE THE RESULTS FOR COLUMN %d:\n",j);
    }

L120:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    if (isconj) {
	zprcn6_(nout, &nc, sname, iorder, uplo, trans, &n, &k, &ralpha, &lda, 
		&rbeta, &ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1);
    } else {
	zprcn4_(nout, &nc, sname, iorder, uplo, trans, &n, &k, &alpha, &lda, &
		beta, &ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1);
    }

L130:
    return 0;

/* 9994 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $     F4.1, ', A,', I3, ',', F4.1, ', C,', I3, ')               ', */
/*     $      '          .' ) */
/* 9993 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $      '(', F4.1, ',', F4.1, ') , A,', I3, ',(', F4.1, ',', F4.1, */
/*     $      '), C,', I3, ')          .' ) */

/*     End of CCHK4. */

} /* zchk4_ */


/* Subroutine */ int zprcn4_(integer* nout, integer* nc, char* sname, integer* iorder, char* uplo, char* transa, integer* n, integer* k, doublecomplex* alpha, integer* lda, doublecomplex* beta, integer* ldc, ftnlen sname_len, ftnlen uplo_len, ftnlen transa_len)
{
    /* Local variables */
    static char ca[14], cu[14], crc[14];

    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cu,ca);
    printf("(          %d %d (%4.1lf,%4.1lf) A %d (%4.1lf,%4.1lf) C %d\n",*n,*k,alpha->r,alpha->i,*lda,beta->r,beta->i,*ldc);

return 0;
} /* zprcn4_ */



/* Subroutine */ int zprcn6_(integer* nout, integer* nc, char* sname, integer* iorder, char* uplo, char* transa, integer* n, integer* k, doublereal* alpha, integer* lda, doublereal* beta, integer* ldc, ftnlen sname_len, ftnlen uplo_len, ftnlen transa_len)
{

    /* Local variables */
    static char ca[14], cu[14], crc[14];

    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cu,ca);
    printf("(          %d %d %4.1lf A %d %4.1lf C %d\n",*n,*k,*alpha,*lda,*beta,*ldc);

return 0;
} /* zprcn6_ */


/* Subroutine */ int zchk5_(char* sname, doublereal* eps, doublereal* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, doublecomplex* alf, integer* nbet, doublecomplex* bet, integer* nmax, doublecomplex* ab, doublecomplex* aa, doublecomplex* as, doublecomplex* bb, doublecomplex* bs, doublecomplex* c__, doublecomplex* cc, doublecomplex* cs, doublecomplex* ct, doublereal* g, doublecomplex* w, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char icht[2+1] = "NC";
    static char ichu[2+1] = "UL";

    /* System generated locals */
    integer c_dim1, c_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7, i__8;
    doublecomplex z__1, z__2;

    /* Local variables */
    static integer jjab;
    static doublecomplex beta;
    static integer ldas, ldbs, ldcs;
    static logical same, isconj;
    static doublecomplex bets;
    static logical tran, null;
    static char uplo[1];
    static integer i__, j, k, n;
    static doublecomplex alpha;
    static doublereal rbeta;
    static logical isame[13];
    extern /* Subroutine */ int zmake_(char*, char*, char*, integer*, integer*, doublecomplex*, integer*, doublecomplex*, integer*, logical*, doublecomplex*, ftnlen, ftnlen, ftnlen);
    static integer nargs;
    extern /* Subroutine */ int zmmch_(char*, char*, integer*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, doublereal*, doublecomplex*, integer*, doublereal*, doublereal*, logical*, integer*, logical*, ftnlen, ftnlen);
    static doublereal rbets;
    static logical reset;
    static char trans[1];
    static logical upper;
    static char uplos[1];
    static integer ia, ib, jc, ma, na, nc;
    extern /* Subroutine */ int zprcn5_(integer*, integer*, char*, integer*, char*, char*, integer*, integer*, doublecomplex*, integer*, integer*, doublecomplex*, integer*, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ int zprcn7_(integer*, integer*, char*, integer*, char*, char*, integer*, integer*, doublecomplex*, integer*, integer*, doublereal*, integer*, ftnlen, ftnlen, ftnlen);
    static integer ik, in, jj, lj, ks, ns;
    static doublereal errmax;
    extern logical lzeres_(char*, char*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, ftnlen, ftnlen);
    static char transs[1], transt[1];
    extern /* Subroutine */ int czher2k_(integer*, char*, char*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, doublereal*, doublecomplex*, integer*, ftnlen, ftnlen);
    static integer laa, lbb, lda, lcc, ldb, ldc;
    static doublecomplex als;
    static integer ict, icu;
    extern /* Subroutine */ int czsyr2k_(integer*, char*, char*, integer*, integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*, doublecomplex*, integer*, ftnlen, ftnlen);
    static doublereal err;
    extern logical lze_(doublecomplex*, doublecomplex*, integer*);

/*  Tests ZHER2K and ZSYR2K. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
    /* Parameter adjustments */
    --idim;
    --alf;
    --bet;
    --w;
    --g;
    --ct;
    --cs;
    --cc;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --bs;
    --bb;
    --as;
    --aa;
    --ab;

    /* Function Body */
/*     .. Executable Statements .. */
    isconj = s_cmp(sname + 7, "he", (ftnlen)2, (ftnlen)2) == 0;

    nargs = 12;
    nc = 0;
    reset = TRUE_;
    errmax = 0.;

    i__1 = *nidim;
    for (in = 1; in <= i__1; ++in) {
	n = idim[in];
/*        Set LDC to 1 more than minimum value if room. */
	ldc = n;
	if (ldc < *nmax) {
	    ++ldc;
	}
/*        Skip tests if not enough room. */
	if (ldc > *nmax) {
	    goto L130;
	}
	lcc = ldc * n;

	i__2 = *nidim;
	for (ik = 1; ik <= i__2; ++ik) {
	    k = idim[ik];

	    for (ict = 1; ict <= 2; ++ict) {
		*(unsigned char *)trans = *(unsigned char *)&icht[ict - 1];
		tran = *(unsigned char *)trans == 'C';
		if (tran && ! isconj) {
		    *(unsigned char *)trans = 'T';
		}
		if (tran) {
		    ma = k;
		    na = n;
		} else {
		    ma = n;
		    na = k;
		}
/*              Set LDA to 1 more than minimum value if room. */
		lda = ma;
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L110;
		}
		laa = lda * na;

/*              Generate the matrix A. */

		if (tran) {
		    i__3 = *nmax << 1;
		    zmake_("ge", " ", " ", &ma, &na, &ab[1], &i__3, &aa[1], &
			    lda, &reset, &c_b1, (ftnlen)2, (ftnlen)1, (ftnlen)
			    1);
		} else {
		    zmake_("ge", " ", " ", &ma, &na, &ab[1], nmax, &aa[1], &
			    lda, &reset, &c_b1, (ftnlen)2, (ftnlen)1, (ftnlen)
			    1);
		}

/*              Generate the matrix B. */

		ldb = lda;
		lbb = laa;
		if (tran) {
		    i__3 = *nmax << 1;
		    zmake_("ge", " ", " ", &ma, &na, &ab[k + 1], &i__3, &bb[1]
			    , &ldb, &reset, &c_b1, (ftnlen)2, (ftnlen)1, (
			    ftnlen)1);
		} else {
		    zmake_("ge", " ", " ", &ma, &na, &ab[k * *nmax + 1], nmax,
			     &bb[1], &ldb, &reset, &c_b1, (ftnlen)2, (ftnlen)
			    1, (ftnlen)1);
		}

		for (icu = 1; icu <= 2; ++icu) {
		    *(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];
		    upper = *(unsigned char *)uplo == 'U';

		    i__3 = *nalf;
		    for (ia = 1; ia <= i__3; ++ia) {
			i__4 = ia;
			alpha.r = alf[i__4].r, alpha.i = alf[i__4].i;

			i__4 = *nbet;
			for (ib = 1; ib <= i__4; ++ib) {
			    i__5 = ib;
			    beta.r = bet[i__5].r, beta.i = bet[i__5].i;
			    if (isconj) {
				rbeta = beta.r;
				z__1.r = rbeta, z__1.i = 0.;
				beta.r = z__1.r, beta.i = z__1.i;
			    }
			    null = n <= 0;
			    if (isconj) {
				null = null ||( (k <= 0 || (alpha.r == 0. && 
					alpha.i == 0.)) && rbeta == 1.);
			    }

/*                       Generate the matrix C. */

			    zmake_(sname + 7, uplo, " ", &n, &n, &c__[
				    c_offset], nmax, &cc[1], &ldc, &reset, &
				    c_b1, (ftnlen)2, (ftnlen)1, (ftnlen)1);

			    ++nc;

/*                       Save every datum before calling the subroutine. */

			    *(unsigned char *)uplos = *(unsigned char *)uplo;
			    *(unsigned char *)transs = *(unsigned char *)
				    trans;
			    ns = n;
			    ks = k;
			    als.r = alpha.r, als.i = alpha.i;
			    i__5 = laa;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				as[i__6].r = aa[i__7].r, as[i__6].i = aa[i__7]
					.i;
/* L10: */
			    }
			    ldas = lda;
			    i__5 = lbb;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				bs[i__6].r = bb[i__7].r, bs[i__6].i = bb[i__7]
					.i;
/* L20: */
			    }
			    ldbs = ldb;
			    if (isconj) {
				rbets = rbeta;
			    } else {
				bets.r = beta.r, bets.i = beta.i;
			    }
			    i__5 = lcc;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				i__6 = i__;
				i__7 = i__;
				cs[i__6].r = cc[i__7].r, cs[i__6].i = cc[i__7]
					.i;
/* L30: */
			    }
			    ldcs = ldc;

/*                       Call the subroutine. */

			    if (isconj) {
				if (*trace) {
				    zprcn7_(ntra, &nc, sname, iorder, uplo, 
					    trans, &n, &k, &alpha, &lda, &ldb,
					     &rbeta, &ldc, (ftnlen)12, (
					    ftnlen)1, (ftnlen)1);
				}
				if (*rewi) {
/*				    al__1.aerr = 0;
				    al__1.aunit = *ntra;
				    f_rew(&al__1);*/
				}
				czher2k_(iorder, uplo, trans, &n, &k, &alpha, 
					&aa[1], &lda, &bb[1], &ldb, &rbeta, &
					cc[1], &ldc, (ftnlen)1, (ftnlen)1);
			    } else {
				if (*trace) {
				    zprcn5_(ntra, &nc, sname, iorder, uplo, 
					    trans, &n, &k, &alpha, &lda, &ldb,
					     &beta, &ldc, (ftnlen)12, (ftnlen)
					    1, (ftnlen)1);
				}
				if (*rewi) {
/*				    al__1.aerr = 0;
				    al__1.aunit = *ntra;
				    f_rew(&al__1);*/
				}
				czsyr2k_(iorder, uplo, trans, &n, &k, &alpha, 
					&aa[1], &lda, &bb[1], &ldb, &beta, &
					cc[1], &ldc, (ftnlen)1, (ftnlen)1);
			    }

/*                       Check if error-exit was taken incorrectly. */

			    if (! infoc_1.ok) {
                                printf("*** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				*fatal = TRUE_;
				goto L150;
			    }

/*                       See what data changed inside subroutines. */

			    isame[0] = *(unsigned char *)uplos == *(unsigned 
				    char *)uplo;
			    isame[1] = *(unsigned char *)transs == *(unsigned 
				    char *)trans;
			    isame[2] = ns == n;
			    isame[3] = ks == k;
			    isame[4] = als.r == alpha.r && als.i == alpha.i;
			    isame[5] = lze_(&as[1], &aa[1], &laa);
			    isame[6] = ldas == lda;
			    isame[7] = lze_(&bs[1], &bb[1], &lbb);
			    isame[8] = ldbs == ldb;
			    if (isconj) {
				isame[9] = rbets == rbeta;
			    } else {
				isame[9] = bets.r == beta.r && bets.i == 
					beta.i;
			    }
			    if (null) {
				isame[10] = lze_(&cs[1], &cc[1], &lcc);
			    } else {
				isame[10] = lzeres_("he", uplo, &n, &n, &cs[1]
					, &cc[1], &ldc, (ftnlen)2, (ftnlen)1);
			    }
			    isame[11] = ldcs == ldc;

/*                       If data was incorrectly changed, report and */
/*                       return. */

			    same = TRUE_;
			    i__5 = nargs;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				same = same && isame[i__ - 1];
				if (! isame[i__ - 1]) {
                                    printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
    				}
/* L40: */
			    }
			    if (! same) {
				*fatal = TRUE_;
				goto L150;
			    }

			    if (! null) {

/*                          Check the result column by column. */

				if (isconj) {
				    *(unsigned char *)transt = 'C';
				} else {
				    *(unsigned char *)transt = 'T';
				}
				jjab = 1;
				jc = 1;
				i__5 = n;
				for (j = 1; j <= i__5; ++j) {
				    if (upper) {
					jj = 1;
					lj = j;
				    } else {
					jj = j;
					lj = n - j + 1;
				    }
				    if (tran) {
					i__6 = k;
					for (i__ = 1; i__ <= i__6; ++i__) {
					    i__7 = i__;
					    i__8 = ((j - 1) << 1) * *nmax + k + 
						    i__;
					    z__1.r = alpha.r * ab[i__8].r - 
						    alpha.i * ab[i__8].i, 
						    z__1.i = alpha.r * ab[
						    i__8].i + alpha.i * ab[
						    i__8].r;
					    w[i__7].r = z__1.r, w[i__7].i = 
						    z__1.i;
					    if (isconj) {
			  i__7 = k + i__;
			  d_cnjg(&z__2, &alpha);
			  i__8 = ((j - 1) << 1) * *nmax + i__;
			  z__1.r = z__2.r * ab[i__8].r - z__2.i * ab[i__8].i, 
				  z__1.i = z__2.r * ab[i__8].i + z__2.i * ab[
				  i__8].r;
			  w[i__7].r = z__1.r, w[i__7].i = z__1.i;
					    } else {
			  i__7 = k + i__;
			  i__8 = ((j - 1) << 1) * *nmax + i__;
			  z__1.r = alpha.r * ab[i__8].r - alpha.i * ab[i__8]
				  .i, z__1.i = alpha.r * ab[i__8].i + alpha.i 
				  * ab[i__8].r;
			  w[i__7].r = z__1.r, w[i__7].i = z__1.i;
					    }
/* L50: */
					}
					i__6 = k << 1;
					i__7 = *nmax << 1;
					i__8 = *nmax << 1;
					zmmch_(transt, "N", &lj, &c__1, &i__6,
						 &c_b2, &ab[jjab], &i__7, &w[
						1], &i__8, &beta, &c__[jj + j 
						* c_dim1], nmax, &ct[1], &g[1]
						, &cc[jc], &ldc, eps, &err, 
						fatal, nout, &c_true, (ftnlen)
						1, (ftnlen)1);
				    } else {
					i__6 = k;
					for (i__ = 1; i__ <= i__6; ++i__) {
					    if (isconj) {
			  i__7 = i__;
			  d_cnjg(&z__2, &ab[(k + i__ - 1) * *nmax + j]);
			  z__1.r = alpha.r * z__2.r - alpha.i * z__2.i, 
				  z__1.i = alpha.r * z__2.i + alpha.i * 
				  z__2.r;
			  w[i__7].r = z__1.r, w[i__7].i = z__1.i;
			  i__7 = k + i__;
			  i__8 = (i__ - 1) * *nmax + j;
			  z__2.r = alpha.r * ab[i__8].r - alpha.i * ab[i__8]
				  .i, z__2.i = alpha.r * ab[i__8].i + alpha.i 
				  * ab[i__8].r;
			  d_cnjg(&z__1, &z__2);
			  w[i__7].r = z__1.r, w[i__7].i = z__1.i;
					    } else {
			  i__7 = i__;
			  i__8 = (k + i__ - 1) * *nmax + j;
			  z__1.r = alpha.r * ab[i__8].r - alpha.i * ab[i__8]
				  .i, z__1.i = alpha.r * ab[i__8].i + alpha.i 
				  * ab[i__8].r;
			  w[i__7].r = z__1.r, w[i__7].i = z__1.i;
			  i__7 = k + i__;
			  i__8 = (i__ - 1) * *nmax + j;
			  z__1.r = alpha.r * ab[i__8].r - alpha.i * ab[i__8]
				  .i, z__1.i = alpha.r * ab[i__8].i + alpha.i 
				  * ab[i__8].r;
			  w[i__7].r = z__1.r, w[i__7].i = z__1.i;
					    }
/* L60: */
					}
					i__6 = k << 1;
					i__7 = *nmax << 1;
					zmmch_("N", "N", &lj, &c__1, &i__6, &
						c_b2, &ab[jj], nmax, &w[1], &
						i__7, &beta, &c__[jj + j * 
						c_dim1], nmax, &ct[1], &g[1], 
						&cc[jc], &ldc, eps, &err, 
						fatal, nout, &c_true, (ftnlen)
						1, (ftnlen)1);
				    }
				    if (upper) {
					jc += ldc;
				    } else {
					jc = jc + ldc + 1;
					if (tran) {
					    jjab += *nmax << 1;
					}
				    }
				    errmax = f2cmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L140;
				    }
/* L70: */
				}
			    }

/* L80: */
			}

/* L90: */
		    }

/* L100: */
		}

L110:
		;
	    }

/* L120: */
	}

L130:
	;
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L160;

L140:
    if (n > 1) {
        printf("      THESE ARE THE RESULTS FOR COLUMN %d:\n",j);
    }

L150:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    if (isconj) {
	zprcn7_(nout, &nc, sname, iorder, uplo, trans, &n, &k, &alpha, &lda, &
		ldb, &rbeta, &ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1);
    } else {
	zprcn5_(nout, &nc, sname, iorder, uplo, trans, &n, &k, &alpha, &lda, &
		ldb, &beta, &ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1);
    }

L160:
    return 0;

/* 9994 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $      '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, ',', F4.1, */
/*     $      ', C,', I3, ')           .' ) */
/* 9993 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $      '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, ',(', F4.1, */
/*     $      ',', F4.1, '), C,', I3, ')    .' ) */

/*     End of ZCHK5. */

} /* zchk5_ */


/* Subroutine */ int zprcn5_(integer* nout, integer* nc, char* sname, integer* iorder, char* uplo, char* transa, integer* n, integer* k, doublecomplex* alpha, integer* lda, integer* ldb, doublecomplex* beta, integer* ldc, ftnlen sname_len, ftnlen uplo_len, ftnlen transa_len)
{
    /* Local variables */
    static char ca[14], cu[14], crc[14];

    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cu,ca);
    printf("%d %d (%4.1lf,%4.1lf) , A, %d, B, %d, (%4.1lf,%4.1lf) , C, %d.\n",*n,*k,alpha->r,alpha->i,*lda,*ldb,beta->r,beta->i,*ldc);

return 0;
} /* zprcn5_ */



/* Subroutine */ int zprcn7_(integer* nout, integer* nc, char* sname, integer* iorder, char* uplo, char* transa, integer* n, integer* k, doublecomplex* alpha, integer* lda, integer* ldb, doublereal* beta, integer* ldc, ftnlen sname_len, ftnlen uplo_len, ftnlen transa_len)
{

    /* Local variables */
    static char ca[14], cu[14], crc[14];

    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cu,ca);
    printf("%d %d (%4.1lf,%4.1lf), A, %d, B, %d, %4.1lf, C, %d.\n",*n,*k,alpha->r,alpha->i,*lda,*ldb,*beta,*ldc);

return 0;
} /* zprcn7_ */


/* Subroutine */ int zmake_(char* type__, char* uplo, char* diag, integer* m, integer* n, doublecomplex* a, integer* nmax, doublecomplex* aa, integer* lda, logical* reset, doublecomplex* transl, ftnlen type_len, ftnlen uplo_len, ftnlen diag_len)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4;
    doublereal d__1;
    doublecomplex z__1, z__2;

    /* Local variables */
    static integer ibeg, iend;
    extern /* Double Complex */ VOID zbeg_(doublecomplex*, logical*);
    static logical unit;
    static integer i__, j;
    static logical lower, upper;
    static integer jj;
    static logical gen, her, tri, sym;


/*  Generates values for an M by N matrix A. */
/*  Stores the values in the array AA in the data structure required */
/*  by the routine, with unwanted elements set to rogue value. */

/*  TYPE is 'ge', 'he', 'sy' or 'tr'. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. External Functions .. */
/*     .. Intrinsic Functions .. */
/*     .. Executable Statements .. */
    /* Parameter adjustments */
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --aa;

    /* Function Body */
    gen = s_cmp(type__, "ge", (ftnlen)2, (ftnlen)2) == 0;
    her = s_cmp(type__, "he", (ftnlen)2, (ftnlen)2) == 0;
    sym = s_cmp(type__, "sy", (ftnlen)2, (ftnlen)2) == 0;
    tri = s_cmp(type__, "tr", (ftnlen)2, (ftnlen)2) == 0;
    upper = (her || sym || tri) && *(unsigned char *)uplo == 'U';
    lower = (her || sym || tri) && *(unsigned char *)uplo == 'L';
    unit = tri && *(unsigned char *)diag == 'U';

/*     Generate data in array A. */

    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    if (gen || (upper && i__ <= j) || (lower && i__ >= j)) {
		i__3 = i__ + j * a_dim1;
		zbeg_(&z__2, reset);
		z__1.r = z__2.r + transl->r, z__1.i = z__2.i + transl->i;
		a[i__3].r = z__1.r, a[i__3].i = z__1.i;
		if (i__ != j) {
/*                 Set some elements to zero */
		    if (*n > 3 && j == *n / 2) {
			i__3 = i__ + j * a_dim1;
			a[i__3].r = 0., a[i__3].i = 0.;
		    }
		    if (her) {
			i__3 = j + i__ * a_dim1;
			d_cnjg(&z__1, &a[i__ + j * a_dim1]);
			a[i__3].r = z__1.r, a[i__3].i = z__1.i;
		    } else if (sym) {
			i__3 = j + i__ * a_dim1;
			i__4 = i__ + j * a_dim1;
			a[i__3].r = a[i__4].r, a[i__3].i = a[i__4].i;
		    } else if (tri) {
			i__3 = j + i__ * a_dim1;
			a[i__3].r = 0., a[i__3].i = 0.;
		    }
		}
	    }
/* L10: */
	}
	if (her) {
	    i__2 = j + j * a_dim1;
	    i__3 = j + j * a_dim1;
	    d__1 = a[i__3].r;
	    z__1.r = d__1, z__1.i = 0.;
	    a[i__2].r = z__1.r, a[i__2].i = z__1.i;
	}
	if (tri) {
	    i__2 = j + j * a_dim1;
	    i__3 = j + j * a_dim1;
	    z__1.r = a[i__3].r + 1., z__1.i = a[i__3].i + 0.;
	    a[i__2].r = z__1.r, a[i__2].i = z__1.i;
	}
	if (unit) {
	    i__2 = j + j * a_dim1;
	    a[i__2].r = 1., a[i__2].i = 0.;
	}
/* L20: */
    }

/*     Store elements in array AS in data structure required by routine. */

    if (s_cmp(type__, "ge", (ftnlen)2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + (j - 1) * *lda;
		i__4 = i__ + j * a_dim1;
		aa[i__3].r = a[i__4].r, aa[i__3].i = a[i__4].i;
/* L30: */
	    }
	    i__2 = *lda;
	    for (i__ = *m + 1; i__ <= i__2; ++i__) {
		i__3 = i__ + (j - 1) * *lda;
		aa[i__3].r = -1e10, aa[i__3].i = 1e10;
/* L40: */
	    }
/* L50: */
	}
    } else if (s_cmp(type__, "he", (ftnlen)2, (ftnlen)2) == 0 || s_cmp(type__,
	     "sy", (ftnlen)2, (ftnlen)2) == 0 || s_cmp(type__, "tr", (ftnlen)
	    2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    if (upper) {
		ibeg = 1;
		if (unit) {
		    iend = j - 1;
		} else {
		    iend = j;
		}
	    } else {
		if (unit) {
		    ibeg = j + 1;
		} else {
		    ibeg = j;
		}
		iend = *n;
	    }
	    i__2 = ibeg - 1;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + (j - 1) * *lda;
		aa[i__3].r = -1e10, aa[i__3].i = 1e10;
/* L60: */
	    }
	    i__2 = iend;
	    for (i__ = ibeg; i__ <= i__2; ++i__) {
		i__3 = i__ + (j - 1) * *lda;
		i__4 = i__ + j * a_dim1;
		aa[i__3].r = a[i__4].r, aa[i__3].i = a[i__4].i;
/* L70: */
	    }
	    i__2 = *lda;
	    for (i__ = iend + 1; i__ <= i__2; ++i__) {
		i__3 = i__ + (j - 1) * *lda;
		aa[i__3].r = -1e10, aa[i__3].i = 1e10;
/* L80: */
	    }
	    if (her) {
		jj = j + (j - 1) * *lda;
		i__2 = jj;
		i__3 = jj;
		d__1 = aa[i__3].r;
		z__1.r = d__1, z__1.i = -1e10;
		aa[i__2].r = z__1.r, aa[i__2].i = z__1.i;
	    }
/* L90: */
	}
    }
    return 0;

/*     End of ZMAKE. */

} /* zmake_ */

/* Subroutine */ int zmmch_(char* transa, char* transb, integer* m, integer* n, integer* kk, doublecomplex* alpha, doublecomplex* a, integer* lda, doublecomplex* b, integer* ldb, doublecomplex* beta, doublecomplex* c__, integer* ldc, doublecomplex* ct, doublereal* g, doublecomplex* cc, integer* ldcc, doublereal* eps, doublereal* err, logical* fatal, integer* nout, logical* mv, ftnlen transa_len, ftnlen transb_len)
{

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, cc_dim1, 
	    cc_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7;
    doublereal d__1, d__2, d__3, d__4, d__5, d__6;
    doublecomplex z__1, z__2, z__3, z__4;

    double sqrt(double);
    /* Local variables */
    static doublereal erri;
    static integer i__, j, k;
    static logical trana, tranb, ctrana, ctranb;

/*  Checks the results of the computational tests. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Intrinsic Functions .. */
/*     .. Statement Functions .. */
/*     .. Statement Function definitions .. */
/*     .. Executable Statements .. */
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
    --ct;
    --g;
    cc_dim1 = *ldcc;
    cc_offset = 1 + cc_dim1 * 1;
    cc -= cc_offset;

    /* Function Body */
    trana = *(unsigned char *)transa == 'T' || *(unsigned char *)transa == 
	    'C';
    tranb = *(unsigned char *)transb == 'T' || *(unsigned char *)transb == 
	    'C';
    ctrana = *(unsigned char *)transa == 'C';
    ctranb = *(unsigned char *)transb == 'C';

/*     Compute expected result, one column at a time, in CT using data */
/*     in A, B and C. */
/*     Compute gauges in G. */

    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {

	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    i__3 = i__;
	    ct[i__3].r = 0., ct[i__3].i = 0.;
	    g[i__] = 0.;
/* L10: */
	}
	if (! trana && ! tranb) {
	    i__2 = *kk;
	    for (k = 1; k <= i__2; ++k) {
		i__3 = *m;
		for (i__ = 1; i__ <= i__3; ++i__) {
		    i__4 = i__;
		    i__5 = i__;
		    i__6 = i__ + k * a_dim1;
		    i__7 = k + j * b_dim1;
		    z__2.r = a[i__6].r * b[i__7].r - a[i__6].i * b[i__7].i, 
			    z__2.i = a[i__6].r * b[i__7].i + a[i__6].i * b[
			    i__7].r;
		    z__1.r = ct[i__5].r + z__2.r, z__1.i = ct[i__5].i + 
			    z__2.i;
		    ct[i__4].r = z__1.r, ct[i__4].i = z__1.i;
		    i__4 = i__ + k * a_dim1;
		    i__5 = k + j * b_dim1;
		    g[i__] += ((d__1 = a[i__4].r, abs(d__1)) + (d__2 = d_imag(
			    &a[i__ + k * a_dim1]), abs(d__2))) * ((d__3 = b[
			    i__5].r, abs(d__3)) + (d__4 = d_imag(&b[k + j * 
			    b_dim1]), abs(d__4)));
/* L20: */
		}
/* L30: */
	    }
	} else if (trana && ! tranb) {
	    if (ctrana) {
		i__2 = *kk;
		for (k = 1; k <= i__2; ++k) {
		    i__3 = *m;
		    for (i__ = 1; i__ <= i__3; ++i__) {
			i__4 = i__;
			i__5 = i__;
			d_cnjg(&z__3, &a[k + i__ * a_dim1]);
			i__6 = k + j * b_dim1;
			z__2.r = z__3.r * b[i__6].r - z__3.i * b[i__6].i, 
				z__2.i = z__3.r * b[i__6].i + z__3.i * b[i__6]
				.r;
			z__1.r = ct[i__5].r + z__2.r, z__1.i = ct[i__5].i + 
				z__2.i;
			ct[i__4].r = z__1.r, ct[i__4].i = z__1.i;
			i__4 = k + i__ * a_dim1;
			i__5 = k + j * b_dim1;
			g[i__] += ((d__1 = a[i__4].r, abs(d__1)) + (d__2 = 
				d_imag(&a[k + i__ * a_dim1]), abs(d__2))) * ((
				d__3 = b[i__5].r, abs(d__3)) + (d__4 = d_imag(
				&b[k + j * b_dim1]), abs(d__4)));
/* L40: */
		    }
/* L50: */
		}
	    } else {
		i__2 = *kk;
		for (k = 1; k <= i__2; ++k) {
		    i__3 = *m;
		    for (i__ = 1; i__ <= i__3; ++i__) {
			i__4 = i__;
			i__5 = i__;
			i__6 = k + i__ * a_dim1;
			i__7 = k + j * b_dim1;
			z__2.r = a[i__6].r * b[i__7].r - a[i__6].i * b[i__7]
				.i, z__2.i = a[i__6].r * b[i__7].i + a[i__6]
				.i * b[i__7].r;
			z__1.r = ct[i__5].r + z__2.r, z__1.i = ct[i__5].i + 
				z__2.i;
			ct[i__4].r = z__1.r, ct[i__4].i = z__1.i;
			i__4 = k + i__ * a_dim1;
			i__5 = k + j * b_dim1;
			g[i__] += ((d__1 = a[i__4].r, abs(d__1)) + (d__2 = 
				d_imag(&a[k + i__ * a_dim1]), abs(d__2))) * ((
				d__3 = b[i__5].r, abs(d__3)) + (d__4 = d_imag(
				&b[k + j * b_dim1]), abs(d__4)));
/* L60: */
		    }
/* L70: */
		}
	    }
	} else if (! trana && tranb) {
	    if (ctranb) {
		i__2 = *kk;
		for (k = 1; k <= i__2; ++k) {
		    i__3 = *m;
		    for (i__ = 1; i__ <= i__3; ++i__) {
			i__4 = i__;
			i__5 = i__;
			i__6 = i__ + k * a_dim1;
			d_cnjg(&z__3, &b[j + k * b_dim1]);
			z__2.r = a[i__6].r * z__3.r - a[i__6].i * z__3.i, 
				z__2.i = a[i__6].r * z__3.i + a[i__6].i * 
				z__3.r;
			z__1.r = ct[i__5].r + z__2.r, z__1.i = ct[i__5].i + 
				z__2.i;
			ct[i__4].r = z__1.r, ct[i__4].i = z__1.i;
			i__4 = i__ + k * a_dim1;
			i__5 = j + k * b_dim1;
			g[i__] += ((d__1 = a[i__4].r, abs(d__1)) + (d__2 = 
				d_imag(&a[i__ + k * a_dim1]), abs(d__2))) * ((
				d__3 = b[i__5].r, abs(d__3)) + (d__4 = d_imag(
				&b[j + k * b_dim1]), abs(d__4)));
/* L80: */
		    }
/* L90: */
		}
	    } else {
		i__2 = *kk;
		for (k = 1; k <= i__2; ++k) {
		    i__3 = *m;
		    for (i__ = 1; i__ <= i__3; ++i__) {
			i__4 = i__;
			i__5 = i__;
			i__6 = i__ + k * a_dim1;
			i__7 = j + k * b_dim1;
			z__2.r = a[i__6].r * b[i__7].r - a[i__6].i * b[i__7]
				.i, z__2.i = a[i__6].r * b[i__7].i + a[i__6]
				.i * b[i__7].r;
			z__1.r = ct[i__5].r + z__2.r, z__1.i = ct[i__5].i + 
				z__2.i;
			ct[i__4].r = z__1.r, ct[i__4].i = z__1.i;
			i__4 = i__ + k * a_dim1;
			i__5 = j + k * b_dim1;
			g[i__] += ((d__1 = a[i__4].r, abs(d__1)) + (d__2 = 
				d_imag(&a[i__ + k * a_dim1]), abs(d__2))) * ((
				d__3 = b[i__5].r, abs(d__3)) + (d__4 = d_imag(
				&b[j + k * b_dim1]), abs(d__4)));
/* L100: */
		    }
/* L110: */
		}
	    }
	} else if (trana && tranb) {
	    if (ctrana) {
		if (ctranb) {
		    i__2 = *kk;
		    for (k = 1; k <= i__2; ++k) {
			i__3 = *m;
			for (i__ = 1; i__ <= i__3; ++i__) {
			    i__4 = i__;
			    i__5 = i__;
			    d_cnjg(&z__3, &a[k + i__ * a_dim1]);
			    d_cnjg(&z__4, &b[j + k * b_dim1]);
			    z__2.r = z__3.r * z__4.r - z__3.i * z__4.i, 
				    z__2.i = z__3.r * z__4.i + z__3.i * 
				    z__4.r;
			    z__1.r = ct[i__5].r + z__2.r, z__1.i = ct[i__5].i 
				    + z__2.i;
			    ct[i__4].r = z__1.r, ct[i__4].i = z__1.i;
			    i__4 = k + i__ * a_dim1;
			    i__5 = j + k * b_dim1;
			    g[i__] += ((d__1 = a[i__4].r, abs(d__1)) + (d__2 =
				     d_imag(&a[k + i__ * a_dim1]), abs(d__2)))
				     * ((d__3 = b[i__5].r, abs(d__3)) + (d__4 
				    = d_imag(&b[j + k * b_dim1]), abs(d__4)));
/* L120: */
			}
/* L130: */
		    }
		} else {
		    i__2 = *kk;
		    for (k = 1; k <= i__2; ++k) {
			i__3 = *m;
			for (i__ = 1; i__ <= i__3; ++i__) {
			    i__4 = i__;
			    i__5 = i__;
			    d_cnjg(&z__3, &a[k + i__ * a_dim1]);
			    i__6 = j + k * b_dim1;
			    z__2.r = z__3.r * b[i__6].r - z__3.i * b[i__6].i, 
				    z__2.i = z__3.r * b[i__6].i + z__3.i * b[
				    i__6].r;
			    z__1.r = ct[i__5].r + z__2.r, z__1.i = ct[i__5].i 
				    + z__2.i;
			    ct[i__4].r = z__1.r, ct[i__4].i = z__1.i;
			    i__4 = k + i__ * a_dim1;
			    i__5 = j + k * b_dim1;
			    g[i__] += ((d__1 = a[i__4].r, abs(d__1)) + (d__2 =
				     d_imag(&a[k + i__ * a_dim1]), abs(d__2)))
				     * ((d__3 = b[i__5].r, abs(d__3)) + (d__4 
				    = d_imag(&b[j + k * b_dim1]), abs(d__4)));
/* L140: */
			}
/* L150: */
		    }
		}
	    } else {
		if (ctranb) {
		    i__2 = *kk;
		    for (k = 1; k <= i__2; ++k) {
			i__3 = *m;
			for (i__ = 1; i__ <= i__3; ++i__) {
			    i__4 = i__;
			    i__5 = i__;
			    i__6 = k + i__ * a_dim1;
			    d_cnjg(&z__3, &b[j + k * b_dim1]);
			    z__2.r = a[i__6].r * z__3.r - a[i__6].i * z__3.i, 
				    z__2.i = a[i__6].r * z__3.i + a[i__6].i * 
				    z__3.r;
			    z__1.r = ct[i__5].r + z__2.r, z__1.i = ct[i__5].i 
				    + z__2.i;
			    ct[i__4].r = z__1.r, ct[i__4].i = z__1.i;
			    i__4 = k + i__ * a_dim1;
			    i__5 = j + k * b_dim1;
			    g[i__] += ((d__1 = a[i__4].r, abs(d__1)) + (d__2 =
				     d_imag(&a[k + i__ * a_dim1]), abs(d__2)))
				     * ((d__3 = b[i__5].r, abs(d__3)) + (d__4 
				    = d_imag(&b[j + k * b_dim1]), abs(d__4)));
/* L160: */
			}
/* L170: */
		    }
		} else {
		    i__2 = *kk;
		    for (k = 1; k <= i__2; ++k) {
			i__3 = *m;
			for (i__ = 1; i__ <= i__3; ++i__) {
			    i__4 = i__;
			    i__5 = i__;
			    i__6 = k + i__ * a_dim1;
			    i__7 = j + k * b_dim1;
			    z__2.r = a[i__6].r * b[i__7].r - a[i__6].i * b[
				    i__7].i, z__2.i = a[i__6].r * b[i__7].i + 
				    a[i__6].i * b[i__7].r;
			    z__1.r = ct[i__5].r + z__2.r, z__1.i = ct[i__5].i 
				    + z__2.i;
			    ct[i__4].r = z__1.r, ct[i__4].i = z__1.i;
			    i__4 = k + i__ * a_dim1;
			    i__5 = j + k * b_dim1;
			    g[i__] += ((d__1 = a[i__4].r, abs(d__1)) + (d__2 =
				     d_imag(&a[k + i__ * a_dim1]), abs(d__2)))
				     * ((d__3 = b[i__5].r, abs(d__3)) + (d__4 
				    = d_imag(&b[j + k * b_dim1]), abs(d__4)));
/* L180: */
			}
/* L190: */
		    }
		}
	    }
	}
	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    i__3 = i__;
	    i__4 = i__;
	    z__2.r = alpha->r * ct[i__4].r - alpha->i * ct[i__4].i, z__2.i = 
		    alpha->r * ct[i__4].i + alpha->i * ct[i__4].r;
	    i__5 = i__ + j * c_dim1;
	    z__3.r = beta->r * c__[i__5].r - beta->i * c__[i__5].i, z__3.i = 
		    beta->r * c__[i__5].i + beta->i * c__[i__5].r;
	    z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
	    ct[i__3].r = z__1.r, ct[i__3].i = z__1.i;
	    i__3 = i__ + j * c_dim1;
	    g[i__] = ((d__1 = alpha->r, abs(d__1)) + (d__2 = d_imag(alpha), 
		    abs(d__2))) * g[i__] + ((d__3 = beta->r, abs(d__3)) + (
		    d__4 = d_imag(beta), abs(d__4))) * ((d__5 = c__[i__3].r, 
		    abs(d__5)) + (d__6 = d_imag(&c__[i__ + j * c_dim1]), abs(
		    d__6)));
/* L200: */
	}

/*        Compute the error ratio for this result. */

	*err = 0.;
	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    i__3 = i__;
	    i__4 = i__ + j * cc_dim1;
	    z__2.r = ct[i__3].r - cc[i__4].r, z__2.i = ct[i__3].i - cc[i__4]
		    .i;
	    z__1.r = z__2.r, z__1.i = z__2.i;
	    erri = ((d__1 = z__1.r, abs(d__1)) + (d__2 = d_imag(&z__1), abs(
		    d__2))) / *eps;
	    if (g[i__] != 0.) {
		erri /= g[i__];
	    }
	    *err = f2cmax(*err,erri);
	    if (*err * sqrt(*eps) >= 1.) {
		goto L230;
	    }
/* L210: */
	}

/* L220: */
    }

/*     If the loop completes, all results are at least half accurate. */
    goto L250;

/*     Report fatal error. */

L230:
    *fatal = TRUE_;
    printf(" ******* FATAL ERROR - COMPUTED RESULT IS LESS THAN HALF ACCURATE *******\n");
    printf("         EXPECTED RESULT                    COMPUTED RESULT\n");
    i__1 = *m;
    for (i__ = 1; i__ <= i__1; ++i__) {
	if (*mv) {
            printf("%7d (%15.6g,%15.6g) (%15.6g,%15.6g)\n",i__,ct[i__].r,ct[i__].i,cc[i__+j*cc_dim1].r,cc[i__+j*cc_dim1].i);
        } else {
            printf("%7d (%15.6g,%15.6g) (%15.6g,%15.6g)\n",i__,cc[i__+j*cc_dim1].r,cc[i__+j*cc_dim1].i,ct[i__].r,ct[i__].i);
	}
/* L240: */
    }
    if (*n > 1) {
        printf("      THESE ARE THE RESULTS FOR COLUMN %d\n",j);
    }

L250:
    return 0;


/*     End of ZMMCH. */

} /* zmmch_ */

logical lze_(doublecomplex* ri, doublecomplex* rj, integer* lr)
{
    /* System generated locals */
    integer i__1, i__2, i__3;
    logical ret_val;

    /* Local variables */
    static integer i__;


/*  Tests if two arrays are identical. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Executable Statements .. */
    /* Parameter adjustments */
    --rj;
    --ri;

    /* Function Body */
    i__1 = *lr;
    for (i__ = 1; i__ <= i__1; ++i__) {
	i__2 = i__;
	i__3 = i__;
	if (ri[i__2].r != rj[i__3].r || ri[i__2].i != rj[i__3].i) {
	    goto L20;
	}
/* L10: */
    }
    ret_val = TRUE_;
    goto L30;
L20:
    ret_val = FALSE_;
L30:
    return ret_val;

/*     End of LZE. */

} /* lze_ */

logical lzeres_(char* type__, char* uplo, integer* m, integer* n, doublecomplex *aa, doublecomplex* as, integer* lda, ftnlen type_len, ftnlen uplo_len)
{
    /* System generated locals */
    integer aa_dim1, aa_offset, as_dim1, as_offset, i__1, i__2, i__3, i__4;
    logical ret_val;

    /* Local variables */
    static integer ibeg, iend, i__, j;
    static logical upper;


/*  Tests if selected elements in two arrays are equal. */

/*  TYPE is 'ge' or 'he' or 'sy'. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Executable Statements .. */
    /* Parameter adjustments */
    as_dim1 = *lda;
    as_offset = 1 + as_dim1 * 1;
    as -= as_offset;
    aa_dim1 = *lda;
    aa_offset = 1 + aa_dim1 * 1;
    aa -= aa_offset;

    /* Function Body */
    upper = *(unsigned char *)uplo == 'U';
    if (s_cmp(type__, "ge", (ftnlen)2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *lda;
	    for (i__ = *m + 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * aa_dim1;
		i__4 = i__ + j * as_dim1;
		if (aa[i__3].r != as[i__4].r || aa[i__3].i != as[i__4].i) {
		    goto L70;
		}
/* L10: */
	    }
/* L20: */
	}
    } else if (s_cmp(type__, "he", (ftnlen)2, (ftnlen)2) == 0 || s_cmp(type__,
	     "sy", (ftnlen)2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    if (upper) {
		ibeg = 1;
		iend = j;
	    } else {
		ibeg = j;
		iend = *n;
	    }
	    i__2 = ibeg - 1;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * aa_dim1;
		i__4 = i__ + j * as_dim1;
		if (aa[i__3].r != as[i__4].r || aa[i__3].i != as[i__4].i) {
		    goto L70;
		}
/* L30: */
	    }
	    i__2 = *lda;
	    for (i__ = iend + 1; i__ <= i__2; ++i__) {
		i__3 = i__ + j * aa_dim1;
		i__4 = i__ + j * as_dim1;
		if (aa[i__3].r != as[i__4].r || aa[i__3].i != as[i__4].i) {
		    goto L70;
		}
/* L40: */
	    }
/* L50: */
	}
    }

/*   60 CONTINUE */
    ret_val = TRUE_;
    goto L80;
L70:
    ret_val = FALSE_;
L80:
    return ret_val;

/*     End of LZERES. */

} /* lzeres_ */

/* Double Complex */ VOID zbeg_(doublecomplex* ret_val, logical* reset)
{
    /* System generated locals */
    doublereal d__1, d__2;
    doublecomplex z__1;

    /* Local variables */
    static integer i__, j, ic, mi, mj;


/*  Generates complex numbers as pairs of random numbers uniformly */
/*  distributed between -0.5 and 0.5. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Scalar Arguments .. */
/*     .. Local Scalars .. */
/*     .. Save statement .. */
/*     .. Intrinsic Functions .. */
/*     .. Executable Statements .. */
    if (*reset) {
/*        Initialize local variables. */
	mi = 891;
	mj = 457;
	i__ = 7;
	j = 7;
	ic = 0;
	*reset = FALSE_;
    }

/*     The sequence of values of I or J is bounded between 1 and 999. */
/*     If initial I or J = 1,2,3,6,7 or 9, the period will be 50. */
/*     If initial I or J = 4 or 8, the period will be 25. */
/*     If initial I or J = 5, the period will be 10. */
/*     IC is used to break up the period by skipping 1 value of I or J */
/*     in 6. */

    ++ic;
L10:
    i__ *= mi;
    j *= mj;
    i__ -= i__ / 1000 * 1000;
    j -= j / 1000 * 1000;
    if (ic >= 5) {
	ic = 0;
	goto L10;
    }
    d__1 = (i__ - 500) / 1001.;
    d__2 = (j - 500) / 1001.;
    z__1.r = d__1, z__1.i = d__2;
     ret_val->r = z__1.r,  ret_val->i = z__1.i;
    return ;

/*     End of ZBEG. */

} /* zbeg_ */

doublereal ddiff_(doublereal* x, doublereal* y)
{
    /* System generated locals */
    doublereal ret_val;


/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Scalar Arguments .. */
/*     .. Executable Statements .. */
    ret_val = *x - *y;
    return ret_val;

/*     End of DDIFF. */

} /* ddiff_ */

/* Main program alias */ /*int zblat3_ () { MAIN__ (); }*/
