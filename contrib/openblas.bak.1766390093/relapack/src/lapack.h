#ifndef LAPACK_H
#define LAPACK_H

extern blasint LAPACK(lsame)(const char *, const char *);
extern blasint LAPACK(xerbla)(const char *, const blasint *, int);

extern const blasint LAPACK(ilaenv)(const blasint *, const char*, const char*, const blasint* ,  int ,  int,  int );

extern void LAPACK(sgetrf2)(const blasint *, const blasint *, float *, const blasint *, blasint *, blasint *);
extern void LAPACK(dgetrf2)(const blasint *, const blasint *, double *, const blasint *, blasint *, blasint *);
extern void LAPACK(cgetrf2)(const blasint *, const blasint *, float *, const blasint *, blasint *, blasint *);
extern void LAPACK(zgetrf2)(const blasint *, const blasint *, double *, const blasint *, blasint *, blasint *);

extern void LAPACK(slaswp)(const blasint *, float *, const blasint *, const blasint *, const blasint *, const blasint *, const blasint *);
extern void LAPACK(dlaswp)(const blasint *, double *, const blasint *, const blasint *, const blasint *, const blasint *, const blasint *);
extern void LAPACK(claswp)(const blasint *, float *, const blasint *, const blasint *, const blasint *, const blasint *, const blasint *);
extern void LAPACK(zlaswp)(const blasint *, double *, const blasint *, const blasint *, const blasint *, const blasint *, const blasint *);

extern void LAPACK(slaset)(const char *, const blasint *, const blasint *, const float *, const float *, float *, const blasint *);
extern void LAPACK(dlaset)(const char *, const blasint *, const blasint *, const double *, const double *, double *, const blasint *);
extern void LAPACK(claset)(const char *, const blasint *, const blasint *, const float *, const float *, float *, const blasint *);
extern void LAPACK(zlaset)(const char *, const blasint *, const blasint *, const double *, const double *, double *, const blasint *);

extern void LAPACK(slacpy)(const char *, const blasint *, const blasint *, const float *, const blasint *, float *, const blasint *);
extern void LAPACK(dlacpy)(const char *, const blasint *, const blasint *, const double *, const blasint *, double *, const blasint *);
extern void LAPACK(clacpy)(const char *, const blasint *, const blasint *, const float *, const blasint *, float *, const blasint *);
extern void LAPACK(zlacpy)(const char *, const blasint *, const blasint *, const double *, const blasint *, double *, const blasint *);

extern void LAPACK(slascl)(const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, const blasint *, float *, const blasint *, blasint *);
extern void LAPACK(dlascl)(const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, const blasint *, double *, const blasint *, blasint *);
extern void LAPACK(clascl)(const char *, const blasint *, const blasint *, const float *, const float *, const blasint *, const blasint *, float *, const blasint *, blasint *);
extern void LAPACK(zlascl)(const char *, const blasint *, const blasint *, const double *, const double *, const blasint *, const blasint *, double *, const blasint *, blasint *);

extern void LAPACK(slauu2)(const char *, const blasint *, float *, const blasint *, blasint *);
extern void LAPACK(dlauu2)(const char *, const blasint *, double *, const blasint *, blasint *);
extern void LAPACK(clauu2)(const char *, const blasint *, float *, const blasint *, blasint *);
extern void LAPACK(zlauu2)(const char *, const blasint *, double *, const blasint *, blasint *);

extern void LAPACK(ssygs2)(const blasint *, const char *, const blasint *, float *, const blasint *, const float *, const blasint *, blasint *);
extern void LAPACK(dsygs2)(const blasint *, const char *, const blasint *, double *, const blasint *, const double *, const blasint *, blasint *);
extern void LAPACK(chegs2)(const blasint *, const char *, const blasint *, float *, const blasint *, const float *, const blasint *, blasint *);
extern void LAPACK(zhegs2)(const blasint *, const char *, const blasint *, double *, const blasint *, const double *, const blasint *, blasint *);

extern void LAPACK(strti2)(const char *, const char *, const blasint *, float *, const blasint *, blasint *);
extern void LAPACK(dtrti2)(const char *, const char *, const blasint *, double *, const blasint *, blasint *);
extern void LAPACK(ctrti2)(const char *, const char *, const blasint *, float *, const blasint *, blasint *);
extern void LAPACK(ztrti2)(const char *, const char *, const blasint *, double *, const blasint *, blasint *);

extern void LAPACK(spotf2)(const char *, const blasint *, float *, const blasint *, blasint *);
extern void LAPACK(dpotf2)(const char *, const blasint *, double *, const blasint *, blasint *);
extern void LAPACK(cpotf2)(const char *, const blasint *, float *, const blasint *, blasint *);
extern void LAPACK(zpotf2)(const char *, const blasint *, double *, const blasint *, blasint *);

extern void LAPACK(spbtf2)(const char *, const blasint *, const blasint *, float *, const blasint *, blasint *);
extern void LAPACK(dpbtf2)(const char *, const blasint *, const blasint *, double *, const blasint *, blasint *);
extern void LAPACK(cpbtf2)(const char *, const blasint *, const blasint *, float *, const blasint *, blasint *);
extern void LAPACK(zpbtf2)(const char *, const blasint *, const blasint *, double *, const blasint *, blasint *);

extern void LAPACK(ssytf2)(const char *, const blasint *, float *, const blasint *, blasint *, blasint *);
extern void LAPACK(dsytf2)(const char *, const blasint *, double *, const blasint *, blasint *, blasint *);
extern void LAPACK(csytf2)(const char *, const blasint *, float *, const blasint *, blasint *, blasint *);
extern void LAPACK(chetf2)(const char *, const blasint *, float *, const blasint *, blasint *, blasint *);
extern void LAPACK(zsytf2)(const char *, const blasint *, double *, const blasint *, blasint *, blasint *);
extern void LAPACK(zhetf2)(const char *, const blasint *, double *, const blasint *, blasint *, blasint *);
extern void LAPACK(ssytf2_rook)(const char *, const blasint *, float *, const blasint *, blasint *, blasint *);
extern void LAPACK(dsytf2_rook)(const char *, const blasint *, double *, const blasint *, blasint *, blasint *);
extern void LAPACK(csytf2_rook)(const char *, const blasint *, float *, const blasint *, blasint *, blasint *);
extern void LAPACK(chetf2_rook)(const char *, const blasint *, float *, const blasint *, blasint *, blasint *);
extern void LAPACK(zsytf2_rook)(const char *, const blasint *, double *, const blasint *, blasint *, blasint *);
extern void LAPACK(zhetf2_rook)(const char *, const blasint *, double *, const blasint *, blasint *, blasint *);

extern void LAPACK(sgetf2)(const blasint *, const blasint *, float *, const blasint *, blasint *, blasint *);
extern void LAPACK(dgetf2)(const blasint *, const blasint *, double *, const blasint *, blasint *, blasint *);
extern void LAPACK(cgetf2)(const blasint *, const blasint *, float *, const blasint *, blasint *, blasint *);
extern void LAPACK(zgetf2)(const blasint *, const blasint *, double *, const blasint *, blasint *, blasint *);

extern void LAPACK(sgbtf2)(const blasint *, const blasint *, const blasint *, const blasint *, float *, const blasint *, blasint *, blasint *);
extern void LAPACK(dgbtf2)(const blasint *, const blasint *, const blasint *, const blasint *, double *, const blasint *, blasint *, blasint *);
extern void LAPACK(cgbtf2)(const blasint *, const blasint *, const blasint *, const blasint *, float *, const blasint *, blasint *, blasint *);
extern void LAPACK(zgbtf2)(const blasint *, const blasint *, const blasint *, const blasint *, double *, const blasint *, blasint *, blasint *);

extern void LAPACK(stgsy2)(const char *, const blasint *, const blasint *, const blasint *, const float *, const blasint *, const float *, const blasint *, float *, const blasint *, const float *, const blasint *, const float *, const blasint *, float *, const blasint *, float *, float *, float *, blasint *, blasint *, blasint *);
extern void LAPACK(dtgsy2)(const char *, const blasint *, const blasint *, const blasint *, const double *, const blasint *, const double *, const blasint *, double *, const blasint *, const double *, const blasint *, const double *, const blasint *, double *, const blasint *, double *, double *, double *, blasint *, blasint *, blasint *);
extern void LAPACK(ctgsy2)(const char *, const blasint *, const blasint *, const blasint *, const float *, const blasint *, const float *, const blasint *, float *, const blasint *, const float *, const blasint *, const float *, const blasint *, float *, const blasint *, float *, float *, float *, blasint *);
extern void LAPACK(ztgsy2)(const char *, const blasint *, const blasint *, const blasint *, const double *, const blasint *, const double *, const blasint *, double *, const blasint *, const double *, const blasint *, const double *, const blasint *, double *, const blasint *, double *, double *, double *, blasint *);

#endif /* LAPACK_H */
