!> \brief \b LA_CONSTANTS is a module for the scaling constants for the compiled Fortran single and double precisions
!
!  =========== DOCUMENTATION ===========
!
! Online html documentation available at
!            http://www.netlib.org/lapack/explore-html/
!
!  Authors:
!  ========
!
!> \author Edward Anderson, Lockheed Martin
!
!> \date May 2016
!
!> \ingroup OTHERauxiliary
!
!> \par Contributors:
!  ==================
!>
!> Weslley Pereira, University of Colorado Denver, USA
!> Nick Papior, Technical University of Denmark, DK
!
!> \par Further Details:
!  =====================
!>
!> \verbatim
!>
!>  Anderson E. (2017)
!>  Algorithm 978: Safe Scaling in the Level 1 BLAS
!>  ACM Trans Math Softw 44:1--28
!>  https://doi.org/10.1145/3061665
!>
!>  Blue, James L. (1978)
!>  A Portable Fortran Program to Find the Euclidean Norm of a Vector
!>  ACM Trans Math Softw 4:15--23
!>  https://doi.org/10.1145/355769.355771
!>
!> \endverbatim
!
module LA_CONSTANTS
!  -- LAPACK auxiliary module --
!  -- LAPACK is a software package provided by Univ. of Tennessee,    --
!  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--

!  Standard constants for 
   integer, parameter :: sp = kind(1.e0)

   real(sp), parameter :: szero = 0.0_sp
   real(sp), parameter :: shalf = 0.5_sp
   real(sp), parameter :: sone = 1.0_sp
   real(sp), parameter :: stwo = 2.0_sp
   real(sp), parameter :: sthree = 3.0_sp
   real(sp), parameter :: sfour = 4.0_sp
   real(sp), parameter :: seight = 8.0_sp
   real(sp), parameter :: sten = 10.0_sp
   complex(sp), parameter :: czero = ( 0.0_sp, 0.0_sp )
   complex(sp), parameter :: chalf = ( 0.5_sp, 0.0_sp )
   complex(sp), parameter :: cone = ( 1.0_sp, 0.0_sp )
   character*1, parameter :: sprefix = 'S'
   character*1, parameter :: cprefix = 'C'

!  Scaling constants
   real(sp), parameter :: sulp = epsilon(0._sp)
   real(sp), parameter :: seps = sulp * 0.5_sp
   real(sp), parameter :: ssafmin = real(radix(0._sp),sp)**max( &
      minexponent(0._sp)-1, &
      1-maxexponent(0._sp) &
   )
   real(sp), parameter :: ssafmax = sone / ssafmin
   real(sp), parameter :: ssmlnum = ssafmin / sulp
   real(sp), parameter :: sbignum = ssafmax * sulp
   real(sp), parameter :: srtmin = sqrt(ssmlnum)
   real(sp), parameter :: srtmax = sqrt(sbignum)

!  Blue's scaling constants
   real(sp), parameter :: stsml = real(radix(0._sp), sp)**ceiling( &
       (minexponent(0._sp) - 1) * 0.5_sp)
   real(sp), parameter :: stbig = real(radix(0._sp), sp)**floor( &
       (maxexponent(0._sp) - digits(0._sp) + 1) * 0.5_sp)
!  ssml >= 1/s, where s was defined in https://doi.org/10.1145/355769.355771
!  The correction was added in https://doi.org/10.1145/3061665 to scale denormalized numbers correctly 
   real(sp), parameter :: sssml = real(radix(0._sp), sp)**( - floor( &
       (minexponent(0._sp) - digits(0._sp)) * 0.5_sp))
!  sbig = 1/S, where S was defined in https://doi.org/10.1145/355769.355771
   real(sp), parameter :: ssbig = real(radix(0._sp), sp)**( - ceiling( &
       (maxexponent(0._sp) + digits(0._sp) - 1) * 0.5_sp))

!  Standard constants for 
   integer, parameter :: dp = kind(1.d0)

   real(dp), parameter :: dzero = 0.0_dp
   real(dp), parameter :: dhalf = 0.5_dp
   real(dp), parameter :: done = 1.0_dp
   real(dp), parameter :: dtwo = 2.0_dp
   real(dp), parameter :: dthree = 3.0_dp
   real(dp), parameter :: dfour = 4.0_dp
   real(dp), parameter :: deight = 8.0_dp
   real(dp), parameter :: dten = 10.0_dp
   complex(dp), parameter :: zzero = ( 0.0_dp, 0.0_dp )
   complex(dp), parameter :: zhalf = ( 0.5_dp, 0.0_dp )
   complex(dp), parameter :: zone = ( 1.0_dp, 0.0_dp )
   character*1, parameter :: dprefix = 'D'
   character*1, parameter :: zprefix = 'Z'

!  Scaling constants
   real(dp), parameter :: dulp = epsilon(0._dp)
   real(dp), parameter :: deps = dulp * 0.5_dp
   real(dp), parameter :: dsafmin = real(radix(0._dp),dp)**max( &
      minexponent(0._dp)-1, &
      1-maxexponent(0._dp) &
   )
   real(dp), parameter :: dsafmax = done / dsafmin
   real(dp), parameter :: dsmlnum = dsafmin / dulp
   real(dp), parameter :: dbignum = dsafmax * dulp
   real(dp), parameter :: drtmin = sqrt(dsmlnum)
   real(dp), parameter :: drtmax = sqrt(dbignum)

!  Blue's scaling constants
   real(dp), parameter :: dtsml = real(radix(0._dp), dp)**ceiling( &
       (minexponent(0._dp) - 1) * 0.5_dp)
   real(dp), parameter :: dtbig = real(radix(0._dp), dp)**floor( &
       (maxexponent(0._dp) - digits(0._dp) + 1) * 0.5_dp)
!  ssml >= 1/s, where s was defined in https://doi.org/10.1145/355769.355771
!  The correction was added in https://doi.org/10.1145/3061665 to scale denormalized numbers correctly 
   real(dp), parameter :: dssml = real(radix(0._dp), dp)**( - floor( &
       (minexponent(0._dp) - digits(0._dp)) * 0.5_dp))
!  sbig = 1/S, where S was defined in https://doi.org/10.1145/355769.355771
   real(dp), parameter :: dsbig = real(radix(0._dp), dp)**( - ceiling( &
       (maxexponent(0._dp) + digits(0._dp) - 1) * 0.5_dp))

end module LA_CONSTANTS
