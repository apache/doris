module LA_XISNAN
   interface LA_ISNAN

   module procedure SISNAN
   module procedure DISNAN

   end interface

contains
   
   logical function SISNAN( x )
   use LA_CONSTANTS, only: wp=>sp
#ifdef USE_IEEE_INTRINSIC
   use, intrinsic :: ieee_arithmetic
#elif USE_ISNAN
   intrinsic :: isnan
#endif
   real(wp) :: x
#ifdef USE_IEEE_INTRINSIC
   sisnan = ieee_is_nan(x)
#elif USE_ISNAN
   sisnan = isnan(x)
#else
   sisnan = SLAISNAN(x,x)

   contains
   logical function SLAISNAN( x, y )
   use LA_CONSTANTS, only: wp=>sp
   real(wp) :: x, y
   SLAISNAN = ( x.ne.y )
   end function SLAISNAN
#endif
   end function SISNAN

   logical function DISNAN( x )
   use LA_CONSTANTS, only: wp=>dp
#ifdef USE_IEEE_INTRINSIC
   use, intrinsic :: ieee_arithmetic
#elif USE_ISNAN
   intrinsic :: isnan
#endif
   real(wp) :: x
#ifdef USE_IEEE_INTRINSIC
   DISNAN = ieee_is_nan(x)
#elif USE_ISNAN
   DISNAN = isnan(x)
#else
   DISNAN = DLAISNAN(x,x)

   contains
   logical function DLAISNAN( x, y )
   use LA_CONSTANTS, only: wp=>dp
   real(wp) :: x, y
   DLAISNAN = ( x.ne.y )
   end function DLAISNAN
#endif
   end function DISNAN

end module LA_XISNAN
