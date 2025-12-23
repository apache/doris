/***************************************************************************
Copyright (c) 2019, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/
 

#include "common.h"
#include <math.h>
#include <altivec.h>
#if defined(DOUBLE)
    #define ABS fabs
#else
    #define ABS fabsf
#endif
#define CABS1(x,i)    ABS(x[i])+ABS(x[i+1])

#define USE_MASK_PERMUTATIONS 1 //with this type of permutation gcc output a little faster code

#if  !defined(USE_MASK_PERMUTATIONS)

static inline __attribute__((always_inline))  __vector float mvec_mergee(__vector float a,__vector float b ){
  __vector float result;
  __asm__ ( 
      "vmrgew %0,%1,%2;\n" 
      : "=v" (result) 
      : "v" (a), 
      "v" (b) 
      : );
  return result;
}

static inline __attribute__((always_inline)) __vector float mvec_mergeo(__vector float a,__vector float b ){
  __vector float result;
  __asm__ ( 
      "vmrgow %0,%1,%2;\n" 
      : "=v" (result) 
      : "v" (a), 
      "v" (b) 
      : );
  return result;
}

#endif

/**
 * Find  maximum index 
 * Warning: requirements n>0  and n % 32 == 0
 * @param n     
 * @param x     pointer to the vector
 * @param maxf  (out) maximum absolute value .( only for output )
 * @return  index 
 */
static BLASLONG   ciamax_kernel_32(BLASLONG n, FLOAT *x, FLOAT *maxf) { 

    BLASLONG index;
    BLASLONG i=0;
#if  defined(USE_MASK_PERMUTATIONS)    
    register __vector unsigned int static_index0 = {0,1,2,3};
#else
    register __vector unsigned int static_index0 = {2,0,3,1};
#endif    
    register __vector unsigned int temp0 = {4,4,4, 4}; //temporary vector register
    register __vector unsigned int temp1=  temp0<<1;  //{8,8,8,8}
    register __vector unsigned int static_index1=static_index0 +temp0; 
    register __vector unsigned int static_index2=static_index0 +temp1; 
    register __vector unsigned int static_index3=static_index1 +temp1;  
    temp0=vec_xor(temp0,temp0);
    temp1=temp1 <<1 ; //{16,16,16,16}
    register __vector unsigned int temp_add=temp1 <<1; //{32,32,32,32}
    register __vector unsigned int quadruple_indices=temp0;//{0,0,0,0}
    register __vector float quadruple_values={0,0,0,0};

    register __vector float * v_ptrx=(__vector float *)x;
#if  defined(USE_MASK_PERMUTATIONS)    
    register __vector unsigned char real_pack_mask = { 0,1,2,3,8,9,10,11,16,17,18,19, 24,25,26,27}; 
    register __vector unsigned char image_pack_mask=  {4, 5, 6, 7, 12, 13, 14, 15, 20, 21, 22, 23, 28, 29, 30, 31}; 
#endif    
    for(; i<n; i+=32 ){
       //absolute temporary complex vectors
       register __vector float v0=vec_abs(v_ptrx[0]);
       register __vector float v1=vec_abs(v_ptrx[1]);
       register __vector float v2=vec_abs(v_ptrx[2]);
       register __vector float v3=vec_abs(v_ptrx[3]);
       register __vector float v4=vec_abs(v_ptrx[4]);
       register __vector float v5=vec_abs(v_ptrx[5]);
       register __vector float v6=vec_abs(v_ptrx[6]);       
       register __vector float v7=vec_abs(v_ptrx[7]);

       //pack complex real and imaginary parts together to sum real+image
#if defined(USE_MASK_PERMUTATIONS)       
       register __vector float t1=vec_perm(v0,v1,real_pack_mask);
       register __vector float ti=vec_perm(v0,v1,image_pack_mask); 
            
       v0=t1+ti; //sum quadruple real with quadruple image
       register __vector float t2=vec_perm(v2,v3,real_pack_mask);
       register __vector float ti2=vec_perm(v2,v3,image_pack_mask); 
       v1=t2+ti2;
       t1=vec_perm(v4,v5,real_pack_mask);
       ti=vec_perm(v4,v5,image_pack_mask);      
       v2=t1+ti; //sum
       t2=vec_perm(v6,v7,real_pack_mask);
       ti2=vec_perm(v6,v7,image_pack_mask); 
       v3=t2+ti2;
#else
       register __vector float t1=mvec_mergee(v0,v1);
       register __vector float ti=mvec_mergeo(v0,v1); 
            
       v0=t1+ti; //sum quadruple real with quadruple image
       register __vector float t2= mvec_mergee(v2,v3);
       register __vector float ti2=mvec_mergeo(v2,v3); 
       v1=t2+ti2;
       t1=mvec_mergee(v4,v5);
       ti=mvec_mergeo(v4,v5);      
       v2=t1+ti; //sum
       t2=mvec_mergee(v6,v7);
       ti2=mvec_mergeo(v6,v7); 
       v3=t2+ti2;

#endif
       // now we have 16 summed elements . lets compare them
       v_ptrx+=8;
       register __vector bool int r1=vec_cmpgt(v1,v0);
       register __vector bool int r2=vec_cmpgt(v3,v2);
       register __vector unsigned int ind2= vec_sel(static_index0,static_index1,r1);
       v0=vec_sel(v0,v1,r1); 
       register __vector unsigned int ind3= vec_sel(static_index2,static_index3,r2);
       v1=vec_sel(v2,v3,r2);
       //final cmp and select index and value for first 16 values
       r1=vec_cmpgt(v1,v0);
       register __vector unsigned int indf0 = vec_sel(ind2,ind3,r1);
       register __vector float vf0= vec_sel(v0,v1,r1); 

       //absolute temporary complex vectors
       v0=vec_abs(v_ptrx[0]);
       v1=vec_abs(v_ptrx[1]);
       v2=vec_abs(v_ptrx[2]);
       v3=vec_abs(v_ptrx[3]);
       v4=vec_abs(v_ptrx[4]);
       v5=vec_abs(v_ptrx[5]);
       v6=vec_abs(v_ptrx[6]);       
       v7=vec_abs(v_ptrx[7]);

       //pack complex real and imaginary parts together to sum real+image
#if defined(USE_MASK_PERMUTATIONS)       
       t1=vec_perm(v0,v1,real_pack_mask);
       ti=vec_perm(v0,v1,image_pack_mask); 
            
       v0=t1+ti; //sum quadruple real with quadruple image
       t2=vec_perm(v2,v3,real_pack_mask);
       ti2=vec_perm(v2,v3,image_pack_mask); 
       v1=t2+ti2;
       t1=vec_perm(v4,v5,real_pack_mask);
       ti=vec_perm(v4,v5,image_pack_mask);      
       v2=t1+ti; //sum
       t2=vec_perm(v6,v7,real_pack_mask);
       ti2=vec_perm(v6,v7,image_pack_mask); 
       v3=t2+ti2;
#else
       t1=mvec_mergee(v0,v1);
       ti=mvec_mergeo(v0,v1); 
            
       v0=t1+ti; //sum quadruple real with quadruple image
       t2=mvec_mergee(v2,v3);
       ti2=mvec_mergeo(v2,v3); 
       v1=t2+ti2;
       t1=mvec_mergee(v4,v5);
       ti=mvec_mergeo(v4,v5);      
       v2=t1+ti; //sum
       t2=mvec_mergee(v6,v7);
       ti2=mvec_mergeo(v6,v7); 
       v3=t2+ti2;

#endif
       // now we have 16 summed elements {from 16 to 31} . lets compare them
       v_ptrx+=8;
       r1=vec_cmpgt(v1,v0);
       r2=vec_cmpgt(v3,v2);
       ind2= vec_sel(static_index0,static_index1,r1);
       v0=vec_sel(v0,v1,r1); 
       ind3= vec_sel(static_index2,static_index3,r2);
       v1=vec_sel(v2,v3,r2);
       //final cmp and select index and value for the second 16 values
       r1=vec_cmpgt(v1,v0);
       register __vector unsigned int indv0 = vec_sel(ind2,ind3,r1);
       register __vector float vv0= vec_sel(v0,v1,r1); 
       indv0+=temp1; //make index from 16->31

       //find final quadruple from 32 elements
       r2=vec_cmpgt(vv0,vf0);
       ind2 = vec_sel( indf0,indv0,r2);
       vv0= vec_sel(vf0,vv0,r2);       
       //get asbolute index
       ind2+=temp0;
       //compare with old quadruple and update 
       r1=vec_cmpgt(vv0,quadruple_values);
       quadruple_indices = vec_sel( quadruple_indices,ind2,r1);
       quadruple_values= vec_sel(quadruple_values,vv0,r1);      

       temp0+=temp_add;     
    }

    //now we have to chose from 4 values and 4 different indices
    // we will compare pairwise if pairs are exactly the same we will choose minimum between index
    // otherwise we will assign index of the maximum value
    float a1,a2,a3,a4;
    unsigned int i1,i2,i3,i4;
    a1=vec_extract(quadruple_values,0);
    a2=vec_extract(quadruple_values,1);
    a3=vec_extract(quadruple_values,2);
    a4=vec_extract(quadruple_values,3);
    i1=vec_extract(quadruple_indices,0);
    i2=vec_extract(quadruple_indices,1);
    i3=vec_extract(quadruple_indices,2);
    i4=vec_extract(quadruple_indices,3);
    if(a1==a2){
      index=i1>i2?i2:i1;
    }else if(a2>a1){
      index=i2;
      a1=a2;
    }else{
       index= i1;
    }

    if(a4==a3){
      i1=i3>i4?i4:i3;
    }else if(a4>a3){
      i1=i4;
      a3=a4;
    }else{
       i1= i3;
    }

    if(a1==a3){
       index=i1>index?index:i1;
       *maxf=a1; 
    }else if(a3>a1){
       index=i1;
       *maxf=a3;
    }else{ 
        *maxf=a1;
    }
    return index; 

}
 
  

 
 

BLASLONG CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
    BLASLONG i = 0;
    BLASLONG ix = 0;
    FLOAT maxf = 0;
    BLASLONG max = 0;
    BLASLONG inc_x2;

    if (n <= 0 || inc_x <= 0) return(max);
     
    if (inc_x == 1) {

      BLASLONG n1 = n & -32;
      if (n1 > 0) {

            max = ciamax_kernel_32(n1, x, &maxf); 
            i = n1;
            ix = n1 << 1;
      }

      while(i < n)
    {
        if( CABS1(x,ix) > maxf )
        {
            max = i;
            maxf = CABS1(x,ix);
        }
        ix += 2;
        i++;
    }
        return (max + 1);

    } else {
 
      inc_x2 = 2 * inc_x;

    maxf = CABS1(x,0);
    ix += inc_x2;
    i++;

    while(i < n)
    {
        if( CABS1(x,ix) > maxf )
        {
            max = i;
            maxf = CABS1(x,ix);
        }
        ix += inc_x2;
        i++;
    }
        return (max + 1);
    }
 
}


