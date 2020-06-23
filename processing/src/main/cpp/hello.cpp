#include "include/org_apache_druid_jni_JniWrapper.h"
#include <cstring>
#include <iostream>
#include <immintrin.h>

JNIEXPORT void JNICALL Java_org_apache_druid_jni_JniWrapper_hello
  (JNIEnv *env, jobject thisObject)
{
  std::cout << "Hello World" << std::endl;
}

JNIEXPORT jboolean JNICALL Java_org_apache_druid_jni_JniWrapper_memoryEquals
  (JNIEnv *env, jobject thisObject, jbyteArray array1, jobject buffer1, jlong offset1, jbyteArray array2, jobject buffer2, jlong offset2, jint length)
{
  char *p1, *p2;

  if (array1 != NULL) {
    p1 = (char*) env->GetPrimitiveArrayCritical(array1, NULL);
  } else {
    p1 = (char*) env->GetDirectBufferAddress(buffer1);
  }

  if (array2 != NULL) {
    p2 = (char*) env->GetPrimitiveArrayCritical(array2, NULL);
  } else {
    p2 = (char*) env->GetDirectBufferAddress(buffer2);
  }

  int cmp = __builtin_memcmp(p1 + offset1, p2 + offset2, length);

  if (array1 != NULL) {
    env->ReleasePrimitiveArrayCritical(array1, p1, 0);
  }

  if (array2 != NULL) {
    env->ReleasePrimitiveArrayCritical(array2, p2, 0);
  }

  return cmp == 0 ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT void JNICALL Java_org_apache_druid_jni_JniWrapper_aggregateCount
  (JNIEnv *env, jobject thisObject, jobject buf, jbyteArray bufArray, jint numRows, jintArray positions, jint positionOffset)
{
  char *cbuf;
  int *cpositions;

  if (bufArray != NULL) {
    cbuf = (char*) env->GetPrimitiveArrayCritical(bufArray, NULL);
  } else {
    cbuf = (char*) env->GetDirectBufferAddress(buf);
  }

  if (cbuf == NULL) {
    env->ThrowNew(env->FindClass("java/lang/RuntimeException"), "oops!");
  }

  cpositions = (int*) env->GetPrimitiveArrayCritical(positions, NULL);

  for (int i = 0; i < numRows; i++) {
    long *agg = (long*) (cbuf + cpositions[i] + positionOffset);
    *agg = *agg + 1;
  }

  /*
  long tmp[4];
  __m256i ones = _mm256_set1_epi64x(1);
  __m256i trueMask = _mm256_cmpeq_epi64(ones, ones);
  __m128i positionOffsets = _mm_set1_epi64x(positionOffset);

  // TODO(gianm: only works if numRows is a multiple of 4
  for (int i = 0; i < numRows; i += 4) {
    // TODO(gianm): try aligned; use pOffsets
    __m128i posv = _mm_lddqu_si128((__m128i const*)(cpositions + i));
    posv = _mm_add_epi64(posv, positionOffsets);

    __m256i aggv = _mm256_i32gather_epi64((long const*) cbuf, posv, 1);
    aggv = _mm256_add_epi64(aggv, ones);

    _mm256_maskstore_epi64((long long*) &tmp, trueMask, aggv);

    for (int j = 0 ; j < 4; j++) {
      long *agg = (long*) (cbuf + cpositions[i] + positionOffset);
      *agg = tmp[j];
    }
  }
  */

  env->ReleasePrimitiveArrayCritical(positions, cpositions, 0);

  if (bufArray != NULL) {
    env->ReleasePrimitiveArrayCritical(bufArray, cbuf, 0);
  }
}

JNIEXPORT void JNICALL Java_org_apache_druid_jni_JniWrapper_aggregateDoubleSum
  (JNIEnv *env, jobject thisObject, jdoubleArray vec, jobject buf, jbyteArray bufArray, jint numRows, jintArray positions, jintArray rows, jint positionOffset)
{
  char *cbuf;
  double *cvec;
  int *cpositions;
  int *crows = NULL;

  if (bufArray != NULL) {
    cbuf = (char*) env->GetPrimitiveArrayCritical(bufArray, NULL);
  } else {
    cbuf = (char*) env->GetDirectBufferAddress(buf);
  }

  if (cbuf == NULL) {
    env->ThrowNew(env->FindClass("java/lang/RuntimeException"), "oops!");
  }

  cvec = (double*) env->GetPrimitiveArrayCritical(vec, NULL);
  cpositions = (int*) env->GetPrimitiveArrayCritical(positions, NULL);

  if (rows != NULL) {
    crows = (int*) env->GetPrimitiveArrayCritical(rows, NULL);

    for (int i = 0; i < numRows; i++) {
      double *agg = (double*) (cbuf + cpositions[i] + positionOffset);
      *agg = *agg + cvec[crows[i]];
    }

    env->ReleasePrimitiveArrayCritical(rows, crows, 0);
  } else {
    for (int i = 0; i < numRows; i++) {
      double *agg = (double*) (cbuf + cpositions[i] + positionOffset);
      *agg = *agg + cvec[i];
    }
  }

  env->ReleasePrimitiveArrayCritical(positions, cpositions, 0);
  env->ReleasePrimitiveArrayCritical(vec, cvec, 0);

  if (bufArray != NULL) {
    env->ReleasePrimitiveArrayCritical(bufArray, cbuf, 0);
  }
}
