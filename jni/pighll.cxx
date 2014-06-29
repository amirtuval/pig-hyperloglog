#include "pighll.hpp"
#include "SerializedHyperLogLog.hpp"

JNIEXPORT jlong JNICALL Java_com_amirtuval_pighll_HyperLogLog_createHll
  (JNIEnv *, jobject, jint b) {
  return (jlong)new SerializedHyperLogLog(b);
}

JNIEXPORT jlong JNICALL Java_com_amirtuval_pighll_HyperLogLog_createHllFromString
  (JNIEnv * env, jobject, jstring hllStr) {
  char *nativeHllStr = (char*)env->GetStringUTFChars(hllStr, JNI_FALSE);
  SerializedHyperLogLog* result = SerializedHyperLogLog::fromString(nativeHllStr);
  env->ReleaseStringUTFChars(hllStr, nativeHllStr);

  return (jlong)result;
}

SerializedHyperLogLog* hllptr(jlong ptr) {
    return (SerializedHyperLogLog*) ptr;
}

JNIEXPORT void JNICALL Java_com_amirtuval_pighll_HyperLogLog_freeHll
  (JNIEnv *, jobject, jlong ptr) {
  delete hllptr(ptr);
}

JNIEXPORT void JNICALL Java_com_amirtuval_pighll_HyperLogLog_addElement
  (JNIEnv * env, jobject, jlong ptr, jstring element) {
  char *nativeElement = (char*)env->GetStringUTFChars(element, JNI_FALSE);
  hllptr(ptr)->add(nativeElement, strlen(nativeElement));
  env->ReleaseStringUTFChars(element, nativeElement);
}

JNIEXPORT jdouble JNICALL Java_com_amirtuval_pighll_HyperLogLog_estimateHll
  (JNIEnv *, jobject, jlong ptr) {
  return hllptr(ptr)->estimate();
}

JNIEXPORT void JNICALL Java_com_amirtuval_pighll_HyperLogLog_mergeHll
  (JNIEnv *, jobject, jlong target, jlong source) {
  hllptr(target)->merge(*hllptr(source));
}

JNIEXPORT jstring JNICALL Java_com_amirtuval_pighll_HyperLogLog_hllAsString
  (JNIEnv * env, jobject, jlong ptr) {

  char serialized[10000];
  hllptr(ptr)->toString(serialized);

  return env->NewStringUTF(serialized);
}
