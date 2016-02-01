#include <jni.h>
#include <pijul.h>
#include <string.h>

JNIEXPORT jlong JNICALL Java_PijulRepository_cOpen(JNIEnv* jenv,jobject jobj,jstring j_path) {
  pijul_repository repo;
  const char* c_path = (*jenv)->GetStringUTFChars(jenv,j_path,0);
  pijul_open_repository(c_path,&repo);
  (*jenv)->ReleaseStringUTFChars(jenv,j_path,c_path);
  return (jlong) repo;
}

JNIEXPORT void JNICALL Java_PijulRepository_cClose(JNIEnv* jenv,jobject jobj,jlong repository) {
  pijul_repository repo=(pijul_repository)repository;
  pijul_close_repository(repo);
}

JNIEXPORT void JNICALL Java_PijulRepository_cAddFile(JNIEnv* jenv,jobject jobj,jlong repository,jstring j_path,jint is_dir) {
  const char* c_path = (*jenv)->GetStringUTFChars(jenv,j_path,0);
  pijul_add_file((pijul_repository)repository,c_path,is_dir);
  (*jenv)->ReleaseStringUTFChars(jenv,j_path,c_path);
}

JNIEXPORT void JNICALL Java_PijulRepository_cNewInternal(JNIEnv* jenv,jobject jobj,jlong repository,jobject x) {
  char* y=(*jenv)->GetByteArrayElements(jenv,x,NULL);
  pijul_new_internal((pijul_repository)repository, y);
  (*jenv)->ReleaseByteArrayElements(jenv,x,y,0);
}

JNIEXPORT void JNICALL Java_PijulRepository_cRegisterHash(JNIEnv* jenv,jobject jobj,jlong repository,jobject internal,jobject external) {
  char* i=(*jenv)->GetByteArrayElements(jenv,internal,NULL);
  char* e=(*jenv)->GetByteArrayElements(jenv,external,NULL);
  pijul_register_hash((pijul_repository)repository, i,e,(*jenv)->GetArrayLength(jenv,external));
  (*jenv)->ReleaseByteArrayElements(jenv,internal,i,0);
  (*jenv)->ReleaseByteArrayElements(jenv,external,e,0);
}

JNIEXPORT jobject JNICALL Java_PijulRepository_cRecord(JNIEnv* jenv,jobject jobj,jlong repository,jstring working_copy) {
  const char* c_path = (*jenv)->GetStringUTFChars(jenv,working_copy,0);
  pijul_changes_t a;
  pijul_updates_t b;
  pijul_record((pijul_repository)repository,c_path,&a,&b);
  (*jenv)->ReleaseStringUTFChars(jenv,working_copy,c_path);

  jclass tupclass = (*jenv)->FindClass(jenv,"scala/Tuple2");
  jmethodID tupcon = (*jenv)->GetMethodID(jenv,tupclass, "<init>", "(Ljava/lang/Object;Ljava/lang/Object;)V");
  jobject tuple = (*jenv)->NewObject(jenv,tupclass,tupcon,a,b);
  return tuple;
}

