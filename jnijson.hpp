//
// Created by anbu.subramanian on 9/11/2015.
//
#pragma once

#include "Types.hpp"
#include "JCallBack.hpp"
#include "Mutex.hpp"
#include "Signal.hpp"
#include "AutoSqlite.hpp"
#include "Schema.hpp"
#include "Thread.hpp"

#define TABLE_CATALOG_GROUP "MI9CatalogGroup"
#define TABLE_CATALOG_GROUP_ITEM "MI9CatalogGroupItem"
#define TABLE_CATALOG_SEASON "MI9CatalogSeason"
#define TABLE_CATALOG_EPISODE "MI9CatalogEpisode"
#define TABLE_CATALOG_CLIP "MI9CatalogClip"
#define TABLE_CATALOG_SHOW "MI9CatalogShow"
#define TABLE_CATALOG_GENRE "MI9CatalogGenre"
#define TABLE_CATALOG_TAG "MI9CatalogTag"

#define TABLE_CATALOG_LIVE_VIDEO "MI9CatalogLiveVideo"
#define TABLE_CATALOG_LIVE_LISTING "MI9CatalogLiveListing"
#define TABLE_CATALOG_LIVE_REGION "MI9CatalogLiveRegion"

// Misc tables.
#define TABLE_CATALOG_CONFIG "MI9CatalogConfig"
#define TABLE_CATALOG_RESUME_TIME "MI9CatalogResumeTime"

#define COLUMN_PRIMARY_KEY "id"

extern "C"
{
    JNIEXPORT jint JNI_OnLoad(JavaVM* vm, void* reserved);

    JNIEXPORT void JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeInit(JNIEnv* env, jobject main);

    // Register a java method.
    JNIEXPORT void JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeRegister(JNIEnv* env, jobject main, jobject obj, jstring name, jstring method, jstring signature);

    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeDbOpen(JNIEnv* env, jobject main, jstring database);
    JNIEXPORT void JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeDbClose(JNIEnv* env, jobject main);

    JNIEXPORT void JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeAddSchema(JNIEnv* env, jobject main, jstring table, jstring columns, jstring types, jstring annotations);

    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseHome(JNIEnv* env, jobject main, jstring document);
    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParsePage(JNIEnv* env, jobject main, jstring url, jstring showId, jstring seasonId, jstring document);
    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParsePageClips(JNIEnv* env, jobject main, jstring url, jstring showId, jstring seasonId, jstring document);
    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeCreateGroups(JNIEnv* env, jobject main);

    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseShows(JNIEnv* env, jobject main, jstring data);
    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseSeasons(JNIEnv* env, jobject main, jstring data);
    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseEpisodes(JNIEnv* env, jobject main, jstring data);
    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseClips(JNIEnv* env, jobject main, jstring data);
    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseGenres(JNIEnv* env, jobject main, jstring data);
    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseConfig(JNIEnv* env, jobject main, jstring document);

    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseVideoStatus(JNIEnv* env, jobject main, jstring data);
    JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseLive(JNIEnv* env, jobject main, jstring data);
};

typedef hash_map<string, JCallBack*> pjcallback_map;
typedef hash_map<string, Schema*> pschema_map;

class JniJson : public Thread
{
public:
    static JniJson*                         pThis;
    static JavaVM* 							pVm;
    static jobject 							mClassLoader;
    static jmethodID 						mFindClassMethod;

    bool									mLoaded;
    Mutex 									mMutex;
    Signal                                  mSignal;
    pjcallback_map	                        mObjMap;
    pschema_map	                            mSchemas;

    std::vector<string*>                    mQueue;

    AutoSqlite*                             mSqlite;

    bool                                    mRun;

    static JniJson*		getInstance();
    static JavaVM* 		getVm();

    void init();

    void objectRegister(JNIEnv* env, const char*, const char*, const char*, jobject);
    //JCallBack* objectFind(const string& name);
    JCallBack* objectFind(const char* name);

    bool dbOpen(const string& database);
    void dbClose();

    void notifyTableChanged(const char* table);

    void addSchema(string tableStr, string colsStr, string typesStr, string anosStr);

    bool parseHome(const string&);
    bool parsePage(const string&, const string&, const string&, const string&);
    bool parsePageClips(const string&, const string&, const string&, const string&);
    bool createGroups();
    bool createGroup(sqlite3_stmt*);
    bool isCuratedGroupId(const string &groupId);
    bool parseShows(const string&);
    bool parseSeasons(const string&);
    bool parseEpisodes(const string&);
    bool parseClips(const string&);
    bool parseGenres(const string&);
    bool parseConfig(const string&);
    bool parseVideoStatus(const string&);
    bool parseLive(const string&);

    string parseGroup(const string& type, const string& url, const string& showId, const string& seasonId, const Value&, long order, bool bDelete);
    string parseClip(const Value& values);

    bool parseItemArray(const char* table, const string&);
    bool parseItemArray(const char* table, const Value&, string_map*);

    string parseItem(const char* table, const Value&);
    string parseItem(const char* table, const Value&, string_map*);
    string parseItem(const char* table, const string& , const Value&, string_map*);
    //string parseItem(const char* table, const string&, const Value&);

    bool printJSONTree(const Value&, unsigned short depth = 0);
    void printJSONValue(const Value&);

    static void str_replace(string &s, const string &search, const string &replace);
    static void str_replace(string &s, const char *search, const char *replace);

    static std::vector<std::string>& split(const std::string &s, char delim, std::vector<std::string> &elems);
    static std::vector<std::string> split(const std::string &s, char delim);

    static uint64_t hash(const std::string &s);

    void run(void*);

private:
    JniJson();
    ~JniJson();

    Schema* getSchema(string name);

    void doNotifyTableChanged(const string* table);
    void queueNotifyTableChanged(const char* table);
    string update(ostringstream& dest, const char* table, const Value&);
    void update(ostringstream& dest, const char* table, const string&, const Value&);
    string update(ostringstream& dest, const char* table, const Value&, string_map*);
    string update(ostringstream& dest, const char* table, const string& , const Value&, string_map*);
    void deleteOldItems(const char* table, vector<string>&);
    void deleteOldItems(const char* table, const char* field, vector<string>&);
    //void deleteOldItems(const char* table, const char* field1, const char* field2, const char* value2, vector<string>&);
    //void removeResumeItems(const char* table, vector<string>&);
    //string getItemList(vector<string>&);
};
