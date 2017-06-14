//
// Created by anbu.subramanian on 9/11/2015.
//

#include "jnijson.hpp"
#include "AutoJniEnv.hpp"

#define NOTIFY_PERIOD (333*1000)

JniJson*	JniJson::pThis = nullptr;
JavaVM*		JniJson::pVm = nullptr;
jobject 	JniJson::mClassLoader = nullptr;
jmethodID 	JniJson::mFindClassMethod = nullptr;

jint JNI_OnLoad( JavaVM* vm, void* reserved )
{
    JNIEnv *env;

    LOG_INFO("Loading native library compiled at " __TIME__ " " __DATE__);

    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6) != JNI_OK)
    {
        return -1;
    }

    JniJson::pVm = vm;

    //auto randomClass = env->FindClass(".../com/");
    //jclass classClass = env->GetObjectClass(randomClass);

    //auto classLoaderClass = env->FindClass("java/lang/ClassLoader");
    //auto getClassLoaderMethod = env->GetMethodID(classClass, "getClassLoader", "()Ljava/lang/ClassLoader;");

    //JniJson::mClassLoader = env->CallObjectMethod(randomClass, getClassLoaderMethod);
    //JniJson::mFindClassMethod = env->GetMethodID(classLoaderClass, "findClass", "(Ljava/lang/String;)Ljava/lang/Class;");

    // Get jclass with env->FindClass.
    // Register methods with env->RegisterNatives.

    return JNI_VERSION_1_6;
}

JNIEXPORT void JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeInit(JNIEnv* env, jobject main)
{
    JniJson::getInstance()->init();
}

JNIEXPORT void JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeRegister(JNIEnv* env, jobject main, jobject obj, jstring name, jstring method, jstring signature)
{
    std::string nameStr(env->GetStringUTFChars(name, 0));
    std::replace( nameStr.begin(), nameStr.end(), '.', '/');

    std::string methodStr(env->GetStringUTFChars(method, 0));

    std::string signatureStr(env->GetStringUTFChars(signature, 0));

    JniJson::getInstance()->objectRegister(env, nameStr.c_str(), methodStr.c_str(), signatureStr.c_str(), obj);
}

JNIEXPORT void JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeAddSchema(JNIEnv* env, jobject main, jstring table, jstring columns, jstring types, jstring annotations)
{
    std::string tableStr(env->GetStringUTFChars(table, 0));
    std::string colsStr(env->GetStringUTFChars(columns, 0));
    std::string typesStr(env->GetStringUTFChars(types, 0));
    std::string anosStr(env->GetStringUTFChars(annotations, 0));
    return JniJson::getInstance()->addSchema(tableStr, colsStr, typesStr, anosStr);
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseHome(JNIEnv* env, jobject main, jstring document)
{
    if(document == nullptr)
    {
        return false;
    }
    std::string documentStr(env->GetStringUTFChars(document, 0));
    return JniJson::getInstance()->parseHome(documentStr);
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParsePage(JNIEnv* env, jobject main, jstring url, jstring showId, jstring seasonId, jstring document)
{
    if ((document == nullptr) || (url == nullptr))
    {
        return false;
    }
    std::string documentStr(env->GetStringUTFChars(document, 0));
    std::string urlStr(env->GetStringUTFChars(url, 0));
    std::string showStr(env->GetStringUTFChars(url, 0));
    std::string seasonStr(env->GetStringUTFChars(url, 0));
    return JniJson::getInstance()->parsePage(urlStr, showStr, seasonStr, documentStr);
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParsePageClips(JNIEnv* env, jobject main, jstring url, jstring showId, jstring seasonId, jstring document)
{
    if ((document == nullptr) || (url == nullptr))
    {
        return false;
    }
    std::string documentStr(env->GetStringUTFChars(document, 0));
    std::string urlStr(env->GetStringUTFChars(url, 0));
    std::string showStr(env->GetStringUTFChars(url, 0));
    std::string seasonStr(env->GetStringUTFChars(url, 0));
    return JniJson::getInstance()->parsePageClips(urlStr, showStr, seasonStr, documentStr);
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeCreateGroups(JNIEnv* env, jobject main)
{
    return JniJson::getInstance()->createGroups();
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseShows(JNIEnv* env, jobject main, jstring document)
{
    if(document == nullptr)
    {
        return false;
    }
    std::string documentStr(env->GetStringUTFChars(document, 0));
    return JniJson::getInstance()->parseShows(documentStr);
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseSeasons(JNIEnv* env, jobject main, jstring document)
{
    if(document == nullptr)
    {
        return false;
    }
    std::string documentStr(env->GetStringUTFChars(document, 0));
    return JniJson::getInstance()->parseSeasons(documentStr);
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseEpisodes(JNIEnv* env, jobject main, jstring document)
{
    if(document == nullptr)
    {
        return false;
    }
    std::string documentStr(env->GetStringUTFChars(document, 0));
    return JniJson::getInstance()->parseEpisodes(documentStr);
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseClips(JNIEnv* env, jobject main, jstring document)
{
    if(document == nullptr)
    {
        return false;
    }
    std::string documentStr(env->GetStringUTFChars(document, 0));
    return JniJson::getInstance()->parseClips(documentStr);
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseGenres(JNIEnv* env, jobject main, jstring document)
{
    if(document == nullptr)
    {
        return false;
    }
    std::string documentStr(env->GetStringUTFChars(document, 0));
    return JniJson::getInstance()->parseGenres(documentStr);
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseConfig(JNIEnv* env, jobject main, jstring document)
{
    if(document == nullptr)
    {
        return false;
    }
    std::string documentStr(env->GetStringUTFChars(document, 0));
    return JniJson::getInstance()->parseConfig(documentStr);
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseVideoStatus(JNIEnv* env, jobject main, jstring document)
{
    if(document == nullptr)
    {
        return false;
    }
    std::string documentStr(env->GetStringUTFChars(document, 0));
    return JniJson::getInstance()->parseVideoStatus(documentStr);
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeParseLive(JNIEnv* env, jobject main, jstring document)
{
    if(document == nullptr)
    {
        return false;
    }
    std::string documentStr(env->GetStringUTFChars(document, 0));
    return JniJson::getInstance()->parseLive(documentStr);
}

JNIEXPORT jboolean JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeDbOpen(JNIEnv* env, jobject main, jstring database)
{
    std::string databaseStr(env->GetStringUTFChars(database, 0));
    JniJson::getInstance()->dbOpen(databaseStr);
}

JNIEXPORT void JNICALL Java_au_com_mi9_ninenow_api_JniJson_nativeDbClose(JNIEnv* env, jobject main)
{
    JniJson::getInstance()->dbClose();
}

JniJson::JniJson()
    : mSchemas(), mObjMap(), mMutex(), mSignal(), mQueue(),
      mSqlite(nullptr), mLoaded(false), mRun(true)
{
    start();
}

JniJson::~JniJson()
{
    mRun = false;

    dbClose();

    pjcallback_map::iterator iter = mObjMap.begin();
    while (iter != mObjMap.end())
    {
        delete iter->second;
        iter++;
    }
}

JniJson* JniJson::getInstance()
{
    if (nullptr == pThis)
    {
        pThis = new JniJson();
    }
    return pThis;
}

JavaVM* JniJson::getVm()
{
    return pVm;
}

void JniJson::init()
{
}

void JniJson::objectRegister(JNIEnv* env, const char* name, const char* method, const char* signature, jobject obj)
{
    JCallBack* pCb = nullptr;

    mMutex.lock();

    pjcallback_map::iterator iter = mObjMap.find(name);
    if (iter != mObjMap.end())
    {
        pCb = iter->second;
    }
    else
    {
        jobject objref = (jobject)env->NewGlobalRef(obj);
        jclass tmp = env->GetObjectClass(objref);
        jclass cls = (jclass)env->NewGlobalRef(tmp);

        jmethodID mid = env->GetMethodID(cls, method, signature);

        pCb = new JCallBack(objref, cls);
        mObjMap[name] = pCb;
    }

    mMutex.unlock();

    if (nullptr != pCb)
    {
        method_map::iterator iter = pCb->mMethods.find(method);
        if (iter == pCb->mMethods.end())
        {
            jmethodID mid = env->GetMethodID(pCb->mClass, method, signature);
            pCb->mMethods[method] = mid;
        }
    }
}

bool JniJson::dbOpen(const string& database)
{
    if (nullptr == mSqlite)
    {
        mSqlite = new AutoSqlite();
    }

    mSqlite->dbOpen(database);
}

void JniJson::dbClose()
{
    if (nullptr != mSqlite)
    {
        mSqlite->dbClose();
    }
}

void JniJson::addSchema(string tableStr, string colsStr, string typesStr, string anosStr)
{
    Schema* pSchema = new Schema(tableStr, colsStr, typesStr, anosStr);
    mSchemas[tableStr] = pSchema;
}

Schema* JniJson::getSchema(string name)
{
    pschema_map::iterator iter = mSchemas.find(name);
    if (iter != mSchemas.end())
    {
        return iter->second;
    }

    return nullptr;
}

//TODO: remove once show/season page api is updated to include tags.
bool JniJson::parsePageClips(const string& url, const string& showId, const string& seasonId, const string& document)
{
    LOG_INFO("Parse Page Clips");
    bool result = false;

    result = parsePage(url, showId, seasonId, document);

    LOG_INFO("Parse Page Tags complete");

    return result;
}

void seasonsQuery(void* pClass, void* pArg, sqlite3_stmt *stmt)
{
    ((JniJson*)pClass)->createGroup(stmt);
}

bool JniJson::createGroup(sqlite3_stmt* stmt)
{
    //LOG_INFO("createGroup");

    string showSlug;
    string seasonSlug;
    string seasonId;
    const char *table = TABLE_CATALOG_GROUP;

    Schema* pSchema = getSchema(table);
    if (nullptr != pSchema)
    {
        int ncols = sqlite3_column_count(stmt);

        for (int i = 0; i < ncols; i++)
        {
            const char *col_name = sqlite3_column_name(stmt, i);

            if (0 == strcmp(col_name, "showSlug"))
            {
                const char *col_val_c = reinterpret_cast<const char *>(sqlite3_column_text(
                        stmt, i));

                //LOG_INFO("createGroup got show:%s", col_val_c);

                if (nullptr != col_val_c)
                {
                    showSlug = col_val_c;
                }
            }
            else if (0 == strcmp(col_name, "slug"))
            {
                const char *col_val_c = reinterpret_cast<const char *>(sqlite3_column_text(
                        stmt, i));

                //LOG_INFO("createGroup got season:%s", col_val_c);

                if (nullptr != col_val_c)
                {
                    seasonSlug = col_val_c;
                }
            }
            else if (0 == strcmp(col_name, COLUMN_PRIMARY_KEY))
            {
                const char *col_val_c = reinterpret_cast<const char *>(sqlite3_column_text(
                        stmt, i));

                //LOG_INFO("createGroup got season id:%s", col_val_c);

                if (nullptr != col_val_c)
                {
                    seasonId = col_val_c;
                }
            }
        }

        std::ostringstream url;
        std::ostringstream getGroup;
        string getGroupStr;
        string urlStr;
        string episodeGroupId = "LATEST-EPISODES-"+seasonId;
        string clipsGroupId = "LATEST-CLIPS-"+seasonId;

        url << "'/pages/tv-series/" << showSlug << "/seasons/" << seasonSlug << "'";
        urlStr = url.str();

        getGroup << "url=" << urlStr;
        getGroupStr = getGroup.str();

        if (!mSqlite->query(table, getGroupStr, false))
        {
            string_map group;

            group["url"] = Field(urlStr, FieldType::STRING, true);
            group["type"] = Field("rail", true);
            group["callToActionText"] = Field("View all", true);
            group["autoQuery"] = Field(1, FieldType::LONG, true);

            // Create default episodes rail
            group[COLUMN_PRIMARY_KEY] = Field(episodeGroupId, true);
            group["sortOrder"] = Field(0, FieldType::LONG, true);
            group["title"] = Field("LATEST EPISODES", true);
            group["itemsType"] = Field("episodes", true);
            group["callToActionType"] = Field("episode-index", true);

            bool shouldNotify = false;
            if (nullptr != pSchema)
            {
                std::ostringstream dest;
                pSchema->getInsert(dest, group);

                string exec = dest.str();
                //LOG_INFO("createGroup exec:%s", exec.c_str());
                if (mSqlite->exec(exec, false))
                    shouldNotify = true;
            }

            // Create default clips rail
            group[COLUMN_PRIMARY_KEY] = Field(clipsGroupId, true);
            group["sortOrder"] = Field(1, FieldType::LONG, true);
            group["title"] = Field("LATEST CLIPS", true);
            group["itemsType"] = Field("clips", true);
            group["callToActionType"] = Field("clip-index", true);
            if (nullptr != pSchema)
            {
                std::ostringstream dest;
                pSchema->getInsert(dest, group);

                string exec = dest.str();
                //LOG_INFO("createGroup exec:%s", exec.c_str());
                if (mSqlite->exec(exec, false))
                    shouldNotify = true;
            }

            if (shouldNotify)
            {
                notifyTableChanged(table);
            }
        }
    }
}

bool JniJson::createGroups()
{
    if (nullptr != mSqlite)
    {
        //LOG_INFO("createGroups");
        string query = "select DISTINCT MI9CatalogSeason.*,MI9CatalogShow.slug AS showSlug from MI9CatalogSeason LEFT JOIN MI9CatalogShow ON (MI9CatalogSeason.show = MI9CatalogShow.id)";
        mSqlite->query(query, this, nullptr, seasonsQuery);
    }
}

bool JniJson::parseHome(const string& document)
{
    Value root;
    Reader reader;
    bool result = false;
    vector<string> group_ids;

    LOG_INFO("Parse Home");

    if (document.length() > 0)
    {
        result = reader.parse(document, root, false);
        const Value& items = root["items"];
        string url = "/pages/home";

        for (int index = 0; index < items.size(); index++)
        {
            if (items[index].isMember("type"))
            {
                string type = items[index]["type"].asString();
                string ids = "";

                LOG_INFO("Parse Home item type: %s", type.c_str());

                // convert to lower case...
                std::transform(type.begin(), type.end(), type.begin(), (int (*)(int)) ::tolower);

                group_ids.push_back(parseGroup(type, url, ids, ids, items[index], index, false));
            }
        }

        if (nullptr != mSqlite)
            mSqlite->doDel(TABLE_CATALOG_GROUP_ITEM, COLUMN_PRIMARY_KEY, "groupId");

        if ((nullptr != mSqlite) && (group_ids.size() > 0))
        {
            std::ostringstream urlStm;
            if ('\'' != url.at(0))
            {
                urlStm << "'" << url << "'";
            }
            else
            {
                urlStm << url;
            }
            string urlStr = urlStm.str();
            mSqlite->prepDel(TABLE_CATALOG_GROUP, COLUMN_PRIMARY_KEY, "url", urlStr.c_str(), group_ids);
            mSqlite->doDel(TABLE_CATALOG_GROUP, COLUMN_PRIMARY_KEY, "url");
        }

        // TODO: Keep track of whether the table content has changed before notifying
        notifyTableChanged(TABLE_CATALOG_GROUP_ITEM);
        notifyTableChanged(TABLE_CATALOG_GROUP);
    }

    LOG_INFO("Parse Home complete");

    return result;
}

bool JniJson::parsePage(const string& url, const string& showId, const string& seasonId, const string& document)
{
    Value root;
    Reader reader;
    bool result = false;
    vector<string> group_ids;

    LOG_INFO("Parse Page");

    if (document.length() > 0)
    {
        result = reader.parse(document, root, false);
        const Value& items = root["items"];

        // Delete auto rails
        if (nullptr != mSqlite)
        {
            std::ostringstream delStrStrm;

            delStrStrm << "url = '" << url << "' and autoQuery = 1";
            string where = delStrStrm.str();
            mSqlite->del(TABLE_CATALOG_GROUP, where.c_str());
        }

        // Delete all groups for this URL.
        /*
        if (nullptr != mSqlite)
        {
            std::ostringstream delStrStrm;

            delStrStrm << "url = '" << url << "'";
            string where = delStrStrm.str();
            mSqlite->del(TABLE_CATALOG_GROUP, where.c_str());
        }
        */

        if (root.isMember("clipTags"))
        {
            const char *tagTable = TABLE_CATALOG_TAG;
            string_map tags_data;
            tags_data[COLUMN_PRIMARY_KEY] = Field(url);
            parseItem(tagTable, root, &tags_data);

            // TODO: Keep track of whether the table content has changed before notifying
            notifyTableChanged(tagTable);
        }

        for (int index = 0; index < items.size(); index++)
        {
            if (items[index].isMember("type"))
            {
                string id = items[index][COLUMN_PRIMARY_KEY].asString();
                string type = items[index]["type"].asString();

                LOG_INFO("Parse Page item. id: %s, type: %s", id.c_str(), type.c_str());

                // convert to lower case...
                std::transform(type.begin(), type.end(), type.begin(), (int (*)(int)) ::tolower);

                // only delete items from non-curated rails
                group_ids.push_back(parseGroup(type, url, showId, seasonId, items[index], index, !isCuratedGroupId(id)));
            }
        }

        if (nullptr != mSqlite)
            mSqlite->doDel(TABLE_CATALOG_GROUP_ITEM, COLUMN_PRIMARY_KEY, "groupId");

        if ((nullptr != mSqlite) && (group_ids.size() > 0))
        {
            std::ostringstream urlStm;
            if ('\'' != url.at(0))
            {
                urlStm << "'" << url << "'";
            }
            else
            {
                urlStm << url;
            }
            string urlStr = urlStm.str();
            mSqlite->prepDel(TABLE_CATALOG_GROUP, COLUMN_PRIMARY_KEY, "url", urlStr.c_str(), group_ids);
            mSqlite->doDel(TABLE_CATALOG_GROUP, COLUMN_PRIMARY_KEY, "url");
        }

        // TODO: Keep track of whether the table content has changed before notifying
        notifyTableChanged(TABLE_CATALOG_GROUP_ITEM);
        notifyTableChanged(TABLE_CATALOG_GROUP);
    }

    LOG_INFO("Parse Page complete");

    return result;
}

bool JniJson::isCuratedGroupId(const string& groupId)
{
    // A group is not curated if the id starts with 'LATEST-'
    return (groupId.find("LATEST-") != 0);
}

string JniJson::parseGroup(const string& type, const string& url, const string& showId, const string& seasonId, const Value& value, long order, bool bDelete)
{
    string_map group_extras;
    vector<string> group_ids;
    vector<string> show_ids;
    vector<string> clip_ids;
    vector<string> genre_ids;
    vector<string> episode_ids;
    string group_id;

    if (value.isMember("items"))
    {
        group_extras["url"] = Field(url);
        group_extras["showId"] = Field(showId);
        group_extras["seasonId"] = Field(seasonId);
        group_extras["sortOrder"] = Field(order, FieldType::LONG);
        group_extras["autoQuery"] = Field(0, FieldType::LONG);
        group_id = parseItem(TABLE_CATALOG_GROUP, value, &group_extras);

        if (0 == type.compare("carousel"))
        {
            LOG_INFO("Parse Group - item type is carousel");

            const Value &subItems = value["items"];
            for (int subIndex = 0; subIndex < subItems.size(); subIndex++)
            {
                string_map item_extras;
                item_extras["groupId"] = group_extras[COLUMN_PRIMARY_KEY];
                item_extras["sortOrder"] = Field(subIndex, FieldType::LONG);

                group_ids.push_back(parseItem(TABLE_CATALOG_GROUP_ITEM, group_extras[COLUMN_PRIMARY_KEY].get(), subItems[subIndex], &item_extras));
            }

            if ((nullptr != mSqlite) && (group_ids.size() > 0))
            {
                mSqlite->prepDel(TABLE_CATALOG_GROUP_ITEM, COLUMN_PRIMARY_KEY, "groupId", group_extras[COLUMN_PRIMARY_KEY].get().c_str(), group_ids);
            }
        }
        else if ((0 == type.compare("rail")) || (0 == type.compare("grid")) || (0 == type.compare("largegrid")))
        {
            //LOG_INFO("Parse Group - recognised type : %s", type.c_str());

            const Value &subItems = value["items"];
            for (int subIndex = 0; subIndex < subItems.size(); subIndex++)
            {
                //LOG_INFO("Parse Home GET rail item type");
                string subType = subItems[subIndex]["type"].asString();

                if (0 == subType.compare("episode"))
                {
                    LOG_INFO("Parse Group - rail GOT item episode");
                    episode_ids.push_back(parseItem(TABLE_CATALOG_EPISODE, subItems[subIndex]));
                }
                else if (0 == subType.compare("clip"))
                {
                    LOG_INFO("Parse Group - rail GOT item clip");
                    clip_ids.push_back(parseClip(subItems[subIndex]));
                }
                else if (0 == subType.compare("tv-series"))
                {
                    LOG_INFO("Parse Group - rail GOT item show");
                    show_ids.push_back(parseItem(TABLE_CATALOG_SHOW, subItems[subIndex]));
                }
                else if (0 == subType.compare("genre"))
                {
                    LOG_INFO("Parse Group - rail GOT item genre");
                    genre_ids.push_back(parseItem(TABLE_CATALOG_GENRE, subItems[subIndex]));
                }

                string_map item_extras;
                item_extras["groupId"] = group_extras[COLUMN_PRIMARY_KEY];
                item_extras["sortOrder"] = Field(subIndex, FieldType::LONG);
                group_ids.push_back(parseItem(TABLE_CATALOG_GROUP_ITEM, group_extras[COLUMN_PRIMARY_KEY].get(), subItems[subIndex], &item_extras));
            }

            if ((nullptr != mSqlite) && (group_ids.size() > 0))
            {
                mSqlite->prepDel(TABLE_CATALOG_GROUP_ITEM, COLUMN_PRIMARY_KEY, "groupId", group_extras[COLUMN_PRIMARY_KEY].get().c_str(), group_ids);
            }
        }
    }

    // TODO: Keep track of whether the table content has changed before notifying
    notifyTableChanged(TABLE_CATALOG_GROUP);
    notifyTableChanged(TABLE_CATALOG_GROUP_ITEM);
    notifyTableChanged(TABLE_CATALOG_EPISODE);
    notifyTableChanged(TABLE_CATALOG_CLIP);
    notifyTableChanged(TABLE_CATALOG_SHOW);
    notifyTableChanged(TABLE_CATALOG_GENRE);

    return group_id;
}

/*string JniJson::getItemList(vector<string>& ids)
{
    std::ostringstream dest;
    std::vector<string>::iterator it;
    bool bFirst = true;

    for (it = ids.begin(); it != ids.end(); it++)
    {
        if (!bFirst)
        {
            dest << ",";
        }

        dest << *it;

        bFirst = false;
    }
}*/

bool JniJson::parseLive(const string& document)
{
    Value root;
    Reader reader;
    bool result = false;

    LOG_INFO("Parse Live");

    if (document.length() > 0)
    {
        string_map data;

        vector<string> ids;

        result = reader.parse(document, root, false);

        Schema* pSchema = getSchema(TABLE_CATALOG_LIVE_REGION);
        if ((nullptr != mSqlite) && (nullptr != pSchema))
        {
            string_map data;
            std::ostringstream dest;

            pSchema->getValues(data, root);
            string where = "pk=1";

            switch (mSqlite->query(TABLE_CATALOG_LIVE_REGION, where, data))
            {
                case ITEM_NEW:
                    pSchema->getInsert(dest, data);
                    break;

                case ITEM_UNMODIFIED:
                    break;

                case ITEM_MODIFIED:
                    pSchema->getUpdate(dest, data);
                    break;
            }

            string exec = dest.str();
            mSqlite->exec(exec);
        }

        const Value& items = root["channels"];

        for (int index = 0; index < items.size(); index++)
        {
            //TODO: hopefully COLUMN_PRIMARY_KEY will be moved in to the channel object.
            if (items[index].isMember("listings") && items[index].isMember(COLUMN_PRIMARY_KEY))
            {
                string_map extras;
                string id = parseItem(TABLE_CATALOG_LIVE_VIDEO, items[index]);
                ids.push_back(id);

                // Delete all old listing for this channel...
                //TODO: figure out how to update the table instead...
                //TODO: i.e. remove items past now.
                if (nullptr != mSqlite)
                {
                    std::ostringstream dest;

                    dest << "mChannel = " << id;
                    string where = dest.str();
                    mSqlite->del(TABLE_CATALOG_LIVE_LISTING, where.c_str());
                }

                extras["mChannel"] = Field(id, "string");

                parseItemArray(TABLE_CATALOG_LIVE_LISTING, items[index]["listings"], &extras);
            }
        }

        deleteOldItems(TABLE_CATALOG_LIVE_VIDEO, ids);

        // TODO: Keep track of whether the table content has changed before notifying
        notifyTableChanged(TABLE_CATALOG_LIVE_VIDEO);
        notifyTableChanged(TABLE_CATALOG_LIVE_LISTING);
        notifyTableChanged(TABLE_CATALOG_LIVE_REGION);
    }
}

bool JniJson::parseShows(const string& document)
{
    bool result = parseItemArray(TABLE_CATALOG_SHOW, document);
    // TODO: Keep track of whether the table content has changed before notifying
    notifyTableChanged(TABLE_CATALOG_SHOW);
    return result;
}

bool JniJson::parseSeasons(const string& document)
{
    bool result = parseItemArray(TABLE_CATALOG_SEASON, document);
    // TODO: Keep track of whether the table content has changed before notifying
    notifyTableChanged(TABLE_CATALOG_SEASON);
    return result;
}

bool JniJson::parseEpisodes(const string& document)
{
    bool result = parseItemArray(TABLE_CATALOG_EPISODE, document);
    // TODO: Keep track of whether the table content has changed before notifying
    notifyTableChanged(TABLE_CATALOG_EPISODE);
    return result;
}

bool JniJson::parseClips(const string& document)
{
    Value root;
    Reader reader;
    bool result = false;
    const char* table = TABLE_CATALOG_CLIP;

    if (document.length() > 0)
    {
        vector<string> ids;

        result = reader.parse(document, root, false);

        const Value& items = root["items"];

        for (int index = 0; index < items.size(); index++)
            ids.push_back(parseClip(items[index]));

        deleteOldItems(table, ids);
    }
    // TODO: Keep track of whether the table content has changed before notifying
    notifyTableChanged(table);

    return result;
}

bool JniJson::parseGenres(const string& document)
{
    bool result = parseItemArray(TABLE_CATALOG_GENRE, document);
    // TODO: Keep track of whether the table content has changed before notifying
    notifyTableChanged(TABLE_CATALOG_GENRE);
    return result;
}

bool JniJson::parseConfig(const string& document)
{
    if (document.length() > 0)
    {
        Value root;
        Reader reader;

        reader.parse(document, root, false);

        string_map extras;
        extras[COLUMN_PRIMARY_KEY] = Field(1, FieldType::LONG);
        parseItem(TABLE_CATALOG_CONFIG, root, &extras);
        // TODO: Keep track of whether the table content has changed before notifying
        notifyTableChanged(TABLE_CATALOG_CONFIG);

        return true;
    }

    return false;
}

bool JniJson::parseVideoStatus(const string& document)
{
    Value items;
    Reader reader;
    bool result = false;
    const char * table = TABLE_CATALOG_RESUME_TIME;

    struct timeval tp;
    gettimeofday(&tp, NULL);
    uint64_t ms = tp.tv_sec * 1000 + tp.tv_usec / 1000;

    Schema* pSchema = getSchema(table);
    if ((nullptr != mSqlite) && (nullptr != pSchema))
    {
        vector<string> ids;
        result = reader.parse(document, items, false);
        for (int index = 0; index < items.size(); index++)
        {
            if (items[index].isMember("episodeId") && items[index].isMember("progressTime"))
            {
                std::ostringstream dest;
                string_map data;

                data["resumeType"] = Field("episode", "string");
                pSchema->getValues(data, items[index]);

                string id = data["itemId"].get();
                string where = "itemId="+id;
                ids.push_back(id);

                //pSchema->printValues(table, data);

                switch (mSqlite->query(table, where, data))
                {
                    case ITEM_NEW:
                        pSchema->getInsert(dest, data);
                        break;

                    case ITEM_UNMODIFIED:
                        break;

                    case ITEM_MODIFIED:
                        pSchema->getUpdate(dest, where.c_str(), data);
                        break;
                }

                string exec = dest.str();
                LOG_INFO("Update video status %s", exec.c_str());
                mSqlite->exec(exec);
            }
        }
        LOG_INFO("There are %d items in the watch history", ids.size());
        deleteOldItems(table, "itemId", ids);
    }

    return result;
}

void JniJson::deleteOldItems(const char* table, vector<string>& ids)
{
    if ((nullptr != table) && (ids.size() > 0))
    {
        int deleted = mSqlite->del(table, COLUMN_PRIMARY_KEY, ids);

        if (deleted > 0)
        {
            LOG_INFO("Deleted %d items from %s", deleted, table);
        }
    }
}

void JniJson::deleteOldItems(const char* table, const char* field, vector<string>& ids)
{
    if ((nullptr != table) && (ids.size() > 0))
    {
        int deleted = mSqlite->del(table, field, ids);
        if (deleted > 0)
        {
            LOG_INFO("Deleted %d items from %s", deleted, table);
        }
    }
}

/*void JniJson::deleteOldItems(const char* table, const char* field1, const char* field2, const char* value2, vector<string>& ids)
{
    if ((nullptr != table) && (ids.size() > 0))
    {
        int deleted = mSqlite->del(table, field1, field2, value2, ids);
        if (deleted > 0)
        {
            //LOG_INFO("Deleted %d items from %s", deleted, table);
            notifyTableChanged(table);
        }
    }
}*/

/*void JniJson::removeResumeItems(const char* table, vector<string>& ids)
{
    if (mSqlite->update(table, ids, "mResumePosition = 0"))
        notifyTableChanged(table);
}*/

bool JniJson::parseItemArray(const char* table, const string& document)
{
    Value root;
    Reader reader;
    bool result = false;

    if (document.length() > 0)
    {
        vector<string> ids;

        result = reader.parse(document, root, false);

        const Value& items = root["items"];

        for (int index = 0; index < items.size(); index++)
            ids.push_back(parseItem(table, items[index]));

        deleteOldItems(table, ids);
    }

    return result;
}

bool JniJson::parseItemArray(const char* table, const Value& items, string_map* extras)
{
    //LOG_INFO("parseItemArray %s %d", table, items.size());

    for (int index = 0; index < items.size(); index++)
        parseItem(table, items[index], extras);

    return true;
}

string JniJson::parseClip(const Value& values)
{
    const char *clipTable = TABLE_CATALOG_CLIP;

    /*
    const char *tagTable = TABLE_CATALOG_TAG;

    if (values.isMember("tags"))
    {
        string_map data;
        parseItemArray(tagTable, values["tags"], &data);
    }
    */

    return parseItem(clipTable, values);
}

string JniJson::parseItem(const char* table, const Value &values)
{
    std::ostringstream dest;
    string id = update(dest, table, values);
    mSqlite->exec(dest.str());

    return id;
}

/*string JniJson::parseItem(const char* table, const string& id, const Value& values)
{
    std::ostringstream dest;
    update(dest, table, id, values);
    string exec = dest.str();
    if (mSqlite->exec(exec))
        notifyTableChanged(table);

    return id;
}*/

string JniJson::parseItem(const char* table, const Value& values, string_map* extras)
{
    std::ostringstream dest;
    string id = update(dest, table, values, extras);
    mSqlite->exec(dest.str());

    return id;
}

string JniJson::parseItem(const char* table, const string& groupid , const Value& values, string_map* extras)
{
    std::ostringstream dest;
    string id = update(dest, table, groupid, values, extras);
    mSqlite->exec(dest.str());

    return id;
}

string JniJson::update(ostringstream& dest, const char* table, const Value& value)
{
    LOG_INFO("update1 %s", table);

    Schema* pSchema = getSchema(table);

    if ((nullptr != mSqlite) && (nullptr != pSchema))
    {
        string_map data;
        pSchema->getValues(data, value);

        string id = data[COLUMN_PRIMARY_KEY].get();

        std::ostringstream whereStm;
        whereStm << "id=" << id;
        string where = whereStm.str();

        //pSchema->printValues(table, data);

        switch (mSqlite->query(table, where, data))
        {
            case ITEM_NEW:
                pSchema->getInsert(dest, data);
                break;

            case ITEM_UNMODIFIED:
                break;

            case ITEM_MODIFIED:
                pSchema->getUpdate(dest, data);
                break;
        }

        //LOG_INFO("update %s %s", table, dest.str().c_str());

        return id;
    }

    return "0";
}

void JniJson::update(ostringstream& dest, const char* table, const string& id, const Value& value)
{
    LOG_INFO("update2 %s %s", table, id.c_str());

    Schema* pSchema = getSchema(table);

    if ((nullptr != mSqlite) && (nullptr != pSchema))
    {
        string_map data;
        pSchema->getValues(data, value);
        data[COLUMN_PRIMARY_KEY] = Field(id, "string");

        string where = "id=" + data[COLUMN_PRIMARY_KEY].get();

        //pSchema->printValues(table, data);

        switch (mSqlite->query(table, where, data))
        {
            case ITEM_NEW:
                pSchema->getInsert(dest, data);
                break;

            case ITEM_UNMODIFIED:
                break;

            case ITEM_MODIFIED:
                pSchema->getUpdate(dest, data);
                break;
        }

        //LOG_INFO("update %s %s", table, dest.str().c_str());
    }
}

string JniJson::update(ostringstream& dest, const char* table, const Value& value, string_map* extras)
{
    LOG_INFO("update3 %s", table);

    Schema* pSchema = getSchema(table);

    if ((nullptr != mSqlite) && (nullptr != pSchema))
    {
        pSchema->getValues(*extras, value);

        string id = (*extras)[COLUMN_PRIMARY_KEY].get();
        string where = "id=" + id;

        //pSchema->printValues(table, *extras);

        switch (mSqlite->query(table, where, *extras))
        {
            case ITEM_NEW:
                pSchema->getInsert(dest, *extras);
                break;

            case ITEM_UNMODIFIED:
                break;

            case ITEM_MODIFIED:
                pSchema->getUpdate(dest, *extras);
                break;
        }

        //LOG_INFO("update %s %s", table, dest.str().c_str());
        return id;
    }

    return "0";
}

string JniJson::update(ostringstream& dest, const char* table, const string& groupid, const Value& value, string_map* extras)
{
    LOG_INFO("update4 %s", table);

    Schema* pSchema = getSchema(table);

    if ((nullptr != mSqlite) && (nullptr != pSchema))
    {
        pSchema->getValues(*extras, value);

        string id = (*extras)[COLUMN_PRIMARY_KEY].get();

        std::ostringstream whereStm;
        whereStm << "id=" << id;
        whereStm << " and groupid=";

        if ('\'' != groupid.at(0))
        {
            whereStm << "'" << groupid << "'";
        }
        else
        {
            whereStm << groupid;
        }

        string where = whereStm.str();

        //pSchema->printValues(table, *extras);

        switch (mSqlite->query(table, where, *extras))
        {
            case ITEM_NEW:
                pSchema->getInsert(dest, *extras);
                break;

            case ITEM_UNMODIFIED:
                break;

            case ITEM_MODIFIED:
                pSchema->getUpdate(dest, *extras);
                break;
        }

        //LOG_INFO("update %s %s", table, dest.str().c_str());
        return id;
    }

    return "0";
}

void JniJson::printJSONValue(const Json::Value &val) {
    if (val.isString()) {
        LOG_INFO("string(%s)", val.asString().c_str());
    } else if (val.isBool()) {
        LOG_INFO("bool(%d)", val.asBool());
    } else if (val.isInt()) {
        LOG_INFO("int(%d)", val.asInt());
    } else if (val.isUInt()) {
        LOG_INFO("uint(%u)", val.asUInt());
    } else if (val.isDouble()) {
        LOG_INFO("double(%f)", val.asDouble());
    }
    else {
        LOG_INFO("unknown type=[%d]", val.type());
    }
}

bool JniJson::printJSONTree(const Json::Value &root, unsigned short depth /* = 0 */) {
    depth += 1;
    LOG_INFO(" {type=[%d], size=%d}", root.type(), root.size());

    if (root.size() > 0) {
        LOG_INFO("\n");
        for (Json::ValueIterator itr = root.begin(); itr != root.end(); itr++) {
            // Print depth.
            for (int tab = 0; tab < depth; tab++) {
                LOG_INFO("-");
            }
            LOG_INFO(" subvalue(");
            printJSONValue(itr.key());
            LOG_INFO(") -");
            printJSONTree(*itr, depth);
        }
        return true;
    } else {
        LOG_INFO(" ");
        printJSONValue(root);
        LOG_INFO("\n");
    }
    return true;
}

/*JCallBack* JniJson::objectFind(const string& name)
{
    pjcallback_map::iterator iter = mObjMap.find(name);
    if (iter != mObjMap.end())
    {
        return iter->second;
    }
    return nullptr;
}*/

JCallBack* JniJson::objectFind(const char* name)
{
    pjcallback_map::iterator iter = mObjMap.find(name);
    if (iter != mObjMap.end())
    {
        return iter->second;
    }
    return nullptr;
}

void JniJson::run(void *pthis)
{
    while (mRun)
    {
        while ((mQueue.size() > 0) && (mRun))
        {
            mMutex.lock();
            string* table = mQueue.front();
            mQueue.erase(mQueue.begin());
            mMutex.unlock();

            doNotifyTableChanged(table);
            delete table;
        }

        if (mRun)
            usleep(NOTIFY_PERIOD);
    }
}

void JniJson::doNotifyTableChanged(const string* table)
{
    //LOG_INFO("doNotifyTableChanged:%s", table->c_str());
    JCallBack* pCb = objectFind("notifyChange");
    if (nullptr != pCb)
    {
        AutoJniEnv env;
        jstring jTableStr = env->NewStringUTF(table->c_str());
        env->CallVoidMethod(pCb->mObject, pCb->getMethod("notifyChange"), jTableStr);
    }
}

void JniJson::queueNotifyTableChanged(const char* table)
{
    mMutex.lock();
    std::vector<string*>::iterator iter = mQueue.begin();
    bool bFound = false;

    while(iter != mQueue.end())
    {
        if (*(*iter) == table)
        {
            bFound = true;
            break;
        }
        iter++;
    }

    if (!bFound)
    {
        LOG_INFO("notifyTableChanged:%s", table);
        mQueue.push_back(new string(table));
    }
    mMutex.unlock();
}

void JniJson::notifyTableChanged(const char* table)
{
    queueNotifyTableChanged(table);

    if (0 == strcmp(table, TABLE_CATALOG_RESUME_TIME))
    {
        queueNotifyTableChanged(TABLE_CATALOG_EPISODE);
        queueNotifyTableChanged(TABLE_CATALOG_GROUP_ITEM);
    }
}

void JniJson::str_replace(string &s, const string &search, const string &replace)
{
    str_replace(s, search.c_str(), replace.c_str());
}

void JniJson::str_replace(string &s, const char *search, const char *replace)
{
    size_t repl_len = strlen(replace);
    size_t srch_len = strlen(search);

    for( size_t pos = 0; ; pos += repl_len )
    {
        pos = s.find(search, pos);
        if(pos == string::npos) break;

        s.erase(pos, srch_len);
        s.insert(pos, replace);
    }
}

std::vector<std::string>& JniJson::split(const std::string &s, char delim, std::vector<std::string> &elems)
{
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim))
    {
        elems.push_back(item);
    }
    return elems;
}

std::vector<std::string> JniJson::split(const std::string &s, char delim)
{
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}

uint64_t JniJson::hash(const std::string &s)
{
    uint64_t hash = 5381;
    const char *str = s.c_str();

    if (nullptr != str)
    {
        int c;

        while (c = *str++)
            hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }

    return hash;
}

