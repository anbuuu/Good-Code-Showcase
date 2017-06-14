package au.com.mi9.ninenow.api;

import android.content.Context;
import android.text.TextUtils;

import com.github.kevinsawicki.http.HttpRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.json.JSONObject;

import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import au.com.mi9.jumpin.app.BuildConfig;
import au.com.mi9.jumpin.app.MI9Application;
import au.com.mi9.ninenow.db.MI9Authenticator;
import au.com.mi9.ninenow.db.MI9DbHelper;
import au.com.mi9.ninenow.db.SettingName;
import au.com.mi9.ninenow.models.MI9GlobalConfig;
import au.com.mi9.ninenow.models.MI9ModelRefreshToken;
import au.com.mi9.ninenow.models.catalog.MI9CatalogEpisode;
import au.com.mi9.ninenow.models.catalog.MI9CatalogLastResponse;
import au.com.mi9.ninenow.utils.MI9Log;
import au.com.mi9.ninenow.utils.MI9NetworkUtils;

/**
 * Created by anbu.subramanian on 19/11/2015.
 */
public class MI9ApiManager
{
    private static final String TAG = MI9ApiManager.class.getSimpleName();

    public static final String PARAM_TOKEN = "token";

    public static final String PARAM_DEVICE = "device";
    public static final String ID_DEVICE = "android";

    public static final String PARAM_CLIENT = "client_id";

    public static final String PARAM_TAKE = "take";
    public static final String VAL_TAKE = "99999";

    private static MI9ApiManager INSTANCE = null;

    ThreadPoolExecutor mThreadPool = new ThreadPoolExecutor(
            16, 16, Long.MAX_VALUE, TimeUnit.NANOSECONDS,
            new LinkedBlockingQueue<Runnable>(), new ThreadPoolExecutor.AbortPolicy());

    final JniJson mJsonParser = new JniJson();

    public static MI9ApiManager getInstance()
    {
        if (null == INSTANCE)
        {
            INSTANCE = new MI9ApiManager();
        }

        return INSTANCE;
    }

    private MI9ApiManager()
    {
        mJsonParser.dbOpen(MI9DbHelper.getDbPath(MI9Application.getInstance().getApplicationContext()));
        mThreadPool.prestartAllCoreThreads();
    }

    private static String getApiSafely(Context context, String baseUrl, String endpoint, Object... urlParams)
    {
        try
        {
            return getApi(context, baseUrl, endpoint, urlParams);
        }
        catch (Exception e)
        {
            MI9Log.e(TAG, "Error occurred during getApi.", e);
        }
        return null;
    }

    private static String getApi(Context context, String baseUrl, String endpoint, Object... urlParams)
    {
        String url = baseUrl + endpoint;

        // Determines whether we should prevent the request from firing.
        if (!shouldCallApi(context))
        {
            MI9Log.d(TAG, "getApi was not called for url: " + url);
            return null;
        }

        HttpRequest request;
        String response = null;
        long timer = System.currentTimeMillis();

        MI9CatalogLastResponse lastResponse = MI9Application.getDb().getModel(
                MI9CatalogLastResponse.class, String.format("mEndpoint MATCH '%s'", endpoint),
                null, false, false);

        request = HttpRequest.get(url, true, urlParams);
        request.acceptGzipEncoding().uncompress(true);

        if (null != lastResponse)
        {
            request.ifNoneMatch(lastResponse.mEtag);
            request.ifModifiedSince(lastResponse.mLastResponse);
        }
        else
        {
            lastResponse = new MI9CatalogLastResponse(endpoint, timer);
        }

       MI9Log.d(TAG, request.toString());

        switch (request.code())
        {
            case HttpURLConnection.HTTP_OK:
            {
                response = request.body();
                lastResponse.mLastResponse = System.currentTimeMillis();
                lastResponse.mEtag = request.eTag();
                MI9Application.getDb().saveModel(lastResponse, String.format("mEndpoint MATCH '%s'", endpoint));
                MI9Log.d(TAG, String.format("Get API '%s' - took:%d ms", endpoint, (System.currentTimeMillis() - timer)));
            }
            break;

            case HttpURLConnection.HTTP_NOT_MODIFIED:
                break;

            default:
                MI9Log.e(TAG, String.format("Get API '%s' - failed", endpoint));
                break;
        }

        return response;
    }

    private static String getWebApiSafely(Context context, String baseUrl, String endpoint, String cookies, Object... urlParams)
    {
        try
        {
            return getWebApi(context, baseUrl, endpoint, cookies, urlParams);
        }
        catch (Exception e)
        {
           MI9Log.e(TAG, "Error occurred during getApi.", e);
        }
        return null;
    }

    private static String getWebApi(Context context, String baseUrl, String endpoint, String cookies, Object... urlParams)
    {
        String url = baseUrl + endpoint;

        // Determines whether we should prevent the request from firing.
        if (!shouldCallApi(context))
        {
           MI9Log.d(TAG, "getApi was not called for url: " + url);
            return null;
        }

        HttpRequest request;
        String response = null;
        long timer = System.currentTimeMillis();

        request = HttpRequest.get(url, true, urlParams);
        request.acceptGzipEncoding().uncompress(true);
        request.acceptJson();
        request.header("Cookie", cookies);

       MI9Log.d(TAG, request.toString());

        switch (request.code())
        {
            case HttpURLConnection.HTTP_OK:
            {
                response = request.body();
                MI9Log.d(TAG, String.format("Get API '%s' - took:%d ms", endpoint, (System.currentTimeMillis() - timer)));
            }
            break;

            case HttpURLConnection.HTTP_NOT_MODIFIED:
                break;

            default:
                MI9Log.e(TAG, String.format("Get API '%s' - failed", endpoint));
                break;
        }

        return response;
    }

    private static void postApiSafely(Context context, String baseUrl, String endpoint, JSONObject data, Object... urlParams)
    {
        try
        {
            postApi(context, baseUrl, endpoint, data, urlParams);
        }
        catch (Exception e)
        {
           MI9Log.e(TAG, "Error occurred during postApi.", e);
        }
    }

    private static void postApi(Context context, String baseUrl, String endpoint, JSONObject data, Object... urlParams)
    {
        String url = baseUrl + endpoint;

        // Determines whether we should prevent the request from firing.
        if (!shouldCallApi(context))
        {
           MI9Log.d(TAG, "postApi was not called for url: " + url);
            return;
        }

        HttpRequest request;
        long timer = System.currentTimeMillis();

        request = HttpRequest.post(url, true, urlParams);

        if (null != data)
        {
            request.send(data.toString());
        }

       MI9Log.d(TAG, request.toString());

        switch (request.code())
        {
            case HttpURLConnection.HTTP_OK:
            case HttpURLConnection.HTTP_CREATED:
            {
                String response = request.body();
                MI9Log.d(TAG, String.format("Post API '%s' - took:%d ms - %s", endpoint, (System.currentTimeMillis() - timer), response));
            }
            break;

            default:
            {
                String response = request.body();
                MI9Log.e(TAG, String.format("Post API '%s' - failed - %s", endpoint, response));
            }
            break;
        }
    }

    private static void deleteApiSafely(Context context, String baseUrl, String endpoint, JSONObject data, Object... urlParams)
    {
        try
        {
            deleteApi(context, baseUrl, endpoint, data, urlParams);
        }
        catch (Exception e)
        {
           MI9Log.e(TAG, "Error occurred during deleteApi.", e);
        }
    }

    private static void deleteApi(Context context, String baseUrl, String endpoint, JSONObject data, Object... urlParams)
    {
        String url = baseUrl + endpoint;

        // Determines whether we should prevent the request from firing.
        if (!shouldCallApi(context))
        {
           MI9Log.d(TAG, "deleteApi was not called for url: " + url);
            return;
        }

        HttpRequest request;
        long timer = System.currentTimeMillis();

        request = HttpRequest.delete(url, true, urlParams);

        if (null != data)
        {
            request.send(data.toString());
        }

       MI9Log.d(TAG, request.toString());

        if (request.ok())
        {
            MI9Log.d(TAG, String.format("Delete API '%s' - took:%d ms", endpoint, (System.currentTimeMillis() - timer)));
        }
        else
        {
            MI9Log.e(TAG, String.format("Delete API '%s' - failed", endpoint));
            String response = request.body();
            MI9Log.e(TAG, String.format("Post API '%s' - failed - %s", endpoint, response));
        }
    }

    public static String getApiHome(Context context)
    {
        return getApiSafely(context, MI9GlobalConfig.URL_TVAPI, "/pages/home", PARAM_DEVICE, ID_DEVICE);
    }

    public void parseApiHome(Context context)
    {
        try
        {
            String response = MI9ApiManager.getApiHome(context);

            if (!TextUtils.isEmpty(response))
            {
                long timer = System.currentTimeMillis();

                mJsonParser.parseHome(response);

                MI9Log.d(TAG, String.format("onPerformSync - native parse home - took:%d ms", (System.currentTimeMillis() - timer)));
            }
        }
        catch (Throwable e)
        {
            MI9Log.e(TAG, "Exception while parsing home", e);
        }
    }

    public static String getApiShowPage(Context context, String url) {
        return getApiSafely(context, MI9GlobalConfig.URL_TVAPI, url, PARAM_DEVICE, ID_DEVICE);
    }

    /**
     * Two requests happen here, one to get and parse the show page data, and another to get and parse the clips tags
     */
    public void parseApiShowPage(Context context, final String url, final String showId, final String seasonId)
    {
        // Safeguard against memory leaks.
        final Context applicationContext = context.getApplicationContext();

        mThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                //Get and parse show page data
                try
                {
                    String response = MI9ApiManager.getApiShowPage(applicationContext, url);
                    long timer = System.currentTimeMillis();
                    mJsonParser.parsePage(url, showId, seasonId, response);
                    MI9Log.d(TAG, String.format("onPerformSync - native parse shows - took:%d ms", (System.currentTimeMillis() - timer)));
                }
                catch(Exception e)
                {
                    MI9Log.e(TAG, "Exception while parsing show page", e);
                }
            }
        });
    }

    //TODO: remove once show/season page api is updated to include tags.
    public static String getApiShowClipsPage(Context context, String url)
    {
        return getApiSafely(context, MI9GlobalConfig.URL_TVAPI, url+"/clips", PARAM_DEVICE, ID_DEVICE);
    }

    public static String getApiShows(Context context)
    {
        return getApiSafely(context, MI9GlobalConfig.URL_TVAPI, "/tv-series", PARAM_DEVICE, ID_DEVICE, PARAM_TAKE, VAL_TAKE);
    }

    public void parseApiShows(Context context)
    {
        // Safeguard against memory leaks.
        final Context applicationContext = context.getApplicationContext();

        mThreadPool.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    String response = MI9ApiManager.getApiShows(applicationContext);

                    long timer = System.currentTimeMillis();

                    mJsonParser.parseShows(response);

                    MI9Log.d(TAG, String.format("onPerformSync - native parse shows - took:%d ms", (System.currentTimeMillis() - timer)));
                }
                catch (Exception e)
                {
                    MI9Log.e(TAG, "Exception while parsing shows", e);
                }
            }
        });
    }

    public static String getApiSeasons(Context context)
    {
        return getApiSafely(context, MI9GlobalConfig.URL_TVAPI, "/seasons", PARAM_DEVICE, ID_DEVICE, PARAM_TAKE, VAL_TAKE);
    }

    public void parseApiSeasons(Context context)
    {
        // Safeguard against memory leaks.
        final Context applicationContext = context.getApplicationContext();

        mThreadPool.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    String response = MI9ApiManager.getApiSeasons(applicationContext);

                    long timer = System.currentTimeMillis();

                    mJsonParser.parseSeasons(response);

                    MI9Log.d(TAG, String.format("onPerformSync - native parse seasons - took:%d ms", (System.currentTimeMillis() - timer)));
                }
                catch (Exception e)
                {
                    MI9Log.e(TAG, "Exception while parsing seasons", e);
                }
            }
        });
    }

    public static String getApiEpisodes(Context context)
    {
        return getApiSafely(context, MI9GlobalConfig.URL_TVAPI, "/episodes", PARAM_DEVICE, ID_DEVICE, PARAM_TAKE, VAL_TAKE);
    }

    public void parseApiEpisodes(Context context)
    {
        // Safeguard against memory leaks.
        final Context applicationContext = context.getApplicationContext();

        mThreadPool.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    String response = MI9ApiManager.getApiEpisodes(applicationContext);

                    long timer = System.currentTimeMillis();

                    mJsonParser.parseEpisodes(response);

                    MI9Log.d(TAG, String.format("onPerformSync - native parse episodes - took:%d ms", (System.currentTimeMillis() - timer)));
                }
                catch (Exception e)
                {
                    MI9Log.e(TAG, "Exception while parsing episodes", e);
                }
            }
        });
    }

    public static String getApiClips(Context context)
    {
        return getApiSafely(context, MI9GlobalConfig.URL_TVAPI, "/clips", PARAM_DEVICE, ID_DEVICE, PARAM_TAKE, VAL_TAKE);
    }

    public void parseApiClips(Context context)
    {
        // Safeguard against memory leaks.
        final Context applicationContext = context.getApplicationContext();

        mThreadPool.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    String response = MI9ApiManager.getApiClips(applicationContext);

                    long timer = System.currentTimeMillis();

                    mJsonParser.parseClips(response);

                    MI9Log.d(TAG, String.format("onPerformSync - native parse clips - took:%d ms", (System.currentTimeMillis() - timer)));
                }
                catch (Exception e)
                {
                    MI9Log.e(TAG, "Exception while parsing clips", e);
                }
            }
        });
    }

    public static String getApiGenres(Context context)
    {
        return getApiSafely(context, MI9GlobalConfig.URL_TVAPI, "/genres", PARAM_DEVICE, ID_DEVICE, PARAM_TAKE, VAL_TAKE);
    }

    public void parseApiGenres(Context context)
    {
        // Safeguard against memory leaks.
        final Context applicationContext = context.getApplicationContext();

        mThreadPool.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    String response = MI9ApiManager.getApiGenres(applicationContext);

                    long timer = System.currentTimeMillis();

                    mJsonParser.parseGenres(response);

                    MI9Log.d(TAG, String.format("onPerformSync - native parse genres - took:%d ms", (System.currentTimeMillis() - timer)));
                }
                catch (Exception e)
                {
                    MI9Log.e(TAG, "Exception while parsing genres", e);
                }
            }
        });
    }

    public static String getApiConfig(Context context)
    {
        return getApiSafely(context, MI9GlobalConfig.URL_TVAPI, "/config/android/config.json", PARAM_DEVICE, ID_DEVICE);
    }

    public void parseApiConfig(Context context)
    {
        // Safeguard against memory leaks.
        final Context applicationContext = context.getApplicationContext();

        mThreadPool.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    String response = MI9ApiManager.getApiGenres(applicationContext);

                    long timer = System.currentTimeMillis();

                    mJsonParser.parseGenres(response);

                    MI9Log.d(TAG, String.format("onPerformSync - native parse genres - took:%d ms", (System.currentTimeMillis() - timer)));
                }
                catch (Exception e)
                {
                    MI9Log.e(TAG, "Exception while parsing Api Config", e);
                }
            }
        });
    }

    public static String getApiUser(Context context, String authToken)
    {
        return getApiSafely(context, MI9GlobalConfig.URL_USERAPI, "/user", PARAM_TOKEN, authToken, PARAM_CLIENT, MI9GlobalConfig.ID_CLIENT);
    }

    public static String getApiVideoStatus(Context context, String authToken)
    {
        String response = getApiSafely(context, MI9GlobalConfig.URL_USERAPI, "/video-status", PARAM_TOKEN, authToken, PARAM_CLIENT, MI9GlobalConfig.ID_CLIENT);
        MI9Log.d(TAG, String.format("getApiVideoStatus '%s'", response));
        return response;
    }

    public void parseApiVideoStatus(Context context)
    {
        // Safeguard against memory leaks.
        final Context applicationContext = context.getApplicationContext();

        mThreadPool.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    String response = MI9ApiManager.getApiVideoStatus(applicationContext, MI9Authenticator.getAuthToken(applicationContext));

                    long timer = System.currentTimeMillis();

                    mJsonParser.parseVideoStatus(response);

                    MI9Log.d(TAG, String.format("onPerformSync - native parse video status - took:%d ms", (System.currentTimeMillis() - timer)));
                }
                catch (Exception e)
                {
                    MI9Log.e(TAG, "Exception while parsing Api Video Status", e);
                }
            }
        });
    }

    public static String getApiLive(Context context)
    {
        return getApiSafely(context, MI9GlobalConfig.URL_TVAPI, "/pages/livestreams", PARAM_DEVICE, ID_DEVICE);
    }

    public void parseApiLive(Context context)
    {
        // Safeguard against memory leaks.
        final Context applicationContext = context.getApplicationContext();

        mThreadPool.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    String response = MI9ApiManager.getApiLive(applicationContext);

                    long timer = System.currentTimeMillis();

                    mJsonParser.parseLive(response);

                    MI9Log.d(TAG, String.format("onPerformSync - native parse live - took:%d ms", (System.currentTimeMillis() - timer)));
                }
                catch (Exception e)
                {
                    MI9Log.e(TAG, "Exception while parsing Api Live", e);
                }
            }
        });
    }

    public void refreshToken(final Context context, final String refreshToken)
    {
        // Safeguard against memory leaks.
        final Context applicationContext = context.getApplicationContext();

        mThreadPool.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    String refreshTokenCookie = String.format("%s=%s;",
                            BuildConfig.SSO_REFRESH_TOKEN,
                            refreshToken
                            );

                    String response = MI9ApiManager.getWebApiSafely(
                            applicationContext,
                            MI9GlobalConfig.URL_SSO_LOGIN,
                            "/api/login", refreshTokenCookie,
                            PARAM_CLIENT, MI9GlobalConfig.ID_CLIENT);

                    long timer = System.currentTimeMillis();

                    GsonBuilder gsonBuilder = new GsonBuilder();
                    Gson gson = gsonBuilder.create();
                    MI9ModelRefreshToken refreshTokenResponse = gson.fromJson(response, MI9ModelRefreshToken.class);

                    if (
                        (null != refreshTokenResponse) &&
                        (!TextUtils.isEmpty(refreshTokenResponse.access_token)) &&
                        (!TextUtils.isEmpty(refreshTokenResponse.ttl)) )
                    {
                        try
                        {
                            DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
                            DateTime dt = fmt.parseDateTime(refreshTokenResponse.ttl);
                            long interval = System.currentTimeMillis() - dt.getMillis();
                            if (interval > 0)
                            {
                                MI9Application.getDb().putSetting(
                                        SettingName.Setting_TokenRefreshPeriod,
                                        interval);
                            }
                        }
                        catch (Exception e)
                        {
                            MI9Log.e(TAG, "Error parsing TTL", e);
                        }

                        MI9Authenticator.updateUserData(context,
                                MI9Authenticator.UserDataKeys.REFRESH_TOKEN,
                                refreshTokenResponse.access_token);
                    }

                    MI9Log.d(TAG, String.format("onPerformSync - update refresh token - took:%d ms", (System.currentTimeMillis() - timer)));
                }
                catch (Exception e)
                {
                    MI9Log.e(TAG, "Exception while refreshing token", e);
                }
            }
        });
    }

    public static void savePlaybackPosition(Context context, final MI9CatalogEpisode episode, final String authToken)
    {
        // Safeguard against memory leaks.
        final Context applicationContext = context.getApplicationContext();

        if ((null != episode) && (null != episode.season))
        {
            Map<String, Object> data = new HashMap<>();
            //TODO: convert to seconds if required by API.
            data.put("progressTime", episode.mResumePosition);
            data.put("seasonId", episode.season.getId());
            final JSONObject obj = new JSONObject(data);

            Thread thread = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    postApiSafely(applicationContext, MI9GlobalConfig.URL_USERAPI, String.format("/video-status/%s", episode.getId()),
                            obj, PARAM_TOKEN, authToken, PARAM_CLIENT, MI9GlobalConfig.ID_CLIENT);
                }
            });

            thread.start();

        }
    }

    public static void deleteWatchHistoryForSeason(Context context, String seasonId, String authToken)
    {
        final Context applicationContext = context.getApplicationContext();

        deleteApiSafely(applicationContext, MI9GlobalConfig.URL_USERAPI, String.format("/watch-history/%s", seasonId),
                null, PARAM_TOKEN, authToken, PARAM_CLIENT, MI9GlobalConfig.ID_CLIENT);
    }

    /**
     * Determines whether the api should be called.
     *
     * At the moment, the only reason not to call the service is if the Internet is not available.
     * @return true if an api can be called.
     */
    private static boolean shouldCallApi(Context context)
    {

        boolean callApi = context != null && MI9NetworkUtils.isNetworkAvailable(context);
        if (!callApi)
        {
           MI9Log.d(TAG, "shouldCallApi. Network not available");
        }
        return callApi;
    }
}
