package net.thegreshams.firebase4j.service;

import mou.com.promises.Promise;
import mou.com.promises.PromiseError;
import net.thegreshams.firebase4j.error.FirebaseException;
import net.thegreshams.firebase4j.error.JacksonUtilityException;
import net.thegreshams.firebase4j.model.FirebaseResponse;
import net.thegreshams.firebase4j.util.JacksonUtility;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Extensible version of bane73/firebase4j/services/Firebase.java
 */
public class Firebase {

    public static final String FIREBASE_API_JSON_EXTENSION
            = ".json";
    protected static final Logger LOGGER = Logger.getRootLogger();


    ///////////////////////////////////////////////////////////////////////////////
//
// PROPERTIES & CONSTRUCTORS
//
///////////////////////////////////////////////////////////////////////////////
    protected final String baseUrl;
    protected final CloseableHttpClient client;
    private String secureToken = null;
    private Boolean useJsonExt = true;
    private final ExecutorService executor;

    public Firebase(String baseUrl, int threadPoolSize) throws FirebaseException {
        if (threadPoolSize <= 0) {
            this.executor = null;
        } else this.executor = Executors.newFixedThreadPool(threadPoolSize);

        if (baseUrl == null || baseUrl.trim().isEmpty()) {
            String msg = "baseUrl cannot be null or empty; was: '" + baseUrl + "'";
            LOGGER.error(msg);
            throw new FirebaseException(msg);
        }
        this.baseUrl = baseUrl.trim();
        LOGGER.info("intialized with base-url: " + this.baseUrl);
        client = HttpClients.createDefault();

    }

    /**
     * Overloaded constructor for cases where you need to prevent adding the json extension to the url.
     *
     * @param baseUrl
     * @param useJsonExtension include the json extension to the URL?
     * @throws FirebaseException
     */
    public Firebase(String baseUrl, Boolean useJsonExtension, int threadPoolSize) throws FirebaseException {
        this(baseUrl, threadPoolSize);
        useJsonExt = useJsonExtension;
    }

    public Firebase(String baseUrl, String secureToken, int threadPoolSize) throws FirebaseException {
        if (threadPoolSize <= 0) {
            this.executor = null;
        } else this.executor = Executors.newFixedThreadPool(threadPoolSize);
        if (baseUrl == null || baseUrl.trim().isEmpty()) {
            String msg = "baseUrl cannot be null or empty; was: '" + baseUrl + "'";
            LOGGER.error(msg);
            throw new FirebaseException(msg);
        }
        this.secureToken = secureToken;
        this.baseUrl = baseUrl.trim();
        LOGGER.info("intialized with base-url: " + this.baseUrl);
        client = HttpClients.createDefault();
    }


///////////////////////////////////////////////////////////////////////////////
//
// PUBLIC API
//
///////////////////////////////////////////////////////////////////////////////

    public static NameValuePair query(String name, String value) {
        return new BasicNameValuePair(name, value);
    }

    /**
     * GETs data from the base-url.
     *
     * @return {@link FirebaseResponse}
     * @throws {@link FirebaseException}
     */
    public FirebaseResponse get() throws FirebaseException {
        return this.get(null);
    }

    /**
     * GETs data from the provided-path relative to the base-url.
     *
     * @param path -- if null/empty, refers to the base-url
     * @return {@link FirebaseResponse}
     * @throws {@link FirebaseException}
     */
    public FirebaseResponse get(String path, NameValuePair... queries) throws FirebaseException {

        // make the request
        String url = this.buildFullUrlFromRelativePath(path, queries);
        HttpGet request = new HttpGet(url);
        HttpResponse httpResponse = this.makeRequest(request);

        // process the response
        FirebaseResponse response = this.processResponse(FirebaseRestMethod.GET, httpResponse);

        return response;
    }

    /**
     * PATCHs data to the base-url
     *
     * @param data -- can be null/empty
     * @return
     * @throws {@link FirebaseException}
     * @throws {@link JacksonUtilityException}
     */

    public FirebaseResponse patch(Map<String, Object> data) throws FirebaseException, JacksonUtilityException {
        return this.patch(null, data);
    }

    /**
     * PATCHs data on the provided-path relative to the base-url.
     *
     * @param path -- if null/empty, refers to the base-url
     * @param data -- can be null/empty
     * @return {@link FirebaseResponse}
     * @throws {@link FirebaseException}
     * @throws {@link JacksonUtilityException}
     */

    public FirebaseResponse patch(String path, Map<String, Object> data, NameValuePair... queries) throws FirebaseException, JacksonUtilityException {
        // make the request
        String url = this.buildFullUrlFromRelativePath(path, queries);
        //HttpPut request = new HttpPut( url );
        HttpPatch request = new HttpPatch(url);
        request.setEntity(this.buildEntityFromDataMap(data));
        HttpResponse httpResponse = this.makeRequest(request);

        // process the response
        FirebaseResponse response = this.processResponse(FirebaseRestMethod.PATCH, httpResponse);

        return response;
    }

    /**
     * @param jsonData
     * @return
     */

    public FirebaseResponse patch(String jsonData) throws FirebaseException {
        return this.patch(null, jsonData);
    }

    /**
     * @param path
     * @param jsonData
     * @return
     * @throws FirebaseException
     */

    public FirebaseResponse patch(String path, String jsonData, NameValuePair... queries) throws FirebaseException {
        // make the request
        String url = this.buildFullUrlFromRelativePath(path, queries);
        HttpPatch request = new HttpPatch(url);
        request.setEntity(this.buildEntityFromJsonData(jsonData));
        HttpResponse httpResponse = this.makeRequest(request);
        // process the response
        return this.processResponse(FirebaseRestMethod.PATCH, httpResponse);
    }

    /**
     * PUTs data to the base-url (ie: creates or overwrites).
     * If there is already data at the base-url, this data overwrites it.
     * If data is null/empty, any data existing at the base-url is deleted.
     *
     * @param data -- can be null/empty
     * @return {@link FirebaseResponse}
     * @throws {@link JacksonUtilityException}
     * @throws {@link FirebaseException}
     */
    public FirebaseResponse put(Map<String, Object> data) throws JacksonUtilityException, FirebaseException {
        return this.put(null, data);
    }

    /**
     * PUTs data to the provided-path relative to the base-url (ie: creates or overwrites).
     * If there is already data at the path, this data overwrites it.
     * If data is null/empty, any data existing at the path is deleted.
     *
     * @param path -- if null/empty, refers to base-url
     * @param data -- can be null/empty
     * @return {@link FirebaseResponse}
     * @throws {@link JacksonUtilityException}
     * @throws {@link FirebaseException}
     */
    public FirebaseResponse put(String path, Map<String, Object> data, NameValuePair... queries) throws JacksonUtilityException, FirebaseException {

        // make the request
        String url = this.buildFullUrlFromRelativePath(path, queries);
        HttpPut request = new HttpPut(url);
        request.setEntity(this.buildEntityFromDataMap(data));
        HttpResponse httpResponse = this.makeRequest(request);

        // process the response
        FirebaseResponse response = this.processResponse(FirebaseRestMethod.PUT, httpResponse);

        return response;
    }

    /**
     * PUTs data to the provided-path relative to the base-url (ie: creates or overwrites).
     * If there is already data at the path, this data overwrites it.
     * If data is null/empty, any data existing at the path is deleted.
     *
     * @param jsonData -- can be null/empty
     * @return {@link FirebaseResponse}
     * @throws {@link FirebaseException}
     */
    public FirebaseResponse put(String jsonData) throws FirebaseException {
        return this.put(null, jsonData);
    }

    /**
     * PUTs data to the provided-path relative to the base-url (ie: creates or overwrites).
     * If there is already data at the path, this data overwrites it.
     * If data is null/empty, any data existing at the path is deleted.
     *
     * @param path     -- if null/empty, refers to base-url
     * @param jsonData -- can be null/empty
     * @return {@link FirebaseResponse}
     * @throws {@link FirebaseException}
     */
    public FirebaseResponse put(String path, String jsonData, NameValuePair... queries) throws FirebaseException {

        // make the request
        String url = this.buildFullUrlFromRelativePath(path, queries);
        HttpPut request = new HttpPut(url);
        request.setEntity(this.buildEntityFromJsonData(jsonData));
        HttpResponse httpResponse = this.makeRequest(request);

        // process the response
        FirebaseResponse response = this.processResponse(FirebaseRestMethod.PUT, httpResponse);

        return response;
    }

    /**
     * POSTs data to the base-url (ie: creates).
     * <p>
     * NOTE: the Firebase API does not treat this method in the conventional way, but instead defines it
     * as 'PUSH'; the API will insert this data under the base-url but associated with a Firebase-
     * generated key; thus, every use of this method will result in a new insert even if the data already
     * exists.
     *
     * @param data -- can be null/empty but will result in no data being POSTed
     * @return {@link FirebaseResponse}
     * @throws {@link JacksonUtilityException}
     * @throws {@link FirebaseException}
     */
    public FirebaseResponse post(Map<String, Object> data) throws JacksonUtilityException, FirebaseException {
        return this.post(null, data);
    }

    /**
     * POSTs data to the provided-path relative to the base-url (ie: creates).
     * <p>
     * NOTE: the Firebase API does not treat this method in the conventional way, but instead defines it
     * as 'PUSH'; the API will insert this data under the provided path but associated with a Firebase-
     * generated key; thus, every use of this method will result in a new insert even if the provided path
     * and data already exist.
     *
     * @param path -- if null/empty, refers to base-url
     * @param data -- can be null/empty but will result in no data being POSTed
     * @return {@link FirebaseResponse}
     * @throws {@link JacksonUtilityException}
     * @throws {@link FirebaseException}
     */
    public FirebaseResponse post(String path, Map<String, Object> data, NameValuePair... queries) throws JacksonUtilityException, FirebaseException {

        // make the request
        String url = this.buildFullUrlFromRelativePath(path, queries);
        HttpPost request = new HttpPost(url);
        request.setEntity(this.buildEntityFromDataMap(data));
        HttpResponse httpResponse = this.makeRequest(request);

        // process the response

        return this.processResponse(FirebaseRestMethod.POST, httpResponse);
    }

    /**
     * POSTs data to the base-url (ie: creates).
     * <p>
     * NOTE: the Firebase API does not treat this method in the conventional way, but instead defines it
     * as 'PUSH'; the API will insert this data under the base-url but associated with a Firebase-
     * generated key; thus, every use of this method will result in a new insert even if the provided data
     * already exists.
     *
     * @param jsonData -- can be null/empty but will result in no data being POSTed
     * @return {@link FirebaseResponse}
     * @throws {@link FirebaseException}
     */
    public FirebaseResponse post(String jsonData) throws FirebaseException {
        return this.post(null, jsonData);
    }

    /**
     * POSTs data to the provided-path relative to the base-url (ie: creates).
     * <p>
     * NOTE: the Firebase API does not treat this method in the conventional way, but instead defines it
     * as 'PUSH'; the API will insert this data under the provided path but associated with a Firebase-
     * generated key; thus, every use of this method will result in a new insert even if the provided path
     * and data already exist.
     *
     * @param path     -- if null/empty, refers to base-url
     * @param jsonData -- can be null/empty but will result in no data being POSTed
     * @return {@link FirebaseResponse}
     * @throws {@link FirebaseException}
     */
    public FirebaseResponse post(String path, String jsonData, NameValuePair... queries) throws FirebaseException {

        // make the request
        String url = this.buildFullUrlFromRelativePath(path, queries);
        HttpPost request = new HttpPost(url);
        request.setEntity(this.buildEntityFromJsonData(jsonData));
        HttpResponse httpResponse = this.makeRequest(request);

        // process the response
        FirebaseResponse response = this.processResponse(FirebaseRestMethod.POST, httpResponse);

        return response;
    }

    /**
     * DELETEs data from the base-url.
     *
     * @return {@link FirebaseResponse}
     * @throws {@link FirebaseException}
     */
    public FirebaseResponse delete() throws FirebaseException {
        return this.delete(null);
    }


///////////////////////////////////////////////////////////////////////////////
//
// PRIVATE API
//
///////////////////////////////////////////////////////////////////////////////

    /**
     * DELETEs data from the provided-path relative to the base-url.
     *
     * @param path -- if null/empty, refers to the base-url
     * @return {@link FirebaseResponse}
     * @throws {@link FirebaseException}
     */
    public FirebaseResponse delete(String path, NameValuePair... queries) throws FirebaseException {

        // make the request
        String url = this.buildFullUrlFromRelativePath(path, queries);
        HttpDelete request = new HttpDelete(url);
        HttpResponse httpResponse = this.makeRequest(request);

        // process the response
        FirebaseResponse response = this.processResponse(FirebaseRestMethod.DELETE, httpResponse);

        return response;
    }

    private StringEntity buildEntityFromDataMap(Map<String, Object> dataMap) throws FirebaseException, JacksonUtilityException {

        String jsonData = JacksonUtility.GET_JSON_STRING_FROM_MAP(dataMap);

        return this.buildEntityFromJsonData(jsonData);
    }

    private StringEntity buildEntityFromJsonData(String jsonData) throws FirebaseException {

        StringEntity result = null;
        try {

            result = new StringEntity(jsonData, "UTF-8");

        } catch (Throwable t) {

            String msg = "unable to create entity from data; data was: " + jsonData;
            LOGGER.error(msg);
            throw new FirebaseException(msg, t);

        }

        return result;
    }

    protected String buildFullUrlFromRelativePath(String path, NameValuePair[] queries) {

        // massage the path (whether it's null, empty, or not) into a full URL
        if (path == null) {
            path = "";
        }
        path = path.trim();
        if (!path.isEmpty() && !path.startsWith("/")) {
            path = "/" + path;
        }

        String url = this.baseUrl + path;

        if (useJsonExt) url += Firebase.FIREBASE_API_JSON_EXTENSION;

        if (queries.length > 0) {
            StringBuilder builder = new StringBuilder(url);
            builder.append('?');
            for (NameValuePair e : queries) {
                builder.append(e.getName()).append('=');
                builder.append(URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
                builder.append('&');
            }
            if (secureToken != null) {
                builder.append("access_token=").append(secureToken);
            } else {
                builder.setLength(builder.length() - 1);
            }
            url = builder.toString();
        } else if (secureToken != null) {
            url += "?access_token=" + secureToken;
        }

        LOGGER.info("built full url to '" + url + "' using relative-path of '" + path + "'");

        return url;
    }

    private HttpResponse makeRequest(HttpRequestBase request) throws FirebaseException {

        HttpResponse response = null;

        // sanity-check
        if (request == null) {

            String msg = "request cannot be null";
            LOGGER.error(msg);
            throw new FirebaseException(msg);
        }

        try {
            response = client.execute(request);
        } catch (Throwable t) {

            String msg = "unable to receive response from request(" + request.getMethod() + ") @ " + request.getURI();
            LOGGER.error(msg);
            throw new FirebaseException(msg, t);
        }
        return response;
    }


///////////////////////////////////////////////////////////////////////////////
//
// INTERNAL CLASSES
//
///////////////////////////////////////////////////////////////////////////////

    private FirebaseResponse processResponse(FirebaseRestMethod method, HttpResponse httpResponse) throws FirebaseException {

        FirebaseResponse response = null;

        // sanity-checks
        if (method == null) {

            String msg = "method cannot be null";
            LOGGER.error(msg);
            throw new FirebaseException(msg);
        }
        if (httpResponse == null) {

            String msg = "httpResponse cannot be null";
            LOGGER.error(msg);
            throw new FirebaseException(msg);
        }

        // get the response-entity
        HttpEntity entity = httpResponse.getEntity();

        // get the response-code
        int code = httpResponse.getStatusLine().getStatusCode();

        // set the response-success
        boolean success = false;
        switch (method) {
            case DELETE:
                if (httpResponse.getStatusLine().getStatusCode() == 204
                        && "No Content".equalsIgnoreCase(httpResponse.getStatusLine().getReasonPhrase())) {
                    success = true;
                }
                break;
            case PATCH:
            case PUT:
            case POST:
            case GET:
                if (httpResponse.getStatusLine().getStatusCode() == 200
                        && "OK".equalsIgnoreCase(httpResponse.getStatusLine().getReasonPhrase())) {
                    success = true;
                }
                break;
            default:
                break;

        }

        // get the response-body
        Writer writer = new StringWriter();
        if (entity != null) {

            try {

                InputStream is = entity.getContent();
                char[] buffer = new char[1024];
                Reader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                int n;
                while ((n = reader.read(buffer)) != -1) {
                    writer.write(buffer, 0, n);
                }

            } catch (Throwable t) {

                String msg = "unable to read response-content; read up to this point: '" + writer.toString() + "'";
                writer = new StringWriter(); // don't want to later give jackson partial JSON it might choke on
                LOGGER.error(msg);
                throw new FirebaseException(msg, t);

            }
        }

        // convert response-body to map
        Map<String, Object> body = null;
        try {

            body = JacksonUtility.GET_JSON_STRING_AS_MAP(writer.toString());

        } catch (JacksonUtilityException jue) {

            String msg = "unable to convert response-body into map; response-body was: '" + writer.toString() + "'";
            LOGGER.error(msg);
            throw new FirebaseException(msg, jue);
        }

        // build the response
        response = new FirebaseResponse(success, code, body, writer.toString());


        return response;
    }
////////////////////////////////////////////////////
/////////////////// NEW FEATURES ///////////////////
////////////////////////////////////////////////////

    /**
     * @param path
     * @return promise of firebase response, the promise error is always a throwable
     */
    public Promise<FirebaseResponse> getAsync(String path, NameValuePair... queries) {
        if (executor == null) throw new RuntimeException("There is no thread pool!");
        return new Promise<FirebaseResponse>((resolver, exceptionHandler) -> executor.execute(() -> {
            try {
                FirebaseResponse result = get(path, queries);
                resolver.run(result);
            } catch (Throwable e) {
                if (exceptionHandler == null) throw new RuntimeException(e);
                exceptionHandler.handle(new PromiseError(e));
            }
        }));
    }

    public Promise<FirebaseResponse> patchAsync(String path, Map<String, Object> data, NameValuePair... queries) {
        if (executor == null) throw new RuntimeException("There is no thread pool!");

        return new Promise<>((resolve, exceptionHandler) -> executor.execute(() -> {
            try {
                FirebaseResponse result = patch(path, data, queries);
                resolve.run(result);
            } catch (Throwable e) {
                if (exceptionHandler == null) throw new RuntimeException(e);
                exceptionHandler.handle(new PromiseError(e));
            }
        }));
    }

    public Promise<FirebaseResponse> putAsync(String path, Map<String, Object> data, NameValuePair... queries) {
        if (executor == null) throw new RuntimeException("There is no thread pool!");

        return new Promise<>((resolve, exceptionHandler) -> executor.execute(() -> {
            try {
                FirebaseResponse result = put(path, data, queries);
                resolve.run(result);
            } catch (Throwable e) {
                if (exceptionHandler == null) throw new RuntimeException(e);
                exceptionHandler.handle(new PromiseError(e));
            }
        }));
    }

    public Promise<FirebaseResponse> postAsync(String path, Map<String, Object> data, NameValuePair... queries) {
        if (executor == null) throw new RuntimeException("There is no thread pool!");
        return new Promise<FirebaseResponse>((resolve, exceptionHandler) -> executor.execute(() -> {
            try {
                FirebaseResponse result = post(path, data, queries);
                resolve.run(result);
            } catch (Throwable e) {
                if (exceptionHandler == null) throw new RuntimeException(e);
                exceptionHandler.handle(new PromiseError(e));
            }
        }));
    }

    public Promise<FirebaseResponse> deleteAsync(String path, NameValuePair... queries) {
        if (executor == null) throw new RuntimeException("There is no thread pool!");

        return new Promise<>((resolve, exceptionHandler) -> executor.execute(() -> {
            try {
                FirebaseResponse result = delete(path, queries);
                resolve.run(result);
            } catch (FirebaseException e) {
                if (exceptionHandler == null) throw new RuntimeException(e);
                exceptionHandler.handle(new PromiseError(e));
            }
        }));
    }

    public enum FirebaseRestMethod {

        GET,
        PATCH,
        PUT,
        POST,
        DELETE
    }
}




