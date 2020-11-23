package net.thegreshams.firebase4j.service;

import mou.com.promises.Promise;
import mou.com.promises.PromiseError;
import net.thegreshams.firebase4j.error.FirebaseException;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RealtimeDatabase extends Firebase {

    private static final Logger LOGGER = Logger.getRootLogger();
    private final HashMap<String, Object> cache = new HashMap<>();
    private final ArrayList<RealtimeDatabaseConnection> connections = new ArrayList<>();
    public RealtimeDatabase(String baseUrl, int threadPoolSize) throws FirebaseException {
        super(baseUrl, threadPoolSize);
    }

    public RealtimeDatabase(String baseUrl, Boolean useJsonExtension, int threadPoolSize) throws FirebaseException {
        super(baseUrl, useJsonExtension, threadPoolSize);
    }

    public RealtimeDatabase(String baseUrl, String secureToken, int threadPoolSize) throws FirebaseException {
        super(baseUrl, secureToken, threadPoolSize);
    }

    public void shutdown() {
        connections.forEach(RealtimeDatabaseConnection::disconnect);
        executor.shutdown();
    }

    public Promise<Object> onValue(String path, int bufferSize, NameValuePair... queries) {
        var con = new RealtimeDatabaseConnection(path, queries);
        connections.add(con);
        return new Promise<>((resolve, errorHandler) -> {
            new Thread(() -> {
                String url = buildFullUrlFromRelativePath(path, queries);
                HttpGet httpget = new HttpGet(url);
                httpget.setHeader("accept", "text/event-stream");
                CloseableHttpResponse response;
                try {
                    response = client.execute(httpget);
                    LOGGER.info("Start streaming:" + url);
                } catch (IOException e) {
                    e.printStackTrace();
                    LOGGER.error("Couldn't make the request!");
                    errorHandler.handle(new PromiseError(ErrorCode.UNKNOWN));
                    return;
                }
                StatusLine statusLine = response.getStatusLine();
                HttpEntity entity = response.getEntity();
                if (statusLine.getStatusCode() >= 300) {
                    LOGGER.error(statusLine.getStatusCode() + " " + statusLine.getReasonPhrase());
                    switch (statusLine.getStatusCode()) {
                        case 401:
                            errorHandler.handle(new PromiseError(ErrorCode.PERMISSION_DENIED));
                            break;
                        default:
                            errorHandler.handle(new PromiseError(ErrorCode.UNKNOWN));
                            break;
                    }
                    return;
                }
                if (entity == null) {
                    LOGGER.error("Response contains no content");
                    errorHandler.handle(new PromiseError(ErrorCode.UNKNOWN));
                    return;
                }
                if (!entity.isStreaming()) {
                    try {
                        EntityUtils.consume(entity);
                    } catch (IOException e) {
                        LOGGER.error("Exception thrown while consuming the HttpEntity");
                        e.printStackTrace();
                    }
                    LOGGER.error("The entity isn't streaming!");
                    errorHandler.handle(new PromiseError(ErrorCode.UNKNOWN));
                    return;
                }
                final byte[] buffer = new byte[bufferSize];
                final ObjectMapper mapper = new ObjectMapper();
                final TypeReference<HashMap<String, Object>> typeRef = new TypeReference<>() {
                };
                while (con.isOnline()) {
                    int bytesLength;
                    InputStream stream;
                    try {
                        stream = entity.getContent();
                        bytesLength = stream.read(buffer);
                    } catch (IOException e) {
                        e.printStackTrace();
                        LOGGER.error("Exception thrown while reading the stream");
                        errorHandler.handle(new PromiseError(ErrorCode.UNKNOWN));
                        break;
                    }
                    if (bytesLength > 0) {

                        String[] rawResponse = new String(Arrays.copyOf(buffer, bytesLength)).split("\n");
                        String event = rawResponse[0].substring(7); // after 'event: '

                        if ("put".equals(event) || "patch".equals(event)) {
                            String jsonData = rawResponse[1].substring(6); // after 'data: '
                            try {
                                Map<String, Object> data = mapper.readValue(jsonData, typeRef);
                                // update cache
                                putToCache(path + data.get("path"), data.get("data"));
                                //LOGGER.debug("Updated cache = " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(cache));
                                resolve.run(getFromCache(path));
                            } catch (IOException e) {
                                e.printStackTrace();
                                LOGGER.error("Exception thrown while parsing json data");
                                errorHandler.handle(new PromiseError(ErrorCode.UNKNOWN));
                                break;
                            }
                        } else if ("auth_revoked".equals(event)) {
                            LOGGER.error("Authentication revoked!");
                            errorHandler.handle(new PromiseError(ErrorCode.AUTH_REVOKED));
                            break;
                        } else if ("cancel".equals(event)) {
                            LOGGER.error("Streaming canceled!");
                            errorHandler.handle(new PromiseError(ErrorCode.CANCELED_BY_END_POINT));
                            break;
                        }

                    } else {
                        // end of stream
                        LOGGER.error("Stream ended!");
                        errorHandler.handle(new PromiseError(ErrorCode.STREAM_ENDED_UNEXPECTEDLY));
                        break;
                    }
                }
                try {
                    EntityUtils.consume(entity);
                } catch (IOException e) {
                    e.printStackTrace();
                    LOGGER.error("Exception thrown while consuming the HttpEntity");
                }
            }).start();
        });
    }

    public Promise<Object> onValue(String path, NameValuePair... queries) {
        return onValue(path, 2048, queries);
    }

    private Object getFromCache(String path) {
        var current = cache;
        var keys = path.split("/");
        for (int i = 0; i < keys.length - 1; i++) {
            String key = keys[i];
            if (current.containsKey(key)) {
                current = (HashMap<String, Object>) current.get(key);
            } else {
                return new HashMap<String, Object>();
            }
        }
        return current.get(keys[keys.length - 1]);
    }

    private synchronized void putToCache(String path, Object newData) {
        var current = cache;
        var keys = path.split("/");
        if (keys.length == 1) {
            // overwrite root
            current.put("root", newData);
            return;
        }
        for (int i = 0; i < keys.length - 1; i++) {
            String key = keys[i];
            if (current.containsKey(key)) {
                current = (HashMap<String, Object>) current.get(key);
            } else {
                var a = new HashMap<String, Object>();
                current.put(key, a);
                current = a;
            }
        }
        current.put(keys[keys.length - 1], newData);
    }

    public enum ErrorCode {
        AUTH_REVOKED,
        STREAM_ENDED_UNEXPECTEDLY,
        CANCELED_BY_END_POINT,
        PERMISSION_DENIED,
        UNKNOWN
    }

    protected static class RealtimeDatabaseConnection {
        private final String path;
        private final NameValuePair[] queries;
        private boolean online = true;

        public RealtimeDatabaseConnection(String path, NameValuePair[] queries) {
            this.path = path;
            this.queries = queries;
        }

        public void disconnect() {
            online = false;
        }

        public boolean isOnline() {
            return online;
        }

        public String getPath() {
            return path;
        }

        public NameValuePair[] getQueries() {
            return queries;
        }
    }

}
