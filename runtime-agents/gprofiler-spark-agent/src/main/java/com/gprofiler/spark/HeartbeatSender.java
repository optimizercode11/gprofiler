package com.gprofiler.spark;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class HeartbeatSender {

    private static final String TARGET_URL;
    private static final int INTERVAL_SECONDS = 60;

    static {
        String host = System.getenv("GPROFILER_HOST");
        if (host == null || host.isEmpty()) {
            host = "127.0.0.1";
        }
        String port = System.getenv("GPROFILER_PORT");
        if (port == null || port.isEmpty()) {
            port = "12345";
        }
        TARGET_URL = "http://" + host + ":" + port + "/spark";
    }

    private static final AtomicBoolean profilingEnabled = new AtomicBoolean(false);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "gprofiler-spark-sender");
        t.setDaemon(true);
        return t;
    });

    private static final Gson gson = new Gson();

    public static void start() {
        Logger.info("Starting HeartbeatSender to " + TARGET_URL);
        scheduler.scheduleAtFixedRate(HeartbeatSender::sendHeartbeat, 0, INTERVAL_SECONDS, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(HeartbeatSender::consumeThreadUpdates, 0, 1, TimeUnit.MILLISECONDS);
    }

    private static void consumeThreadUpdates() {
        try {
            Thread t;
            // Drain the queue of available updates
            while ((t = Agent.threadUpdateQueue.poll()) != null) {
                sendThreadInfo(t);
            }
        } catch (Exception e) {
            Logger.error("Error consuming thread updates", e);
        }
    }

    private static void sendHeartbeat() {
        try {
            JsonObject metadata = SparkMetadata.getMetadata();
            String jsonString = gson.toJson(metadata);

            Logger.debug("Sending heartbeat: " + jsonString);
            String responseBody = post(jsonString);
            Logger.debug("Heartbeat response: " + responseBody);

            if (responseBody != null) {
                try {
                    JsonObject response = gson.fromJson(responseBody, JsonObject.class);
                    boolean shouldProfile = false;
                    if (response.has("profile")) {
                        shouldProfile = response.get("profile").getAsBoolean();
                    }

                    if (shouldProfile && !profilingEnabled.get()) {
                        Logger.info("Profiling enabled by server response");
                        profilingEnabled.set(true);
                        // Initial thread dump
                        sendAllThreads();
                    } else if (!shouldProfile && profilingEnabled.get()) {
                        Logger.info("Profiling disabled by server response");
                        profilingEnabled.set(false);
                    }
                } catch (Exception e) {
                    Logger.error("Failed to parse heartbeat response", e);
                }
            }

        } catch (Exception e) {
            Logger.error("Error sending heartbeat", e);
        }
    }

    public static void sendAllThreads() {
        if (!profilingEnabled.get()) return;

        scheduler.execute(() -> {
            try {
                JsonObject payload = SparkMetadata.getMetadata(); // Includes PID/AppID
                payload.addProperty("type", "thread_info");

                JsonArray threadsArray = new JsonArray();
                Map<Thread, StackTraceElement[]> stacks = Thread.getAllStackTraces();
                for (Thread t : stacks.keySet()) {
                    JsonObject threadInfo = new JsonObject();
                    threadInfo.addProperty("tid", t.getId());
                    threadInfo.addProperty("name", t.getName());
                    threadsArray.add(threadInfo);
                }
                payload.add("threads", threadsArray);

                Logger.debug("Sending all threads info (count: " + stacks.size() + ")");
                sendPayload(payload);
            } catch (Exception e) {
                Logger.error("Error sending all threads", e);
            }
        });
    }

    public static void sendThreadInfo(Thread t) {
        if (!profilingEnabled.get()) return;

        scheduler.execute(() -> {
            try {
                JsonObject payload = SparkMetadata.getMetadata();
                payload.addProperty("type", "thread_info");

                JsonArray threadsArray = new JsonArray();
                JsonObject threadInfo = new JsonObject();
                threadInfo.addProperty("tid", t.getId());
                threadInfo.addProperty("name", t.getName());
                threadsArray.add(threadInfo);

                payload.add("threads", threadsArray);

                Logger.debug("Sending thread info update for: " + t.getName());
                sendPayload(payload);
            } catch (Exception e) {
                Logger.error("Error sending thread info", e);
            }
        });
    }

    private static void sendPayload(JsonObject payload) {
        try {
            post(gson.toJson(payload));
        } catch (Exception e) {
            Logger.error("Error sending payload", e);
        }
    }

    private static String post(String jsonInputString) {
        HttpURLConnection con = null;
        try {
            URL url = new URL(TARGET_URL);
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json; utf-8");
            con.setRequestProperty("Accept", "application/json");
            con.setDoOutput(true);

            try (OutputStream os = con.getOutputStream()) {
                byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String responseLine = null;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                return response.toString();
            }

        } catch (Exception e) {
            Logger.error("HTTP POST failed to " + TARGET_URL, e);
            return null;
        } finally {
            if (con != null) {
                con.disconnect();
            }
        }
    }
}
