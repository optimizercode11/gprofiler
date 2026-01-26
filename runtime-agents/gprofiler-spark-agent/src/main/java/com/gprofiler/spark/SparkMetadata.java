package com.gprofiler.spark;

import java.lang.management.ManagementFactory;
import com.google.gson.JsonObject;

public class SparkMetadata {

    public static JsonObject getMetadata() {
        JsonObject json = new JsonObject();

        // Extract PID (Java 8 compatible)
        String pid = getPid();
        if (pid != null) {
            try {
                json.addProperty("pid", Long.parseLong(pid));
            } catch (NumberFormatException e) {
                // Should not happen if getPid works as expected
                json.addProperty("pid_raw", pid);
            }
        }

        // Extract Spark properties
        String appId = System.getProperty("spark.app.id");
        String appName = System.getProperty("spark.app.name");

        if (appId == null && pid != null) {
            appId = "unknown-app-" + pid;
        }
        if (appName == null) {
            appName = "Unknown Spark App";
        }

        if (appId != null) {
            json.addProperty("spark.app.id", appId);
        }
        if (appName != null) {
            json.addProperty("spark.app.name", appName);
        }

        return json;
    }

    private static String getPid() {
        try {
            // This returns "pid@hostname"
            String jvmName = ManagementFactory.getRuntimeMXBean().getName();
            return jvmName.split("@")[0];
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
