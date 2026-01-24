package com.gprofiler.spark;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    private static void log(String level, String message) {
        System.out.println(String.format("[%s] [%s] %s", dtf.format(LocalDateTime.now()), level, message));
    }

    private static void logErr(String level, String message, Throwable t) {
        System.err.println(String.format("[%s] [%s] %s", dtf.format(LocalDateTime.now()), level, message));
        if (t != null) {
            t.printStackTrace(System.err);
        }
    }

    public static void info(String message) {
        log("INFO", message);
    }

    public static void debug(String message) {
        // Can be toggled via env var if needed, for now enable to see it "locally" as requested
        log("DEBUG", message);
    }

    public static void warn(String message) {
        logErr("WARN", message, null);
    }

    public static void error(String message) {
        logErr("ERROR", message, null);
    }

    public static void error(String message, Throwable t) {
        logErr("ERROR", message, t);
    }
}
