package com.gprofiler.spark;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    private static final String LOG_FILE_PATH = "/tmp/gprofiler-agent.log";
    private static PrintWriter fileWriter;

    static {
        try {
            fileWriter = new PrintWriter(new FileWriter(LOG_FILE_PATH, true));
        } catch (IOException e) {
            System.err.println("gProfiler Agent: Failed to initialize file logger: " + e.getMessage());
        }
    }

    private static synchronized void logToFile(String formattedMessage, Throwable t) {
        if (fileWriter != null) {
            fileWriter.println(formattedMessage);
            if (t != null) {
                t.printStackTrace(fileWriter);
            }
            fileWriter.flush();
        }
    }

    private static void log(String level, String message) {
        String formatted = String.format("[%s] [%s] %s", dtf.format(LocalDateTime.now()), level, message);
        System.out.println(formatted);
        logToFile(formatted, null);
    }

    private static void logErr(String level, String message, Throwable t) {
        String formatted = String.format("[%s] [%s] %s", dtf.format(LocalDateTime.now()), level, message);
        System.err.println(formatted);
        if (t != null) {
            t.printStackTrace(System.err);
        }
        logToFile(formatted, t);
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
