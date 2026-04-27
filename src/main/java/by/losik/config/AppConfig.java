package by.losik.config;

import io.vertx.core.json.JsonObject;
import java.util.concurrent.atomic.AtomicReference;

public class AppConfig {
    private static final AtomicReference<JsonObject> config = new AtomicReference<>();

    public static void load(JsonObject cfg) {
        config.set(cfg);
    }

    public static JsonObject get() {
        return config.get();
    }

    public static int httpPort() {
        return config.get().getInteger("http.port", 8080);
    }

    public static int metricsPort() {
        return config.get().getInteger("metrics.port", 8079);
    }

    public static String nfsPath() {
        return config.get().getString("nfs.path", "/mnt/nfs");
    }

    public static long fileUploadTimeout() {
        return config.get().getLong("file.upload.timeout", 30000L);
    }

    public static boolean corsEnabled() {
        return config.get().getBoolean("cors.enabled", true);
    }

    public static String corsAllowedOrigins() {
        return config.get().getString("cors.allowed.origins", "*");
    }

    public static String corsAllowedMethods() {
        return config.get().getString("cors.allowed.methods", "GET,POST,PUT,DELETE,OPTIONS");
    }

    public static String corsAllowedHeaders() {
        return config.get().getString("cors.allowed.headers", "Content-Type,Authorization");
    }

    public static String tmpPath() {
        return config.get().getString("temp.file.path", "/tmp/file-upload");
    }

    public static int maxPageSize() {
        return config.get().getInteger("max.page.size", 1000);
    }

    public static int defaultPageSize() {
        return config.get().getInteger("default.page.size", 100);
    }
}