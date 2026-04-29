package by.losik.config;

import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class AppConfig {
    private static final AtomicReference<JsonObject> config = new AtomicReference<>();

    public static void load(JsonObject cfg) {
        config.set(cfg);
    }

    public static JsonObject get() {
        return config.get();
    }

    public static boolean isClustered() {
        return config.get().getBoolean("clustered", false);
    }

    public static int httpPort() {
        return config.get().getInteger("http.port", 8080);
    }

    public static int metricsPort() {
        return config.get().getInteger("metrics.port", 8079);
    }

    public static String nodeId() {
        return config.get().getString("node.id", "node-" + System.currentTimeMillis());
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

    public static List<String> nfsVolumes() {
        return Arrays.stream(
                        System.getenv()
                                .getOrDefault("NFS_VOLUMES",
                                        System.getenv()
                                                .getOrDefault("NFS_MOUNT_PATH", "/mnt/nfs"))
                                .split(","))
                .map(String::trim).collect(Collectors.toList());
    }

    public static List<String> readOnlyVolumes() {
        return Arrays.stream(
                        System.getenv()
                                .getOrDefault("READ_ONLY_VOLUMES","")
                                .split(","))
                .map(String::trim).collect(Collectors.toList());
    }

    public static String volumeStrategy() {
        return config.get().getString("volume.strategy", "hash");
    }


}