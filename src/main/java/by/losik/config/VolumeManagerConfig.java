package by.losik.config;

import by.losik.volume.VolumeManager;
import io.vertx.core.Vertx;

public class VolumeManagerConfig {
    private static volatile VolumeManager instance;

    public static synchronized void init(Vertx vertx, String nodeId) {
        if (instance != null) {
            throw new IllegalStateException("VolumeManager already initialized");
        }
        instance = new VolumeManager(vertx, nodeId);
    }

    public static VolumeManager volumeManager() {
        if (instance == null) {
            throw new IllegalStateException("VolumeManager not initialized. Call init() first.");
        }
        return instance;
    }
}