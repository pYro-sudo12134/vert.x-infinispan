package by.losik.config;

import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;

public class VertxConfig {
    private static Vertx vertxInstance;

    public static Vertx initVertx(VertxOptions vertxOptions) {
        if (vertxInstance == null) {
            ClusterManager clusterManager = ClusterManagerConfig.initClusterManager();

            vertxInstance = io.vertx.core.Vertx.builder()
                    .with(vertxOptions)
                    .withClusterManager(clusterManager)
                    .buildClustered()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();
        }
        return vertxInstance;
    }

    public static Vertx vertx() {
        if (vertxInstance == null) {
            throw new VertxException("Vert.x not initialized. Call initVertx() first.");
        }

        return vertxInstance;
    }
}