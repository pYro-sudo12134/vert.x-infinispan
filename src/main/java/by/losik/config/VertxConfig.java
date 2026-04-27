package by.losik.config;

import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertxConfig {
    private static final Logger log = LoggerFactory.getLogger(VertxConfig.class);
    private static Vertx vertxInstance;

    public static synchronized Vertx initVertx(VertxOptions vertxOptions, boolean clustered) {
        if (vertxInstance == null) {
            if (clustered) {
                ClusterManager clusterManager = ClusterManagerConfig.initClusterManager();
                vertxInstance = Vertx.builder()
                        .with(vertxOptions)
                        .withClusterManager(clusterManager)
                        .buildClustered()
                        .toCompletionStage().toCompletableFuture().join();
                log.info("Vert.x started in clustered mode");
            } else {
                vertxInstance = Vertx.vertx(vertxOptions);
                log.info("Vert.x started in standalone mode");
            }
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