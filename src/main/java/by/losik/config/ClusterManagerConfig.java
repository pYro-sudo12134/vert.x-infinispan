package by.losik.config;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;
import org.infinispan.api.exception.InfinispanException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public class ClusterManagerConfig {
    private static ClusterManager clusterManagerInstance;
    @Contract(" -> new")
    public static synchronized @NotNull ClusterManager initClusterManager() {
        if(clusterManagerInstance == null) {
            System.setProperty("jgroups.bind_addr", System.getenv().getOrDefault("JGROUPS_BIND_ADDR", "0.0.0.0"));
            System.setProperty("jgroups.tcp.port", System.getenv().getOrDefault("JGROUPS_TCP_PORT", "7800"));

            clusterManagerInstance = new InfinispanClusterManager();

            return clusterManagerInstance;
        }
        return clusterManagerInstance;
    }

    public static ClusterManager clusterManager() {
        if (clusterManagerInstance == null) {
            throw new InfinispanException("Infinispan not initialized. Call initClusterManager() first.");
        }

        return clusterManagerInstance;
    }

    public static EmbeddedCacheManager getCacheContainer() {
        ClusterManager cm = clusterManager();
        if (cm instanceof InfinispanClusterManager) {
            return (EmbeddedCacheManager) ((InfinispanClusterManager) cm).getCacheContainer();
        }
        throw new InfinispanException("ClusterManager is not InfinispanClusterManager");
    }
}
