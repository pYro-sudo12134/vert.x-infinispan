package by.losik.config;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;
import org.infinispan.api.exception.InfinispanException;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jboss.marshalling.core.JBossUserMarshaller;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public class ClusterManagerConfig {
    private static ClusterManager clusterManagerInstance;
    @Contract(" -> new")
    public static @NotNull ClusterManager initClusterManager() {
        if(clusterManagerInstance == null) {
            System.setProperty("jgroups.bind_addr", System.getenv().getOrDefault("JGROUPS_BIND_ADDR", "0.0.0.0"));
            System.setProperty("jgroups.tcp.port", System.getenv().getOrDefault("JGROUPS_TCP_PORT", "7800"));

            GlobalConfigurationBuilder globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder();
            globalConfig.serialization().marshaller(new JBossUserMarshaller());

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
}
