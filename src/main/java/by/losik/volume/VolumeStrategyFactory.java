package by.losik.volume;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class VolumeStrategyFactory {
    private static final Logger log = LoggerFactory.getLogger(VolumeStrategyFactory.class);

    private static final Map<String, VolumeSelectionStrategy> strategies = new ConcurrentHashMap<>();

    static {
        registerStrategy(new HashVolumeStrategy());
        registerStrategy(new RoundRobinVolumeStrategy());
        registerStrategy(new LeastUsedVolumeStrategy());
    }

    public static void registerStrategy(VolumeSelectionStrategy strategy) {
        strategies.put(strategy.getName().toLowerCase(), strategy);
        log.info("Registered volume strategy: {}", strategy.getName());
    }

    public static VolumeSelectionStrategy getStrategy(@NotNull String name) {
        VolumeSelectionStrategy strategy = strategies.get(name.toLowerCase());
        if (strategy == null) {
            log.warn("Strategy '{}' not found, using default 'hash'", name);
            strategy = strategies.get("hash");
        }
        return strategy;
    }
}