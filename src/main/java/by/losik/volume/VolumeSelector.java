package by.losik.volume;

import by.losik.config.AppConfig;
import by.losik.meta.FileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class VolumeSelector {
    private static final Logger log = LoggerFactory.getLogger(VolumeSelector.class);

    private final List<String> volumes;
    private final VolumeSelectionStrategy strategy;
    private final VolumeManager volumeManager;
    private final ConcurrentHashMap<String, AtomicLong> volumeFileCount = new ConcurrentHashMap<>();

    public VolumeSelector(VolumeManager volumeManager) {
        this.volumeManager = volumeManager;
        this.volumes = AppConfig.nfsVolumes();
        String strategyName = AppConfig.volumeStrategy();
        this.strategy = VolumeStrategyFactory.getStrategy(strategyName);

        for (String volume : volumes) {
            volumeFileCount.put(volume, new AtomicLong(0));
        }

        log.info("VolumeSelector initialized with volumes: {}, strategy: {}",
                volumes, strategy.getName());
    }

    public String selectVolume(FileMetadata metadata) {
        if (volumes.isEmpty()) {
            throw new IllegalStateException("No volumes configured");
        }

        Map<String, VolumeState> states = volumeManager.getVolumes();

        List<String> writableVolumes = volumes.stream()
                .filter(v -> {
                    VolumeState state = states.get(v);
                    return state != null && state.isHealthy() && !state.isReadOnly();
                })
                .collect(Collectors.toList());

        if (writableVolumes.isEmpty()) {
            throw new IllegalStateException("No writable and healthy volumes available");
        }

        String selected = strategy.selectVolume(writableVolumes, metadata);

        volumeFileCount.computeIfPresent(selected, (k, v) -> {
            v.incrementAndGet();
            return v;
        });

        log.debug("Selected volume '{}' for file '{}' using strategy '{}' (readOnly excluded)",
                selected, metadata.getFileId(), strategy.getName());

        return selected;
    }

    public void updateVolumeStats(String volumePath, long fileCount, long usedSpace) {
        if (strategy instanceof StatAwareVolumeStrategy) {
            ((StatAwareVolumeStrategy) strategy).updateStats(volumePath, fileCount, usedSpace);
        }

        AtomicLong counter = volumeFileCount.get(volumePath);
        if (counter != null) {
            counter.set(fileCount);
        }
    }

    public VolumeSelectionStrategy getStrategy() {
        return strategy;
    }

    public List<String> getVolumes() {
        return volumes;
    }

    public Map<String, Long> getAllFileCounts() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, AtomicLong> entry : volumeFileCount.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }

    public String getInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("VolumeSelector {\n");
        sb.append("  strategy: ").append(strategy.getName()).append("\n");
        sb.append("  volumes: [").append(String.join(", ", volumes)).append("]\n");
        sb.append("  distribution:\n");
        for (Map.Entry<String, Long> entry : getAllFileCounts().entrySet()) {
            sb.append("    ").append(entry.getKey()).append(": ").append(entry.getValue()).append(" files\n");
        }
        sb.append("}");
        return sb.toString();
    }
}