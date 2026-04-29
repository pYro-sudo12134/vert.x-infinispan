package by.losik.volume;

import by.losik.meta.FileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeastUsedVolumeStrategy implements StatAwareVolumeStrategy {
    private static final Logger log = LoggerFactory.getLogger(LeastUsedVolumeStrategy.class);

    private final Map<String, VolumeStats> stats = new ConcurrentHashMap<>();

    @Override
    public String selectVolume(List<String> volumes, FileMetadata metadata) {
        if (volumes == null || volumes.isEmpty()) {
            throw new IllegalArgumentException("Volumes list is empty");
        }

        if (volumes.size() == 1) {
            return volumes.get(0);
        }

        for (String volume : volumes) {
            stats.computeIfAbsent(volume, v -> new VolumeStats(v, 0, 0));
        }

        String selected = stats.values().stream()
                .min(Comparator.comparingLong(a -> a.fileCount))
                .map(VolumeStats::getVolumePath)
                .orElse(volumes.get(0));

        log.debug("Least-used strategy: selected={} (files={})",
                selected, stats.get(selected).fileCount);

        return selected;
    }

    public void updateStats(String volumePath, long fileCount, long usedSpace) {
        VolumeStats stat = stats.get(volumePath);
        if (stat != null) {
            stat.fileCount = fileCount;
            stat.usedSpace = usedSpace;
            stat.lastUpdate = System.currentTimeMillis();
        } else {
            stats.put(volumePath, new VolumeStats(volumePath, fileCount, usedSpace));
        }
    }

    @Override
    public String getName() {
        return "least-used";
    }

    private static class VolumeStats {
        final String volumePath;
        long fileCount;
        long usedSpace;
        long lastUpdate;

        VolumeStats(String volumePath, long fileCount, long usedSpace) {
            this.volumePath = volumePath;
            this.fileCount = fileCount;
            this.usedSpace = usedSpace;
            this.lastUpdate = System.currentTimeMillis();
        }

        String getVolumePath() { return volumePath; }
    }
}