package by.losik.volume;

import by.losik.meta.FileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class RoundRobinVolumeStrategy implements VolumeSelectionStrategy {
    private static final Logger log = LoggerFactory.getLogger(RoundRobinVolumeStrategy.class);

    private final AtomicLong counter = new AtomicLong(0);

    @Override
    public String selectVolume(List<String> volumes, FileMetadata metadata) {
        if (volumes == null || volumes.isEmpty()) {
            throw new IllegalArgumentException("Volumes list is empty");
        }

        if (volumes.size() == 1) {
            return volumes.get(0);
        }

        int index = (int) (counter.getAndIncrement() % volumes.size());
        String selected = volumes.get(index);

        log.debug("Round-robin strategy: index={}/{}, selected={}",
                index, volumes.size(), selected);

        return selected;
    }

    @Override
    public String getName() {
        return "round-robin";
    }
}