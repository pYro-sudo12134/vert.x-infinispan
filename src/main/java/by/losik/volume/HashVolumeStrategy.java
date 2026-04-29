package by.losik.volume;

import by.losik.meta.FileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HashVolumeStrategy implements VolumeSelectionStrategy {
    private static final Logger log = LoggerFactory.getLogger(HashVolumeStrategy.class);

    @Override
    public String selectVolume(List<String> volumes, FileMetadata metadata) {
        if (volumes == null || volumes.isEmpty()) {
            throw new IllegalArgumentException("Volumes list is empty");
        }

        if (volumes.size() == 1) {
            return volumes.get(0);
        }

        String fileId = metadata.getFileId();
        int index = Math.abs(fileId.hashCode() % volumes.size());
        String selected = volumes.get(index);

        log.debug("Hash strategy: fileId={}, hash={}, index={}/{}, selected={}",
                fileId, fileId.hashCode(), index, volumes.size(), selected);

        return selected;
    }

    @Override
    public String getName() {
        return "hash";
    }
}