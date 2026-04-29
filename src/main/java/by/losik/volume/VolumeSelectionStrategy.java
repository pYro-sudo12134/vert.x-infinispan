package by.losik.volume;

import by.losik.meta.FileMetadata;

import java.util.List;

public interface VolumeSelectionStrategy {
    String selectVolume(List<String> volumes, FileMetadata metadata);

    String getName();
}