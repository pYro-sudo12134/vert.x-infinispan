package by.losik.volume;

public interface StatAwareVolumeStrategy extends VolumeSelectionStrategy {
    void updateStats(String volumePath, long fileCount, long usedSpace);
}