package by.losik.volume;

import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public class VolumeState {
    private final String path;
    private volatile boolean healthy;
    private volatile boolean readOnly;
    private final AtomicLong fileCount;
    private final AtomicLong usedSpace;
    private Instant lastScanned;

    public VolumeState(String path) {
        this.path = path;
        this.healthy = true;
        this.readOnly = false;
        this.fileCount = new AtomicLong(0);
        this.usedSpace = new AtomicLong(0);
        this.lastScanned = Instant.now();
    }

    public String getPath() { return path; }
    public boolean isHealthy() { return healthy; }
    public boolean isReadOnly() { return readOnly; }
    public long getFileCount() { return fileCount.get(); }
    public long getUsedSpace() { return usedSpace.get(); }
    public Instant getLastScanned() { return lastScanned; }

    public void setHealthy(boolean healthy) { this.healthy = healthy; }
    public void setReadOnly(boolean readOnly) { this.readOnly = readOnly; }
    public void setLastScanned(Instant lastScanned) { this.lastScanned = lastScanned; }

    public long incrementAndGetFileCount() {
        return fileCount.incrementAndGet();
    }

    public long decrementAndGetFileCount() {
        return fileCount.decrementAndGet();
    }

    public void addUsedSpace(long space) {
        usedSpace.addAndGet(space);
    }

    public void removeUsedSpace(long space) {
        usedSpace.addAndGet(-space);
    }

    public void setFileCount(long count) {
        fileCount.set(count);
    }

    public void setUsedSpace(long space) {
        usedSpace.set(space);
    }

    public JsonObject toJson() {
        return new JsonObject()
                .put("path", path)
                .put("healthy", healthy)
                .put("readOnly", readOnly)
                .put("fileCount", fileCount.get())
                .put("usedSpaceGB", usedSpace.get() / (1024.0 * 1024 * 1024))
                .put("lastScanned", lastScanned.toString());
    }
}