package by.losik.volume;

import io.vertx.core.json.JsonObject;

import java.time.Instant;

public class MigrationTask {
    private final String fileId;
    private final String sourcePath;
    private final String targetPath;
    private final long size;
    private final JsonObject metadata;
    private final Instant startTime;
    private volatile String status; // PENDING, COPYING, COMMITTING, COMPLETED, FAILED
    private volatile String error;

    public MigrationTask(String fileId, String sourcePath, String targetPath,
                         long size, JsonObject metadata) {
        this.fileId = fileId;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.size = size;
        this.metadata = metadata;
        this.startTime = Instant.now();
        this.status = "PENDING";
    }

    public String getFileId() { return fileId; }
    public String getStatus() { return status; }
    public String getError() { return error; }

    public void setStatus(String status) { this.status = status; }
    public void setError(String error) { this.error = error; }

    public JsonObject toJson() {
        return new JsonObject()
                .put("fileId", fileId)
                .put("sourcePath", sourcePath)
                .put("targetPath", targetPath)
                .put("size", size)
                .put("status", status)
                .put("startTime", startTime.toString())
                .put("error", error != null ? error : "")
                .put("durationMs", System.currentTimeMillis() - startTime.toEpochMilli());
    }
}