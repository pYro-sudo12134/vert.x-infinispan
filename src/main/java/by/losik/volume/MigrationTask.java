package by.losik.volume;

import by.losik.constant.AppConstants;
import io.vertx.core.json.JsonObject;

import java.time.Instant;

public class MigrationTask {
    private final String fileId;
    private final String sourcePath;
    private final String targetPath;
    private final long size;
    private final Instant startTime;
    private volatile String status; // PENDING, COPYING, COMMITTING, COMPLETED, FAILED
    private volatile String error;

    public MigrationTask(String fileId, String sourcePath, String targetPath,
                         long size) {
        this.fileId = fileId;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
        this.size = size;
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
                .put(AppConstants.FIELD_FILE_ID, fileId)
                .put(AppConstants.FIELD_SOURCE_PATH, sourcePath)
                .put(AppConstants.FIELD_TARGET_PATH, targetPath)
                .put(AppConstants.FIELD_SIZE, size)
                .put(AppConstants.FIELD_STATUS, status)
                .put(AppConstants.FIELD_START_TIME, startTime.toString())
                .put(AppConstants.FIELD_ERROR, error != null ? error : "")
                .put(AppConstants.FIELD_DURATION_MS, System.currentTimeMillis() - startTime.toEpochMilli());
    }
}