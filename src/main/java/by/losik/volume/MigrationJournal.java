package by.losik.volume;

import by.losik.config.EventBusConfig;
import by.losik.meta.FileMetadata;
import by.losik.util.MetadataHelper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Lock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MigrationJournal {
    private static final Logger log = LoggerFactory.getLogger(MigrationJournal.class);

    private final Vertx vertx;
    private final String journalPath;
    private final Map<String, JournalEntry> entries = new ConcurrentHashMap<>();

    public MigrationJournal(Vertx vertx, String journalPath) {
        this.vertx = vertx;
        this.journalPath = journalPath;
        loadJournal();
    }

    public Future<Void> migrate(String fileId, String sourcePath, String targetPath,
                                long size, JsonObject metadata) {
        log.info("Migration requested: fileId={}, from={}, to={}, size={}",
                fileId, sourcePath, targetPath, size);

        if (sourcePath.equals(targetPath)) {
            log.info("Source and target paths are the same for file {}, skipping migration", fileId);
            return Future.succeededFuture();
        }

        JournalEntry entry = new JournalEntry(fileId, sourcePath, targetPath, size);
        entries.put(entry.getId(), entry);
        persistJournal();

        return executeMigration(entry, metadata);
    }

    private Future<Void> executeMigration(JournalEntry entry, JsonObject metadata) {
        Promise<Void> promise = Promise.promise();

        entry.setStatus(Status.COPYING);
        persistJournal();

        copyFileWithChecksum(entry.getSourcePath(), entry.getTargetPath())
                .compose(checksum -> {
                    entry.setChecksum(checksum);
                    entry.setStatus(Status.READY);
                    persistJournal();

                    return commitMigration(entry, metadata);
                })
                .onSuccess(v -> {
                    entry.setStatus(Status.COMMITTED);
                    persistJournal();
                    log.info("Migration committed for file: {}", entry.getFileId());
                    promise.complete();
                })
                .onFailure(err -> {
                    entry.setStatus(Status.FAILED);
                    entry.setErrorMessage(err.getMessage());
                    persistJournal();
                    log.error("Migration failed for file: {}", entry.getFileId(), err);
                    promise.fail(err);
                });

        return promise.future();
    }

    private Future<String> copyFileWithChecksum(String source, String target) {
        Promise<String> promise = Promise.promise();

        log.info("Copying file from {} to {}", source, target);

        vertx.fileSystem().exists(target, existsResult -> {
            if (existsResult.succeeded() && existsResult.result()) {
                log.debug("Target file already exists, deleting: {}", target);
                vertx.fileSystem().delete(target, deleteResult -> {
                    if (deleteResult.failed()) {
                        log.warn("Failed to delete existing target file: {}, continuing anyway", target, deleteResult.cause());
                    }
                    doCopyWithChecksum(source, target, promise);
                });
            } else {
                doCopyWithChecksum(source, target, promise);
            }
        });

        return promise.future();
    }

    private void doCopyWithChecksum(String source, String target, Promise<String> promise) {
        computeChecksum(source).onComplete(sourceChecksumResult -> {
            if (sourceChecksumResult.failed()) {
                log.error("Failed to compute source checksum for {}", source, sourceChecksumResult.cause());
                promise.fail(sourceChecksumResult.cause());
                return;
            }

            String sourceChecksum = sourceChecksumResult.result();
            log.debug("Source checksum: {}", sourceChecksum);

            vertx.fileSystem().open(source, new OpenOptions().setRead(true), openSrc -> {
                if (openSrc.failed()) {
                    log.error("Failed to open source file: {}", source, openSrc.cause());
                    promise.fail(openSrc.cause());
                    return;
                }

                vertx.fileSystem().open(target, new OpenOptions().setWrite(true).setCreate(true), openDst -> {
                    if (openDst.failed()) {
                        log.error("Failed to open target file: {}", target, openDst.cause());
                        promise.fail(openDst.cause());
                        return;
                    }

                    openSrc.result().pipeTo(openDst.result())
                            .onSuccess(v -> {
                                log.debug("File copy completed, verifying checksum...");
                                computeChecksum(target).onComplete(targetChecksumResult -> {
                                    if (targetChecksumResult.failed()) {
                                        log.error("Failed to compute target checksum for {}", target, targetChecksumResult.cause());
                                        promise.fail(targetChecksumResult.cause());
                                        return;
                                    }

                                    String targetChecksum = targetChecksumResult.result();
                                    log.debug("Target checksum: {}", targetChecksum);

                                    if (sourceChecksum.equals(targetChecksum)) {
                                        log.info("File copied successfully with checksum match: {} -> {}", source, target);
                                        promise.complete(sourceChecksum);
                                    } else {
                                        String error = String.format("Checksum mismatch: source=%s, target=%s",
                                                sourceChecksum, targetChecksum);
                                        log.error(error);
                                        promise.fail(error);
                                    }
                                });
                            })
                            .onFailure(err -> {
                                log.error("Failed to pipe file from {} to {}", source, target, err);
                                promise.fail(err);
                            });
                });
            });
        });
    }

    private Future<String> computeChecksum(String filePath) {
        Promise<String> promise = Promise.promise();

        vertx.fileSystem().readFile(filePath, result -> {
            if (result.succeeded()) {
                try {
                    MessageDigest digest = MessageDigest.getInstance("SHA-256");
                    byte[] hash = digest.digest(result.result().getBytes());
                    String checksum = Base64.getEncoder().encodeToString(hash);
                    promise.complete(checksum);
                } catch (Exception e) {
                    promise.fail(e);
                }
            } else {
                promise.fail(result.cause());
            }
        });

        return promise.future();
    }

    private Future<Void> commitMigration(@NotNull JournalEntry entry, JsonObject oldMetadata) {
        Promise<Void> promise = Promise.promise();
        String lockKey = "file-lock-" + entry.getFileId();

        acquireLockWithRetry(lockKey, 5, 1000, lockResult -> {
            if (lockResult.failed()) {
                promise.fail("Failed to acquire lock: " + lockResult.cause().getMessage());
                return;
            }

            Lock lock = lockResult.result();
            String fileName = oldMetadata.getString("fileName");
            String contentType = oldMetadata.getString("contentType");

            if (fileName == null || fileName.isBlank()) {
                lock.release();
                promise.fail("fileName is missing");
                return;
            }

            FileMetadata newMetadata = new FileMetadata(
                    entry.getFileId(), fileName, entry.getTargetPath(), contentType,
                    entry.getSize(), Instant.parse(oldMetadata.getString("createdAt")), Instant.now()
            );

            updateMetadataDirect(entry.getFileId(), newMetadata)
                    .onSuccess(v ->
                            vertx.fileSystem().delete(entry.getSourcePath(), deleteResult -> {
                                lock.release();
                                promise.complete();
                            }))
                    .onFailure(err -> {
                        lock.release();
                        promise.fail(err);
                    });
        });

        return promise.future();
    }

    private Future<Void> updateMetadataDirect(String fileId, FileMetadata metadata) {
        Promise<Void> promise = Promise.promise();

        vertx.sharedData().<String, FileMetadata>getClusterWideMap("file.metadata", ar -> {
            if (ar.succeeded()) {
                ar.result().put(fileId, metadata, putResult -> {
                    if (putResult.succeeded()) {
                        promise.complete();
                    } else {
                        promise.fail(putResult.cause());
                    }
                });
            } else {
                promise.fail(ar.cause());
            }
        });

        return promise.future();
    }

    private void acquireLockWithRetry(String key, int maxRetries, long delay, Handler<AsyncResult<Lock>> handler) {
        vertx.sharedData().getLockWithTimeout(key, 10000, result -> {
            if (result.succeeded()) {
                handler.handle(Future.succeededFuture(result.result()));
            } else if (maxRetries > 0) {
                vertx.setTimer(delay, tid -> acquireLockWithRetry(key, maxRetries - 1, delay, handler));
            } else {
                handler.handle(Future.failedFuture(result.cause()));
            }
        });
    }

    public Future<Void> rollback(String fileId) {
        Promise<Void> promise = Promise.promise();

        JournalEntry entry = entries.values().stream()
                .filter(e -> e.getFileId().equals(fileId))
                .findFirst()
                .orElse(null);

        if (entry == null) {
            promise.fail("No migration found for file: " + fileId);
            return promise.future();
        }

        vertx.fileSystem().exists(entry.getTargetPath(), exists -> {
            if (exists.succeeded() && exists.result()) {
                vertx.fileSystem().delete(entry.getTargetPath(), delete ->
                        log.info("Rolled back: deleted temp file {}", entry.getTargetPath()));
            }

            entry.setStatus(Status.ROLLED_BACK);
            persistJournal();
            promise.complete();
        });

        return promise.future();
    }

    public Future<Void> recover() {
        Promise<Void> promise = Promise.promise();

        List<JournalEntry> pending = entries.values().stream()
                .filter(e -> e.getStatus() == Status.READY)
                .toList();

        if (pending.isEmpty()) {
            promise.complete();
            return promise.future();
        }

        log.info("Found {} pending migrations, attempting recovery", pending.size());

        AtomicInteger recovered = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);

        for (JournalEntry entry : pending) {
            MetadataHelper.getMetadata(entry.getFileId()).onComplete(metadataResult -> {
                if (metadataResult.succeeded()) {
                    JsonObject metadata = metadataResult.result();

                    String currentPath = metadata.getString("filePath");
                    if (currentPath.equals(entry.getTargetPath())) {
                        entry.setStatus(Status.COMMITTED);
                        persistJournal();
                        recovered.incrementAndGet();
                    } else {
                        commitMigration(entry, metadata)
                                .onSuccess(v -> {
                                    entry.setStatus(Status.COMMITTED);
                                    persistJournal();
                                    recovered.incrementAndGet();
                                })
                                .onFailure(err -> {
                                    log.error("Recovery failed for file: {}", entry.getFileId(), err);
                                    failed.incrementAndGet();
                                });
                    }
                } else {
                    log.error("Failed to get metadata for file: {}", entry.getFileId());
                    failed.incrementAndGet();
                }

                if (recovered.get() + failed.get() == pending.size()) {
                    log.info("Recovery completed: recovered={}, failed={}", recovered.get(), failed.get());
                    promise.complete();
                }
            });
        }

        return promise.future();
    }

    private void loadJournal() {
        vertx.fileSystem().exists(journalPath, exists -> {
            if (exists.succeeded() && exists.result()) {
                vertx.fileSystem().readFile(journalPath, read -> {
                    if (read.succeeded()) {
                        JsonArray array = new JsonArray(read.result());
                        for (int i = 0; i < array.size(); i++) {
                            JournalEntry entry = JournalEntry.fromJson(array.getJsonObject(i));
                            if (entry.getStatus() != Status.COMMITTED &&
                                    entry.getStatus() != Status.ROLLED_BACK) {
                                entries.put(entry.getId(), entry);
                                log.warn("Found incomplete migration: {} for file {}",
                                        entry.getStatus(), entry.getFileId());
                            }
                        }
                    }
                });
            }
        });
    }

    private void persistJournal() {
        JsonArray array = new JsonArray();
        for (JournalEntry entry : entries.values()) {
            array.add(entry.toJson());
        }
        vertx.fileSystem().writeFile(journalPath, array.toBuffer());
    }

    public Future<JsonObject> getMigrationStatus(String fileId) {
        Promise<JsonObject> promise = Promise.promise();

        JournalEntry entry = entries.values().stream()
                .filter(e -> e.getFileId().equals(fileId))
                .findFirst()
                .orElse(null);

        if (entry == null) {
            promise.fail("No migration found for file: " + fileId);
        } else {
            promise.complete(new JsonObject()
                    .put("status", entry.getStatus().toString())
                    .put("error", entry.getErrorMessage() != null ? entry.getErrorMessage() : "")
                    .put("checksum", entry.getChecksum() != null ? entry.getChecksum() : "")
                    .put("createdAt", entry.createdAt.toString())
                    .put("updatedAt", entry.updatedAt.toString()));
        }

        return promise.future();
    }

    public enum Status {
        PENDING,
        COPYING,
        READY,
        COMMITTED,
        FAILED,
        ROLLED_BACK
    }

    public static class JournalEntry {
        private final String id;
        private final String fileId;
        private final String sourcePath;
        private final String targetPath;
        private final long size;
        private final Instant createdAt;
        private Status status;
        private String checksum;
        private String errorMessage;
        private Instant updatedAt;

        public JournalEntry(String fileId, String sourcePath, String targetPath, long size) {
            this.id = UUID.randomUUID().toString();
            this.fileId = fileId;
            this.sourcePath = sourcePath;
            this.targetPath = targetPath;
            this.size = size;
            this.status = Status.PENDING;
            this.createdAt = Instant.now();
            this.updatedAt = this.createdAt;
        }

        public static JournalEntry fromJson(JsonObject json) {
            return new JournalEntry(
                    json.getString("fileId"),
                    json.getString("sourcePath"),
                    json.getString("targetPath"),
                    json.getLong("size")
            );
        }

        public String getId() {
            return id;
        }

        public String getFileId() {
            return fileId;
        }

        public String getSourcePath() {
            return sourcePath;
        }

        public String getTargetPath() {
            return targetPath;
        }

        public long getSize() {
            return size;
        }

        public Status getStatus() {
            return status;
        }

        public void setStatus(Status status) {
            this.status = status;
            this.updatedAt = Instant.now();
        }

        public String getChecksum() {
            return checksum;
        }

        public void setChecksum(String checksum) {
            this.checksum = checksum;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String error) {
            this.errorMessage = error;
        }

        public JsonObject toJson() {
            return new JsonObject()
                    .put("id", id)
                    .put("fileId", fileId)
                    .put("sourcePath", sourcePath)
                    .put("targetPath", targetPath)
                    .put("size", size)
                    .put("status", status.toString())
                    .put("checksum", checksum != null ? checksum : "")
                    .put("error", errorMessage != null ? errorMessage : "")
                    .put("createdAt", createdAt.toString())
                    .put("updatedAt", updatedAt.toString());
        }
    }
}