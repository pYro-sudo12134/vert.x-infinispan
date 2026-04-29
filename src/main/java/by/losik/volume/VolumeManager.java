package by.losik.volume;

import by.losik.config.AppConfig;
import by.losik.config.EventBusConfig;
import by.losik.meta.FileMetadata;
import by.losik.util.MetadataHelper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class VolumeManager implements VolumeManagerFacade {
    private static final Logger log = LoggerFactory.getLogger(VolumeManager.class);

    private final Vertx vertx;
    private final String nodeId;
    private final VolumeSelector selector;
    private final MigrationJournal journal;
    private final Map<String, VolumeState> volumes = new ConcurrentHashMap<>();
    private final Map<String, MigrationTask> activeMigrations = new ConcurrentHashMap<>();

    public VolumeManager(Vertx vertx, String nodeId) {
        this.vertx = vertx;
        this.nodeId = nodeId;
        this.selector = new VolumeSelector(this);
        this.journal = new MigrationJournal(vertx, "/data/migration-journal.json");

        initVolumes();
        startHealthChecker();
        recoverAfterCrash();
    }

    private void initVolumes() {
        List<String> readOnlyVolumes = AppConfig.readOnlyVolumes();

        for (String volumePath : selector.getVolumes()) {
            VolumeState state = new VolumeState(volumePath);
            state.setHealthy(true);

            if (readOnlyVolumes.contains(volumePath)) {
                state.setReadOnly(true);
                log.info("Volume {} is read-only (will not accept new files)", volumePath);
            }

            volumes.put(volumePath, state);

            countFilesOnVolume(volumePath).onSuccess(count -> {
                state.setFileCount(count);
                selector.updateVolumeStats(volumePath, count, 0);
                log.info("Volume {} initialized with {} files, readOnly={}",
                        volumePath, count, state.isReadOnly());
            });
        }
    }

    @Override
    public VolumeSelectionStrategy getStrategy() {
        return selector.getStrategy();
    }

    @Override
    public String selectVolume(FileMetadata metadata) {
        return selector.selectVolume(metadata);
    }

    @Override
    public Map<String, VolumeState> getVolumes() {
        return Collections.unmodifiableMap(volumes);
    }

    @Override
    public Future<Void> addVolume(String volumePath) {
        Promise<Void> promise = Promise.promise();

        if (volumes.containsKey(volumePath)) {
            promise.fail("Volume already exists: " + volumePath);
            return promise.future();
        }

        vertx.fileSystem().exists(volumePath, exists -> {
            if (exists.failed() || !exists.result()) {
                vertx.fileSystem().mkdirs(volumePath, mkdir -> {
                    if (mkdir.failed()) {
                        promise.fail("Cannot create volume directory: " + mkdir.cause().getMessage());
                        return;
                    }
                    registerVolume(volumePath, promise);
                });
            } else {
                registerVolume(volumePath, promise);
            }
        });

        return promise.future();
    }

    private void registerVolume(String volumePath, @NotNull Promise<Void> promise) {
        VolumeState state = new VolumeState(volumePath);
        state.setHealthy(true);
        volumes.put(volumePath, state);

        broadcastVolumeEvent("added", volumePath);

        log.info("Volume added: {}", volumePath);
        promise.complete();
    }

    @Override
    public Future<Void> removeVolume(String volumePath, String targetVolume) {
        Promise<Void> promise = Promise.promise();

        VolumeState state = volumes.get(volumePath);
        if (state == null) {
            promise.fail("Volume not found: " + volumePath);
            return promise.future();
        }

        if (state.isReadOnly()) {
            promise.fail("Volume is read-only");
            return promise.future();
        }

        String target = targetVolume;
        if (target == null) {
            target = volumes.keySet().stream()
                    .filter(v -> !v.equals(volumePath))
                    .findFirst()
                    .orElse(null);
        }

        if (target == null) {
            promise.fail("No target volume available for migration");
            return promise.future();
        }

        log.info("Removing volume {} with migration to {}", volumePath, target);

        migrateAllFiles(volumePath, target)
                .compose(v -> unmountVolume(volumePath))
                .onSuccess(v -> {
                    volumes.remove(volumePath);
                    broadcastVolumeEvent("removed", volumePath);
                    promise.complete();
                })
                .onFailure(promise::fail);

        return promise.future();
    }

    @Override
    public Future<Void> migrateAllFiles(String sourceVolume, String targetVolume) {
        Promise<Void> promise = Promise.promise();

        log.info("Starting migration of all files from {} to {}", sourceVolume, targetVolume);

        getFilesOnVolume(sourceVolume).onComplete(filesResult -> {
            if (filesResult.failed()) {
                promise.fail(filesResult.cause());
                return;
            }

            List<String> fileIds = filesResult.result();
            if (fileIds.isEmpty()) {
                promise.complete();
                return;
            }

            log.info("Migrating {} files from {} to {}", fileIds.size(), sourceVolume, targetVolume);

            AtomicLong migrated = new AtomicLong(0);
            AtomicLong failed = new AtomicLong(0);

            for (String fileId : fileIds) {
                migrateFile(fileId, targetVolume).onComplete(result -> {
                    if (result.succeeded()) {
                        migrated.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                        log.error("Failed to migrate file {}: {}", fileId, result.cause().getMessage());
                    }

                    if (migrated.get() + failed.get() == fileIds.size()) {
                        log.info("Migration completed: migrated={}, failed={}", migrated.get(), failed.get());
                        if (failed.get() > 0) {
                            promise.fail("Migration partially failed: " + failed.get() + " files");
                        } else {
                            promise.complete();
                        }
                    }
                });
            }
        });

        return promise.future();
    }

    @Override
    public Future<Void> migrateFile(String fileId, String targetVolume) {
        Promise<Void> promise = Promise.promise();

        MetadataHelper.getMetadata(fileId).onComplete(metadataResult -> {
            if (metadataResult.failed()) {
                promise.fail(metadataResult.cause());
                return;
            }

            JsonObject metadata = metadataResult.result();
            String sourcePath = metadata.getString("filePath");
            String sourceVolume = extractVolumeFromPath(sourcePath);

            if (sourceVolume != null && sourceVolume.equals(targetVolume)) {
                log.info("File {} is already on target volume {}, skipping migration", fileId, targetVolume);
                promise.complete();
                return;
            }

            String fileName = metadata.getString("fileName");
            String targetPath = targetVolume + "/" + fileId + "_" + fileName;
            long size = metadata.getLong("size");

            log.info("Migrating file: {} from {} to {}", fileId, sourcePath, targetPath);

            MigrationTask task = new MigrationTask(fileId, sourcePath, targetPath, size, metadata);
            activeMigrations.put(fileId, task);

            journal.migrate(fileId, sourcePath, targetPath, size, metadata)
                    .onSuccess(v -> {
                        activeMigrations.remove(fileId);
                        updateVolumeStatistics(targetVolume, +1);
                        updateVolumeStatistics(sourceVolume, -1);
                        promise.complete();
                    })
                    .onFailure(err -> {
                        activeMigrations.remove(fileId);
                        promise.fail(err);
                    });
        });

        return promise.future();
    }

    @Override
    public Optional<MigrationTask> getMigrationStatus(String fileId) {
        return Optional.ofNullable(activeMigrations.get(fileId));
    }

    @Override
    public Future<Void> rollbackMigration(String fileId) {
        Promise<Void> promise = Promise.promise();

        MigrationTask task = activeMigrations.get(fileId);
        if (task == null) {
            promise.fail("No active migration for file: " + fileId);
            return promise.future();
        }

        journal.rollback(fileId)
                .onSuccess(v -> {
                    activeMigrations.remove(fileId);
                    promise.complete();
                })
                .onFailure(promise::fail);

        return promise.future();
    }

    private Future<List<String>> getFilesOnVolume(String volumePath) {
        Promise<List<String>> promise = Promise.promise();
        List<String> files = new ArrayList<>();

        MetadataHelper.listAllFiles().onComplete(filesResult -> {
            if (filesResult.succeeded()) {
                JsonArray allFiles = filesResult.result();
                for (int i = 0; i < allFiles.size(); i++) {
                    JsonObject file = allFiles.getJsonObject(i);
                    String filePath = file.getString("filePath");
                    if (filePath.startsWith(volumePath)) {
                        files.add(file.getString("fileId"));
                    }
                }
                promise.complete(files);
            } else {
                promise.fail(filesResult.cause());
            }
        });

        return promise.future();
    }

    private Future<Long> countFilesOnVolume(String volumePath) {
        Promise<Long> promise = Promise.promise();

        getFilesOnVolume(volumePath).onComplete(result -> {
            if (result.succeeded()) {
                promise.complete((long) result.result().size());
            } else {
                promise.complete(0L);
            }
        });

        return promise.future();
    }

    private Future<Void> unmountVolume(String volumePath) {
        Promise<Void> promise = Promise.promise();

        vertx.executeBlocking(p -> {
            try {
                Process process = Runtime.getRuntime()
                        .exec(new String[]{"umount", "-l", volumePath});
                int exitCode = process.waitFor();
                if (exitCode == 0) {
                    p.complete();
                } else {
                    p.fail("umount failed with code: " + exitCode);
                }
            } catch (Exception e) {
                p.fail(e);
            }
        }, false, result -> {
            if (result.succeeded()) {
                log.info("Volume unmounted: {}", volumePath);
                promise.complete();
            } else {
                log.warn("Failed to unmount {}: {}", volumePath, result.cause().getMessage());
                promise.complete();
            }
        });

        return promise.future();
    }

    private void updateVolumeStatistics(String volumePath, int delta) {
        VolumeState state = volumes.get(volumePath);
        if (state != null) {
            state.setFileCount(state.getFileCount() + delta);
            selector.updateVolumeStats(volumePath, state.getFileCount(), 0);
        }
    }

    private String extractVolumeFromPath(String filePath) {
        for (String volume : volumes.keySet()) {
            if (filePath.startsWith(volume)) {
                return volume;
            }
        }
        return null;
    }

    private void broadcastVolumeEvent(String event, String volumePath) {
        JsonObject message = new JsonObject()
                .put("type", "volume_" + event)
                .put("nodeId", nodeId)
                .put("volumePath", volumePath)
                .put("timestamp", System.currentTimeMillis());

        EventBusConfig.eventBus().publish("volume.events", message);
    }

    private void startHealthChecker() {
        vertx.setPeriodic(10000, id -> {
            for (VolumeState state : volumes.values()) {
                vertx.fileSystem().exists(state.getPath(), result -> {
                    boolean healthy = result.succeeded() && result.result();
                    if (healthy != state.isHealthy()) {
                        state.setHealthy(healthy);
                        log.warn("Volume {} health changed: {}", state.getPath(), healthy);
                    }
                });
            }
        });
    }

    private void recoverAfterCrash() {
        journal.recover().onComplete(result -> {
            if (result.succeeded()) {
                log.info("Migration recovery completed");
            } else {
                log.error("Migration recovery failed", result.cause());
            }
        });
    }

    @Override
    public void incrementFileCount(String volumePath) {
        VolumeState state = volumes.get(volumePath);
        if (state != null) {
            long newCount = state.incrementAndGetFileCount();
            selector.updateVolumeStats(volumePath, newCount, state.getUsedSpace());
            log.debug("Volume {} file count increased to {}", volumePath, newCount);
        }
    }

    @Override
    public void decrementFileCount(String volumePath) {
        VolumeState state = volumes.get(volumePath);
        if (state != null) {
            long newCount = state.decrementAndGetFileCount();
            selector.updateVolumeStats(volumePath, newCount, state.getUsedSpace());
            log.debug("Volume {} file count decreased to {}", volumePath, newCount);
        }
    }

    @Override
    public JsonObject getStatus() {
        JsonObject status = new JsonObject();
        status.put("nodeId", nodeId);
        status.put("strategy", selector.getStrategy().getName());

        JsonArray volumesArray = new JsonArray();
        for (VolumeState state : volumes.values()) {
            volumesArray.add(state.toJson());
        }
        status.put("volumes", volumesArray);

        JsonArray migrationsArray = new JsonArray();
        for (MigrationTask task : activeMigrations.values()) {
            migrationsArray.add(task.toJson());
        }
        status.put("activeMigrations", migrationsArray);

        return status;
    }

    public boolean isVolumeReadOnly(String volumePath) {
        VolumeState state = volumes.get(volumePath);
        return state != null && state.isReadOnly();
    }
}