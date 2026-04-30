package by.losik.verticle;

import by.losik.config.AppConfig;
import by.losik.config.ClusterManagerConfig;
import by.losik.config.EventBusConfig;
import by.losik.constant.AppConstants;
import by.losik.meta.FileMetadata;
import by.losik.service.FileQueryService;
import by.losik.volume.VolumeManager;
import by.losik.volume.VolumeManagerFacade;
import by.losik.volume.VolumeState;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.cluster.infinispan.InfinispanAsyncMap;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FileProcessorVerticle extends AbstractVerticle implements FileProcessor {
    private static final Logger log = LoggerFactory.getLogger(FileProcessorVerticle.class);
    public static final String FILE_METADATA_MAP = "file.metadata";
    private static final String FILE_METADATA_MAP_INDEXED = "file.metadata.indexed";
    private AsyncMap<String, FileMetadata> fileMetadataMap;
    private FileQueryService queryService;
    private VolumeManagerFacade volumeManager;

    @Override
    public void start(Promise<Void> startPromise) {
        String nodeId = AppConfig.nodeId();

        volumeManager = new VolumeManager(vertx, nodeId);

        log.info("Starting FileProcessorVerticle with {} volumes", volumeManager.getVolumes().size());
        log.info("Volume strategy: {}", volumeManager.getStrategy().getName());

        for (String volumePath : volumeManager.getVolumes().keySet()) {
            vertx.fileSystem().mkdirs(volumePath, mkdir -> {
                if (mkdir.succeeded()) {
                    log.debug("Volume directory ensured: {}", volumePath);
                } else {
                    log.warn("Failed to ensure volume directory: {}", volumePath, mkdir.cause());
                }
            });
        }

        if (AppConfig.isClustered()) {
            vertx.sharedData().<String, FileMetadata>getClusterWideMap(FILE_METADATA_MAP, ar -> {
                if (ar.succeeded()) {
                    fileMetadataMap = ar.result();
                    initQueryService();
                    setupEventBusConsumer();
                    restoreAllMetadataFromNfs(Promise.promise());
                } else {
                    log.error("Failed to initialize file metadata map", ar.cause());
                    startPromise.fail(ar.cause());
                }
            });
        } else {
            vertx.sharedData().<String, FileMetadata>getAsyncMap(FILE_METADATA_MAP, ar -> {
                if (ar.succeeded()) {
                    fileMetadataMap = ar.result();
                    setupEventBusConsumer();
                    startPromise.tryComplete();
                    log.info("FileProcessorVerticle started in non-clustered mode");
                } else {
                    log.error("Failed to initialize file metadata map", ar.cause());
                    startPromise.fail(ar.cause());
                }
            });
        }
    }

    private void restoreAllMetadataFromNfs(Promise<Void> startPromise) {
        List<String> volumes = new ArrayList<>(volumeManager.getVolumes().keySet());

        if (volumes.isEmpty()) {
            log.warn("No volumes configured, nothing to restore");
            startPromise.tryComplete();
            return;
        }

        vertx.sharedData().getLockWithTimeout("metadata-restore-lock", 60000, lockResult -> {
            if (lockResult.failed()) {
                log.info("Another node/instance is already restoring metadata, skipping");
                startPromise.tryComplete();
                return;
            }

            Lock lock = lockResult.result();
            log.info("Acquired restore lock, starting metadata restoration from {} volumes", volumes.size());

            AtomicInteger totalFiles = new AtomicInteger(0);
            AtomicInteger restored = new AtomicInteger(0);
            AtomicInteger failed = new AtomicInteger(0);
            AtomicInteger completedVolumes = new AtomicInteger(0);

            for (String volumePath : volumes) {
                scanVolumeAndRestore(volumePath, restored, failed, totalFiles)
                        .onComplete(v -> {
                            int completed = completedVolumes.incrementAndGet();
                            if (completed == volumes.size()) {
                                log.info("Metadata restoration completed: restored={}, failed={}, total={}",
                                        restored.get(), failed.get(), totalFiles.get());
                                lock.release();
                                startPromise.tryComplete();
                            }
                        });
            }
        });
    }

    @Override
    public Future<Void> scanVolumeAndRestore(String volumePath, AtomicInteger restored,
                                             AtomicInteger failed, AtomicInteger totalFiles) {
        Promise<Void> promise = Promise.promise();

        vertx.executeBlocking(p -> {
            List<Path> files = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(Path.of(volumePath))) {
                for (Path path : stream) {
                    files.add(path);
                }
            } catch (IOException e) {
                p.fail(e);
                return;
            }
            p.complete(files);
        }, asyncResult -> {
            if (asyncResult.failed()) {
                log.error("Failed to scan volume: {}", volumePath, asyncResult.cause());
                promise.fail(asyncResult.cause());
                return;
            }

            @SuppressWarnings("unchecked")
            List<Path> files = (List<Path>) asyncResult.result();
            totalFiles.addAndGet(files.size());

            if (files.isEmpty()) {
                log.info("No files found in volume: {}", volumePath);
                promise.complete();
                return;
            }

            log.info("Found {} files in volume: {}", files.size(), volumePath);

            AtomicInteger processed = new AtomicInteger(0);
            int total = files.size();

            for (Path path : files) {
                String fileName = path.getFileName().toString();
                int underscoreIndex = fileName.indexOf('_');

                if (underscoreIndex <= 0) {
                    log.warn("Skipping invalid file name format: {} in volume {}", fileName, volumePath);
                    restored.incrementAndGet();
                    if (processed.incrementAndGet() == total) {
                        promise.complete();
                    }
                    continue;
                }

                String fileId = fileName.substring(0, underscoreIndex);
                String actualFileName = fileName.substring(underscoreIndex + 1);

                fileMetadataMap.get(fileId, getResult -> {
                    if (getResult.succeeded() && getResult.result() != null) {
                        restored.incrementAndGet();
                        if (processed.incrementAndGet() == total) {
                            promise.complete();
                        }
                        return;
                    }

                    try {
                        FileTime lastModified = Files.getLastModifiedTime(path);
                        Instant updatedAt = lastModified.toInstant();
                        Instant createdAt = getCreationTime(path, updatedAt);

                        String contentType = Files.probeContentType(path);
                        if (contentType == null) {
                            contentType = "application/octet-stream";
                        }

                        FileMetadata metadata = new FileMetadata(
                                fileId, actualFileName, path.toString(), contentType,
                                Files.size(path), createdAt, updatedAt
                        );

                        fileMetadataMap.put(fileId, metadata, putResult -> {
                            if (putResult.succeeded()) {
                                syncToIndexedCache(fileId, metadata);
                                restored.incrementAndGet();
                                volumeManager.incrementFileCount(volumePath);
                                log.debug("Restored metadata for fileId={} on volume {}", fileId, volumePath);
                            } else {
                                log.error("Failed to restore metadata for fileId={}", fileId, putResult.cause());
                                failed.incrementAndGet();
                            }

                            if (processed.incrementAndGet() == total) {
                                promise.complete();
                            }
                        });
                    } catch (IOException e) {
                        log.error("Failed to read file: {}", path, e);
                        failed.incrementAndGet();
                        if (processed.incrementAndGet() == total) {
                            promise.complete();
                        }
                    }
                });
            }
        });

        return promise.future();
    }

    private Instant getCreationTime(Path path, Instant fallback) {
        try {
            FileTime creationTime = (FileTime) Files.getAttribute(path, "creationTime");
            return creationTime != null ? creationTime.toInstant() : fallback;
        } catch (Exception e) {
            return fallback;
        }
    }

    private void initQueryService() {
        try {
            EmbeddedCacheManager cacheManager = ClusterManagerConfig.getCacheContainer();
            Cache<String, FileMetadata> indexedCache = cacheManager.getCache(FILE_METADATA_MAP_INDEXED);
            queryService = new FileQueryService(vertx, indexedCache);
            log.info("QueryService initialized successfully with indexed cache");
        } catch (Exception e) {
            log.warn("Failed to initialize QueryService, falling back to slow list: {}", e.getMessage());
        }
    }

    private void setupEventBusConsumer() {
        log.debug("Setting up EventBus consumers");
        EventBusConfig.eventBus().consumer(EventBusConfig.FILE_UPLOAD_ADDRESS, this::handleRequest);
        EventBusConfig.eventBus().consumer(EventBusConfig.FILE_GET_ADDRESS, this::handleRequest);
        EventBusConfig.eventBus().consumer(EventBusConfig.FILE_DELETE_ADDRESS, this::handleRequest);
        EventBusConfig.eventBus().consumer(EventBusConfig.FILE_LIST_ADDRESS, this::handleRequest);
        EventBusConfig.eventBus().consumer(EventBusConfig.FILE_UPDATE_ADDRESS, this::handleRequest);
        EventBusConfig.eventBus().consumer(EventBusConfig.FILE_DOWNLOAD_ADDRESS, this::handleRequest);
        log.info("EventBus consumers registered");
    }

    @Override
    public void handleRequest(@NotNull Message<Object> message) {
        JsonObject msg = (JsonObject) message.body();
        String action = msg.getString(AppConstants.FIELD_ACTION);

        Optional.ofNullable(action)
                .map(this::resolveAction)
                .ifPresentOrElse(
                        handler -> handler.handle(msg, message),
                        () -> {
                            log.warn("Unknown action: {}", action);
                            message.fail(AppConstants.HTTP_BAD_REQUEST, AppConstants.ERR_UNKNOWN_ACTION);
                        }
                );
    }

    private ActionHandler resolveAction(String action) {
        return switch (action) {
            case AppConstants.ACTION_UPLOAD -> this::handleUpload;
            case AppConstants.ACTION_UPDATE -> this::handleUpdate;
            case AppConstants.ACTION_DELETE -> this::handleDelete;
            case AppConstants.ACTION_GET -> this::handleGet;
            case AppConstants.ACTION_LIST -> this::handleList;
            case AppConstants.ACTION_DOWNLOAD -> this::handleDownload;
            default -> null;
        };
    }

    @Override
    public void handleUpload(@NotNull JsonObject msg, Message<Object> message) {
        String fileId = msg.getString(AppConstants.FIELD_FILE_ID);
        String fileName = msg.getString(AppConstants.FIELD_FILE_NAME);
        String tempPath = msg.getString(AppConstants.FIELD_FILE_PATH);
        String contentType = msg.getString(AppConstants.FIELD_CONTENT_TYPE);

        if (fileName == null || fileName.isBlank()) {
            log.error("Upload rejected: fileName is null or blank for fileId={}", fileId);
            message.fail(AppConstants.HTTP_BAD_REQUEST, "fileName is required");
            return;
        }

        FileMetadata tempMetadata = new FileMetadata(
                fileId, fileName, "", contentType, 0, Instant.now(), Instant.now()
        );

        String selectedVolume = volumeManager.selectVolume(tempMetadata);
        String targetPath = selectedVolume + "/" + fileId + "_" + fileName;

        log.info("Handling upload: fileId={}, fileName={}, targetVolume={}", fileId, fileName, selectedVolume);

        withFileLock(fileId, (metadataMap, releaseLock) ->
                metadataMap.get(fileId, getResult -> {
                    if (getResult.succeeded() && getResult.result() != null) {
                        log.warn("Upload failed - file already exists: fileId={}", fileId);
                        releaseLock.run();
                        message.fail(AppConstants.HTTP_CONFLICT, "File with this ID already exists");
                        return;
                    }

                    vertx.fileSystem().props(tempPath, propsResult -> {
                        if (propsResult.failed()) {
                            log.error("Failed to get props for temp file: {}", tempPath, propsResult.cause());
                            releaseLock.run();
                            message.fail(AppConstants.HTTP_INTERNAL_ERROR, "Failed to read uploaded file");
                            return;
                        }

                        long size = propsResult.result().size();
                        Instant now = Instant.now();

                        FileMetadata metadata = new FileMetadata(
                                fileId, fileName, targetPath, contentType, size, now, now
                        );

                        vertx.fileSystem().copy(tempPath, targetPath, copyResult -> {
                            if (copyResult.failed()) {
                                log.error("Failed to copy file to {}", targetPath, copyResult.cause());
                                releaseLock.run();
                                message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_COPY_TO_NFS);
                                return;
                            }

                            metadataMap.put(fileId, metadata, putResult -> {
                                vertx.fileSystem().delete(tempPath)
                                        .onFailure(err -> log.warn("Temp cleanup failed: {}", tempPath, err));

                                if (putResult.succeeded()) {
                                    syncToIndexedCache(fileId, metadata);
                                    volumeManager.incrementFileCount(selectedVolume);
                                    releaseLock.run();
                                    log.info("Upload completed: fileId={}, size={}", fileId, size);
                                    message.reply(new JsonObject().put(AppConstants.FIELD_STATUS, AppConstants.STATUS_OK));
                                } else {
                                    log.error("Failed to save metadata for {}", fileId, putResult.cause());
                                    vertx.fileSystem().delete(targetPath)
                                            .onFailure(deleteErr -> log.warn("Failed to cleanup target file"));
                                    releaseLock.run();
                                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_SAVE_METADATA);
                                }
                            });
                        });
                    });
                }), message);
    }

    @Override
    public void handleUpdate(@NotNull JsonObject msg, Message<Object> message) {
        String fileId = msg.getString(AppConstants.FIELD_FILE_ID);
        String fileName = msg.getString(AppConstants.FIELD_FILE_NAME);
        String tempPath = msg.getString(AppConstants.FIELD_FILE_PATH);
        String contentType = msg.getString(AppConstants.FIELD_CONTENT_TYPE);
        boolean skipFileCopy = msg.getBoolean("skipFileCopy", false);
        Long providedSize = msg.getLong("size");

        if (fileName == null || fileName.isBlank()) {
            log.error("Update rejected: fileName is null or blank for fileId={}", fileId);
            message.fail(AppConstants.HTTP_BAD_REQUEST, "fileName is required for update");
            return;
        }

        log.info("Update: fileId={}, newFileName={}, skipFileCopy={}", fileId, fileName, skipFileCopy);

        withFileLock(fileId, (map, releaseLock) ->
                map.get(fileId, getResult -> {
                    if (getResult.failed() || getResult.result() == null) {
                        releaseLock.run();
                        message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
                        return;
                    }

                    FileMetadata oldMetadata = getResult.result();
                    String oldVolume = extractVolumeFromPath(oldMetadata.getFilePath());
                    Instant now = Instant.now();

                    if (skipFileCopy) {
                        long size = providedSize != null ? providedSize : oldMetadata.getSize();
                        String targetPath = tempPath != null ? tempPath : oldMetadata.getFilePath();

                        FileMetadata newMetadata = new FileMetadata(
                                fileId, fileName, targetPath, contentType,
                                size, oldMetadata.getCreatedAt(), now
                        );

                        map.put(fileId, newMetadata, putResult -> {
                            try {
                                if (putResult.succeeded()) {
                                    syncToIndexedCache(fileId, newMetadata);
                                    if (!targetPath.equals(oldMetadata.getFilePath())) {
                                        volumeManager.decrementFileCount(oldVolume);
                                        String newVolume = extractVolumeFromPath(targetPath);
                                        if (newVolume != null) {
                                            volumeManager.incrementFileCount(newVolume);
                                        }
                                    }
                                    log.info("Metadata update completed for fileId={}", fileId);
                                    message.reply(new JsonObject().put(AppConstants.FIELD_STATUS, AppConstants.STATUS_UPDATED));
                                } else {
                                    log.error("Failed to save metadata for {}", fileId, putResult.cause());
                                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_SAVE_METADATA);
                                }
                            } finally {
                                releaseLock.run();
                            }
                        });
                        return;
                    }

                    if (tempPath == null || tempPath.isBlank()) {
                        log.error("Update rejected: tempPath is null or blank for fileId={}", fileId);
                        releaseLock.run();
                        message.fail(AppConstants.HTTP_BAD_REQUEST, "file data is required for update");
                        return;
                    }

                    String selectedVolume = volumeManager.selectVolume(oldMetadata);
                    String targetPath = selectedVolume + "/" + fileId + "_" + fileName;

                    vertx.fileSystem().props(tempPath, propsResult -> {
                        if (propsResult.failed()) {
                            log.error("Failed to get props for temp file: {}", tempPath, propsResult.cause());
                            releaseLock.run();
                            message.fail(AppConstants.HTTP_INTERNAL_ERROR, "Failed to read uploaded file");
                            return;
                        }

                        long size = propsResult.result().size();

                        FileMetadata newMetadata = new FileMetadata(
                                fileId, fileName, targetPath, contentType,
                                size, oldMetadata.getCreatedAt(), now
                        );

                        vertx.fileSystem().delete(oldMetadata.getFilePath(), deleteResult -> {
                            if (deleteResult.failed()) {
                                log.warn("Failed to delete old file: {}, continuing anyway", oldMetadata.getFilePath(), deleteResult.cause());
                            }

                            vertx.fileSystem().copy(tempPath, targetPath, copyResult -> {
                                if (copyResult.failed()) {
                                    log.error("Failed to copy file to {}", targetPath, copyResult.cause());
                                    releaseLock.run();
                                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_COPY_TO_NFS);
                                    return;
                                }

                                map.put(fileId, newMetadata, putResult -> {
                                    vertx.fileSystem().delete(tempPath)
                                            .onFailure(err -> log.warn("Temp cleanup failed: {}", tempPath, err));

                                    syncToIndexedCache(fileId, newMetadata);
                                    volumeManager.decrementFileCount(oldVolume);
                                    volumeManager.incrementFileCount(selectedVolume);
                                    releaseLock.run();

                                    if (putResult.succeeded()) {
                                        log.info("Update completed: fileId={}", fileId);
                                        message.reply(new JsonObject().put(AppConstants.FIELD_STATUS, AppConstants.STATUS_UPDATED));
                                    } else {
                                        log.error("Failed to save metadata for {}", fileId, putResult.cause());
                                        message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_SAVE_METADATA);
                                    }
                                });
                            });
                        });
                    });
                }), message);
    }

    @Override
    public void handleDelete(@NotNull JsonObject msg, Message<Object> message) {
        String fileId = msg.getString(AppConstants.FIELD_FILE_ID);
        log.info("Handling delete: fileId={}", fileId);

        withFileLock(fileId, (metadataMap, releaseLock) ->
                metadataMap.remove(fileId, removeResult -> {
                    if (removeResult.succeeded()) {
                        FileMetadata metadata = removeResult.result();
                        if (metadata != null) {
                            String volumePath = extractVolumeFromPath(metadata.getFilePath());

                            removeFromIndexedCache(fileId);
                            vertx.fileSystem().delete(metadata.getFilePath(), deleteResult -> {
                                volumeManager.decrementFileCount(volumePath);
                                releaseLock.run();
                                if (deleteResult.succeeded()) {
                                    log.info("Delete completed: fileId={}", fileId);
                                    message.reply(new JsonObject().put(AppConstants.FIELD_STATUS, AppConstants.STATUS_DELETED));
                                } else {
                                    log.warn("File deleted but physical file may remain: {}", fileId);
                                    message.reply(new JsonObject().put(AppConstants.FIELD_STATUS, AppConstants.STATUS_DELETED_BUT_FILE_MAY_REMAIN));
                                }
                            });
                        } else {
                            releaseLock.run();
                            message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
                        }
                    } else {
                        releaseLock.run();
                        message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_REMOVE_METADATA);
                    }
                }), message);
    }

    @Override
    public void handleGet(@NotNull JsonObject msg, Message<Object> message) {
        String fileId = msg.getString(AppConstants.FIELD_FILE_ID);
        log.debug("Get metadata: fileId={}", fileId);

        fileMetadataMap.get(fileId, getResult -> {
            if (getResult.succeeded() && getResult.result() != null) {
                message.reply(metadataToJson(getResult.result()));
            } else {
                restoreFileFromVolumes(fileId, message);
            }
        });
    }

    private void restoreFileFromVolumes(String fileId, Message<Object> message) {
        List<String> volumes = new ArrayList<>(volumeManager.getVolumes().keySet());
        log.debug("Searching for file {} across {} volumes", fileId, volumes.size());

        findFileInVolumes(fileId, volumes, 0, message);
    }

    private void findFileInVolumes(String fileId, @NotNull List<String> volumes, int index, Message<Object> message) {
        if (index >= volumes.size()) {
            message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
            return;
        }

        String volumePath = volumes.get(index);

        vertx.executeBlocking(promise -> {
            Path found = null;
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(Path.of(volumePath))) {
                for (Path path : stream) {
                    if (path.getFileName().toString().startsWith(fileId + "_")) {
                        found = path;
                        break;
                    }
                }
            } catch (IOException e) {
                promise.fail(e);
                return;
            }
            promise.complete(found);
        }, asyncResult -> {
            if (asyncResult.succeeded() && asyncResult.result() != null) {
                Path filePath = (Path) asyncResult.result();
                restoreAndCacheMetadata(fileId, filePath, message);
            } else {
                findFileInVolumes(fileId, volumes, index + 1, message);
            }
        });
    }

    private void restoreAndCacheMetadata(String fileId, @NotNull Path filePath, Message<Object> message) {
        String fullFileName = filePath.getFileName().toString();
        int underscoreIndex = fullFileName.indexOf('_');
        String fileName = underscoreIndex > 0 ? fullFileName.substring(underscoreIndex + 1) : fullFileName;

        vertx.fileSystem().props(filePath.toString(), propsResult -> {
            if (propsResult.succeeded()) {
                Instant now = Instant.now();
                FileMetadata metadata = new FileMetadata(
                        fileId, fileName, filePath.toString(), "application/octet-stream",
                        propsResult.result().size(), now, now
                );

                fileMetadataMap.put(fileId, metadata, putResult -> {
                    if (putResult.succeeded()) {
                        syncToIndexedCache(fileId, metadata);
                        String volume = extractVolumeFromPath(filePath.toString());
                        volumeManager.incrementFileCount(volume);
                        log.info("Restored metadata for fileId={} from volume {}", fileId, volume);
                        message.reply(metadataToJson(metadata));
                    } else {
                        log.error("Failed to cache restored metadata for {}", fileId, putResult.cause());
                        message.fail(AppConstants.HTTP_INTERNAL_ERROR, "Failed to cache restored metadata");
                    }
                });
            } else {
                message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
            }
        });
    }

    @Override
    public void handleList(@NotNull JsonObject msg, Message<Object> message) {
        int page = msg.getInteger(AppConstants.FIELD_PAGE, 1);
        int size = msg.getInteger(AppConstants.FIELD_SIZE, AppConfig.defaultPageSize());
        size = Math.min(size, AppConfig.maxPageSize());
        String prefix = msg.getString(AppConstants.FIELD_PREFIX, "");
        String sort = msg.getString(AppConstants.FIELD_SORT, "name");
        String order = msg.getString(AppConstants.FIELD_ORDER, "asc");

        if (queryService != null) {
            queryService.listFiles(page, size, prefix, sort, order)
                    .onSuccess(message::reply)
                    .onFailure(err -> {
                        log.error("QueryService list failed", err);
                        message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_LIST_FAILED);
                    });
        } else if (AppConfig.isClustered()) {
            handleListWithKeyStream(page, size, prefix, sort, order, message);
        } else {
            handleListWithFileSystemScan(page, size, prefix, sort, order, message);
        }
    }

    private void handleListWithFileSystemScan(int page, int size, String prefix,
                                              String sort, String order, Message<Object> message) {
        boolean asc = !"desc".equalsIgnoreCase(order);
        List<String> volumes = new ArrayList<>(volumeManager.getVolumes().keySet());

        vertx.executeBlocking(promise -> {
            List<FileMetadata> allMetadata = new ArrayList<>();

            for (String volumePath : volumes) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(Path.of(volumePath))) {
                    for (Path path : stream) {
                        String fileName = path.getFileName().toString();
                        int underscoreIndex = fileName.indexOf('_');
                        if (underscoreIndex > 0) {
                            String fileId = fileName.substring(0, underscoreIndex);
                            String actualFileName = fileName.substring(underscoreIndex + 1);
                            if (prefix.isEmpty() || actualFileName.startsWith(prefix)) {
                                try {
                                    FileTime lastModified = Files.getLastModifiedTime(path);
                                    Instant updatedAt = lastModified.toInstant();
                                    Instant createdAt = getCreationTime(path, updatedAt);
                                    String contentType = Files.probeContentType(path);
                                    if (contentType == null) contentType = "application/octet-stream";

                                    FileMetadata meta = new FileMetadata(
                                            fileId, actualFileName, path.toString(), contentType,
                                            Files.size(path), createdAt, updatedAt
                                    );
                                    allMetadata.add(meta);
                                } catch (IOException e) {
                                    log.warn("Failed to read file metadata for: {}", path, e);
                                }
                            }
                        }
                    }
                } catch (IOException e) {
                    log.warn("Failed to scan volume: {}", volumePath, e);
                }
            }
            promise.complete(allMetadata);
        }, asyncResult -> {
            if (asyncResult.succeeded()) {
                @SuppressWarnings("unchecked")
                List<FileMetadata> allMetadata = (List<FileMetadata>) asyncResult.result();
                processResults(allMetadata, page, size, sort, asc, message);
            } else {
                log.error("Filesystem scan failed", asyncResult.cause());
                message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_LIST_FAILED);
            }
        });
    }

    private void handleListWithKeyStream(int page, int size, String prefix,
                                         String sort, String order, Message<Object> message) {
        boolean asc = !"desc".equalsIgnoreCase(order);

        try {
            InfinispanAsyncMap<String, FileMetadata> infinispanMap = InfinispanAsyncMap.unwrap(fileMetadataMap);
            ReadStream<String> keyStream = infinispanMap.keyStream();

            List<FileMetadata> allMetadata = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger pending = new AtomicInteger(0);
            AtomicInteger keysProcessed = new AtomicInteger(0);

            keyStream.handler(key -> {
                keysProcessed.incrementAndGet();
                pending.incrementAndGet();

                fileMetadataMap.get(key, getResult -> {
                    try {
                        if (getResult.succeeded() && getResult.result() != null) {
                            FileMetadata meta = getResult.result();
                            if (prefix.isEmpty() || meta.getFileName().startsWith(prefix)) {
                                allMetadata.add(meta);
                            }
                        }
                    } finally {
                        if (pending.decrementAndGet() == 0) {
                            processResults(allMetadata, page, size, sort, asc, message);
                        }
                    }
                });
            });

            keyStream.endHandler(v -> {
                if (pending.get() == 0) {
                    processResults(allMetadata, page, size, sort, asc, message);
                }
                log.debug("Key stream completed, total keys: {}", keysProcessed.get());
            });

            keyStream.exceptionHandler(err -> {
                log.error("Key stream failed", err);
                message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_LIST_FAILED);
            });

        } catch (Exception e) {
            log.error("Failed to unwrap InfinispanAsyncMap", e);
            message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_LIST_FAILED);
        }
    }

    private void processResults(@NotNull List<FileMetadata> metadata, int page, int size,
                                String sort, boolean asc, Message<Object> message) {
        metadata.sort((a, b) -> {
            int cmp = switch (sort) {
                case "size" -> Long.compare(a.getSize(), b.getSize());
                case "date", "createdAt" -> a.getCreatedAt().compareTo(b.getCreatedAt());
                case "updatedAt" -> a.getUpdatedAt().compareTo(b.getUpdatedAt());
                default -> a.getFileName().compareTo(b.getFileName());
            };
            return asc ? cmp : -cmp;
        });

        int total = metadata.size();
        int start = (page - 1) * size;
        int end = Math.min(start + size, total);

        JsonArray filesArray = new JsonArray();
        for (int i = start; i < end; i++) {
            FileMetadata m = metadata.get(i);
            filesArray.add(metadataToJson(m));
        }

        message.reply(new JsonObject()
                .put(AppConstants.FIELD_FILES, filesArray)
                .put(AppConstants.FIELD_PAGE, page)
                .put(AppConstants.FIELD_SIZE, size)
                .put(AppConstants.FIELD_TOTAL, total));
    }

    @Override
    public void handleDownload(@NotNull JsonObject msg, Message<Object> message) {
        String fileId = msg.getString(AppConstants.FIELD_FILE_ID);

        fileMetadataMap.get(fileId, getResult -> {
            if (getResult.succeeded() && getResult.result() != null) {
                FileMetadata metadata = getResult.result();
                String filePath = metadata.getFilePath();
                String fileName = metadata.getFileName();
                String contentType = metadata.getContentType();

                vertx.fileSystem().exists(filePath, existsResult -> {
                    if (existsResult.succeeded() && existsResult.result()) {
                        vertx.fileSystem().props(filePath, propsResult -> {
                            if (propsResult.succeeded()) {
                                JsonObject response = new JsonObject()
                                        .put(AppConstants.FIELD_STATUS, AppConstants.STATUS_OK)
                                        .put(AppConstants.FIELD_FILE_PATH, filePath)
                                        .put(AppConstants.FIELD_FILE_NAME, fileName)
                                        .put(AppConstants.FIELD_CONTENT_TYPE, contentType)
                                        .put(AppConstants.FIELD_SIZE, propsResult.result().size());
                                message.reply(response);
                            } else {
                                message.fail(AppConstants.HTTP_INTERNAL_ERROR, "Failed to get file properties");
                            }
                        });
                    } else {
                        message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
                    }
                });
            } else {
                message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
            }
        });
    }

    private void withFileLock(String fileId, RunnableWithCompletion action, Message<Object> message) {
        if (fileMetadataMap == null) {
            message.fail(AppConstants.HTTP_INTERNAL_ERROR, "Metadata map not initialized");
            return;
        }

        long operationTimeout = 60000;
        AtomicBoolean completed = new AtomicBoolean(false);

        vertx.sharedData().getLockWithTimeout("file-lock-" + fileId, AppConfig.fileUploadTimeout(), lockResult -> {
            if (lockResult.failed()) {
                message.fail(AppConstants.HTTP_INTERNAL_ERROR, "Failed to acquire lock: " + lockResult.cause().getMessage());
                return;
            }

            Lock lock = lockResult.result();
            AtomicInteger released = new AtomicInteger(0);

            Runnable safeRelease = () -> {
                if (released.compareAndSet(0, 1)) {
                    lock.release();
                    log.debug("Lock released for fileId={}", fileId);
                }
            };

            long timerId = vertx.setTimer(operationTimeout, tid -> {
                if (!completed.get()) {
                    log.error("Operation timeout for fileId={}, forcing lock release", fileId);
                    safeRelease.run();
                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, "Operation timeout");
                }
            });

            try {
                action.run(fileMetadataMap, () -> {
                    if (!completed.getAndSet(true)) {
                        vertx.cancelTimer(timerId);
                        safeRelease.run();
                    }
                });
            } catch (Exception e) {
                if (!completed.getAndSet(true)) {
                    vertx.cancelTimer(timerId);
                    safeRelease.run();
                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, e.getMessage());
                }
            }
        });
    }

    private @Nullable String extractVolumeFromPath(String filePath) {
        for (VolumeState state : volumeManager.getVolumes().values()) {
            if (filePath.startsWith(state.getPath())) {
                return state.getPath();
            }
        }
        return null;
    }

    private void syncToIndexedCache(String fileId, FileMetadata metadata) {
        Optional.ofNullable(queryService).ifPresent(
                fileQueryService -> {
                    try {
                        ClusterManagerConfig.getCacheContainer()
                                .getCache(FILE_METADATA_MAP_INDEXED)
                                .put(fileId, metadata);
                        log.debug("Synced to indexed cache: fileId={}", fileId);
                    } catch (Exception e) {
                        log.warn("Failed to sync to indexed cache: {}", e.getMessage());
                    }
                }
        );
    }

    private void removeFromIndexedCache(String fileId) {
        Optional.ofNullable(queryService).ifPresent(
                fileQueryService -> {
                    try {
                        ClusterManagerConfig.getCacheContainer()
                                .getCache(FILE_METADATA_MAP_INDEXED)
                                .remove(fileId);
                        log.debug("Removed from indexed cache: fileId={}", fileId);
                    } catch (Exception e) {
                        log.warn("Failed to remove from indexed cache: {}", e.getMessage());
                    }
                });
    }

    private JsonObject metadataToJson(@NotNull FileMetadata m) {
        return new JsonObject()
                .put(AppConstants.FIELD_FILE_ID, m.getFileId())
                .put(AppConstants.FIELD_FILE_NAME, m.getFileName())
                .put(AppConstants.FIELD_FILE_PATH, m.getFilePath())
                .put(AppConstants.FIELD_CONTENT_TYPE, m.getContentType())
                .put(AppConstants.FIELD_SIZE, m.getSize())
                .put(AppConstants.FIELD_CREATED_AT, m.getCreatedAt().toString())
                .put(AppConstants.FIELD_UPDATED_AT, m.getUpdatedAt().toString());
    }
}