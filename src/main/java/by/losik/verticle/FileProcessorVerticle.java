package by.losik.verticle;

import by.losik.config.AppConfig;
import by.losik.config.ClusterManagerConfig;
import by.losik.config.EventBusConfig;
import by.losik.constant.AppConstants;
import by.losik.meta.FileMetadata;
import by.losik.service.FileQueryService;
import io.vertx.core.AbstractVerticle;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class FileProcessorVerticle extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(FileProcessorVerticle.class);
    private static final String FILE_METADATA_MAP = "file.metadata";
    private static final String FILE_METADATA_MAP_INDEXED = "file.metadata.indexed";
    private AsyncMap<String, FileMetadata> fileMetadataMap;
    private FileQueryService queryService;
    private String nfsPath;

    @Override
    public void start(Promise<Void> startPromise) {
        this.nfsPath = AppConfig.nfsPath();
        log.info("Starting FileProcessorVerticle with NFS path: {}", nfsPath);

        vertx.fileSystem().mkdirsBlocking(nfsPath);
        log.debug("NFS directory ensured at: {}", nfsPath);

        vertx.sharedData().<String, FileMetadata>getClusterWideMap(FILE_METADATA_MAP, ar -> {
            if (ar.succeeded()) {
                fileMetadataMap = ar.result();
                initQueryService();
                setupEventBusConsumer();
                startPromise.complete();
                log.info("FileProcessorVerticle started successfully");
            } else {
                log.error("Failed to initialize file metadata map: {}", ar.cause().getMessage(), ar.cause());
                startPromise.fail(ar.cause());
            }
        });
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
        log.info("EventBus consumers registered for addresses: {}, {}, {}, {}, {}",
                EventBusConfig.FILE_UPLOAD_ADDRESS,
                EventBusConfig.FILE_GET_ADDRESS,
                EventBusConfig.FILE_DELETE_ADDRESS,
                EventBusConfig.FILE_LIST_ADDRESS,
                EventBusConfig.FILE_UPDATE_ADDRESS);
    }

    private void handleRequest(@NotNull Message<Object> message) {
        JsonObject msg = (JsonObject) message.body();
        String action = msg.getString(AppConstants.FIELD_ACTION);

        log.debug("Received request with action: {}, reply address: {}", action, message.replyAddress());

        Optional.ofNullable(action)
                .map(this::resolveAction)
                .ifPresentOrElse(
                        handler -> {
                            log.debug("Dispatching action '{}' to handler", action);
                            handler.handle(msg, message);
                        },
                        () -> {
                            log.warn("Unknown action received: {}, from: {}", action, message.replyAddress());
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
            default -> null;
        };
    }

    private void handleUpload(@NotNull JsonObject msg, Message<Object> message) {
        String fileId = msg.getString(AppConstants.FIELD_FILE_ID);
        String fileName = msg.getString(AppConstants.FIELD_FILE_NAME);
        String tempPath = msg.getString(AppConstants.FIELD_FILE_PATH);
        String targetPath = nfsPath + "/" + fileId + "_" + fileName;
        String contentType = msg.getString(AppConstants.FIELD_CONTENT_TYPE);

        log.info("Handling upload: fileId={}, fileName={}, tempPath={}", fileId, fileName, tempPath);

        withFileLock(fileId, (metadataMap, releaseLock) ->
                metadataMap.get(fileId, getResult -> {
                    if (getResult.succeeded() && getResult.result() != null) {
                        log.warn("Upload failed - file already exists: fileId={}", fileId);
                        releaseLock.run();
                        message.fail(AppConstants.HTTP_CONFLICT, "File with this ID already exists");
                        return;
                    }

                    vertx.fileSystem().props(tempPath, propsResult -> {
                        long size = propsResult.succeeded() ? propsResult.result().size() : 0;
                        Instant now = Instant.now();

                        log.debug("File properties - size: {} bytes, tempPath: {}", size, tempPath);

                        var metadata = new FileMetadata(
                                fileId, fileName, targetPath, contentType,
                                size, now, now
                        );

                        vertx.fileSystem().copy(tempPath, targetPath, copyResult -> {
                            if (copyResult.succeeded()) {
                                log.debug("File copied successfully from {} to {}", tempPath, targetPath);
                                metadataMap.put(fileId, metadata, putResult -> {
                                    vertx.fileSystem().delete(tempPath)
                                            .onFailure(err -> log.warn("Temp file cleanup failed: {}", tempPath, err));

                                    if (putResult.succeeded()) {
                                        syncToIndexedCache(fileId, metadata);

                                        vertx.fileSystem().delete(tempPath, deleteResult -> {
                                            if (deleteResult.failed()) {
                                                log.error("Failed to delete temp file: {}, error: {}", tempPath, deleteResult.cause().getMessage());
                                            } else {
                                                log.debug("Temp file deleted: {}", tempPath);
                                            }
                                        });
                                        releaseLock.run();
                                        log.info("Upload completed successfully: fileId={}, fileName={}, size={}", fileId, fileName, size);
                                        message.reply(new JsonObject().put(AppConstants.FIELD_STATUS, AppConstants.STATUS_OK));
                                    } else {
                                        log.error("Failed to save metadata for fileId={}: {}", fileId, putResult.cause().getMessage());
                                        deleteFileOnly(targetPath,
                                                () -> {
                                                    releaseLock.run();
                                                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_SAVE_METADATA);
                                                },
                                                () -> {
                                                    log.error("Cleanup failed for targetPath: {}", targetPath);
                                                    releaseLock.run();
                                                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_SAVE_METADATA + " and cleanup failed");
                                                }
                                        );
                                    }
                                });
                            } else {
                                log.error("Failed to copy file to NFS from {} to {}: {}", tempPath, targetPath, copyResult.cause().getMessage());
                                releaseLock.run();
                                message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_COPY_TO_NFS);
                            }
                        });
                    });
                }), message);
    }

    private void handleUpdate(@NotNull JsonObject msg, Message<Object> message) {
        String fileId = msg.getString(AppConstants.FIELD_FILE_ID);
        String fileName = msg.getString(AppConstants.FIELD_FILE_NAME);
        String tempPath = msg.getString(AppConstants.FIELD_FILE_PATH);
        String targetPath = nfsPath + "/" + fileId + "_" + fileName;
        String contentType = msg.getString(AppConstants.FIELD_CONTENT_TYPE);

        log.info("Handling update: fileId={}, newFileName={}, tempPath={}", fileId, fileName, tempPath);

        withFileLock(fileId, (metadataMap, releaseLock) ->
                metadataMap.get(fileId, getResult -> {
                    if (getResult.failed() || getResult.result() == null) {
                        log.warn("Update failed - file not found: fileId={}", fileId);
                        releaseLock.run();
                        message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
                        return;
                    }

                    FileMetadata oldMetadata = getResult.result();
                    log.debug("Found existing file: fileId={}, oldPath={}", fileId, oldMetadata.getFilePath());

                    vertx.fileSystem().props(tempPath, propsResult -> {
                        long size = propsResult.succeeded() ? propsResult.result().size() : 0;
                        Instant now = Instant.now();

                        var newMetadata = new FileMetadata(
                                fileId, fileName, targetPath, contentType,
                                size, oldMetadata.getCreatedAt(), now
                        );

                        vertx.fileSystem().delete(oldMetadata.getFilePath(), deleteResult -> {
                            if (deleteResult.failed()) {
                                log.warn("Failed to delete old file {}: {}", oldMetadata.getFilePath(), deleteResult.cause().getMessage());
                            } else {
                                log.debug("Old file deleted: {}", oldMetadata.getFilePath());
                            }

                            vertx.fileSystem().copy(tempPath, targetPath, copyResult -> {
                                if (copyResult.succeeded()) {
                                    log.debug("New file copied to: {}", targetPath);
                                    metadataMap.put(fileId, newMetadata, putResult -> {
                                        vertx.fileSystem().delete(tempPath)
                                                .onFailure(err -> log.warn("Temp file cleanup failed: {}", tempPath, err));

                                        if (putResult.succeeded()) {
                                            syncToIndexedCache(fileId, newMetadata);

                                            releaseLock.run();
                                            log.info("Update completed successfully: fileId={}, newFileName={}, size={}", fileId, fileName, size);
                                            message.reply(new JsonObject().put(AppConstants.FIELD_STATUS, AppConstants.STATUS_UPDATED));
                                        } else {
                                            log.error("Failed to update metadata for fileId={}: {}", fileId, putResult.cause().getMessage());
                                            deleteFileOnly(targetPath,
                                                    () -> {
                                                        releaseLock.run();
                                                        message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_SAVE_METADATA);
                                                    },
                                                    () -> {
                                                        log.error("Cleanup failed for targetPath: {}", targetPath);
                                                        releaseLock.run();
                                                        message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_SAVE_METADATA + " and cleanup failed");
                                                    }
                                            );
                                        }
                                    });
                                } else {
                                    log.error("Failed to copy file for update from {} to {}: {}", tempPath, targetPath, copyResult.cause().getMessage());
                                    releaseLock.run();
                                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_COPY_TO_NFS);
                                }
                            });
                        });
                    });
                }), message);
    }

    private void handleDelete(@NotNull JsonObject msg, Message<Object> message) {
        String fileId = msg.getString(AppConstants.FIELD_FILE_ID);
        log.info("Handling delete: fileId={}", fileId);

        withFileLock(fileId, (metadataMap, releaseLock) ->
                metadataMap.remove(fileId, removeResult -> {
                    if (removeResult.succeeded()) {
                        FileMetadata metadata = removeResult.result();
                        if (metadata != null) {
                            log.debug("Metadata removed for fileId={}, path={}", fileId, metadata.getFilePath());

                            removeFromIndexedCache(fileId);

                            vertx.fileSystem().delete(metadata.getFilePath(), deleteResult -> {
                                releaseLock.run();
                                if (deleteResult.succeeded()) {
                                    log.info("Delete completed successfully: fileId={}", fileId);
                                    message.reply(new JsonObject().put(AppConstants.FIELD_STATUS, AppConstants.STATUS_DELETED));
                                } else {
                                    log.warn("File deleted but physical file may remain: fileId={}, path={}, error={}",
                                            fileId, metadata.getFilePath(), deleteResult.cause().getMessage());
                                    message.reply(new JsonObject().put(AppConstants.FIELD_STATUS, AppConstants.STATUS_DELETED_BUT_FILE_MAY_REMAIN));
                                }
                            });
                        } else {
                            log.warn("Delete failed - file not found: fileId={}", fileId);
                            releaseLock.run();
                            message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
                        }
                    } else {
                        log.error("Failed to remove metadata for fileId={}: {}", fileId, removeResult.cause().getMessage());
                        releaseLock.run();
                        message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_REMOVE_METADATA);
                    }
                }), message);
    }

    private void handleGet(@NotNull JsonObject msg, Message<Object> message) {
        String fileId = msg.getString(AppConstants.FIELD_FILE_ID);
        log.debug("Handling get metadata: fileId={}", fileId);

        fileMetadataMap.get(fileId, getResult ->
                Optional.ofNullable(getResult.result())
                        .ifPresentOrElse(
                                metadata -> {
                                    log.debug("Metadata found for fileId={}, fileName={}", fileId, metadata.getFileName());
                                    message.reply(new JsonObject()
                                            .put(AppConstants.FIELD_FILE_ID, metadata.getFileId())
                                            .put(AppConstants.FIELD_FILE_NAME, metadata.getFileName())
                                            .put(AppConstants.FIELD_CONTENT_TYPE, metadata.getContentType())
                                            .put(AppConstants.FIELD_FILE_PATH, metadata.getFilePath())
                                            .put(AppConstants.FIELD_CREATED_AT, metadata.getCreatedAt().toString())
                                            .put(AppConstants.FIELD_UPDATED_AT, metadata.getUpdatedAt().toString()));
                                },
                                () -> vertx.executeBlocking(promise -> {
                                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(
                                            Path.of(nfsPath),
                                            path -> path.getFileName().toString().startsWith(fileId + "_"))) {
                                        Optional<Path> found = stream.iterator().hasNext() ?
                                                Optional.of(stream.iterator().next()) : Optional.empty();
                                        promise.complete(found.orElse(null));
                                    } catch (IOException e) {
                                        promise.fail(e);
                                    }
                                }, asyncResult -> {
                                    if (asyncResult.succeeded() && asyncResult.result() != null) {
                                        Path filePath = (Path) asyncResult.result();
                                        String fileName = filePath.getFileName().toString().substring(fileId.length() + 1);

                                        vertx.fileSystem().props(filePath.toString(), propsResult -> {
                                            if (propsResult.succeeded()) {
                                                Instant now = Instant.now();
                                                FileMetadata newMetadata = new FileMetadata(
                                                        fileId, fileName, filePath.toString(), "application/octet-stream",
                                                        propsResult.result().size(), now, now
                                                );
                                                fileMetadataMap.put(fileId, newMetadata, putResult -> {
                                                    log.info("Discovered file from NFS: {}", fileId);

                                                    syncToIndexedCache(fileId, newMetadata);

                                                    message.reply(new JsonObject()
                                                            .put(AppConstants.FIELD_FILE_ID, newMetadata.getFileId())
                                                            .put(AppConstants.FIELD_FILE_NAME, newMetadata.getFileName())
                                                            .put(AppConstants.FIELD_CONTENT_TYPE, newMetadata.getContentType())
                                                            .put(AppConstants.FIELD_FILE_PATH, newMetadata.getFilePath())
                                                            .put(AppConstants.FIELD_CREATED_AT, newMetadata.getCreatedAt().toString())
                                                            .put(AppConstants.FIELD_UPDATED_AT, newMetadata.getUpdatedAt().toString()));
                                                });
                                            } else {
                                                message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
                                            }
                                        });
                                    } else {
                                        message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
                                    }
                                })
                        )
        );
    }

    private void handleList(@NotNull JsonObject msg, Message<Object> message) {
        int page = msg.getInteger(AppConstants.FIELD_PAGE, 1);
        int size = msg.getInteger(AppConstants.FIELD_SIZE, AppConfig.defaultPageSize());
        size = Math.min(size, AppConfig.maxPageSize());
        String prefix = msg.getString(AppConstants.FIELD_PREFIX, "");
        String sort = msg.getString(AppConstants.FIELD_SORT, "name");
        String order = msg.getString(AppConstants.FIELD_ORDER, "asc");

        if (queryService != null) {
            log.debug("Using QueryService for list");
            queryService.listFiles(page, size, prefix, sort, order)
                    .onSuccess(message::reply)
                    .onFailure(err -> {
                        log.error("QueryService list failed", err);
                        message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_LIST_FAILED);
                    });
        } else {
            log.debug("Using fallback list (slow)");
            handleListFallback(msg, message);
        }
    }

    private void handleListFallback(@NotNull JsonObject msg, Message<Object> message) {
        int page = msg.getInteger(AppConstants.FIELD_PAGE, 1);
        int size = msg.getInteger(AppConstants.FIELD_SIZE, AppConfig.defaultPageSize());
        size = Math.min(size, AppConfig.maxPageSize());
        String prefix = msg.getString(AppConstants.FIELD_PREFIX, "");
        String sort = msg.getString(AppConstants.FIELD_SORT, "name");
        String order = msg.getString(AppConstants.FIELD_ORDER, "asc");
        boolean asc = !"desc".equalsIgnoreCase(order);
        int finalSize = size;

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
                            processResults(allMetadata, page, finalSize, sort, asc, message);
                        }
                    }
                });
            });

            keyStream.endHandler(v -> {
                if (pending.get() == 0) {
                    processResults(allMetadata, page, finalSize, sort, asc, message);
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

    private void processResults(List<FileMetadata> metadata, int page, int size, String sort, boolean asc, Message<Object> message) {
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
            filesArray.add(new JsonObject()
                    .put(AppConstants.FIELD_FILE_ID, m.getFileId())
                    .put(AppConstants.FIELD_FILE_NAME, m.getFileName())
                    .put(AppConstants.FIELD_CONTENT_TYPE, m.getContentType())
                    .put(AppConstants.FIELD_SIZE, m.getSize())
                    .put(AppConstants.FIELD_CREATED_AT, m.getCreatedAt().toString())
                    .put(AppConstants.FIELD_UPDATED_AT, m.getUpdatedAt().toString()));
        }

        log.info("List completed: total={}, returned={}, page={}, size={}", total, filesArray.size(), page, size);

        message.reply(new JsonObject()
                .put(AppConstants.FIELD_FILES, filesArray)
                .put(AppConstants.FIELD_PAGE, page)
                .put(AppConstants.FIELD_SIZE, size)
                .put(AppConstants.FIELD_TOTAL, total));
    }

    private void withFileLock(String fileId, RunnableWithCompletion action, Message<Object> message) {
        log.debug("Acquiring lock for fileId={}", fileId);

        vertx.sharedData().<String, FileMetadata>getClusterWideMap(FILE_METADATA_MAP, mapResult -> {
            if (mapResult.failed()) {
                log.error("Failed to get metadata map for lock: fileId={}", fileId);
                message.fail(AppConstants.HTTP_INTERNAL_ERROR, "Failed to get metadata map");
                return;
            }

            vertx.sharedData().getLockWithTimeout("file-lock-" + fileId, AppConfig.fileUploadTimeout(), lockResult -> {
                if (lockResult.failed()) {
                    log.warn("Failed to acquire lock for fileId={}: {}", fileId, lockResult.cause().getMessage());
                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, "Failed to acquire lock: " + lockResult.cause().getMessage());
                    return;
                }

                Lock lock = lockResult.result();
                log.debug("Lock acquired for fileId={}", fileId);
                try {
                    action.run(mapResult.result(), () -> {
                        log.debug("Releasing lock for fileId={}", fileId);
                        lock.release();
                    });
                } catch (Exception e) {
                    log.error("Error during locked operation for fileId={}: {}", fileId, e.getMessage(), e);
                    lock.release();
                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, e.getMessage());
                }
            });
        });
    }

    private void deleteFileOnly(String filePath, Runnable onSuccess, Runnable onFailure) {
        log.debug("Attempting to delete file: {}", filePath);
        vertx.fileSystem().delete(filePath, deleteResult -> {
            if (deleteResult.succeeded()) {
                log.debug("File deleted successfully: {}", filePath);
                onSuccess.run();
            } else {
                log.warn("Failed to delete file: {}, error: {}", filePath, deleteResult.cause().getMessage());
                onFailure.run();
            }
        });
    }

    private void syncToIndexedCache(String fileId, FileMetadata metadata) {
        if (queryService != null) {
            try {
                ClusterManagerConfig.getCacheContainer()
                        .getCache(FILE_METADATA_MAP_INDEXED)
                        .put(fileId, metadata);
                log.debug("Synced to indexed cache: fileId={}", fileId);
            } catch (Exception e) {
                log.warn("Failed to sync to indexed cache for fileId={}: {}", fileId, e.getMessage());
            }
        }
    }

    private void removeFromIndexedCache(String fileId) {
        if (queryService != null) {
            try {
                ClusterManagerConfig.getCacheContainer()
                        .getCache(FILE_METADATA_MAP_INDEXED)
                        .remove(fileId);
                log.debug("Removed from indexed cache: fileId={}", fileId);
            } catch (Exception e) {
                log.warn("Failed to remove from indexed cache for fileId={}: {}", fileId, e.getMessage());
            }
        }
    }
}