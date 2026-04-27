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
import java.nio.file.attribute.FileTime;
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

        if (AppConfig.isClustered()) {
            vertx.sharedData().<String, FileMetadata>getClusterWideMap(FILE_METADATA_MAP, ar -> {
                if (ar.succeeded()) {
                    fileMetadataMap = ar.result();
                    initQueryService();
                    setupEventBusConsumer();
                    restoreAllMetadataFromNfs(startPromise);
                    startPromise.complete();
                    log.info("FileProcessorVerticle started successfully");
                    log.warn("The server is launched in clustered mode");
                } else {
                    log.error("Failed to initialize file metadata map: {}", ar.cause().getMessage(), ar.cause());
                    startPromise.fail(ar.cause());
                }
            });
        } else {
            vertx.sharedData().<String, FileMetadata>getAsyncMap(FILE_METADATA_MAP, ar -> {
                if (ar.succeeded()) {
                    fileMetadataMap = ar.result();
                    setupEventBusConsumer();
                    startPromise.complete();
                    log.info("FileProcessorVerticle started successfully");
                    log.warn("The server is launched in non-clustered mode");
                } else {
                    log.error("Failed to initialize file metadata map: {}", ar.cause().getMessage(), ar.cause());
                    startPromise.fail(ar.cause());
                }
            });
        }
    }

    private void restoreAllMetadataFromNfs(Promise<Void> startPromise) {
        log.info("Restoring metadata from NFS directory: {}", nfsPath);

        vertx.executeBlocking(promise -> {
            List<Path> files = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(Path.of(nfsPath))) {
                for (Path path : stream) {
                    files.add(path);
                }
            } catch (IOException e) {
                promise.fail(e);
                return;
            }
            promise.complete(files);
        }, asyncResult -> {
            if (asyncResult.failed()) {
                log.error("Failed to scan NFS directory", asyncResult.cause());
                startPromise.fail(asyncResult.cause());
                return;
            }

            @SuppressWarnings("unchecked")
            List<Path> files = (List<Path>) asyncResult.result();

            if (files.isEmpty()) {
                log.info("No existing files found in NFS directory");
                startPromise.complete();
                return;
            }

            log.info("Found {} files in NFS directory, restoring metadata", files.size());

            AtomicInteger restored = new AtomicInteger(0);
            AtomicInteger failed = new AtomicInteger(0);
            int total = files.size();

            for (Path path : files) {
                String fileName = path.getFileName().toString();
                int underscoreIndex = fileName.indexOf('_');

                if (underscoreIndex <= 0) {
                    log.warn("Skipping invalid file name format: {}", fileName);
                    checkRestoreCompletion(total, restored.incrementAndGet(), failed.get(), startPromise);
                    continue;
                }

                String fileId = fileName.substring(0, underscoreIndex);
                String actualFileName = fileName.substring(underscoreIndex + 1);

                fileMetadataMap.get(fileId, getResult -> {
                    if (getResult.succeeded() && getResult.result() != null) {
                        checkRestoreCompletion(total, restored.incrementAndGet(), failed.get(), startPromise);
                    } else {
                        try {
                            FileTime lastModified = Files.getLastModifiedTime(path);
                            Instant updatedAt = lastModified.toInstant();

                            Instant createdAt;
                            try {
                                FileTime creationTime = (FileTime) Files.getAttribute(path, "creationTime");
                                createdAt = creationTime != null ? creationTime.toInstant() : updatedAt;
                            } catch (Exception e) {
                                createdAt = updatedAt;
                            }

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
                                    log.debug("Restored metadata for fileId={}", fileId);
                                    checkRestoreCompletion(total, restored.incrementAndGet(), failed.get(), startPromise);
                                } else {
                                    log.error("Failed to put metadata for fileId={}", fileId, putResult.cause());
                                    checkRestoreCompletion(total, restored.get(), failed.incrementAndGet(), startPromise);
                                }
                            });
                        } catch (IOException e) {
                            log.error("Failed to read file: {}", path, e);
                            checkRestoreCompletion(total, restored.get(), failed.incrementAndGet(), startPromise);
                        }
                    }
                });
            }
        });
    }

    private void checkRestoreCompletion(int total, int restored, int failed, Promise<Void> startPromise) {
        if (restored + failed >= total) {
            log.info("Metadata restoration completed: restored={}, failed={}, total={}", restored, failed, total);
            startPromise.complete();
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
        log.info("EventBus consumers registered for addresses: {}, {}, {}, {}, {}, {}",
                EventBusConfig.FILE_UPLOAD_ADDRESS,
                EventBusConfig.FILE_GET_ADDRESS,
                EventBusConfig.FILE_DELETE_ADDRESS,
                EventBusConfig.FILE_LIST_ADDRESS,
                EventBusConfig.FILE_UPDATE_ADDRESS,
                EventBusConfig.FILE_DOWNLOAD_ADDRESS);
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
            case AppConstants.ACTION_DOWNLOAD -> this::handleDownload;
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

        fileMetadataMap.get(fileId, getResult -> {
            if (getResult.succeeded() && getResult.result() != null) {
                log.info("GET: Found in cache: fileId={}", fileId);
                message.reply(new JsonObject()
                        .put(AppConstants.FIELD_FILE_ID, getResult.result().getFileId())
                        .put(AppConstants.FIELD_FILE_NAME, getResult.result().getFileName())
                        .put(AppConstants.FIELD_CONTENT_TYPE, getResult.result().getContentType())
                        .put(AppConstants.FIELD_FILE_PATH, getResult.result().getFilePath())
                        .put(AppConstants.FIELD_CREATED_AT, getResult.result().getCreatedAt().toString())
                        .put(AppConstants.FIELD_UPDATED_AT, getResult.result().getUpdatedAt().toString()));
            } else {
                log.info("GET: Not in cache, attempting to restore from NFS: fileId={}", fileId);

                vertx.executeBlocking(promise -> {
                    String nfsPathStr = nfsPath;
                    log.debug("Scanning NFS directory: {} for pattern: {}_*", nfsPathStr, fileId);

                    Path nfsDir = Path.of(nfsPathStr);
                    log.debug("NFS directory exists: {}", Files.exists(nfsDir));

                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(nfsDir)) {
                        List<String> allFiles = new ArrayList<>();
                        Path found = null;

                        for (Path path : stream) {
                            String fileName = path.getFileName().toString();
                            allFiles.add(fileName);

                            if (fileName.startsWith(fileId + "_")) {
                                found = path;
                                log.info("Found matching file: {}", fileName);
                                break;
                            }
                        }

                        if (found == null) {
                            log.warn("No file found starting with: {}_", fileId);
                            log.debug("All files in directory: {}", allFiles);
                        }

                        promise.complete(found);
                    } catch (IOException e) {
                        log.error("Error scanning NFS directory: {}", nfsPathStr, e);
                        promise.fail(e);
                    }
                }, asyncResult -> {
                    if (asyncResult.succeeded() && asyncResult.result() != null) {
                        Path filePath = (Path) asyncResult.result();
                        String fullFileName = filePath.getFileName().toString();
                        int underscoreIndex = fullFileName.indexOf('_');
                        String fileName = underscoreIndex > 0 ?
                                fullFileName.substring(underscoreIndex + 1) : fullFileName;

                        log.info("Restoring metadata for fileId={}, fileName={}, path={}",
                                fileId, fileName, filePath);

                        vertx.fileSystem().props(filePath.toString(), propsResult -> {
                            if (propsResult.succeeded()) {
                                Instant now = Instant.now();
                                FileMetadata newMetadata = new FileMetadata(
                                        fileId, fileName, filePath.toString(), "application/octet-stream",
                                        propsResult.result().size(), now, now
                                );

                                fileMetadataMap.put(fileId, newMetadata, putResult -> {
                                    if (putResult.succeeded()) {
                                        log.info("Successfully restored and cached metadata for fileId={}", fileId);
                                        syncToIndexedCache(fileId, newMetadata);
                                        message.reply(new JsonObject()
                                                .put(AppConstants.FIELD_FILE_ID, newMetadata.getFileId())
                                                .put(AppConstants.FIELD_FILE_NAME, newMetadata.getFileName())
                                                .put(AppConstants.FIELD_CONTENT_TYPE, newMetadata.getContentType())
                                                .put(AppConstants.FIELD_FILE_PATH, newMetadata.getFilePath())
                                                .put(AppConstants.FIELD_CREATED_AT, newMetadata.getCreatedAt().toString())
                                                .put(AppConstants.FIELD_UPDATED_AT, newMetadata.getUpdatedAt().toString()));
                                    } else {
                                        log.error("Failed to cache restored metadata for fileId={}", fileId, putResult.cause());
                                        message.fail(AppConstants.HTTP_INTERNAL_ERROR, "Failed to cache restored metadata");
                                    }
                                });
                            } else {
                                log.error("Failed to get file properties for restored file: {}", filePath);
                                message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
                            }
                        });
                    } else {
                        log.warn("File not found in NFS for fileId={}", fileId);
                        if (asyncResult.cause() != null) {
                            log.warn("Error cause: {}", asyncResult.cause().getMessage());
                        }
                        message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
                    }
                });
            }
        });
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
        } else if (AppConfig.isClustered()) {
            log.debug("Using Infinispan keyStream for list (clustered fallback)");
            handleListWithKeyStream(page, size, prefix, sort, order, message);
        } else {
            log.debug("Using filesystem scan for list (non-clustered)");
            handleListWithFileSystemScan(page, size, prefix, sort, order, message);
        }
    }

    private void handleDownload(@NotNull JsonObject msg, Message<Object> message) {
        String fileId = msg.getString(AppConstants.FIELD_FILE_ID);
        log.info("Handling download: fileId={}", fileId);

        fileMetadataMap.get(fileId, getResult -> {
            if (getResult.succeeded() && getResult.result() != null) {
                FileMetadata metadata = getResult.result();
                String filePath = metadata.getFilePath();

                log.debug("Download requested for fileId={}, path={}", fileId, filePath);

                vertx.fileSystem().exists(filePath, existsResult -> {
                    if (existsResult.succeeded() && existsResult.result()) {
                        vertx.fileSystem().props(filePath, propsResult -> {
                            if (propsResult.succeeded()) {
                                JsonObject response = new JsonObject()
                                        .put(AppConstants.FIELD_STATUS, AppConstants.STATUS_OK)
                                        .put(AppConstants.FIELD_FILE_PATH, filePath)
                                        .put(AppConstants.FIELD_FILE_NAME, metadata.getFileName())
                                        .put(AppConstants.FIELD_CONTENT_TYPE, metadata.getContentType())
                                        .put(AppConstants.FIELD_SIZE, propsResult.result().size());
                                message.reply(response);
                                log.info("Download prepared: fileId={}, size={}", fileId, propsResult.result().size());
                            } else {
                                log.error("Failed to get file properties for fileId={}: {}", fileId, propsResult.cause().getMessage());
                                message.fail(AppConstants.HTTP_INTERNAL_ERROR, "Failed to get file properties");
                            }
                        });
                    } else {
                        log.warn("Download failed - file not found on disk: fileId={}, path={}", fileId, filePath);
                        message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
                    }
                });
            } else {
                log.warn("Download failed - metadata not found: fileId={}", fileId);
                message.fail(AppConstants.HTTP_NOT_FOUND, AppConstants.ERR_FILE_NOT_FOUND);
            }
        });
    }

    private void handleListWithKeyStream(int page, int size, String prefix, String sort, String order, Message<Object> message) {
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

    private void handleListWithFileSystemScan(int page, int size, String prefix,
                                              String sort, String order, Message<Object> message) {
        boolean asc = !"desc".equalsIgnoreCase(order);

        vertx.executeBlocking(promise -> {
            List<FileMetadata> allMetadata = new ArrayList<>();

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(Path.of(nfsPath))) {
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
                                Instant createdAt;
                                try {
                                    FileTime creationTime = (FileTime) Files.getAttribute(path, "creationTime");
                                    createdAt = creationTime != null ? creationTime.toInstant() : updatedAt;
                                } catch (Exception e) {
                                    createdAt = updatedAt;
                                }

                                String contentType = Files.probeContentType(path);
                                if (contentType == null) {
                                    contentType = "application/octet-stream";
                                }

                                FileMetadata meta = new FileMetadata(
                                        fileId,
                                        actualFileName,
                                        path.toString(),
                                        contentType,
                                        Files.size(path),
                                        createdAt,
                                        updatedAt
                                );
                                allMetadata.add(meta);
                            } catch (IOException e) {
                                log.warn("Failed to read file metadata for: {}", path, e);
                            }
                        }
                    }
                }
                promise.complete(allMetadata);
            } catch (IOException e) {
                promise.fail(e);
            }
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

    private void processResults(@NotNull List<FileMetadata> metadata, int page, int size, String sort, boolean asc, Message<Object> message) {
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