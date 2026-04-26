package by.losik.verticle;

import by.losik.config.AppConfig;
import by.losik.config.EventBusConfig;
import by.losik.constant.AppConstants;
import by.losik.meta.FileMetadata;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Lock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class FileProcessorVerticle extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(FileProcessorVerticle.class);
    private static final String FILE_METADATA_MAP = "file.metadata";
    private AsyncMap<String, FileMetadata> fileMetadataMap;
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

                log.info("File metadata map successfully initialized");
                setupEventBusConsumer();
                startPromise.complete();
                log.info("FileProcessorVerticle started successfully");
            } else {
                log.error("Failed to initialize file metadata map: {}", ar.cause().getMessage(), ar.cause());
                startPromise.fail(ar.cause());
            }
        });
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
                    log.debug("Found existing file: fileId={}, oldPath={}", fileId, oldMetadata.filePath());

                    vertx.fileSystem().props(tempPath, propsResult -> {
                        long size = propsResult.succeeded() ? propsResult.result().size() : 0;
                        Instant now = Instant.now();

                        var newMetadata = new FileMetadata(
                                fileId, fileName, targetPath, contentType,
                                size, oldMetadata.createdAt(), now
                        );

                        vertx.fileSystem().delete(oldMetadata.filePath(), deleteResult -> {
                            if (deleteResult.failed()) {
                                log.warn("Failed to delete old file {}: {}", oldMetadata.filePath(), deleteResult.cause().getMessage());
                            } else {
                                log.debug("Old file deleted: {}", oldMetadata.filePath());
                            }

                            vertx.fileSystem().copy(tempPath, targetPath, copyResult -> {
                                if (copyResult.succeeded()) {
                                    log.debug("New file copied to: {}", targetPath);
                                    metadataMap.put(fileId, newMetadata, putResult -> {
                                        vertx.fileSystem().delete(tempPath)
                                                .onFailure(err -> log.warn("Temp file cleanup failed: {}", tempPath, err));

                                        if (putResult.succeeded()) {
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
                            log.debug("Metadata removed for fileId={}, path={}", fileId, metadata.filePath());
                            vertx.fileSystem().delete(metadata.filePath(), deleteResult -> {
                                releaseLock.run();
                                if (deleteResult.succeeded()) {
                                    log.info("Delete completed successfully: fileId={}", fileId);
                                    message.reply(new JsonObject().put(AppConstants.FIELD_STATUS, AppConstants.STATUS_DELETED));
                                } else {
                                    log.warn("File deleted but physical file may remain: fileId={}, path={}, error={}",
                                            fileId, metadata.filePath(), deleteResult.cause().getMessage());
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
                                    log.debug("Metadata found for fileId={}, fileName={}", fileId, metadata.fileName());
                                    message.reply(new JsonObject()
                                            .put(AppConstants.FIELD_FILE_ID, metadata.fileId())
                                            .put(AppConstants.FIELD_FILE_NAME, metadata.fileName())
                                            .put(AppConstants.FIELD_CONTENT_TYPE, metadata.contentType())
                                            .put(AppConstants.FIELD_FILE_PATH, metadata.filePath())
                                            .put(AppConstants.FIELD_CREATED_AT, metadata.createdAt().toString())
                                            .put(AppConstants.FIELD_UPDATED_AT, metadata.updatedAt().toString()));
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
                                                    message.reply(new JsonObject()
                                                            .put(AppConstants.FIELD_FILE_ID, newMetadata.fileId())
                                                            .put(AppConstants.FIELD_FILE_NAME, newMetadata.fileName())
                                                            .put(AppConstants.FIELD_CONTENT_TYPE, newMetadata.contentType())
                                                            .put(AppConstants.FIELD_FILE_PATH, newMetadata.filePath())
                                                            .put(AppConstants.FIELD_CREATED_AT, newMetadata.createdAt().toString())
                                                            .put(AppConstants.FIELD_UPDATED_AT, newMetadata.updatedAt().toString()));
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
        boolean asc = !AppConstants.ORDER_DESC.equals(msg.getString(AppConstants.FIELD_ORDER, "asc"));
        int finalSize = size;

        log.debug("Handling list request: page={}, size={}, prefix={}, sort={}, asc={}", page, size, prefix, sort, asc);

        fileMetadataMap.keys(keysResult -> {
            if (keysResult.succeeded()) {
                var keys = keysResult.result();
                log.debug("Total keys in metadata map: {}", keys.size());

                if (keys.isEmpty()) {
                    log.debug("No files found, returning empty list");
                    message.reply(new JsonObject().put(AppConstants.FIELD_FILES, new JsonArray())
                            .put(AppConstants.FIELD_TOTAL, 0)
                            .put(AppConstants.FIELD_PAGE, page)
                            .put(AppConstants.FIELD_SIZE, finalSize));
                    return;
                }

                var allFiles = new JsonArray();
                var pending = new AtomicInteger(keys.size());

                keys.forEach(fileId ->
                        fileMetadataMap.get(fileId, getResult -> {
                            Optional.ofNullable(getResult.result())
                                    .filter(metadata -> prefix.isEmpty() || metadata.fileName().startsWith(prefix))
                                    .ifPresent(metadata -> allFiles.add(new JsonObject()
                                            .put(AppConstants.FIELD_FILE_ID, metadata.fileId())
                                            .put(AppConstants.FIELD_FILE_NAME, metadata.fileName())
                                            .put(AppConstants.FIELD_CONTENT_TYPE, metadata.contentType())
                                            .put(AppConstants.FIELD_SIZE, metadata.size())
                                            .put(AppConstants.FIELD_CREATED_AT, metadata.createdAt().toString())));

                            if (pending.decrementAndGet() == 0) {
                                var sorted = new JsonArray();
                                var list = new ArrayList<JsonObject>();
                                for (int i = 0; i < allFiles.size(); i++) {
                                    list.add((JsonObject) allFiles.getValue(i));
                                }

                                log.debug("Collected {} files matching prefix '{}'", list.size(), prefix);

                                list.sort((a, b) -> {
                                    int cmp = switch (sort) {
                                        case AppConstants.FIELD_SIZE -> Long.compare(
                                                a.getLong(AppConstants.FIELD_SIZE, 0L),
                                                b.getLong(AppConstants.FIELD_SIZE, 0L));
                                        case AppConstants.FIELD_DATE, AppConstants.FIELD_CREATED_AT ->
                                                a.getString(AppConstants.FIELD_CREATED_AT, "")
                                                        .compareTo(b.getString(AppConstants.FIELD_CREATED_AT, ""));
                                        default -> a.getString(AppConstants.FIELD_FILE_NAME, "")
                                                .compareTo(b.getString(AppConstants.FIELD_FILE_NAME, ""));
                                    };
                                    return asc ? cmp : -cmp;
                                });

                                int start = (page - 1) * finalSize;
                                int end = Math.min(start + finalSize, list.size());
                                for (int i = start; i < end; i++) {
                                    sorted.add(list.get(i));
                                }

                                log.info("List request completed: page={}, size={}, total={}, returned={}",
                                        page, finalSize, list.size(), sorted.size());

                                message.reply(new JsonObject()
                                        .put(AppConstants.FIELD_FILES, sorted)
                                        .put(AppConstants.FIELD_PAGE, page)
                                        .put(AppConstants.FIELD_SIZE, finalSize)
                                        .put(AppConstants.FIELD_TOTAL, list.size()));
                            }
                        })
                );
            } else {
                log.error("Failed to list files: {}", keysResult.cause().getMessage());
                message.fail(AppConstants.HTTP_INTERNAL_ERROR, AppConstants.ERR_LIST_FAILED);
            }
        });
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
}