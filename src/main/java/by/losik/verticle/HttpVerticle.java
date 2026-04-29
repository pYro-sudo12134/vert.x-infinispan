package by.losik.verticle;

import by.losik.config.AppConfig;
import by.losik.config.EventBusConfig;
import by.losik.config.RouterConfig;
import by.losik.config.VolumeManagerConfig;
import by.losik.constant.AppConstants;
import by.losik.volume.MigrationTask;
import by.losik.volume.VolumeManager;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class HttpVerticle extends AbstractVerticle implements HttpProcessor {
    private static final Logger log = LoggerFactory.getLogger(HttpVerticle.class);

    @Override
    public void start() {
        log.info("Starting HttpVerticle on port: {}", AppConfig.httpPort());

        Router router = RouterConfig.createRouter();

        if (AppConfig.corsEnabled()) {
            CorsHandler corsHandler = CorsHandler.create();

            String origins = AppConfig.corsAllowedOrigins();
            if (origins.equals("*")) {
                corsHandler.addRelativeOrigin(".*");
            } else {
                corsHandler.addRelativeOrigins(
                        Arrays.stream(origins.split(","))
                                .map(String::trim)
                                .map(origin -> origin.replace(".", "\\.").replace("*", ".*"))
                                .collect(Collectors.toList()));
            }

            corsHandler.allowedMethods(
                            Arrays.stream(AppConfig.corsAllowedMethods().split(","))
                                    .map(String::trim)
                                    .map(HttpMethod::valueOf)
                                    .collect(Collectors.toSet()))
                    .allowedHeaders(
                            Arrays.stream(AppConfig.corsAllowedHeaders().split(","))
                                    .map(String::trim)
                                    .collect(Collectors.toSet()));

            router.route().handler(corsHandler);
        }

        router.route().handler(BodyHandler.create(AppConfig.tmpPath()));
        router.post("/upload").handler(this::uploadFile);
        router.get("/files/:id").handler(this::getFile);
        router.delete("/files/:id").handler(this::deleteFile);
        router.put("/files/:id").handler(this::updateFile);
        router.get("/files").handler(this::listFiles);
        router.get("/download/:id").handler(this::downloadFile);
        router.post("/admin/volumes").handler(this::addVolume);
        router.delete("/admin/volumes/:path").handler(this::removeVolume);
        router.get("/admin/volumes/status").handler(this::getVolumeStatus);
        router.post("/admin/migrate/:fileId").handler(this::migrateFile);
        router.get("/admin/migrate/status/:fileId").handler(this::getMigrationStatus);
        router.post("/admin/migrate/rollback/:fileId").handler(this::rollbackMigration);
        router.post("/admin/migrate/volume/:source/:target").handler(this::migrateVolume);
        router.get("/health").handler(ctx -> {
            log.debug("Health check request received");
            ctx.response().end(AppConstants.STATUS_OK);
        });



        vertx.createHttpServer()
                .requestHandler(router)
                .listen(AppConfig.httpPort())
                .onSuccess(server -> log.info("HTTP server started successfully on port {}", AppConfig.httpPort()))
                .onFailure(err -> log.error("Failed to start HTTP server on port {}: {}", AppConfig.httpPort(), err.getMessage(), err));
    }

    @Override
    public void uploadFile(@NotNull RoutingContext ctx) {
        log.info("Received upload request from: {}", ctx.request().remoteAddress());

        Optional.of(ctx.fileUploads())
                .filter(uploads -> !uploads.isEmpty())
                .map(uploads -> uploads.iterator().next())
                .ifPresentOrElse(upload -> {
                    String fileId = UUID.randomUUID().toString();
                    String tempPath = upload.uploadedFileName();

                    log.debug("Processing upload: fileId={}, fileName={}, contentType={}, size={}, tempPath={}",
                            fileId, upload.fileName(), upload.contentType(), upload.size(), tempPath);

                    var message = new JsonObject()
                            .put(AppConstants.FIELD_ACTION, AppConstants.ACTION_UPLOAD)
                            .put(AppConstants.FIELD_FILE_ID, fileId)
                            .put(AppConstants.FIELD_FILE_NAME, upload.fileName())
                            .put(AppConstants.FIELD_FILE_PATH, tempPath)
                            .put(AppConstants.FIELD_CONTENT_TYPE, upload.contentType());

                    var options = new DeliveryOptions()
                            .setSendTimeout(AppConfig.fileUploadTimeout());

                    EventBusConfig.eventBus().request(EventBusConfig.FILE_UPLOAD_ADDRESS, message, options, reply -> {
                        if (reply.succeeded()) {
                            log.info("Upload successful: fileId={}, fileName={}", fileId, upload.fileName());
                            ctx.response()
                                    .setStatusCode(AppConstants.HTTP_CREATED)
                                    .putHeader("Content-Type", "application/json")
                                    .end(new JsonObject().put(AppConstants.FIELD_FILE_ID, fileId).encode());
                        } else {
                            log.error("Upload failed for fileId={}, fileName={}: {}",
                                    fileId, upload.fileName(), reply.cause().getMessage());

                            vertx.fileSystem().delete(tempPath)
                                    .onFailure(err -> log.warn("Temp file cleanup failed: {}", tempPath, err));

                            ctx.response()
                                    .setStatusCode(AppConstants.HTTP_INTERNAL_ERROR)
                                    .end(AppConstants.ERR_UPLOAD_FAILED + ": " + reply.cause().getMessage());
                        }
                    });
                }, () -> {
                    log.warn("Upload request rejected - no file provided from: {}", ctx.request().remoteAddress());
                    ctx.response()
                            .setStatusCode(AppConstants.HTTP_BAD_REQUEST)
                            .end(AppConstants.ERR_NO_FILE);
                });
    }

    @Override
    public void getFile(@NotNull RoutingContext ctx) {
        String fileId = ctx.pathParam(AppConstants.FIELD_ID);
        log.debug("Received get file request: fileId={}, from={}", fileId, ctx.request().remoteAddress());

        var message = new JsonObject()
                .put(AppConstants.FIELD_ACTION, AppConstants.ACTION_GET)
                .put(AppConstants.FIELD_FILE_ID, fileId);

        EventBusConfig.eventBus().request(EventBusConfig.FILE_GET_ADDRESS, message, reply -> {
            if (reply.succeeded()) {
                log.debug("Get file successful: fileId={}", fileId);
                ctx.response()
                        .putHeader("Content-Type", "application/json")
                        .end(reply.result().body().toString());
            } else {
                log.warn("Get file failed - file not found: fileId={}", fileId);
                ctx.response()
                        .setStatusCode(AppConstants.HTTP_NOT_FOUND)
                        .end(AppConstants.ERR_FILE_NOT_FOUND);
            }
        });
    }

    @Override
    public void deleteFile(@NotNull RoutingContext ctx) {
        String fileId = ctx.pathParam(AppConstants.FIELD_ID);
        log.info("Received delete request: fileId={}, from={}", fileId, ctx.request().remoteAddress());

        var message = new JsonObject()
                .put(AppConstants.FIELD_ACTION, AppConstants.ACTION_DELETE)
                .put(AppConstants.FIELD_FILE_ID, fileId);

        EventBusConfig.eventBus().request(EventBusConfig.FILE_DELETE_ADDRESS, message, reply -> {
            if (reply.succeeded()) {
                log.info("Delete successful: fileId={}", fileId);
                ctx.response()
                        .setStatusCode(AppConstants.HTTP_NO_CONTENT)
                        .end();
            } else {
                log.error("Delete failed for fileId={}: {}", fileId, reply.cause().getMessage());
                ctx.response()
                        .setStatusCode(AppConstants.HTTP_INTERNAL_ERROR)
                        .end(AppConstants.ERR_DELETE_FAILED);
            }
        });
    }

    @Override
    public void updateFile(@NotNull RoutingContext ctx) {
        String fileId = ctx.pathParam(AppConstants.FIELD_ID);
        log.info("Received update request: fileId={}, from={}", fileId, ctx.request().remoteAddress());

        Optional.of(ctx.fileUploads())
                .filter(uploads -> !uploads.isEmpty())
                .map(uploads -> uploads.iterator().next())
                .ifPresentOrElse(upload -> {
                    String tempPath = upload.uploadedFileName();
                    var getMessage = new JsonObject()
                            .put(AppConstants.FIELD_ACTION, AppConstants.ACTION_GET)
                            .put(AppConstants.FIELD_FILE_ID, fileId);

                    EventBusConfig.eventBus().request(EventBusConfig.FILE_GET_ADDRESS, getMessage, getReply -> {
                        if (getReply.failed()) {
                            log.error("Update failed - file not found: fileId={}", fileId);
                            ctx.response().setStatusCode(AppConstants.HTTP_NOT_FOUND).end(AppConstants.ERR_FILE_NOT_FOUND);
                            return;
                        }

                        JsonObject metadata = (JsonObject) getReply.result().body();
                        String existingFileName = metadata.getString(AppConstants.FIELD_FILE_NAME);

                        log.debug("Processing update: fileId={}, existingFileName={}, contentType={}, tempPath={}",
                                fileId, existingFileName, upload.contentType(), tempPath);

                        var updateMessage = new JsonObject()
                                .put(AppConstants.FIELD_ACTION, AppConstants.ACTION_UPDATE)
                                .put(AppConstants.FIELD_FILE_ID, fileId)
                                .put(AppConstants.FIELD_FILE_NAME, existingFileName)
                                .put(AppConstants.FIELD_FILE_PATH, tempPath)
                                .put(AppConstants.FIELD_CONTENT_TYPE, upload.contentType());

                        EventBusConfig.eventBus().request(EventBusConfig.FILE_UPDATE_ADDRESS, updateMessage, updateReply -> {
                            if (updateReply.succeeded()) {
                                log.info("Update successful: fileId={}, fileName={}", fileId, existingFileName);
                                ctx.response()
                                        .setStatusCode(AppConstants.HTTP_OK)
                                        .end(AppConstants.STATUS_UPDATED);
                            } else {
                                log.error("Update failed for fileId={}: {}", fileId, updateReply.cause().getMessage());
                                vertx.fileSystem().delete(tempPath)
                                        .onFailure(err -> log.warn("Temp file cleanup failed: {}", tempPath, err));
                                ctx.response()
                                        .setStatusCode(AppConstants.HTTP_INTERNAL_ERROR)
                                        .end(AppConstants.ERR_UPDATE_FAILED);
                            }
                        });
                    });
                }, () -> {
                    log.warn("Update request rejected - no file provided: fileId={}", fileId);
                    ctx.response()
                            .setStatusCode(AppConstants.HTTP_BAD_REQUEST)
                            .end(AppConstants.ERR_NO_FILE);
                });
    }

    @Override
    public void listFiles(@NotNull RoutingContext ctx) {
        int page = Integer.parseInt(ctx.request().getParam(AppConstants.FIELD_PAGE, "1"));
        int size = Integer.parseInt(ctx.request().getParam(AppConstants.FIELD_SIZE, "100"));
        String prefix = ctx.request().getParam(AppConstants.FIELD_PREFIX, "");
        String sort = ctx.request().getParam(AppConstants.FIELD_SORT, "name");
        String order = ctx.request().getParam(AppConstants.FIELD_ORDER, "asc");

        log.debug("Received list request: page={}, size={}, prefix={}, sort={}, order={}, from={}",
                page, size, prefix, sort, order, ctx.request().remoteAddress());

        var message = new JsonObject()
                .put(AppConstants.FIELD_ACTION, AppConstants.ACTION_LIST)
                .put(AppConstants.FIELD_PAGE, page)
                .put(AppConstants.FIELD_SIZE, size)
                .put(AppConstants.FIELD_PREFIX, prefix)
                .put(AppConstants.FIELD_SORT, sort)
                .put(AppConstants.FIELD_ORDER, order);

        EventBusConfig.eventBus().request(EventBusConfig.FILE_LIST_ADDRESS, message, reply -> {
            if (reply.succeeded()) {
                log.debug("List request successful: page={}, size={}", page, size);
                ctx.response()
                        .putHeader("Content-Type", "application/json")
                        .end(reply.result().body().toString());
            } else {
                log.error("List request failed: {}", reply.cause().getMessage());
                ctx.response()
                        .setStatusCode(AppConstants.HTTP_INTERNAL_ERROR)
                        .end(AppConstants.ERR_LIST_FAILED);
            }
        });
    }

    @Override
    public void downloadFile(@NotNull RoutingContext ctx) {
        String fileId = ctx.pathParam(AppConstants.FIELD_ID);
        log.info("Received download request: fileId={}", fileId);

        var message = new JsonObject()
                .put(AppConstants.FIELD_ACTION, AppConstants.ACTION_DOWNLOAD)
                .put(AppConstants.FIELD_FILE_ID, fileId);

        EventBusConfig.eventBus().request(EventBusConfig.FILE_DOWNLOAD_ADDRESS, message, reply -> {
            if (reply.succeeded()) {
                JsonObject response = (JsonObject) reply.result().body();
                String filePath = response.getString(AppConstants.FIELD_FILE_PATH);
                String fileName = response.getString(AppConstants.FIELD_FILE_NAME);
                String contentType = response.getString(AppConstants.FIELD_CONTENT_TYPE);
                long size = response.getLong(AppConstants.FIELD_SIZE);

                ctx.response()
                        .setStatusCode(AppConstants.HTTP_OK)
                        .putHeader("Content-Type", contentType)
                        .putHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"")
                        .putHeader("Content-Length", String.valueOf(size));

                vertx.fileSystem().open(filePath, new OpenOptions().setRead(true), openResult -> {
                    if (openResult.succeeded()) {
                        AsyncFile asyncFile = openResult.result();
                        asyncFile.pipeTo(ctx.response())
                                .onSuccess(v -> log.info("Download completed: fileId={}, fileName={}, size={}", fileId, fileName, size))
                                .onFailure(err -> {
                                    log.error("Pipe failed for fileId={}", fileId, err);
                                    if (!ctx.response().ended()) {
                                        ctx.response().setStatusCode(AppConstants.HTTP_INTERNAL_ERROR).end("Download failed");
                                    }
                                });
                    } else {
                        ctx.response().setStatusCode(AppConstants.HTTP_INTERNAL_ERROR).end("Failed to open file");
                    }
                });
            } else {
                ctx.response()
                        .setStatusCode(AppConstants.HTTP_NOT_FOUND)
                        .end(AppConstants.ERR_FILE_NOT_FOUND);
            }
        });
    }

    @Override
    public void addVolume(@NotNull RoutingContext ctx) {
        JsonObject body = ctx.body().asJsonObject();
        String volumePath = body.getString("path");

        if (volumePath == null || volumePath.isBlank()) {
            ctx.response().setStatusCode(AppConstants.HTTP_BAD_REQUEST).end("Missing 'path' parameter");
            return;
        }

        VolumeManagerConfig.volumeManager().addVolume(volumePath)
                .onSuccess(v -> {
                    log.info("Volume added via API: {}", volumePath);
                    ctx.response().end(new JsonObject()
                            .put(AppConstants.FIELD_STATUS, "success")
                            .put(AppConstants.FIELD_MESSAGE, "Volume added: " + volumePath)
                            .encode());
                })
                .onFailure(err -> {
                    log.error("Failed to add volume: {}", volumePath, err);
                    ctx.response().setStatusCode(AppConstants.HTTP_INTERNAL_ERROR).end(new JsonObject()
                            .put(AppConstants.FIELD_STATUS, "error")
                            .put(AppConstants.FIELD_MESSAGE, err.getMessage())
                            .encode());
                });
    }

    @Override
    public void removeVolume(@NotNull RoutingContext ctx) {
        String volumePath = ctx.pathParam("path");
        String targetVolume = ctx.request().getParam("target");

        if (volumePath == null || volumePath.isBlank()) {
            ctx.response().setStatusCode(AppConstants.HTTP_BAD_REQUEST).end("Missing volume path");
            return;
        }

        VolumeManagerConfig.volumeManager().removeVolume(volumePath, targetVolume)
                .onSuccess(v -> {
                    log.info("Volume removed via API: {}", volumePath);
                    ctx.response().end(new JsonObject()
                            .put(AppConstants.FIELD_STATUS, "success")
                            .put(AppConstants.FIELD_MESSAGE, "Volume removed: " + volumePath)
                            .encode());
                })
                .onFailure(err -> {
                    log.error("Failed to remove volume: {}", volumePath, err);
                    ctx.response().setStatusCode(AppConstants.HTTP_INTERNAL_ERROR).end(new JsonObject()
                            .put(AppConstants.FIELD_STATUS, AppConstants.ERR_DELETE_FAILED)
                            .put(AppConstants.FIELD_MESSAGE, err.getMessage())
                            .encode());
                });
    }

    @Override
    public void getVolumeStatus(@NotNull RoutingContext ctx) {
        ctx.response()
                .putHeader("Content-Type", "application/json")
                .end(VolumeManagerConfig.volumeManager().getStatus().encodePrettily());
    }

    @Override
    public void migrateFile(@NotNull RoutingContext ctx) {
        String fileId = ctx.pathParam(AppConstants.FIELD_FILE_ID);
        JsonObject body = ctx.body().asJsonObject();
        String targetVolume = body.getString(AppConstants.FIELD_TARGET_VOLUME);

        if (fileId == null || fileId.isBlank()) {
            ctx.response().setStatusCode(AppConstants.HTTP_BAD_REQUEST).end("Missing fileId");
            return;
        }

        if (targetVolume == null || targetVolume.isBlank()) {
            ctx.response().setStatusCode(AppConstants.HTTP_BAD_REQUEST).end("Missing targetVolume");
            return;
        }

        if (VolumeManagerConfig.volumeManager().isVolumeReadOnly(targetVolume)) {
            ctx.response().setStatusCode(AppConstants.HTTP_BAD_REQUEST)
                    .end("Target volume is read-only: " + targetVolume);
            return;
        }

        EventBusConfig.eventBus().send(EventBusConfig.MIGRATION_EXECUTE_ADDRESS,
                new JsonObject()
                        .put(AppConstants.FIELD_FILE_ID, fileId)
                        .put(AppConstants.FIELD_TARGET_VOLUME, targetVolume));

        ctx.response().end(new JsonObject()
                .put(AppConstants.FIELD_STATUS, "accepted")
                .put(AppConstants.FIELD_MESSAGE, "Migration started in background")
                .put(AppConstants.FIELD_FILE_ID, fileId)
                .put(AppConstants.FIELD_TARGET_VOLUME, targetVolume)
                .encode());
    }

    @Override
    public void getMigrationStatus(@NotNull RoutingContext ctx) {
        String fileId = ctx.pathParam(AppConstants.FIELD_FILE_ID);

        if (fileId == null || fileId.isBlank()) {
            ctx.response().setStatusCode(AppConstants.HTTP_BAD_REQUEST).end("Missing fileId");
            return;
        }

        VolumeManager vm = VolumeManagerConfig.volumeManager();

        Optional<MigrationTask> active = vm.getMigrationStatus(fileId);
        if (active.isPresent()) {
            ctx.response().end(active.get().toJson().encode());
            return;
        }

        vm.getJournalStatusFromFile(fileId).onComplete(statusResult -> {
            if (statusResult.succeeded()) {
                JsonObject result = statusResult.result();
                String status = result.getString(AppConstants.FIELD_STATUS);
                String error = result.getString(AppConstants.FIELD_ERROR);

                if ("FAILED".equals(status) && error != null && !error.isEmpty()) {
                    ctx.response().setStatusCode(AppConstants.HTTP_INTERNAL_ERROR).end(new JsonObject()
                            .put(AppConstants.FIELD_STATUS, status)
                            .put(AppConstants.FIELD_ERROR, error)
                            .put(AppConstants.FIELD_FILE_ID, fileId)
                            .put("sourcePath", result.getString("sourcePath"))
                            .put("targetPath", result.getString("targetPath"))
                            .encode());
                } else {
                    ctx.response().end(result.encode());
                }
            } else {
                ctx.response().setStatusCode(AppConstants.HTTP_NOT_FOUND).end(new JsonObject()
                        .put(AppConstants.FIELD_STATUS, "not_found")
                        .put(AppConstants.FIELD_FILE_ID, fileId)
                        .encode());
            }
        });
    }

    @Override
    public void rollbackMigration(@NotNull RoutingContext ctx) {
        String fileId = ctx.pathParam(AppConstants.FIELD_FILE_ID);

        if (fileId == null || fileId.isBlank()) {
            ctx.response().setStatusCode(AppConstants.HTTP_BAD_REQUEST).end("Missing fileId");
            return;
        }

        EventBusConfig.eventBus().send(EventBusConfig.MIGRATION_ROLLBACK_ADDRESS,
                new JsonObject().put(AppConstants.FIELD_FILE_ID, fileId));

        ctx.response().end(new JsonObject()
                .put(AppConstants.FIELD_STATUS, "accepted")
                .put(AppConstants.FIELD_MESSAGE, "Rollback started in background")
                .put(AppConstants.FIELD_FILE_ID, fileId)
                .encode());
    }

    @Override
    public void migrateVolume(@NotNull RoutingContext ctx) {
        String sourceVolume = ctx.pathParam("source");
        String targetVolume = ctx.pathParam("target");

        if (sourceVolume == null || sourceVolume.isBlank()) {
            ctx.response().setStatusCode(AppConstants.HTTP_BAD_REQUEST).end("Missing source volume");
            return;
        }

        if (targetVolume == null || targetVolume.isBlank()) {
            ctx.response().setStatusCode(AppConstants.HTTP_BAD_REQUEST).end("Missing target volume");
            return;
        }

        EventBusConfig.eventBus().send(EventBusConfig.MIGRATION_VOLUME_ADDRESS,
                new JsonObject()
                        .put(AppConstants.FIELD_SOURCE_VOLUME, sourceVolume)
                        .put(AppConstants.FIELD_TARGET_VOLUME, targetVolume));

        ctx.response().end(new JsonObject()
                .put(AppConstants.FIELD_STATUS, "accepted")
                .put(AppConstants.FIELD_MESSAGE, "Volume migration started in background")
                .put(AppConstants.FIELD_SOURCE_VOLUME, sourceVolume)
                .put(AppConstants.FIELD_TARGET_VOLUME, targetVolume)
                .encode());
    }
}