package by.losik.verticle;

import by.losik.config.AppConfig;
import by.losik.config.EventBusConfig;
import by.losik.config.RouterConfig;
import by.losik.constant.AppConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
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

public class HttpVerticle extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(HttpVerticle.class);

    @Override
    public void start() {
        log.info("Starting HttpVerticle on port: {}", AppConfig.httpPort());

        Router router = RouterConfig.router();

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

    private void uploadFile(@NotNull RoutingContext ctx) {
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

    private void getFile(@NotNull RoutingContext ctx) {
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

    private void deleteFile(@NotNull RoutingContext ctx) {
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

    private void updateFile(@NotNull RoutingContext ctx) {
        String fileId = ctx.pathParam(AppConstants.FIELD_ID);
        log.info("Received update request: fileId={}, from={}", fileId, ctx.request().remoteAddress());

        Optional.of(ctx.fileUploads())
                .filter(uploads -> !uploads.isEmpty())
                .map(uploads -> uploads.iterator().next())
                .ifPresentOrElse(upload -> {
                    String tempPath = upload.uploadedFileName();

                    log.debug("Processing update: fileId={}, newFileName={}, contentType={}, tempPath={}",
                            fileId, upload.fileName(), upload.contentType(), tempPath);

                    var message = new JsonObject()
                            .put(AppConstants.FIELD_ACTION, AppConstants.ACTION_UPDATE)
                            .put(AppConstants.FIELD_FILE_ID, fileId)
                            .put(AppConstants.FIELD_FILE_NAME, upload.fileName())
                            .put(AppConstants.FIELD_FILE_PATH, tempPath)
                            .put(AppConstants.FIELD_CONTENT_TYPE, upload.contentType());

                    EventBusConfig.eventBus().request(EventBusConfig.FILE_UPDATE_ADDRESS, message, reply -> {
                        if (reply.succeeded()) {
                            log.info("Update successful: fileId={}, newFileName={}", fileId, upload.fileName());
                            ctx.response()
                                    .setStatusCode(AppConstants.HTTP_OK)
                                    .end(AppConstants.STATUS_UPDATED);
                        } else {
                            log.error("Update failed for fileId={}, fileName={}: {}",
                                    fileId, upload.fileName(), reply.cause().getMessage());

                            vertx.fileSystem().delete(tempPath)
                                    .onFailure(err -> log.warn("Temp file cleanup failed: {}", tempPath, err));
                            ctx.response()
                                    .setStatusCode(AppConstants.HTTP_INTERNAL_ERROR)
                                    .end(AppConstants.ERR_UPDATE_FAILED);
                        }
                    });
                }, () -> {
                    log.warn("Update request rejected - no file provided: fileId={}", fileId);
                    ctx.response()
                            .setStatusCode(AppConstants.HTTP_BAD_REQUEST)
                            .end(AppConstants.ERR_NO_FILE);
                });
    }

    private void listFiles(@NotNull RoutingContext ctx) {
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
}