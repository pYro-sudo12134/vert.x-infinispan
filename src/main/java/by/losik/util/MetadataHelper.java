package by.losik.util;

import by.losik.config.EventBusConfig;
import by.losik.constant.AppConstants;
import by.losik.meta.FileMetadata;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class MetadataHelper {

    public static Future<JsonObject> getMetadata(String fileId) {
        Promise<JsonObject> promise = Promise.promise();

        EventBusConfig.eventBus().request(
                EventBusConfig.FILE_GET_ADDRESS,
                new JsonObject().put(AppConstants.FIELD_ACTION, AppConstants.ACTION_GET)
                        .put(AppConstants.FIELD_FILE_ID, fileId),
                reply -> {
                    if (reply.succeeded()) {
                        promise.complete((JsonObject) reply.result().body());
                    } else {
                        promise.fail(reply.cause());
                    }
                }
        );

        return promise.future();
    }

    public static Future<JsonArray> listAllFiles() {
        Promise<JsonArray> promise = Promise.promise();

        EventBusConfig.eventBus().<JsonObject>request(
                EventBusConfig.FILE_LIST_ADDRESS,
                new JsonObject().put("action", AppConstants.ACTION_LIST),
                reply -> {
                    if (reply.succeeded()) {
                        JsonArray files = reply.result().body().getJsonArray("files");
                        promise.complete(files);
                    } else {
                        promise.fail(reply.cause());
                    }
                }
        );

        return promise.future();
    }
}