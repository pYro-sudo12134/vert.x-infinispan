package by.losik.verticle;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.util.concurrent.atomic.AtomicInteger;

public interface FileProcessor {

    void handleRequest(Message<Object> message);

    void handleUpload(JsonObject msg, Message<Object> message);

    void handleUpdate(JsonObject msg, Message<Object> message);

    void handleDelete(JsonObject msg, Message<Object> message);

    void handleGet(JsonObject msg, Message<Object> message);

    void handleList(JsonObject msg, Message<Object> message);

    void handleDownload(JsonObject msg, Message<Object> message);

    Future<Void> scanVolumeAndRestore(String volumePath, java.util.concurrent.atomic.AtomicInteger restored,
                                      AtomicInteger failed,
                                      AtomicInteger totalFiles);
}