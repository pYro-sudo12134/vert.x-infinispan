package by.losik.verticle;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public interface MigrationHandler {
    void handleMigration(Message<JsonObject> message);

    void handleRollback(Message<JsonObject> message);

    void handleStatus(Message<JsonObject> message);

    void handleVolumeMigration(Message<JsonObject> message);
}