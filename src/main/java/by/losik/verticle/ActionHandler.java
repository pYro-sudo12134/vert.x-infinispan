package by.losik.verticle;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

@FunctionalInterface
public interface ActionHandler {
    void handle(JsonObject msg, Message<Object> message);
}