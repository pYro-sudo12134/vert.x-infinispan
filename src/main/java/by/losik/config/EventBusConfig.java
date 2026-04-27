package by.losik.config;

import io.vertx.core.eventbus.EventBus;

public class EventBusConfig {
    public static final String FILE_UPLOAD_ADDRESS = "file.upload";
    public static final String FILE_GET_ADDRESS = "file.get";
    public static final String FILE_DELETE_ADDRESS = "file.delete";
    public static final String FILE_LIST_ADDRESS = "file.list";
    public static final String FILE_UPDATE_ADDRESS = "file.update";
    public static final String FILE_DOWNLOAD_ADDRESS = "file.download";
    public static EventBus eventBus() {
        return VertxConfig.vertx().eventBus();
    }
}
