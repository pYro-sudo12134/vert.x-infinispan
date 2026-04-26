package by.losik.verticle;

import by.losik.meta.FileMetadata;
import io.vertx.core.shareddata.AsyncMap;

@FunctionalInterface
public interface RunnableWithCompletion {
    void run(AsyncMap<String, FileMetadata> map, Runnable releaseLock);
}