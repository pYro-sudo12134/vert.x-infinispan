package by.losik.verticle;

import io.vertx.ext.web.RoutingContext;

public interface HttpProcessor {
    void uploadFile(RoutingContext ctx);

    void getFile(RoutingContext ctx);

    void deleteFile(RoutingContext ctx);

    void updateFile(RoutingContext ctx);

    void listFiles(RoutingContext ctx);

    void downloadFile(RoutingContext ctx);

    void addVolume(RoutingContext ctx);

    void removeVolume(RoutingContext ctx);

    void getVolumeStatus(RoutingContext ctx);

    void migrateFile(RoutingContext ctx);

    void getMigrationStatus(RoutingContext ctx);

    void rollbackMigration(RoutingContext ctx);

    void migrateVolume(RoutingContext ctx);
}