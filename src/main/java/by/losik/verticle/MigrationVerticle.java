package by.losik.verticle;

import by.losik.config.EventBusConfig;
import by.losik.config.VolumeManagerConfig;
import by.losik.constant.AppConstants;
import by.losik.volume.MigrationTask;
import by.losik.volume.VolumeManager;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class MigrationVerticle extends AbstractVerticle implements MigrationHandler {
    private static final Logger log = LoggerFactory.getLogger(MigrationVerticle.class);

    private VolumeManager volumeManager;

    @Override
    public void start(@NotNull Promise<Void> startPromise) {
        volumeManager = VolumeManagerConfig.volumeManager();

        vertx.eventBus().consumer(EventBusConfig.MIGRATION_EXECUTE_ADDRESS, this::handleMigration);
        vertx.eventBus().consumer(EventBusConfig.MIGRATION_ROLLBACK_ADDRESS, this::handleRollback);
        vertx.eventBus().consumer(EventBusConfig.MIGRATION_STATUS_ADDRESS, this::handleStatus);
        vertx.eventBus().consumer(EventBusConfig.MIGRATION_VOLUME_ADDRESS, this::handleVolumeMigration);

        log.info("MigrationVerticle started, listening on: {}, {}, {}, {}",
                EventBusConfig.MIGRATION_EXECUTE_ADDRESS, EventBusConfig.MIGRATION_ROLLBACK_ADDRESS,
                EventBusConfig.MIGRATION_STATUS_ADDRESS, EventBusConfig.MIGRATION_VOLUME_ADDRESS);

        startPromise.complete();
    }

    @Override
    public void handleMigration(@NotNull Message<JsonObject> message) {
        JsonObject body = message.body();
        String fileId = body.getString(AppConstants.FIELD_FILE_ID);
        String targetVolume = body.getString(AppConstants.FIELD_TARGET_VOLUME);

        if (fileId == null || targetVolume == null) {
            message.fail(AppConstants.HTTP_BAD_REQUEST, "Missing fileId or targetVolume");
            return;
        }

        log.info("Starting migration: fileId={} -> {}", fileId, targetVolume);

        volumeManager.migrateFile(fileId, targetVolume)
                .onSuccess(v -> {
                    log.info("Migration completed: fileId={} -> {}", fileId, targetVolume);
                    message.reply(new JsonObject()
                            .put(AppConstants.FIELD_STATUS, "completed")
                            .put(AppConstants.FIELD_FILE_ID, fileId)
                            .put(AppConstants.FIELD_TARGET_VOLUME, targetVolume));
                })
                .onFailure(err -> {
                    log.error("Migration failed: fileId={} -> {}", fileId, targetVolume, err);
                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, err.getMessage());
                });
    }

    @Override
    public void handleRollback(@NotNull Message<JsonObject> message) {
        JsonObject body = message.body();
        String fileId = body.getString(AppConstants.FIELD_FILE_ID);

        if (fileId == null) {
            message.fail(AppConstants.HTTP_BAD_REQUEST, "Missing fileId");
            return;
        }

        log.info("Rolling back migration: fileId={}", fileId);

        volumeManager.rollbackMigration(fileId)
                .onSuccess(v -> {
                    log.info("Rollback completed: fileId={}", fileId);
                    message.reply(new JsonObject()
                            .put(AppConstants.FIELD_STATUS, "rolled_back")
                            .put(AppConstants.FIELD_FILE_ID, fileId));
                })
                .onFailure(err -> {
                    log.error("Rollback failed: fileId={}", fileId, err);
                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, err.getMessage());
                });
    }

    @Override
    public void handleStatus(@NotNull Message<JsonObject> message) {
        JsonObject body = message.body();
        String fileId = body.getString(AppConstants.FIELD_FILE_ID);

        if (fileId == null) {
            message.fail(AppConstants.HTTP_BAD_REQUEST, "Missing fileId");
            return;
        }

        Optional<MigrationTask> task = volumeManager.getMigrationStatus(fileId);

        if (task.isPresent()) {
            message.reply(task.get().toJson());
        } else {
            message.reply(new JsonObject()
                    .put(AppConstants.FIELD_STATUS, "not_found")
                    .put(AppConstants.FIELD_FILE_ID, fileId));
        }
    }

    @Override
    public void handleVolumeMigration(@NotNull Message<JsonObject> message) {
        JsonObject body = message.body();
        String sourceVolume = body.getString(AppConstants.FIELD_SOURCE_VOLUME);
        String targetVolume = body.getString(AppConstants.FIELD_TARGET_VOLUME);

        if (sourceVolume == null || targetVolume == null) {
            message.fail(AppConstants.HTTP_BAD_REQUEST, "Missing sourceVolume or targetVolume");
            return;
        }

        log.info("Starting volume migration: {} -> {}", sourceVolume, targetVolume);

        volumeManager.migrateAllFiles(sourceVolume, targetVolume)
                .onSuccess(v -> {
                    log.info("Volume migration completed: {} -> {}", sourceVolume, targetVolume);
                    message.reply(new JsonObject()
                            .put(AppConstants.FIELD_STATUS, "completed")
                            .put(AppConstants.FIELD_SOURCE_VOLUME, sourceVolume)
                            .put(AppConstants.FIELD_TARGET_VOLUME, targetVolume));
                })
                .onFailure(err -> {
                    log.error("Volume migration failed: {} -> {}", sourceVolume, targetVolume, err);
                    message.fail(AppConstants.HTTP_INTERNAL_ERROR, err.getMessage());
                });
    }
}