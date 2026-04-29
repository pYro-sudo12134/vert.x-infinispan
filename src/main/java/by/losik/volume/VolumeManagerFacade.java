package by.losik.volume;

import by.losik.meta.FileMetadata;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.Map;
import java.util.Optional;

public interface VolumeManagerFacade {

    VolumeSelectionStrategy getStrategy();

    String selectVolume(FileMetadata metadata);

    Map<String, VolumeState> getVolumes();

    Future<Void> addVolume(String volumePath);

    Future<Void> removeVolume(String volumePath, String targetVolume);

    Future<Void> migrateAllFiles(String sourceVolume, String targetVolume);

    Future<Void> migrateFile(String fileId, String targetVolume);

    Optional<MigrationTask> getMigrationStatus(String fileId);

    Future<Void> rollbackMigration(String fileId);

    void incrementFileCount(String volumePath);

    void decrementFileCount(String volumePath);

    JsonObject getStatus();
}