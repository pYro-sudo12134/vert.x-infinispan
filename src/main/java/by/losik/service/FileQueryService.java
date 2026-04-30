package by.losik.service;

import by.losik.constant.AppConstants;
import by.losik.meta.FileMetadata;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.infinispan.Cache;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class FileQueryService {
    private static final Logger log = LoggerFactory.getLogger(FileQueryService.class);
    private final Cache<String, FileMetadata> cache;
    private final Vertx vertx;

    public FileQueryService(Vertx vertx, Cache<String, FileMetadata> cache) {
        this.vertx = vertx;
        this.cache = cache;
    }

    public Future<JsonObject> listFiles(int page, int size, String prefix, String sort, String order) {
        Promise<JsonObject> promise = Promise.promise();

        vertx.executeBlocking(() -> {
            try {
                QueryFactory queryFactory = Search.getQueryFactory(cache);

                String sortField = getSortField(sort);
                boolean ascending = !"desc".equalsIgnoreCase(order);

                StringBuilder queryString = new StringBuilder("FROM by.losik.meta.FileMetadata");
                if (prefix != null && !prefix.isEmpty()) {
                    queryString.append(" WHERE fileName LIKE :prefix");
                }
                queryString.append(" ORDER BY ").append(sortField);
                queryString.append(ascending ? " ASC" : " DESC");

                Query<FileMetadata> query = queryFactory.create(queryString.toString());

                if (prefix != null && !prefix.isEmpty()) {
                    query.setParameter("prefix", prefix + "%");
                }

                long total = query.execute().hitCount().orElse(0L);

                query.startOffset((long) (page - 1) * size).maxResults(size);

                JsonArray filesArray = new JsonArray(
                        query.list()
                                .stream().map(metadata ->
                                        new JsonObject()
                                                .put(AppConstants.FIELD_FILE_ID, metadata.getFileId())
                                                .put(AppConstants.FIELD_FILE_NAME, metadata.getFileName())
                                                .put(AppConstants.FIELD_FILE_PATH, metadata.getFilePath())
                                                .put(AppConstants.FIELD_CONTENT_TYPE, metadata.getContentType())
                                                .put(AppConstants.FIELD_SIZE, metadata.getSize())
                                                .put(AppConstants.FIELD_CREATED_AT, metadata.getCreatedAt().toString())
                                                .put(AppConstants.FIELD_UPDATED_AT, metadata.getUpdatedAt().toString()))
                                .collect(Collectors.toList()));

                log.info("List returned {} files (total: {})", filesArray.size(), total);

                return new JsonObject()
                        .put("files", filesArray)
                        .put("page", page)
                        .put("size", size)
                        .put("total", total);

            } catch (Exception e) {
                log.error("Query execution failed", e);
                throw new RuntimeException("Failed to list files: " + e.getMessage(), e);
            }
        }, false, promise);

        return promise.future();
    }

    private String getSortField(String sort) {
        return switch (sort) {
            case "size" -> "size";
            case "date", "createdAt" -> "createdAt";
            case "updatedAt" -> "updatedAt";
            case "name", "fileName" -> "fileName";
            default -> "filePath";
        };
    }
}