package by.losik.service;

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

                int offset = (page - 1) * size;
                query.startOffset(offset).maxResults(size);

                List<FileMetadata> resultList = query.list();

                JsonArray filesArray = new JsonArray();
                for (FileMetadata meta : resultList) {
                    filesArray.add(new JsonObject()
                            .put("fileId", meta.getFileId())
                            .put("fileName", meta.getFileName())
                            .put("contentType", meta.getContentType())
                            .put("size", meta.getSize())
                            .put("createdAt", meta.getCreatedAt().toString())
                            .put("updatedAt", meta.getUpdatedAt().toString()));
                }

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
            default -> "fileName";
        };
    }
}