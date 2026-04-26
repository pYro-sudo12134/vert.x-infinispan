package by.losik.meta;

import java.io.Serializable;
import java.time.Instant;

public record FileMetadata(
        String fileId,
        String fileName,
        String filePath,
        String contentType,
        long size,
        Instant createdAt,
        Instant updatedAt
) implements Serializable {}