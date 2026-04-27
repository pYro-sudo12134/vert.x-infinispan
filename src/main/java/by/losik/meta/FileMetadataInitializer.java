package by.losik.meta;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
        includeClasses = FileMetadata.class,
        schemaFileName = "file-metadata.proto",
        schemaPackageName = "by.losik.meta"
)
public interface FileMetadataInitializer extends GeneratedSchema {
}