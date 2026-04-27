package by.losik.meta;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

@Indexed
public class FileMetadata implements Serializable {

    private String fileId;
    private String fileName;
    private String filePath;
    private String contentType;
    private long size;
    private Instant createdAt;
    private Instant updatedAt;

    public FileMetadata() {}

    @ProtoFactory
    public FileMetadata(String fileId, String fileName, String filePath,
                        String contentType, long size, Instant createdAt, Instant updatedAt) {
        this.fileId = fileId;
        this.fileName = fileName;
        this.filePath = filePath;
        this.contentType = contentType;
        this.size = size;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    @ProtoField(number = 1)
    public String getFileId() { return fileId; }
    public void setFileId(String fileId) { this.fileId = fileId; }

    @ProtoField(number = 2)
    @Basic(sortable = true)
    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }

    @ProtoField(number = 3)
    public String getFilePath() { return filePath; }
    public void setFilePath(String filePath) { this.filePath = filePath; }

    @ProtoField(number = 4)
    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { this.contentType = contentType; }

    @ProtoField(number = 5, required = true, defaultValue = "0")
    public long getSize() { return size; }
    public void setSize(long size) { this.size = size; }

    @ProtoField(number = 6)
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    @ProtoField(number = 7)
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileMetadata that = (FileMetadata) o;
        return size == that.size &&
                Objects.equals(fileId, that.fileId) &&
                Objects.equals(fileName, that.fileName) &&
                Objects.equals(filePath, that.filePath) &&
                Objects.equals(contentType, that.contentType) &&
                Objects.equals(createdAt, that.createdAt) &&
                Objects.equals(updatedAt, that.updatedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileId, fileName, filePath, contentType, size, createdAt, updatedAt);
    }

    @Override
    public String toString() {
        return "FileMetadata{" +
                "fileId='" + fileId + '\'' +
                ", fileName='" + fileName + '\'' +
                ", size=" + size +
                '}';
    }
}