package network.genaro.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class Bucket {

    private String id;
    private String bucketId;
    private boolean nameIsEncrypted;
    private String encryptionKey;
    private String created;
    private String name;
    private long usedStorage;

    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(String bucketId) {
        this.bucketId = bucketId;
    }

    public boolean isNameIsEncrypted() {
        return nameIsEncrypted;
    }

    public void setNameIsEncrypted(boolean nameIsEncrypted) {
        this.nameIsEncrypted = nameIsEncrypted;
    }

    public String getEncryptionKey() {
        return encryptionKey;
    }

    public void setEncryptionKey(String encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getUsedStorage() {
        return usedStorage;
    }

    public void setUsedStorage(long usedStorage) {
        this.usedStorage = usedStorage;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Bucket{" +
                "id='" + id + '\'' +
                ", bucketId='" + bucketId + '\'' +
                ", nameIsEncrypted=" + nameIsEncrypted +
                ", encryptionKey='" + encryptionKey + '\'' +
                ", created='" + created + '\'' +
                ", name='" + name + '\'' +
                ", usedStorage=" + usedStorage +
                '}';
    }
}
