package network.genaro.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public final class Bucket {

    private String id;
    private String bucketId;
    private boolean nameIsEncrypted;
    private String encryptionKey;
    private String created;
    private String name;
    private String type;
    private long  limitStorage;
    private long  usedStorage;
    private long  timeStart;
    private long  timeEnd;

    public String getBucketId() {
        return bucketId;
    }

    public void setBucketId(String bucketId) {
        this.bucketId = bucketId;
    }

    public boolean getNameIsEncrypted() { return nameIsEncrypted; }

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

    public long getLimitStorage() { return limitStorage; }

    public void setLimitStorage(long limitStorage) { this.limitStorage = limitStorage; }

    public long getUsedStorage() { return usedStorage; }

    public void setUsedStorage(long usedStorage) { this.usedStorage = usedStorage; }

    public long getTimeStart() { return timeStart; }

    public void setTimeStart(long timeStart) { this.timeStart = timeStart; }

    public long getTimeEnd() { return timeEnd; }

    public void setTimeEnd(long timeEnd) { this.timeEnd = timeEnd; }

    public String getId() { return id; }

    public void setId(String id) { this.id = id; }

    public String getType() { return type; }

    public void setType(String type) { this.type = type; }

    @Override
    public String toString() {
        return "Bucket{" +
                "id='" + id + '\'' +
                ", bucketId='" + bucketId + '\'' +
                ", nameIsEncrypted=" + nameIsEncrypted +
                ", encryptionKey='" + encryptionKey + '\'' +
                ", type=" + type + '\'' +
                ", created='" + created + '\'' +
                ", name='" + name + '\'' +
                ", usedStorage=" + usedStorage +
                '}';
    }
}
