package network.genaro.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
final class Frame {
    private String user;
    private long storageSize;
    private long size;
    private boolean locked;
    private String created;
    private String id;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public long getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(long storageSize) {
        this.storageSize = storageSize;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Frame{" +
                "user='" + user + '\'' +
                ", storageSize=" + storageSize +
                ", size=" + size +
                ", locked=" + locked +
                ", created='" + created + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
