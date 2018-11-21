package network.genaro.storage;


// {"user":"0x5d14313c94f1b26d23f4ce3a49a2e136a88a584b",
// "shards":[],
// "storageSize":0,
// "size":0,
// "locked":false,
// "created":"2018-11-14T10:44:06.594Z",
// "id":"5bebfc767d314d2eca8339e0"}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class Frame {
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
