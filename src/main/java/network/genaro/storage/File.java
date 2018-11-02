package network.genaro.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class File {
    private String filename;
    private String mimetype;
    private long size;
    private String id;
    private String created;
    private String rsaKey;
    private String rsaCtr;
    private boolean isShareFile;

    public boolean isShareFile() {
        return isShareFile;
    }

    public void setShareFile(boolean shareFile) {
        isShareFile = shareFile;
    }


    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getMimetype() {
        return mimetype;
    }

    public void setMimetype(String mimetype) {
        this.mimetype = mimetype;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getRsaKey() {
        return rsaKey;
    }

    public void setRsaKey(String rsaKey) {
        this.rsaKey = rsaKey;
    }

    public String getRsaCtr() {
        return rsaCtr;
    }

    public void setRsaCtr(String rsaCtr) {
        this.rsaCtr = rsaCtr;
    }

    @Override
    public String toString() {
        return "File{" +
                "filename='" + filename + '\'' +
                ", mimetype='" + mimetype + '\'' +
                ", size=" + size +
                ", id='" + id + '\'' +
                ", created='" + created + '\'' +
                ", isShareFile=" + isShareFile +
                '}';
    }
}
