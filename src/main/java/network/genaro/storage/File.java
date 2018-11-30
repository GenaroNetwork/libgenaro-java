package network.genaro.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class File {
    public class Hmac {
        String value;
        String type;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return "Hmac{" +
                    "value='" + value + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }
    public class Erasure {
        String type;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return "Erasure{" +
                    "type='" + type + '\'' +
                    '}';
        }
    }

    // get file info field
    private String bucket;
    private String frame;
    private Hmac hmac;
    private Erasure erasure;
    private String index;

    private String filename;
    private String mimetype;
    private long size;
    private String id;
    private String created;
    private String rsaKey;
    private String rsaCtr;
    private boolean isShareFile;

    public boolean isShareFile() { return isShareFile; }

    public void setShareFile(boolean shareFile) {
        isShareFile = shareFile;
    }

    public String getFilename() { return filename; }

    public void setFilename(String filename) { this.filename = filename; }

    public String getMimetype() { return mimetype; }

    public void setMimetype(String mimetype) { this.mimetype = mimetype; }

    public long getSize() { return size; }

    public void setSize(long size) { this.size = size; }

    public String getId() { return id; }

    public void setId(String id) { this.id = id; }

    public String getCreated() { return created; }

    public void setCreated(String created) { this.created = created; }

    public String getRsaKey() { return rsaKey; }

    public void setRsaKey(String rsaKey) { this.rsaKey = rsaKey; }

    public String getRsaCtr() { return rsaCtr; }

    public void setRsaCtr(String rsaCtr) { this.rsaCtr = rsaCtr; }

    public String getBucket() { return bucket; }

    public void setBucket(String bucket) { this.bucket = bucket; }

    public String getFrame() { return frame; }

    public void setFrame(String frame) { this.frame = frame; }

    public Hmac getHmac() { return hmac; }

    public void setHmac(Hmac hmac) { this.hmac = hmac; }

    public Erasure getErasure() { return erasure; }

    public void setErasure(Erasure erasure) { this.erasure = erasure; }

    public String getIndex() { return index; }

    public void setIndex(String index) { this.index = index; }

    @Override
    public String toString() {
        return "File{" +
                "bucket='" + bucket + '\'' +
                ", frame='" + frame + '\'' +
                ", hmac=" + hmac +
                ", erasure=" + erasure +
                ", index='" + index + '\'' +
                ", filename='" + filename + '\'' +
                ", mimetype='" + mimetype + '\'' +
                ", size=" + size +
                ", id='" + id + '\'' +
                ", created='" + created + '\'' +
                ", rsaKey='" + rsaKey + '\'' +
                ", rsaCtr='" + rsaCtr + '\'' +
                ", isShareFile=" + isShareFile +
                '}';
    }
}
