package network.genaro.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class Pointer {
    private int index;
    private String hash;
    private long size;
    boolean parity;

    // token & operation & Farmer will be null if no farmer found
    private String token;
    private String operation;
    private Farmer farmer;

    private long downloadedSize;

    private boolean isMissing;

    // extra
    private PointerStatus status;

    public PointerStatus getStatus() {
        return status;
    }

    public void setStatus(PointerStatus status) {
        this.status = status;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Farmer getFarmer() {
        return farmer;
    }

    public void setFarmer(Farmer farmer) {
        this.farmer = farmer;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean isParity() {
        return parity;
    }

    public void setParity(boolean parity) {
        this.parity = parity;
    }

    @Override
    public String toString() {
        return "Pointer{" +
                "index=" + index +
                ", hash='" + hash + '\'' +
                ", size=" + size +
                ", parity=" + parity +
                ", token='" + token + '\'' +
                ", operation='" + operation + '\'' +
                ", farmer=" + farmer +
                '}';
    }

    public String toBriefString() {
        return "Pointer{" +
                "index=" + index +
                ", size=" + size +
                ", parity=" + parity +
                ", farmer=" + farmer.toBriefString() +
                '}';
    }

    //
    public boolean isPointCreated() {
        return token != null && farmer != null;
    }

    public boolean isMissing() {
        return isMissing;
    }

    public void setMissing(boolean missing) {
        isMissing = missing;
    }

    public long getDownloadedSize() {
        return downloadedSize;
    }

    public void setDownloadedSize(long downloadedSize) {
        this.downloadedSize = downloadedSize;
    }

    public enum PointerStatus
    {
        POINTER_BEING_REPLACED,
        POINTER_ERROR_REPORTED,
        POINTER_ERROR,
        POINTER_CREATED,
        POINTER_BEING_DOWNLOADED,
        POINTER_DOWNLOADED,
        POINTER_MISSING,
        POINTER_FINISHED
    }

}

