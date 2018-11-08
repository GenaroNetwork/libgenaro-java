package network.genaro.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/*

[{"index":0,
"hash":"44ab343bf59d0ab5e7cb1deebcc485b9344c3638",
"size":86918,
"parity":false,
"token":"beeb1cc8256bea01faa67bced8da003eb3944a67",

"farmer":{
"userAgent":"8.7.3",
"protocol":"1.2.0-local",
"address":"59.46.230.210",
"port":9001,
"nodeID":"70a8a597a49aa732860218292b73f6bbc2f63925",
"lastSeen":"2018-11-06T09:34:47.564Z"},

"operation":"PULL"}]
 */
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
    //
    public boolean isPointCreated() {
        return token != null && farmer != null;
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

