package network.genaro.storage;

/* encryption info for AES encryption */
public class EncryptionInfo {
    private byte[] index;
    private byte[] key;
    private byte[] ctr;

    // encryption of key by RSA
    private byte[] rsaKey;

    // encryption of ctr by RSA
    private byte[] rsaCtr;

    public EncryptionInfo(byte[] index, byte[] key, byte[] ctr) {
        this.index = index;
        this.key = key;
        this.ctr = ctr;
    }

    public byte[] getIndex() {
        return index;
    }

    public void setIndex(byte[] index) {
        this.index = index;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getCtr() {
        return ctr;
    }

    public void setCtr(byte[] ctr) {
        this.ctr = ctr;
    }

    public byte[] getRsaKey() {
        return rsaKey;
    }

    public void setRsaKey(byte[] rsaKey) {
        this.rsaKey = rsaKey;
    }

    public byte[] getRsaCtr() {
        return rsaCtr;
    }

    public void setRsaCtr(byte[] rsaCtr) {
        this.rsaCtr = rsaCtr;
    }
}
