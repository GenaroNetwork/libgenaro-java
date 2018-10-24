package network.genaro.storage;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.RIPEMD160Digest;
import org.bouncycastle.util.encoders.Hex;

public final class cryptoUtil {

    static final int RIPEMD160_DIGEST_SIZE = 20;
    static final String BUCKET_NAME_MAGIC = "398734aab3c4c30c9f22590e83a95f7e43556a45fc2b3060e0c39fde31f50272";

    protected static byte[] string2Bytes(final String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }
    private static String bytes2String(final byte[] input) {
        return new String(input, StandardCharsets.UTF_8);
    }
    private static String bytes2HexString(final byte[] input) {
        return Hex.toHexString(input);
    }
    private static byte[] hexString2Bytes(final String input) {
        return Hex.decode(input);
    }


    private static byte[] sha256(final byte[] input) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return digest.digest(input);
    }

    private static byte[] sha512(final byte[] input) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-512");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(1);
        }
        digest.update(input);
        return digest.digest();
    }

    private static byte[] ripemd160(final byte[] input) {
        Digest ripemd160DG = new RIPEMD160Digest();
        ripemd160DG.update(input, 0, input.length);
        byte[] out = new byte[RIPEMD160_DIGEST_SIZE];
        ripemd160DG.doFinal(out, 0);
        return out;
    }

    public static byte[] ripemd160Sha256(final byte[] input) {
        byte[] sha256bytes = sha256(input);
        return ripemd160(sha256bytes);
    }

    public static byte[] ripemd160Sha256Double(final byte[] input) {
        return ripemd160Sha256(ripemd160Sha256(input));
    }

    public static String ripemd160Sha256HexString(final byte[] input) {
        return bytes2String(ripemd160Sha256(input));
    }

    public static String ripemd160Sha256HexStringDouble(final byte[] input) {
        return bytes2String(ripemd160Sha256Double(input));
    }

    public static String getDeterministicKey(final String keyHex, final String idHex ) {
        byte[] sha512input = hexString2Bytes(keyHex + idHex);
        return bytes2HexString(sha512(sha512input));
    }

//    public static String generateBucketKey(final byte[] privKey, final String bucketId) {
//
//    }
}
