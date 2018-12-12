package network.genaro.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.*;
import java.util.Arrays;
import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.commons.lang3.ArrayUtils;

import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.DERSequenceGenerator;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.RIPEMD160Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.digests.SHA512Digest;
import org.bouncycastle.crypto.ec.CustomNamedCurves;
import org.bouncycastle.crypto.generators.PKCS5S2ParametersGenerator;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.crypto.params.ECPrivateKeyParameters;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.signers.ECDSASigner;
import org.bouncycastle.crypto.signers.HMacDSAKCalculator;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public final class CryptoUtil {

    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    private static final int RIPEMD160_DIGEST_SIZE = 20;
    private static final int AES_GCM_DIGEST_LENGTH = 16;
    private static final int AES_GCM_IV_LENGTH = 32;
    public static final int AES_BLOCK_SIZE = 16;

    private static final int SEED_ITERATIONS = 2048;
    private static final int SEED_KEY_SIZE = 512;
    public static final byte[] BUCKET_NAME_MAGIC = Hex.decode("398734aab3c4c30c9f22590e83a95f7e43556a45fc2b3060e0c39fde31f50272");
    public static final byte[] BUCKET_META_MAGIC = Hex.decode("42964710327258a0a3239a41a2d5e2d7468a393d3413d2aa26a4a2c856c90251");

    private static final X9ECParameters CURVE_PARAMS = CustomNamedCurves.getByName("secp256k1");
    private static final ECDomainParameters CURVE = new ECDomainParameters(
            CURVE_PARAMS.getCurve(), CURVE_PARAMS.getG(), CURVE_PARAMS.getN(), CURVE_PARAMS.getH());

    public static byte[] sha256(final byte[] input) throws NoSuchAlgorithmException {
        return MessageDigest.getInstance("SHA-256").digest(input);
    }

    public static byte[] sha512(final byte[] input) throws NoSuchAlgorithmException {
        return MessageDigest.getInstance("SHA-512").digest(input);
    }

    public static byte[] hmacSha512(final byte[] key, final byte[] input) {
        HMac hmac = new HMac(new SHA512Digest());
        hmac.init(new KeyParameter(key));
        byte[] result = new byte[hmac.getMacSize()];

        hmac.update(input, 0, input.length);
        hmac.doFinal(result, 0);

        return result;
    }

    public static byte[] hmacSha512Half(final byte[] key, final byte[] input) {
        byte[] decryptKey = hmacSha512(key, input);
        return Arrays.copyOfRange(decryptKey, 0, decryptKey.length / 2);
    }

    public static byte[] ripemd160(final byte[] input) {
        Digest ripemd160DG = new RIPEMD160Digest();
        ripemd160DG.update(input, 0, input.length);
        byte[] out = new byte[RIPEMD160_DIGEST_SIZE];
        ripemd160DG.doFinal(out, 0);
        return out;
    }

    public static byte[] ripemd160Sha256(final byte[] input) throws NoSuchAlgorithmException {
        byte[] sha256bytes = sha256(input);
        return ripemd160(sha256bytes);
    }

    public static byte[] ripemd160Sha256Double(final byte[] input) throws NoSuchAlgorithmException {
        return ripemd160Sha256(ripemd160Sha256(input));
    }

    public static String ripemd160Sha256HexString(final byte[] input) throws NoSuchAlgorithmException {
        return Hex.toHexString(ripemd160Sha256(input));
    }

    public static String ripemd160Sha256HexStringDouble(final byte[] input) throws NoSuchAlgorithmException {
        return Hex.toHexString(ripemd160Sha256Double(input));
    }

    public static byte[] generateDeterministicKey(final byte[] key, final byte[] id ) throws NoSuchAlgorithmException {
        byte[] sha512input = ArrayUtils.addAll(key, id);
        return Arrays.copyOfRange(sha512(sha512input), 0, 32);
    }

    public static byte[] generateGenaroSeed(byte[] key) {
        PKCS5S2ParametersGenerator gen = new PKCS5S2ParametersGenerator(new SHA512Digest());
        gen.init(key, "mnemonic".getBytes(UTF_8), SEED_ITERATIONS);
        return ((KeyParameter) gen.generateDerivedParameters(SEED_KEY_SIZE)).getKey();
    }

    public static byte[] generateBucketKey(final byte[] privKey, final byte[] bucketId) throws NoSuchAlgorithmException {
        byte[] seed = generateGenaroSeed(privKey);
        return generateDeterministicKey(seed, bucketId);
    }

    public static byte[] generateFileKey(final byte[] privKey, final byte[] bucketId, final byte[] index) throws NoSuchAlgorithmException {
        byte[] bKey = generateBucketKey(privKey, bucketId);
        return generateDeterministicKey(bKey, index);
    }

    public static String encryptMeta(final byte[] fileMeta, final byte[] encryptKey, final byte[] encryptIv)
            throws NoSuchAlgorithmException, NoSuchPaddingException, BadPaddingException, IllegalBlockSizeException,
            InvalidKeyException, NoSuchProviderException, InvalidAlgorithmParameterException {
        if (encryptIv.length != AES_GCM_IV_LENGTH) {
            throw new IllegalArgumentException("IV length must be " + AES_GCM_IV_LENGTH);
        }

        Cipher c = Cipher.getInstance("AES/GCM/NoPadding", "BC");
        SecretKeySpec k = new SecretKeySpec(encryptKey, "AES");
        c.init(Cipher.ENCRYPT_MODE, k, new IvParameterSpec(encryptIv));
        byte[] cipherPlusDigest = c.doFinal(fileMeta);

        // GCM digest + iv + cipher text
        byte[] encryptedData = new byte[cipherPlusDigest.length + encryptIv.length];
        // digest
        System.arraycopy(cipherPlusDigest,
                cipherPlusDigest.length - AES_GCM_DIGEST_LENGTH,
                encryptedData,
                0,
                AES_GCM_DIGEST_LENGTH);
        // iv
        System.arraycopy(encryptIv,
                0,
                encryptedData,
                AES_GCM_DIGEST_LENGTH,
                encryptIv.length);
        // cipher text
        System.arraycopy(cipherPlusDigest,
                0,
                encryptedData,
                AES_GCM_DIGEST_LENGTH + encryptIv.length,
                cipherPlusDigest.length - AES_GCM_DIGEST_LENGTH);

        return Base64.toBase64String(encryptedData);
    }

    public static byte[] decryptMeta(final String base64Secret, final byte[] decryptKey) throws NoSuchAlgorithmException, InvalidKeyException,
                IllegalBlockSizeException, BadPaddingException, NoSuchPaddingException, NoSuchProviderException, InvalidAlgorithmParameterException {
        byte[] encryptedData = Base64.decode(base64Secret);

        // get IV
        byte[] decryptIv = Arrays.copyOfRange(encryptedData, AES_GCM_DIGEST_LENGTH, AES_GCM_DIGEST_LENGTH + AES_GCM_IV_LENGTH);
        // make cipher + Digest
        byte[] cipherPlusDigest = new byte[encryptedData.length - AES_GCM_IV_LENGTH];
        int cipherTextLen = encryptedData.length - AES_GCM_DIGEST_LENGTH - AES_GCM_IV_LENGTH;
        // fill digest
        System.arraycopy(encryptedData, 0, cipherPlusDigest, cipherTextLen, AES_GCM_DIGEST_LENGTH);
        // fill cipher text
        System.arraycopy(encryptedData, AES_GCM_DIGEST_LENGTH + AES_GCM_IV_LENGTH, cipherPlusDigest, 0, cipherTextLen);

        Cipher c = Cipher.getInstance("AES/GCM/NoPadding", "BC");
        SecretKeySpec k = new SecretKeySpec(decryptKey, "AES");
        c.init(Cipher.DECRYPT_MODE, k, new IvParameterSpec(decryptIv));

        return c.doFinal(cipherPlusDigest);
    }

    public static String encryptMetaHmacSha512(byte[] meta, byte[] privKey, byte[] bucketId)
            throws NoSuchAlgorithmException, NoSuchPaddingException, BadPaddingException, IllegalBlockSizeException,
            InvalidKeyException, NoSuchProviderException, InvalidAlgorithmParameterException {
        byte[] bucketKey = CryptoUtil.generateBucketKey(privKey, bucketId);
        byte[] key = CryptoUtil.hmacSha512Half(bucketKey, BUCKET_META_MAGIC);
        byte[] nameIv = CryptoUtil.hmacSha512Half(bucketKey, meta);

        return  CryptoUtil.encryptMeta(meta, key, nameIv);
    }

    public static String decryptMetaHmacSha512(String bufferBase64, byte[] privKey, byte[] bucketId)
            throws NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException,
            NoSuchPaddingException, NoSuchProviderException, InvalidAlgorithmParameterException {
        byte[] bk = CryptoUtil.generateBucketKey(privKey, bucketId);
        byte[] decryptKey = CryptoUtil.hmacSha512Half(bk, BUCKET_META_MAGIC);
        byte[] realName = CryptoUtil.decryptMeta(bufferBase64, decryptKey);

        return BasicUtil.bytes2String(realName);
    }

    public static String sha256EscdaSign(final BigInteger ecPrivateKey, final String message) throws NoSuchAlgorithmException {
        return sha256EscdaSign(ecPrivateKey, BasicUtil.string2Bytes(message));
    }

    public static String sha256EscdaSign(final BigInteger ecPrivateKey, final byte[] message) throws NoSuchAlgorithmException {
        byte[] hash = sha256(message);
        return escdaSign(ecPrivateKey, hash);
    }

    private static String escdaSign(final BigInteger ecPrivateKey,final byte[] hash) {
        ECDSASigner signer = new ECDSASigner(new HMacDSAKCalculator(new SHA256Digest()));

        ECPrivateKeyParameters privKey = new ECPrivateKeyParameters(ecPrivateKey, CURVE);
        signer.init(true, privKey);
        BigInteger[] components = signer.generateSignature(hash);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            DERSequenceGenerator seq = new DERSequenceGenerator(baos);
            seq.addObject(new ASN1Integer(components[0]));
            seq.addObject(new ASN1Integer(components[1]));
            seq.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] sig = baos.toByteArray();
        return Hex.toHexString(sig);
    }

    int increment_ctr_aes_iv(byte[] iv, long bytes_position)
    {
        if (bytes_position % AES_BLOCK_SIZE != 0) {
            return 1;
        }

        long times = bytes_position / AES_BLOCK_SIZE;

        while (times > 0) {
            int i = AES_BLOCK_SIZE - 1;
            if (++(iv)[i] == 0) {
                while (i > 0 && ++(iv)[--i] == 0);
            }
            times--;
        }

        return 0;
    }
}
