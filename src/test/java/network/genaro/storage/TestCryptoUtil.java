package network.genaro.storage;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.DERSequenceGenerator;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.digests.SHA512Digest;
import org.bouncycastle.crypto.ec.CustomNamedCurves;
import org.bouncycastle.crypto.generators.PKCS5S2ParametersGenerator;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.crypto.params.ECPrivateKeyParameters;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.signers.ECDSASigner;
import org.bouncycastle.crypto.signers.HMacDSAKCalculator;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.bouncycastle.util.encoders.Hex;

import org.web3j.crypto.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.*;
import static java.nio.charset.StandardCharsets.UTF_8;

import static network.genaro.storage.CryptoUtil.sha256EscdaSign;

@Test()
public class TestCryptoUtil {
    private static String V3JSON = "{ \"address\": \"fbad65391d2d2eafda9b27326d1e81d52a6a3dc8\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"e968751f3d60827b6e62e3ff6c024ecc82f33a6c55428be33249c83edba444ca\", \"cipherparams\": { \"iv\": \"e80d9ec9ba6241a143c756ec78066ad9\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"ea7cb2b004db67d3103b3790caced7a96b636762f280b243e794fb5bef8ef74b\" }, \"mac\": \"cdb3789e77be8f2a7ab4d205bf1b54e048ad3f5b080b96e07759de7442e050d2\" }, \"id\": \"e28f31b4-1f43-428b-9b12-ab586638d4b1\", \"version\": 3 }";

    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    public void test() throws Exception {
        byte[] byts = BasicUtil.string2Bytes("123kkk");
        byte[] ripemded = CryptoUtil.ripemd160Sha256(byts);
        String hexx = Hex.toHexString(ripemded);
        Assert.assertEquals(hexx, "8792d67cdcf37195da42c6a8db27745424647d69");
    }

    public void testSha256() throws Exception {
        byte[] bbb = CryptoUtil.sha256((BasicUtil.string2Bytes("abcde")));
        String hresult = Hex.toHexString(bbb);
        Assert.assertEquals(hresult, "36bbe50ed96841d10443bcb670d6554f0a34b761be67ec9c4a8ad2c0c44ca42c");
    }

    public void testRipemd160() {
        byte[] byts = BasicUtil.string2Bytes("123kkk");
        byte[] out = CryptoUtil.ripemd160(byts);
        Assert.assertEquals(Hex.toHexString(out), "5d3405a87994ba690252b17a7cf33d774448df9c");
    }

    public void testRipemd160Sha256() throws Exception {
        byte[] bbb = CryptoUtil.ripemd160Sha256(Hex.decode("36217336fd8deee3"));
        String hresult = Hex.toHexString(bbb);
        Assert.assertEquals(hresult, "47bf19535be7d2058875c28dbac9fa3070bbd809");
    }

    public void string2HexTest() {
        String tt = Hex.toHexString(Hex.decode("12343213ef"));
        Assert.assertEquals(tt, "12343213ef");
    }

    public void testDeterministicKey() throws Exception {
        byte[] ss = CryptoUtil.generateDeterministicKey(Hex.decode("1625348fba"), Hex.decode("385960ffa4"));
        Assert.assertEquals(Hex.toHexString(ss), "296195601e0557bef8963a418c53489f4216e8fe033768b5ca2a9bfb02188296");
    }

    public void testMnemonic2Seed() {
        byte[] seed = MnemonicUtils.generateSeed("dsa", "");
        byte[] seed2 = MnemonicUtils.generateSeed("29565ea5ecddb8fd624932dc82c24fd5fe9e06a3ccf5c5764e4a64712aa834a6", "");
        String hresult = Hex.toHexString(seed);
        String hresult2 = Hex.toHexString(seed2);
        Assert.assertEquals(hresult, "fd3fd4cca39658475de1e81ec24d05cecf7696bfaebeaabc29d36e552afac67ecc2649d1b83ae3f2ea169083f6ec90e5ce1e8cf7f5343188874745488fc811b7");
    }

    public static byte[] generateGenaroSeed(byte[] key) {
        final int SEED_ITERATIONS = 2048;
        final int SEED_KEY_SIZE = 512;

        PKCS5S2ParametersGenerator gen = new PKCS5S2ParametersGenerator(new SHA512Digest());
        gen.init(key, "mnemonic".getBytes(UTF_8), SEED_ITERATIONS);

        return ((KeyParameter) gen.generateDerivedParameters(SEED_KEY_SIZE)).getKey();
    }

    public void testMnemonic2Seed2() throws UnsupportedEncodingException {
        String kkk = "29565ea5ecddb8fd624932dc82c24fd5fe9e06a3ccf5c5764e4a64712aa834a6";
        byte[] buf = Hex.decode(kkk);
        System.out.println(BasicUtil.bytes2String(buf));
        byte[] buf2 = kkk.getBytes(UTF_8);
        byte[] seed = MnemonicUtils.generateSeed(kkk, "");
        byte[] seed2 = MnemonicUtils.generateSeed(new String(buf), "");
        byte[] seed3 = generateGenaroSeed(buf2);
        String hresult = Hex.toHexString(seed);
        String hresult2 = Hex.toHexString(seed2);
        String hresult3 = Hex.toHexString(seed3);
//        Assert.assertEquals(hresult, "fd3fd4cca39658475de1e81ec24d05cecf7696bfaebeaabc29d36e552afac67ecc2649d1b83ae3f2ea169083f6ec90e5ce1e8cf7f5343188874745488fc811b7");
        //seed	char *	"575e61fff94f741ccd1c2e3445a28f099ea44b05da7ea4d2d377acb61c339aa0f0e56cd771a2426b353e81dee3976857efbc789efdc3433cd3c0b3887730b9f0"	0x00000001029010e0
    }

    public void testGenerateBucketKey() throws Exception {
        byte[] key = CryptoUtil.generateBucketKey(BasicUtil.string2Bytes("abcde abcde abcde abcde abcde abcde abcde abcde abcde abcde abcd"), Hex.decode("0123456789ab0123456789ab"));
        Assert.assertEquals(Hex.toHexString(key), "b17403c5130847731abd1c233e74002aa666c71497a19c90b7c305479ccd5844");
    }

    public void testGenerateBucketKey2() throws Exception {
        byte[] pkpk = Hex.decode("29565ea5ecddb8fd624932dc82c24fd5fe9e06a3ccf5c5764e4a64712aa834a6");
        String magicBid = "398734aab3c4c30c9f22590e83a95f7e43556a45fc2b3060e0c39fde31f50272";
        byte[] key = CryptoUtil.generateBucketKey(pkpk, Hex.decode(magicBid));
        String kstr = Hex.toHexString(key);
        Assert.assertEquals(kstr, "76afd3c76a52bd9a2fc7449c5701dbaaa2caa2e67e8bfcee3b108c06dacc576a");
    }

    public void testGenerateFileKey() throws Exception {
//        String mnemonic = "abandon";
        String mnemonic = "abcde abcde abcde abcde abcde abcde abcde abcde abcde abcde abcd";
        String bucket_id = "0123456789ab0123456789ab";
        String index = "150589c9593bbebc0e795d8c4fa97304b42c110d9f0095abfac644763beca66e";
        byte[] key = CryptoUtil.generateFileKey(BasicUtil.string2Bytes(mnemonic), Hex.decode(bucket_id), Hex.decode(index));
        Assert.assertEquals(Hex.toHexString(key), "eccd01f6a87991ff0b504718df1da40cb2bcda48099375f5124358771c9ebe2c");
    }

//    public void testAES() {
//        String message = "1234567890"; // e105e1aaf8da 6019753b58409d356e5c1cfc5a053ea8
//        //String message = "ewqew"; //    e105e1aaf8   fe6356a870b6db693a30a152d8e594b8
//        byte[] key = Hex.decode("123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f");
//        byte[] iv  = Hex.decode("123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abcf2");
//        String base64Secret = CryptoUtil.encryptMeta(string2Bytes(message), key, iv);
//        byte[] messageBytes = CryptoUtil.decryptMeta(base64Secret, key);
//        Assert.assertEquals(base64Secret, "ODKPheXwS0J/eUd7eH/LdRI6vC8SOrwvEjq8LxI6vC8SOrwvEjq8LxI6vC8SOrzy4KwePJHJR6JDvg==");
//        Assert.assertEquals(new String(messageBytes), message);
//    }

    public void testAES() throws Exception {
        String message = "1234567890"; // e105e1aaf8da 6019753b58409d356e5c1cfc5a053ea8
        //String message = "ewqew"; //    e105e1aaf8   fe6356a870b6db693a30a152d8e594b8
        byte[] key = Hex.decode("123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f");
        byte[] iv  = Hex.decode("123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abcf2");
        String base64Secret = CryptoUtil.encryptMeta(BasicUtil.string2Bytes(message), key, iv);
        byte[] messageBytes = CryptoUtil.decryptMeta(base64Secret, key);
        Assert.assertEquals(base64Secret, "ODKPheXwS0J/eUd7eH/LdRI6vC8SOrwvEjq8LxI6vC8SOrwvEjq8LxI6vC8SOrzy4KwePJHJR6JDvg==");
        Assert.assertEquals(new String(messageBytes), message);
    }

    public void testDecryptMeta() throws Exception {
        byte[] realnameba = CryptoUtil.decryptMeta("0PkgasRWbaPHhAlRIPf/ZdhopoGRv4nQk8PeZQeyCizXv+DeNGbx48KobaTbRI9r9CTLBwOo", Hex.decode("727324ff68e45f183951f13d7fd70efd653cccf73ef8b60e3cbe7560aacecd8c"));
        String name = new String(realnameba);
        System.out.println(name);
    }

    public void testSignature() throws Exception {

        String v3json = "{ \"address\": \"5d14313c94f1b26d23f4ce3a49a2e136a88a584b\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"12d3a710778aa884d32140466ce6c3932629d922fa1cd6b64996dff9b368743a\", \"cipherparams\": { \"iv\": \"f0eface44a93bac55857d74740912d13\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"62dd6d60fb04429fc8cf32fd39ea5e886d7f84eae258866c14905fa202dbc43d\" }, \"mac\": \"632e92cb1de1a708b2d349b9ae558a4d655c691d3e793fca501a857c7f0c3b1c\" }, \"id\": \"b12b56a5-7eaa-4d90-87b5-cc616e6694d0\", \"version\": 3 }";
        ObjectMapper objectMapper = new ObjectMapper();
        WalletFile walletFile = objectMapper.readValue(v3json, WalletFile.class);
        ECKeyPair ecKeyPair = Wallet.decrypt("123456", walletFile);
        byte[] fakeHash = "message to signmessage to signmu".getBytes(StandardCharsets.UTF_8);

        final X9ECParameters CURVE_PARAMS = CustomNamedCurves.getByName("secp256k1");
        final ECDomainParameters CURVE = new ECDomainParameters(
                CURVE_PARAMS.getCurve(), CURVE_PARAMS.getG(), CURVE_PARAMS.getN(), CURVE_PARAMS.getH());

        ECDSASigner signer = new ECDSASigner(new HMacDSAKCalculator(new SHA256Digest()));

        ECPrivateKeyParameters privKey = new ECPrivateKeyParameters(ecKeyPair.getPrivateKey(), CURVE);
        signer.init(true, privKey);
        BigInteger[] components = signer.generateSignature(fakeHash);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DERSequenceGenerator seq = new DERSequenceGenerator(baos);
        seq.addObject(new ASN1Integer(components[0]));
        seq.addObject(new ASN1Integer((components[1])));
        seq.close();
        byte[] sigsig = baos.toByteArray();
        String sigStr = Hex.toHexString(sigsig);
        System.out.println(sigStr);
    }

    public void testSha256EscdaSign() throws Exception{
        String message = "hello world";
        String v3json = "{ \"address\": \"5d14313c94f1b26d23f4ce3a49a2e136a88a584b\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"12d3a710778aa884d32140466ce6c3932629d922fa1cd6b64996dff9b368743a\", \"cipherparams\": { \"iv\": \"f0eface44a93bac55857d74740912d13\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"62dd6d60fb04429fc8cf32fd39ea5e886d7f84eae258866c14905fa202dbc43d\" }, \"mac\": \"632e92cb1de1a708b2d349b9ae558a4d655c691d3e793fca501a857c7f0c3b1c\" }, \"id\": \"b12b56a5-7eaa-4d90-87b5-cc616e6694d0\", \"version\": 3 }";
        ObjectMapper objectMapper = new ObjectMapper();
        WalletFile walletFile = objectMapper.readValue(v3json, WalletFile.class);
        ECKeyPair ecKeyPair = Wallet.decrypt("123456", walletFile);
        String sig = sha256EscdaSign(ecKeyPair.getPrivateKey(), message);
        System.out.println(sig);
    }

    public void testAesCtr() throws Exception {
        // make Iv byte[]
//        byte[] iv  = Hex.decode("f123abc2f123abcf2123abc2f123abc2");
//        // make key
//        byte[] key = Hex.decode("123abc2fabc2f123abcf123abc2f1232123abc2fabc2f123abcf123abc2f1232");
//        AesCtr aesCtr = new AesCtr(key, iv);
//
//        String message = "I got you in my sight";
//        byte[] encMsg = aesCtr.encrypt(string2Bytes(message));
//        byte[] msg2 = aesCtr.decrypt(encMsg);
//        String deMessage = new String(msg2, StandardCharsets.UTF_8);
//
//
//        System.out.println(deMessage);
    }

    public void testGetPrivateKey() throws Exception {
        Genaro api = new Genaro(null, V3JSON, "lgygn_9982");

        byte[] key = api.getPrivateKey();
        String keyStr = Hex.toHexString(key);
        Assert.assertEquals(keyStr, "61968cf440e7735e28bac3a431a81b49c3298d37910d54f62f1e5a32c518e556");
    }
}
