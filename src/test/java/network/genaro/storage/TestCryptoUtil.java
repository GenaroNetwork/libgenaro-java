package network.genaro.storage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.DERSequenceGenerator;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.ec.CustomNamedCurves;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.crypto.params.ECPrivateKeyParameters;
import org.bouncycastle.crypto.signers.ECDSASigner;
import org.bouncycastle.crypto.signers.HMacDSAKCalculator;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.spec.ECNamedCurveSpec;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.bouncycastle.util.encoders.Hex;
import org.web3j.crypto.*;
import org.web3j.utils.Numeric;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPrivateKeySpec;

import static network.genaro.storage.CryptoUtil.sha256EscdaSign;
import static network.genaro.storage.CryptoUtil.string2Bytes;
import static org.testng.Assert.fail;

@Test()
public class TestCryptoUtil {
    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    public void test() {
        byte[] byts = string2Bytes("123kkk");
        byte[] ripemded = CryptoUtil.ripemd160Sha256(byts);
        String hexx = Hex.toHexString(ripemded);
        Assert.assertEquals(hexx, "8792d67cdcf37195da42c6a8db27745424647d69");
    }

    public void testSha256() {
        byte[] bbb = CryptoUtil.sha256((string2Bytes("abcde")));
        String hresult = Hex.toHexString(bbb);
        Assert.assertEquals(hresult, "36bbe50ed96841d10443bcb670d6554f0a34b761be67ec9c4a8ad2c0c44ca42c");
    }

    public void testRipemd160() {
        byte[] byts = string2Bytes("123kkk");
        byte[] out = CryptoUtil.ripemd160(byts);
        Assert.assertEquals(Hex.toHexString(out), "5d3405a87994ba690252b17a7cf33d774448df9c");
    }

    public void testRipemd160Sha256() {
        byte[] bbb = CryptoUtil.ripemd160Sha256(Hex.decode("36217336fd8deee3"));
        String hresult = Hex.toHexString(bbb);
        Assert.assertEquals(hresult, "47bf19535be7d2058875c28dbac9fa3070bbd809");
    }

    public void string2HexTest() {
        String tt = Hex.toHexString(Hex.decode("12343213ef"));
        Assert.assertEquals(tt, "12343213ef");
    }

    public void testDeterministicKey() {
        byte[] ss = CryptoUtil.generateDeterministicKey(Hex.decode("1625348fba"), Hex.decode("385960ffa4"));
        Assert.assertEquals(Hex.toHexString(ss), "296195601e0557bef8963a418c53489f4216e8fe033768b5ca2a9bfb02188296");
    }

    public void testMnemonic2Seed() {
        byte[] seed = MnemonicUtils.generateSeed("dsa", "");
        String hresult = Hex.toHexString(seed);
        Assert.assertEquals(hresult, "fd3fd4cca39658475de1e81ec24d05cecf7696bfaebeaabc29d36e552afac67ecc2649d1b83ae3f2ea169083f6ec90e5ce1e8cf7f5343188874745488fc811b7");
    }

    public void testGenerateBucketKey() {
        byte[] key = CryptoUtil.generateBucketKey(string2Bytes("abandonabandonabandonabandonabandonabandonabandonabandonabandon"), Hex.decode("0123456789ab0123456789ab"));
        Assert.assertEquals(Hex.toHexString(key), "612a3531d6c2ce886bc9504c963cf4b7b309f443251afe9b432aadf1faa8e008");
    }

    public void testGenerateFileKey() {
        String mnemonic = "abandon";
        String bucket_id = "0123456789ab0123456789ab";
        String index = "150589c9593bbebc0e795d8c4fa97304b42c110d9f0095abfac644763beca66e";
        byte[] key = CryptoUtil.generateFileKey(string2Bytes(mnemonic), Hex.decode(bucket_id), Hex.decode(index));
        Assert.assertEquals(Hex.toHexString(key), "3a70a3bd85f064598513dcfb77693885f64fa3ce63ba64456877ef9d4aaa2062");
    }

    public void testAES() {
        String message = "1234567890"; // e105e1aaf8da 6019753b58409d356e5c1cfc5a053ea8
        //String message = "ewqew"; //    e105e1aaf8   fe6356a870b6db693a30a152d8e594b8
        byte[] key = Hex.decode("123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f");
        byte[] iv  = Hex.decode("123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abcf2");
        String base64Secret = CryptoUtil.encryptMeta(string2Bytes(message), key, iv);
        byte[] messageBytes = CryptoUtil.decryptMeta(base64Secret, key);
        Assert.assertEquals(base64Secret, "ODKPheXwS0J/eUd7eH/LdRI6vC8SOrwvEjq8LxI6vC8SOrwvEjq8LxI6vC8SOrzy4KwePJHJR6JDvg==");
        Assert.assertEquals(new String(messageBytes), message);
    }

    public void testWallet() throws IOException, CipherException {
        WalletUtils.loadCredentials("", "");
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


}