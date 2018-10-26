package network.genaro.storage;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.bouncycastle.util.encoders.Hex;
import org.web3j.crypto.CipherException;
import org.web3j.crypto.MnemonicUtils;
import org.web3j.crypto.WalletUtils;

import java.io.IOException;

import static network.genaro.storage.CryptoUtil.string2Bytes;

@Test()
public class TestCryptoUtil {

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
}