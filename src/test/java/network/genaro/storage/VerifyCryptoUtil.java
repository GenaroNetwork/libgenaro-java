package network.genaro.storage;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.bouncycastle.util.encoders.Hex;

import org.web3j.crypto.MnemonicUtils;

import java.io.UnsupportedEncodingException;
import static java.nio.charset.StandardCharsets.UTF_8;

@Test()
public final class VerifyCryptoUtil {
    public void verfiySha256() throws Exception {
        byte[] bbb = CryptoUtil.sha256((BasicUtil.string2Bytes("abcde")));
        String hresult = Hex.toHexString(bbb);
        Assert.assertEquals(hresult, "36bbe50ed96841d10443bcb670d6554f0a34b761be67ec9c4a8ad2c0c44ca42c");
    }

    public void verfiyRipemd160() {
        byte[] byts = BasicUtil.string2Bytes("123kkk");
        byte[] out = CryptoUtil.ripemd160(byts);
        Assert.assertEquals(Hex.toHexString(out), "5d3405a87994ba690252b17a7cf33d774448df9c");
    }

    public void verfiyRipemd160Sha256() throws Exception {
        byte[] bbb = CryptoUtil.ripemd160Sha256(Hex.decode("36217336fd8deee3"));
        String hresult = Hex.toHexString(bbb);
        Assert.assertEquals(hresult, "47bf19535be7d2058875c28dbac9fa3070bbd809");
    }

//    public void verfiyString2Hex() {
//        String tt = Hex.toHexString(Hex.decode("12343213ef"));
//        Assert.assertEquals(tt, "12343213ef");
//    }

    public void verfiyDeterministicKey() throws Exception {
        byte[] ss = CryptoUtil.generateDeterministicKey(Hex.decode("1625348fba"), Hex.decode("385960ffa4"));
        Assert.assertEquals(Hex.toHexString(ss), "296195601e0557bef8963a418c53489f4216e8fe033768b5ca2a9bfb02188296");
    }

    public void verfiyMnemonic2Seed2() throws UnsupportedEncodingException {
        String mnemonic = "29565ea5ecddb8fd624932dc82c24fd5fe9e06a3ccf5c5764e4a64712aa834a6";
        byte[] buf = Hex.decode(mnemonic);
        byte[] buf2 = mnemonic.getBytes(UTF_8);
        byte[] seed = MnemonicUtils.generateSeed(mnemonic, "");
        byte[] seed2 = MnemonicUtils.generateSeed(new String(buf), "");
        byte[] seed3 = CryptoUtil.generateGenaroSeed(buf2);
        String hresult = Hex.toHexString(seed);
        String hresult2 = Hex.toHexString(seed2);
        String hresult3 = Hex.toHexString(seed3);
        Assert.assertEquals(hresult, "65a0962bf6b22b0fd22cc1a6468d4b94b8597d8126a5bb4366488968c4b652c7f47cac81d8f342611c2c59c8da76d042aa0286ecc2a5146cbe746279a9d857f5");
        Assert.assertEquals(hresult2, "893221aef8e1cd76860a27a38ada301118ace3a3a5655bd28390f68dbbe047eb9712de9947f40397456b323a7eb08084faa252ba41c22c38ed7ef11828eba6a1");
        Assert.assertEquals(hresult3, "65a0962bf6b22b0fd22cc1a6468d4b94b8597d8126a5bb4366488968c4b652c7f47cac81d8f342611c2c59c8da76d042aa0286ecc2a5146cbe746279a9d857f5");
    }

    public void verfiyGenerateBucketKey() throws Exception {
        byte[] key = CryptoUtil.generateBucketKey(BasicUtil.string2Bytes("abcde abcde abcde abcde abcde abcde abcde abcde abcde abcde abcd"), Hex.decode("0123456789ab0123456789ab"));
        Assert.assertEquals(Hex.toHexString(key), "b17403c5130847731abd1c233e74002aa666c71497a19c90b7c305479ccd5844");
    }

    public void verfiyGenerateBucketKey2() throws Exception {
        byte[] pkpk = Hex.decode("29565ea5ecddb8fd624932dc82c24fd5fe9e06a3ccf5c5764e4a64712aa834a6");
        String magicBid = "398734aab3c4c30c9f22590e83a95f7e43556a45fc2b3060e0c39fde31f50272";
        byte[] key = CryptoUtil.generateBucketKey(pkpk, Hex.decode(magicBid));
        String kstr = Hex.toHexString(key);
        Assert.assertEquals(kstr, "76afd3c76a52bd9a2fc7449c5701dbaaa2caa2e67e8bfcee3b108c06dacc576a");
    }

    public void verfiyGenerateFileKey() throws Exception {
        String mnemonic = "abcde abcde abcde abcde abcde abcde abcde abcde abcde abcde abcd";
        String bucket_id = "0123456789ab0123456789ab";
        String index = "150589c9593bbebc0e795d8c4fa97304b42c110d9f0095abfac644763beca66e";
        byte[] key = CryptoUtil.generateFileKey(BasicUtil.string2Bytes(mnemonic), Hex.decode(bucket_id), Hex.decode(index));
        Assert.assertEquals(Hex.toHexString(key), "eccd01f6a87991ff0b504718df1da40cb2bcda48099375f5124358771c9ebe2c");
    }

    public void verfiyAES() throws Exception {
        String message = "1234567890"; // e105e1aaf8da 6019753b58409d356e5c1cfc5a053ea8
        byte[] key = Hex.decode("123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f");
        byte[] iv  = Hex.decode("123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abc2f123abcf2");
        String base64Secret = CryptoUtil.encryptMeta(BasicUtil.string2Bytes(message), key, iv);
        byte[] messageBytes = CryptoUtil.decryptMeta(base64Secret, key);
        Assert.assertEquals(base64Secret, "ODKPheXwS0J/eUd7eH/LdRI6vC8SOrwvEjq8LxI6vC8SOrwvEjq8LxI6vC8SOrzy4KwePJHJR6JDvg==");
        Assert.assertEquals(new String(messageBytes), message);
    }

    public void verfiyDecryptMeta() throws Exception {
        byte[] realnameba = CryptoUtil.decryptMeta("0PkgasRWbaPHhAlRIPf/ZdhopoGRv4nQk8PeZQeyCizXv+DeNGbx48KobaTbRI9r9CTLBwOo", Hex.decode("727324ff68e45f183951f13d7fd70efd653cccf73ef8b60e3cbe7560aacecd8c"));
        String name = new String(realnameba);
        Assert.assertEquals(name, "secret");
    }

//    public void testSignature() throws Exception {
//        String v3json = "{ \"address\": \"5d14313c94f1b26d23f4ce3a49a2e136a88a584b\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"12d3a710778aa884d32140466ce6c3932629d922fa1cd6b64996dff9b368743a\", \"cipherparams\": { \"iv\": \"f0eface44a93bac55857d74740912d13\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"62dd6d60fb04429fc8cf32fd39ea5e886d7f84eae258866c14905fa202dbc43d\" }, \"mac\": \"632e92cb1de1a708b2d349b9ae558a4d655c691d3e793fca501a857c7f0c3b1c\" }, \"id\": \"b12b56a5-7eaa-4d90-87b5-cc616e6694d0\", \"version\": 3 }";
//        ObjectMapper objectMapper = new ObjectMapper();
//        WalletFile walletFile = objectMapper.readValue(v3json, WalletFile.class);
//        ECKeyPair ecKeyPair = Wallet.decrypt("123456", walletFile);
//        byte[] fakeHash = "message to signmessage to signmu".getBytes(StandardCharsets.UTF_8);
//
//        final X9ECParameters CURVE_PARAMS = CustomNamedCurves.getByName("secp256k1");
//        final ECDomainParameters CURVE = new ECDomainParameters(
//                CURVE_PARAMS.getCurve(), CURVE_PARAMS.getG(), CURVE_PARAMS.getN(), CURVE_PARAMS.getH());
//
//        ECDSASigner signer = new ECDSASigner(new HMacDSAKCalculator(new SHA256Digest()));
//
//        ECPrivateKeyParameters privKey = new ECPrivateKeyParameters(ecKeyPair.getPrivateKey(), CURVE);
//        signer.init(true, privKey);
//        BigInteger[] components = signer.generateSignature(fakeHash);
//
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        DERSequenceGenerator seq = new DERSequenceGenerator(baos);
//        seq.addObject(new ASN1Integer(components[0]));
//        seq.addObject(new ASN1Integer((components[1])));
//        seq.close();
//        byte[] sigsig = baos.toByteArray();
//        String sigStr = Hex.toHexString(sigsig);
//        System.out.println(sigStr);
//    }
//
//    public void testSha256EscdaSign() throws Exception{
//        String message = "hello world";
//        String v3json = "{ \"address\": \"5d14313c94f1b26d23f4ce3a49a2e136a88a584b\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"12d3a710778aa884d32140466ce6c3932629d922fa1cd6b64996dff9b368743a\", \"cipherparams\": { \"iv\": \"f0eface44a93bac55857d74740912d13\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"62dd6d60fb04429fc8cf32fd39ea5e886d7f84eae258866c14905fa202dbc43d\" }, \"mac\": \"632e92cb1de1a708b2d349b9ae558a4d655c691d3e793fca501a857c7f0c3b1c\" }, \"id\": \"b12b56a5-7eaa-4d90-87b5-cc616e6694d0\", \"version\": 3 }";
//        ObjectMapper objectMapper = new ObjectMapper();
//        WalletFile walletFile = objectMapper.readValue(v3json, WalletFile.class);
//        ECKeyPair ecKeyPair = Wallet.decrypt("123456", walletFile);
//        String sig = sha256EscdaSign(ecKeyPair.getPrivateKey(), message);
//        System.out.println(sig);
//    }
//
//    public void testAesCtr() throws Exception {
//        // make Iv byte[]
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
//        System.out.println(deMessage);
//    }
}
