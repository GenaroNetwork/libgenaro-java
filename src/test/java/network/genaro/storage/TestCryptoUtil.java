package network.genaro.storage;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.RIPEMD160Digest;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.bouncycastle.util.encoders.Hex;

import static network.genaro.storage.cryptoUtil.RIPEMD160_DIGEST_SIZE;

@Test()
public class TestCryptoUtil {

    public void test() {
        byte[] byts = cryptoUtil.string2Bytes("123kkk");

        byte[] ripemded = cryptoUtil.ripemd160Sha256(byts);
        String hexx = Hex.toHexString(ripemded);
        Assert.assertEquals(hexx, "8792d67cdcf37195da42c6a8db27745424647d69");
    }

    public void testRipemd160() {
        byte[] byts = cryptoUtil.string2Bytes("123kkk");
        Digest ripemd160DG = new RIPEMD160Digest();
        ripemd160DG.update(byts, 0, byts.length);
        byte[] out = new byte[RIPEMD160_DIGEST_SIZE];
        ripemd160DG.doFinal(out, 0);
        Assert.assertEquals(Hex.toHexString(out), "5d3405a87994ba690252b17a7cf33d774448df9c");
    }
}