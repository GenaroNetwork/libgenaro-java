package network.genaro.storage;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.web3j.crypto.CipherException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Test()
public class TestBridgeApi {
    private static String V3JSON = "{ \"address\": \"5d14313c94f1b26d23f4ce3a49a2e136a88a584b\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"12d3a710778aa884d32140466ce6c3932629d922fa1cd6b64996dff9b368743a\", \"cipherparams\": { \"iv\": \"f0eface44a93bac55857d74740912d13\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"62dd6d60fb04429fc8cf32fd39ea5e886d7f84eae258866c14905fa202dbc43d\" }, \"mac\": \"632e92cb1de1a708b2d349b9ae558a4d655c691d3e793fca501a857c7f0c3b1c\" }, \"id\": \"b12b56a5-7eaa-4d90-87b5-cc616e6694d0\", \"version\": 3 }";

    public void TestGetInfo() throws ExecutionException, InterruptedException {
        BridgeApi api = new BridgeApi();
        Future<Map<String, Object>> fu = api.getInfo();
        Map<String, Object> info = fu.get();
        System.out.println(info);
    }

    public void TestListBuckets() throws CipherException, IOException, ExecutionException, InterruptedException {
        BridgeApi api = new BridgeApi();
        GenaroWallet ww = new GenaroWallet(V3JSON, "123456");
        api.logIn(ww);
        Bucket[] bs = api.listBuckets().get();
        for(Bucket b : bs) {
            System.out.println(b.getName());
        }
        Assert.assertNotNull(bs);
    }
}
