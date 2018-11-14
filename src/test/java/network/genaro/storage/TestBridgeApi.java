package network.genaro.storage;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.web3j.crypto.CipherException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Test()
public class TestBridgeApi {
    private static String V3JSON = "{ \"address\": \"5d14313c94f1b26d23f4ce3a49a2e136a88a584b\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"12d3a710778aa884d32140466ce6c3932629d922fa1cd6b64996dff9b368743a\", \"cipherparams\": { \"iv\": \"f0eface44a93bac55857d74740912d13\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"62dd6d60fb04429fc8cf32fd39ea5e886d7f84eae258866c14905fa202dbc43d\" }, \"mac\": \"632e92cb1de1a708b2d349b9ae558a4d655c691d3e793fca501a857c7f0c3b1c\" }, \"id\": \"b12b56a5-7eaa-4d90-87b5-cc616e6694d0\", \"version\": 3 }";

    public void testGetInfo() throws ExecutionException, InterruptedException {
        BridgeApi api = new BridgeApi();
        Future<Map<String, Object>> fu = api.getInfo();
        Map<String, Object> info = fu.get();
        System.out.println(info);
    }

    public void testListBuckets() throws CipherException, IOException, ExecutionException, InterruptedException {
        BridgeApi api = new BridgeApi();
        GenaroWallet ww = new GenaroWallet(V3JSON, "123456");
        api.logIn(ww);
        Bucket[] bs = api.listBuckets().get();
        for(Bucket b : bs) {
            System.out.println(b);
        }
        Assert.assertNotNull(bs);
    }

    public void testDeleteBucket() throws CipherException, IOException, ExecutionException, InterruptedException {
        BridgeApi api = new BridgeApi();
        GenaroWallet ww = new GenaroWallet(V3JSON, "123456");
        api.logIn(ww);
        api.deleteBucket("5bab1b87372c5034d2780d5f").get();
    }

    public void testRenameBucket() throws Exception{
        BridgeApi api = new BridgeApi();
        GenaroWallet ww = new GenaroWallet(V3JSON, "123456");
        api.logIn(ww);
        api.renameBucket("5b8caf912d9c51182068e73f", "new name").get();
    }

    public void testGetBucket() throws Exception{
        BridgeApi api = new BridgeApi();
        GenaroWallet ww = new GenaroWallet(V3JSON, "123456");
        api.logIn(ww);
        Bucket b = api.getBucket("5ba325db152f07179cbe1f80").get();
        System.out.println(b);
    }
    public void testListFiles() throws Exception{
        BridgeApi api = new BridgeApi();
        GenaroWallet ww = new GenaroWallet(V3JSON, "123456");
        api.logIn(ww);
        File[] bs = api.listFiles("5b8caf912d9c51182068e73f").get();
        for (File b : bs) {
            System.out.println(b);
        }
    }

    public void testDeleteFile() throws Exception{
        BridgeApi api = new BridgeApi();
        GenaroWallet ww = new GenaroWallet(V3JSON, "123456");
        api.logIn(ww);
        api.deleteFile("5ba1f145256c9f70c00eae7d", "2ffcbc0c92e429699a84541b").get();
    }

    public void testGetFileInfo() throws Exception{
        BridgeApi api = new BridgeApi();
        GenaroWallet ww = new GenaroWallet(V3JSON, "123456");
        api.logIn(ww);
        File ff = api.getFileInfo("5b8caf912d9c51182068e73f", "4b449ebb8c7970096a40f0ba").get();
        System.out.println(ff);
    }

    public void testGetPointers() throws Exception{
        BridgeApi api = new BridgeApi();
        GenaroWallet ww = new GenaroWallet(V3JSON, "123456");
        api.logIn(ww);
//        List<Pointer> ps = api.getPointersRaw("5b8caf912d9c51182068e73f", "368cd399a92d4b923e37dd67", 10, 0).get();
//        for (Pointer p : ps) {
//            System.out.println(p);
//        }
//        System.out.println();
//        System.out.println();
        List<Pointer> psa = api.getPointers("5b8caf912d9c51182068e73f", "4b449ebb8c7970096a40f0ba").get();
        for (Pointer p : psa) {
            System.out.println(p);
        }
    }

    public void testDownloadFile() throws Exception {
        BridgeApi api = new BridgeApi();
        GenaroWallet ww = new GenaroWallet(V3JSON, "123456");
        api.logIn(ww);

        Downloader d = new Downloader(api, "/Users/lishi/Desktop/cc.epub", "5b8caf912d9c51182068e73f", "368cd399a92d4b923e37dd67", new Progress() {
            @Override
            public void onBegin() {
                System.out.println("onBegin");
            }
            @Override
            public void onEnd() {
                System.out.println("onEnd");
            }
            @Override
            public void onError() {
                System.out.println("onError");
            }
            @Override
            public void onProgress(float progress, String message) {
                System.out.println(message);
                System.out.println("progress: " + progress);
            }
        });

        d.start();
    }
}
