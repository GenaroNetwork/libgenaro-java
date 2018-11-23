package network.genaro.storage;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static network.genaro.storage.Parameters.*;

@Test()
public class TestGenaro {
//    private static String V3JSON = "{ \"address\": \"5d14313c94f1b26d23f4ce3a49a2e136a88a584b\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"12d3a710778aa884d32140466ce6c3932629d922fa1cd6b64996dff9b368743a\", \"cipherparams\": { \"iv\": \"f0eface44a93bac55857d74740912d13\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"62dd6d60fb04429fc8cf32fd39ea5e886d7f84eae258866c14905fa202dbc43d\" }, \"mac\": \"632e92cb1de1a708b2d349b9ae558a4d655c691d3e793fca501a857c7f0c3b1c\" }, \"id\": \"b12b56a5-7eaa-4d90-87b5-cc616e6694d0\", \"version\": 3 }";
    private static String V3JSON = "{ \"address\": \"fbad65391d2d2eafda9b27326d1e81d52a6a3dc8\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"e968751f3d60827b6e62e3ff6c024ecc82f33a6c55428be33249c83edba444ca\", \"cipherparams\": { \"iv\": \"e80d9ec9ba6241a143c756ec78066ad9\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"ea7cb2b004db67d3103b3790caced7a96b636762f280b243e794fb5bef8ef74b\" }, \"mac\": \"cdb3789e77be8f2a7ab4d205bf1b54e048ad3f5b080b96e07759de7442e050d2\" }, \"id\": \"e28f31b4-1f43-428b-9b12-ab586638d4b1\", \"version\": 3 }";

    public void testGetInfo() throws Exception {
        Genaro api = new Genaro();
        Future<String> fu = api.getInfo();
        String info = fu.get();
        System.out.println(info);
    }

    public void testListBuckets() throws Exception {
        Genaro api = new Genaro();
//        GenaroWallet gw = new GenaroWallet(V3JSON, "123456");
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        Bucket[] bs = api.listBuckets().get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
        if(bs.length == 0) {
            System.out.println("No buckets.");
        } else {
            for (Bucket b : bs) {
                System.out.println(b);
            }
        }
//        Assert.assertNotNull(bs);
    }

    public void testDeleteBucket() throws Exception {
        Genaro api = new Genaro();
//        GenaroWallet gw = new GenaroWallet(V3JSON, "123456");
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        api.deleteBucket("5ba1ba64256c9f70c00eae7d").get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public void testRenameBucket() throws Exception{
        Genaro api = new Genaro();
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        api.renameBucket("5ba07a4da5156c7964240cbb", "哈哈").get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public void testGetBucket() throws Exception{
        Genaro api = new Genaro();
//        GenaroWallet gw = new GenaroWallet(V3JSON, "123456");
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        Bucket b = api.getBucket("5ba3397e152f07179cbe1f81").get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
        System.out.println(b);
    }

    public void testListFiles() throws Exception{
        Genaro api = new Genaro();
//        GenaroWallet gw = new GenaroWallet(V3JSON, "123456");
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        File[] bs = api.listFiles("5ba341402e49103d8787e52d").get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
        if(bs.length == 0) {
            System.out.println("No files.");
        } else {
            for (File b : bs) {
                System.out.println(b);
            }
        }
    }

    public void testDeleteFile() throws Exception{
        Genaro api = new Genaro();
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        api.deleteFile("5b8c9da88f21182871068f53", "8a6fc6e509d305dea3a2a0bc")
           .get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public void testGetFileInfo() throws Exception{
        Genaro api = new Genaro();
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        File ff = api.getFileInfo("5b8c9da88f21182871068f53", "42f68c1a5a6efb3b915db64")
                     .get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
        System.out.println(ff);
    }

    public void testGetPointers() throws Exception{
        Genaro api = new Genaro();
//        GenaroWallet gw = new GenaroWallet(V3JSON, "123456");
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);

        List<Pointer> psa = null;
        psa = api.getPointers("5ba341402e49103d8787e52d", "5bf7c98165390d21283c15f5")
                 .get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);

        if(psa.size() == 0) {
            System.out.println("No files.");
        } else {
            for (Pointer p : psa) {
                System.out.println(p);
            }
        }
    }

    public void testDownloadFile() throws Exception {
        Genaro api = new Genaro();
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);

        Downloader d = new Downloader(api, "/Users/dingyi/Genaro/test/download/spam.txt", "5ba341402e49103d8787e52d", "5bf7c98165390d21283c15f5", new Progress() {
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

    public void testRequestFrameId() throws Exception {
        Genaro api = new Genaro();
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        api.requestNewFrame().get();
    }
}
