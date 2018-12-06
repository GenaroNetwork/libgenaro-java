package network.genaro.storage;

import com.sun.tools.javac.code.Symtab;
import org.bouncycastle.util.encoders.Hex;
import org.testng.annotations.Test;
import org.web3j.crypto.CipherException;

import java.io.IOException;
import java.util.List;

@Test()
public class TestGenaro {
//    private static final String V3JSON = "{ \"address\": \"5d14313c94f1b26d23f4ce3a49a2e136a88a584b\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"12d3a710778aa884d32140466ce6c3932629d922fa1cd6b64996dff9b368743a\", \"cipherparams\": { \"iv\": \"f0eface44a93bac55857d74740912d13\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"62dd6d60fb04429fc8cf32fd39ea5e886d7f84eae258866c14905fa202dbc43d\" }, \"mac\": \"632e92cb1de1a708b2d349b9ae558a4d655c691d3e793fca501a857c7f0c3b1c\" }, \"id\": \"b12b56a5-7eaa-4d90-87b5-cc616e6694d0\", \"version\": 3 }";
    private static final String V3JSON = "{ \"address\": \"fbad65391d2d2eafda9b27326d1e81d52a6a3dc8\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"e968751f3d60827b6e62e3ff6c024ecc82f33a6c55428be33249c83edba444ca\", \"cipherparams\": { \"iv\": \"e80d9ec9ba6241a143c756ec78066ad9\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"ea7cb2b004db67d3103b3790caced7a96b636762f280b243e794fb5bef8ef74b\" }, \"mac\": \"cdb3789e77be8f2a7ab4d205bf1b54e048ad3f5b080b96e07759de7442e050d2\" }, \"id\": \"e28f31b4-1f43-428b-9b12-ab586638d4b1\", \"version\": 3 }";
//    private static final String bucketId = "5bfcf4ea7991d267f4eb53b4";
//    private static final String bucketId = "b5e9bd5fd6f571beee9b035f";
    private static final String bucketId = "5ba341402e49103d8787e52d";

    public void testGetInfo() throws Exception {
        Genaro api = new Genaro();
        String info = api.getInfo();
        System.out.println(info);
    }

    public void testGetBuckets() throws Exception {
        Genaro api = new Genaro();
//        GenaroWallet gw = new GenaroWallet(V3JSON, "123456");
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        Bucket[] bs = api.getBuckets();

        if(bs == null) {
            System.out.println("Error!");
        } else if(bs.length == 0) {
            System.out.println("No buckets.");
        } else {
            for (Bucket b : bs) {
                System.out.println(b);
            }
        }
    }

    public void testDeleteBucket() throws Exception {
        Genaro api = new Genaro();
//        GenaroWallet gw = new GenaroWallet(V3JSON, "123456");
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        boolean success = api.deleteBucket("99bcb3bd3d9884905ccd3d62");
        if(success) {
            System.out.println("Delete success.");
        } else {
            System.out.println("Delete failed.");
        }
    }

    public void testRenameBucket() throws Exception {
        Genaro api = new Genaro();
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        boolean success = api.renameBucket(bucketId, "呵呵");

        if(success) {
            System.out.println("Rename success.");
        } else {
            System.out.println("Rename failed.");
        }
    }

    public void testGetBucket() throws Exception {
        Genaro api = new Genaro();
//        GenaroWallet gw = new GenaroWallet(V3JSON, "123456");
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        Bucket b = api.getBucket(bucketId);

        if(b == null) {
            System.out.println("Get Bucket failed.");
        } else {
            System.out.println(b);
        }
    }

    public void testListFiles() throws Exception {
        Genaro api = new Genaro();
//        GenaroWallet gw = new GenaroWallet(V3JSON, "123456");
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        File[] bs = api.listFiles(bucketId);

        if(bs == null) {
            System.out.println("Error!");
        } else if(bs.length == 0) {
            System.out.println("No files.");
        } else {
            for (File b : bs) {
                System.out.println(b);
            }
        }
    }

    public void testIsFileExist() throws Exception {
        Genaro api = new Genaro();
//        GenaroWallet gw = new GenaroWallet(V3JSON, "123456");
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);

        String fileName = "2049k.data";
        String encryptedFileName = CryptoUtil.encryptMetaHmacSha512(BasicUtil.string2Bytes(fileName), gw.getPrivateKey(), Hex.decode(bucketId));

        boolean exist = api.isFileExist(bucketId, encryptedFileName);
        if(exist) {
            System.out.println("File exists.");
        } else {
            System.out.println("File not exists.");
        }
    }

    public void testDeleteFile() throws Exception {
        Genaro api = new Genaro();
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        boolean success = api.deleteFile(bucketId, "2c5b84e3d682afdce73dcdfd");

        if(success) {
            System.out.println("Delete success.");
        } else {
            System.out.println("Delete failed.");
        }
    }

    public void testGetFileInfo() throws Exception {
        Genaro api = new Genaro();
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        File file = api.getFileInfo(bucketId, "8bdbf74c8157254c2bc74cca");

        if(file == null) {
            System.out.println("Get file info failed.");
        } else {
            System.out.println(file);
        }
    }

    public void testGetPointers() throws Exception {
        Genaro api = new Genaro();
//        GenaroWallet gw = new GenaroWallet(V3JSON, "123456");
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);

        List<Pointer> psa = api.getPointers(bucketId, "f40da862c00494bb0430e012");

        if(psa == null) {
            System.out.println("Error!");
        } else if(psa.size() == 0) {
            System.out.println("No such file or pointer invalid.");
        } else {
            for (Pointer p : psa) {
                System.out.println(p);
            }
        }
    }

    public void testRequestFrameId() throws Exception {
        Genaro api = new Genaro();
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        api.logIn(gw);
        Frame frame = api.requestNewFrame();

        if(frame == null) {
            System.out.println("Error!");
        } else {
            System.out.println(frame);
        }
    }

    public void testDownload() throws Exception {
        Genaro api = new Genaro();
        GenaroWallet gw;
        try {
            gw = new GenaroWallet(V3JSON, "lgygn_9982");
        } catch (CipherException | IOException e) {
            System.out.println(e.getMessage());
            throw e;
        }
        api.logIn(gw);

        try {
//            new Downloader(api, bucketId, "5c00ef805a158a5612e66cde", "/Users/dingyi/Genaro/test/download/1.txt", new DownloadProgress() {
//            new Downloader(api, bucketId, "5c0103fd5a158a5612e67461", "/Users/dingyi/Genaro/test/download/aaa.zip", new DownloadProgress() {
//            new Downloader(api, bucketId, "e396ebc515d4a91452ea1765", "/Users/dingyi/Genaro/test/download/aam.data", new DownloadProgress() {
            new Downloader(api, bucketId, "f40da862c00494bb0430e012", "/Users/dingyi/Genaro/test/download/下载器苹果电脑Mac版.zip", new DownloadProgress() {
                @Override
                public void onProgress(float progress) {
                    System.out.println("Progress: " + progress);
                }
            }).start();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    public void testUpload() throws Exception {
        Genaro api = new Genaro();
        GenaroWallet ww = new GenaroWallet(V3JSON, "lgygn_9982");
//        GenaroWallet ww = new GenaroWallet(V3JSON, "123456");
        api.logIn(ww);

        try {
//            new Uploader(api, "/Users/dingyi/Downloads/bzip2-1.0.5-bin.zip", "bzip2-1.0.5-bin.zip", "5ba341402e49103d8787e52d", new UploadProgress() {
//            new Uploader(api, false, "/Users/dingyi/test/2048k.data", "2048n.data", bucketId, new UploadProgress() {
//            new Uploader(api, false, "/Users/dingyi/test/2049k.data", "14.data", bucketId, new UploadProgress() {
            new Uploader(api, false, "/Users/dingyi/Downloads/genaroNetwork-windows.zip", "i.zip", bucketId, new UploadProgress() {
//            new Uploader(api, false, "/Users/dingyi/Downloads/下载器苹果电脑Mac版.zip", "1.zip", bucketId, new UploadProgress() {
//            new Uploader(api, false, "/Users/dingyi/test/1.txt", "2.txt", bucketId, new UploadProgress() {
                @Override
                public void onProgress(float progress) {
                    System.out.println("Progress: " + progress);
                }
            }).start();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }
}
