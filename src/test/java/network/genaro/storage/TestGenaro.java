package network.genaro.storage;

import org.bouncycastle.util.encoders.Hex;
import org.testng.annotations.Test;
import org.web3j.crypto.CipherException;

import java.io.IOException;
import java.util.*;

@Test()
public class TestGenaro {
//    private static final String V3JSON = "{ \"address\": \"5d14313c94f1b26d23f4ce3a49a2e136a88a584b\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"12d3a710778aa884d32140466ce6c3932629d922fa1cd6b64996dff9b368743a\", \"cipherparams\": { \"iv\": \"f0eface44a93bac55857d74740912d13\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"62dd6d60fb04429fc8cf32fd39ea5e886d7f84eae258866c14905fa202dbc43d\" }, \"mac\": \"632e92cb1de1a708b2d349b9ae558a4d655c691d3e793fca501a857c7f0c3b1c\" }, \"id\": \"b12b56a5-7eaa-4d90-87b5-cc616e6694d0\", \"version\": 3 }";
    private static final String V3JSON = "{ \"address\": \"fbad65391d2d2eafda9b27326d1e81d52a6a3dc8\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"e968751f3d60827b6e62e3ff6c024ecc82f33a6c55428be33249c83edba444ca\", \"cipherparams\": { \"iv\": \"e80d9ec9ba6241a143c756ec78066ad9\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"ea7cb2b004db67d3103b3790caced7a96b636762f280b243e794fb5bef8ef74b\" }, \"mac\": \"cdb3789e77be8f2a7ab4d205bf1b54e048ad3f5b080b96e07759de7442e050d2\" }, \"id\": \"e28f31b4-1f43-428b-9b12-ab586638d4b1\", \"version\": 3 }";

//    private static String TestBridgeUrl = "http://118.31.61.119:8080";
//        private static String TestBridgeUrl = "http://127.0.0.1:8080";
    private static String TestBridgeUrl = "http://120.77.247.10:8080";
//    private static String TestBridgeUrl = "http://47.100.33.60:8080";
//        private static final String TestbucketId = "5bfcf4ea7991d267f4eb53b4";
//    private static final String TestbucketId = "b5e9bd5fd6f571beee9b035f";
    private static final String TestbucketId = "5ba341402e49103d8787e52d";
//    private static final String TestbucketId = "5c0e5a8b312cfa12ae9f5bf3";

    public void testGetInfo() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, null);
        String info = api.getInfo();
        System.out.println(info);
    }

    public void testGetBuckets() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, null);
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
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);
        boolean success = api.deleteBucket("99bcb3bd3d9884905ccd3d62");

        if(success) {
            System.out.println("Delete success.");
        } else {
            System.out.println("Delete failed.");
        }
    }

    public void testRenameBucket() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);
        boolean success = api.renameBucket(TestbucketId, "呵呵");

        if(success) {
            System.out.println("Rename success.");
        } else {
            System.out.println("Rename failed.");
        }
    }

    public void testGetBucket() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);
        Bucket b = api.getBucket(null, TestbucketId);

        if(b == null) {
            System.out.println("Get Bucket failed.");
        } else {
            System.out.println(b);
        }
    }

    public void testListFiles() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);
        File[] bs = api.listFiles(TestbucketId);

        if(bs == null) {
            System.out.println("Error!");
        } else if(bs.length == 0) {
            System.out.println("No files.");
        } else {
            for (File b : bs) {
                System.out.println(b.toBriefString());
            }
        }
    }

    public void testIsFileExist() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);

        String fileName = "2049k.data";
        String encryptedFileName = CryptoUtil.encryptMetaHmacSha512(BasicUtil.string2Bytes(fileName), gw.getPrivateKey(), Hex.decode(TestbucketId));

        boolean exist = api.isFileExist(null, TestbucketId, encryptedFileName);
        if(exist) {
            System.out.println("File exists.");
        } else {
            System.out.println("File not exists.");
        }
    }

    public void testDeleteFile() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);
        boolean success = api.deleteFile(TestbucketId, "2c5b84e3d682afdce73dcdfd");

        if(success) {
            System.out.println("Delete success.");
        } else {
            System.out.println("Delete failed.");
        }
    }

    public void testGetFileInfo() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);
        File file = api.getFileInfo(null, TestbucketId, "5c0e6872a72fc61208285155");

        if(file == null) {
            System.out.println("Get file info failed.");
        } else {
            System.out.println(file);
        }
    }

    public void testGetPointers() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);
        List<Pointer> psa = api.getPointers(null, TestbucketId, "f40da862c00494bb0430e012");

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
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);
        Frame frame = api.requestNewFrame(null);

        if(frame == null) {
            System.out.println("Error!");
        } else {
            System.out.println(frame);
        }
    }

    public void testDownload() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);

//            Downloader downloader = new Downloader(api, TestbucketId, "5c0a3006bbdd6f2d157dcedb", "/Users/dingyi/Genaro/test/download/cpor-genaro", new DownloadCallback() {
        Downloader downloader = new Downloader(api, TestbucketId, "5c08d01c963d402a1f3ede80", "/Users/dingyi/Genaro/test/download/r.zip", new DownloadCallback() {
            @Override
            public void onBegin() {
                System.out.println("Download started");
            }
            @Override
            public void onProgress(float progress) {
//                System.out.printf("Download progress: %.1f%%\n", progress * 100);
            }
            @Override
            public void onFail(String error) {
                System.out.println("Download failed, reason: " + error != null ? error : "Unknown");
            }
            @Override
            public void onCancel() {
                System.out.println("Download is cancelled");
            }
            @Override
            public void onFinish() {
                System.out.println("Download finished");
            }
        });

        Thread thread = new Thread(downloader);
        thread.start();

        thread.join();
    }

    public void testDownloadCancel() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);

//            Downloader downloader = new Downloader(api, TestbucketId, "5c0a3006bbdd6f2d157dcedb", "/Users/dingyi/Genaro/test/download/cpor-genaro", new DownloadCallback() {
        Downloader downloader = new Downloader(api, TestbucketId, "5c08d01c963d402a1f3ede80", "/Users/dingyi/Genaro/test/download/r.zip", new DownloadCallback() {
            @Override
            public void onBegin() {
                System.out.println("Download started");
            }
            @Override
            public void onProgress(float progress) {
//                System.out.printf("Download progress: %.1f%%\n", progress * 100);
            }
            @Override
            public void onFail(String error) {
                System.out.println("Download failed, reason: " + error != null ? error : "Unknown");
            }
            @Override
            public void onCancel() {
                System.out.println("Download is cancelled");
            }
            @Override
            public void onFinish() {
                System.out.println("Download finished");
            }
        });

        Thread thread = new Thread(downloader);
        thread.start();

        Thread.sleep(5000);
        downloader.cancel();

        thread.join();
    }

    public void testDownloadParallel() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);

        List<Thread> threads = new ArrayList<>();

        try {
            for(int i = 0; i < 5; i++) {
                Downloader downloader = new Downloader(api, TestbucketId, "5bf7c98165390d21283c15f5", "/Users/dingyi/Genaro/test/download/spam" + i + ".txt", new DownloadCallback() {
                    @Override
                    public void onBegin() {
                        System.out.println("Download started");
                    }
                    @Override
                    public void onProgress(float progress) {
//                        System.out.printf("Download progress: %.1f%%\n", progress * 100);
                    }
                    @Override
                    public void onFail(String error) {
                        System.out.println("Download failed, reason: " + error != null ? error : "Unknown");
                    }
                    @Override
                    public void onCancel() {
                        System.out.println("Download is cancelled");
                    }
                    @Override
                    public void onFinish() {
                        System.out.println("Download finished");
                    }
                });

                Thread thread = new Thread(downloader);
                thread.start();

                threads.add(thread);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }

        threads.stream().forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {

            }
        });
    }

    public void testUpload() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);

//        Uploader uploader = new Uploader(api, false, "/Users/dingyi/test/2097152.data", "5.data", TestbucketId, new UploadCallback() {
        Uploader uploader = new Uploader(api, false, "/Users/dingyi/Downloads/下载器苹果电脑Mac版.zip", "22.zip", TestbucketId, new UploadCallback() {
            @Override
            public void onBegin(long fileSize) {
                System.out.println("Upload started");
            }
            @Override
            public void onProgress(float progress) {
//                System.out.printf("Upload progress: %.1f%%\n", progress * 100);
            }
            @Override
            public void onFail(String error) {
                System.out.println("Upload failed, reason: " + error != null ? error : "Unknown");
            }
            @Override
            public void onCancel() {
                System.out.println("Upload is cancelled");
            }
            @Override
            public void onFinish(String fileId) {
                System.out.println("Upload finished, fileId: " + fileId);
            }
        });

        Thread thread = new Thread(uploader);
        thread.start();

        thread.join();
    }

    public void testUploadCancel() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);

//        Uploader uploader = new Uploader(api, false, "/Users/dingyi/test/2097152.data", "5.data", TestbucketId, new UploadCallback() {
        Uploader uploader = new Uploader(api, false, "/Users/dingyi/Downloads/下载器苹果电脑Mac版.zip", "24.zip", TestbucketId, new UploadCallback() {
            @Override
            public void onBegin(long fileSize) {
                System.out.println("Upload started");
            }
            @Override
            public void onProgress(float progress) {
//                System.out.printf("Upload progress: %.1f%%\n", progress * 100);
            }
            @Override
            public void onFail(String error) {
                System.out.println("Upload failed, reason: " + error != null ? error : "Unknown");
            }
            @Override
            public void onCancel() {
                System.out.println("Upload is cancelled");
            }
            @Override
            public void onFinish(String fileId) {
                System.out.println("Upload finished, fileId: " + fileId);
            }
        });

        Thread thread = new Thread(uploader);
        thread.start();

        Thread.sleep(4000);
        uploader.cancel();

        thread.join();
    }

    public void testUploadParallel() throws Exception {
        GenaroWallet gw = new GenaroWallet(V3JSON, "lgygn_9982");
        Genaro api = new Genaro(TestBridgeUrl, gw);

        List<Thread> threads = new ArrayList<>();

        try {
            for(int i = 0; i < 5; i++) {
                Uploader uploader = new Uploader(api, false, "/Users/dingyi/test/spam.txt", "spam11" + i + ".txt", TestbucketId, new UploadCallback() {
                    @Override
                    public void onBegin(long fileSize) {
                        System.out.println("Upload started");
                    }
                    @Override
                    public void onProgress(float progress) {
//                        System.out.printf("Upload progress: %.1f%%\n", progress * 100);
                    }
                    @Override
                    public void onFail(String error) {
                        System.out.println("Upload failed, reason: " + (error != null ? error : "Unknown"));
                    }
                    @Override
                    public void onCancel() {
                        System.out.println("Upload is cancelled");
                    }
                    @Override
                    public void onFinish(String fileId) {
                        System.out.println("Upload finished, fileId: " + fileId);
                    }
                });

                Thread thread = new Thread(uploader);
                thread.start();

                threads.add(thread);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }

        threads.stream().forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {

            }
        });
    }
}
