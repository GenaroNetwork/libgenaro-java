package network.genaro.storage;

import org.bouncycastle.util.encoders.Hex;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import network.genaro.storage.GenaroCallback.*;

@Test()
public final class TestGenaro {
    private static final String V3JSON = "{ \"address\": \"0000000000000000000000000000000000000000\", \"crypto\": { \"cipher\": \"aes-128-ctr\", \"ciphertext\": \"e968751f3d60827b6e62e3ff6c024ecc82f33a6c55428be33249c83edba444ca\", \"cipherparams\": { \"iv\": \"e80d9ec9ba6241a143c756ec78066ad9\" }, \"kdf\": \"scrypt\", \"kdfparams\": { \"dklen\": 32, \"n\": 262144, \"p\": 1, \"r\": 8, \"salt\": \"ea7cb2b004db67d3103b3790caced7a96b636762f280b243e794fb5bef8ef74b\" }, \"mac\": \"cdb3789e77be8f2a7ab4d205bf1b54e048ad3f5b080b96e07759de7442e050d2\" }, \"id\": \"e28f31b4-1f43-428b-9b12-ab586638d4b1\", \"version\": 3 }";

    private static String TestBridgeUrl = "http://118.31.61.119:8080";
//    private static String TestBridgeUrl = "http://127.0.0.1:8080";
//    private static String TestBridgeUrl = "http://120.77.247.10:8080";
//    private static String TestBridgeUrl = "http://47.100.33.60:8080";
    private static final String TestbucketId = "5c0e433cdaa4e03fe1b5b287";
//    private static final String TestbucketId = "b5e9bd5fd6f571beee9b035f";
//    private static final String TestbucketId = "5ba341402e49103d8787e52d";
//    private static final String TestbucketId = "5c0e5a8b312cfa12ae9f5bf3";

    public void testGetInfo() throws Exception {
       Genaro api = new Genaro(TestBridgeUrl);
       String info = api.getInfo();
       System.out.println(info);
    }

    public void testGetBuckets() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        CompletableFuture<Void> fu = api.getBuckets(new GetBucketsCallback() {
            @Override
            public void onFinish(Bucket[] buckets) {
                if(buckets.length == 0) {
                    System.out.println("No buckets.");
                } else {
                    for (Bucket b : buckets) {
                        System.out.println(b);
                    }
                }
            }
            @Override
            public void onFail(String error) {
                System.out.println("List buckets failed, reason: " + error + ".");
            }
        });

        fu.join();
    }

    public void testDeleteBucket() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        CompletableFuture<Void> fu = api.deleteBucket("5bfcf77cea9b6322c5abd929", new DeleteBucketCallback() {
            @Override
            public void onFinish() {
                System.out.println("Delete bucket success.");
            }

            @Override
            public void onFail(String error) {
                System.out.println("Delete bucket failed, reason: " + error + ".");
            }
        });

        fu.join();
    }

    public void testRenameBucket() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        CompletableFuture<Void> fu = api.renameBucket(TestbucketId, "呵呵", new RenameBucketCallback() {
            @Override
            public void onFinish() {
                System.out.println("Rename bucket success.");
            }
            @Override
            public void onFail(String error) {
                System.out.println("Rename bucket failed, reason: " + error + ".");
            }
        });

        fu.join();
    }

    public void testGetBucket() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        Bucket b = api.getBucket(null, TestbucketId);

        if(b == null) {
            System.out.println("Get Bucket failed.");
        } else {
            System.out.println(b);
        }
    }

    public void testListFiles() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        CompletableFuture<Void> fu = api.listFiles(TestbucketId, new ListFilesCallback() {
            @Override
            public void onFinish(File[] files) {
                if(files.length == 0) {
                    System.out.println("No files.");
                } else {
                    for (File b : files) {
                        System.out.println(b.toBriefString());
                    }
                }
            }
            @Override
            public void onFail(String error) {
                System.out.println("List files failed, reason: " + error + ".");
            }
        });

        fu.join();
    }

    public void testIsFileExist() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        String fileName = "spam.txt";
        String encryptedFileName = CryptoUtil.encryptMetaHmacSha512(BasicUtil.string2Bytes(fileName), api.getPrivateKey(), Hex.decode(TestbucketId));

        boolean exist = api.isFileExist(null, TestbucketId, encryptedFileName);
        if(exist) {
            System.out.println("File exists.");
        } else {
            System.out.println("File not exists.");
        }
    }

    public void testDeleteFile() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        CompletableFuture<Void> fu = api.deleteFile(TestbucketId, "5c10ee10bbdd6f2d157de097", new DeleteFileCallback() {
            @Override
            public void onFinish() {
                System.out.println("Delete file success.");
            }
            @Override
            public void onFail(String error) {
                System.out.println("Delete file failed, reason: " + error + ".");
            }
        });

        fu.join();
    }

    public void testGetFileInfo() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");
        File file;

        try {
            file = api.getFileInfo(null, TestbucketId, "5c0e6872a72fc61208285155");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        System.out.println(file);
    }

    public void testGetPointers() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        List<Pointer> psa;

        try {
            psa = api.requestPointers(null, TestbucketId, "f40da862c00494bb0430e012");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        if(psa.size() == 0) {
            System.out.println("No pointers.");
        } else {
            for (Pointer p : psa) {
                System.out.println(p);
            }
        }
    }

    public void testRequestFrameId() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        Frame frame;
        try {
            frame = api.requestNewFrame(null);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        System.out.println(frame);
    }

    public void testDownload() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

//        Downloader downloader = api.resolveFile(TestbucketId, "5c0a3006bbdd6f2d157dcedb", "/Users/dingyi/Genaro/test/download/cpor-genaro", new ResolveFileCallback() {
//        Downloader downloader = api.resolveFile(TestbucketId, "5c08d01c963d402a1f3ede80", "/Users/dingyi/Genaro/test/download/r.zip", new ResolveFileCallback() {
//        Downloader downloader = api.resolveFile(TestbucketId, "5bf7c98165390d21283c15f5", "/Users/dingyi/Genaro/test/download/spam" + ".txt", true, new ResolveFileCallback() {
        Downloader downloader = api.resolveFile(TestbucketId, "5c0e4c4f6eea1f3f98ff279a", "/Users/dingyi/Genaro/test/download/aaa", true, new ResolveFileCallback() {
            @Override
            public void onBegin() {
                System.out.println("Download started");
            }
            @Override
            public void onProgress(float progress) {
                System.out.printf("Download progress: %.1f%%\n", progress * 100);
            }
            @Override
            public void onFail(String error) {
                System.out.println("Download failed, reason: " + (error != null ? error : "Unknown"));
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

        downloader.join();
    }

    public void testDownloadCancel() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

//            Downloader downloader = api.resolveFile(TestbucketId, "5c0a3006bbdd6f2d157dcedb", "/Users/dingyi/Genaro/test/download/cpor-genaro", new ResolveFileCallback() {
        Downloader downloader = api.resolveFile(TestbucketId, "5c08d01c963d402a1f3ede80", "/Users/dingyi/Genaro/test/download/r.zip", true, new ResolveFileCallback() {
            @Override
            public void onBegin() {
                System.out.println("Download started");
            }
            @Override
            public void onProgress(float progress) {
                System.out.printf("Download progress: %.1f%%\n", progress * 100);
            }
            @Override
            public void onFail(String error) {
                System.out.println("Download failed, reason: " + (error != null ? error : "Unknown"));
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

        Thread.sleep(5000);
        downloader.cancel();
        downloader.join();
    }

    public void testDownloadParallel() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        List<Downloader> downloaders = new ArrayList<>();

        try {
            for(int i = 0; i < 5; i++) {
                Downloader downloader = api.resolveFile(TestbucketId, "5bf7c98165390d21283c15f5", "/Users/dingyi/Genaro/test/download/spam" + i + ".txt", true, new ResolveFileCallback() {
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
                        System.out.println("Download failed, reason: " + (error != null ? error : "Unknown"));
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

                downloaders.add(downloader);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }

        downloaders.stream().forEach(downloader -> downloader.join());
    }

    public void testUpload() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        Uploader uploader = api.storeFile(false, "/Users/dingyi/test/2097152.data", "1.data", TestbucketId, new StoreFileCallback() {
//        Uploader uploader = api.storeFile(false, "/Users/dingyi/Downloads/下载器苹果电脑Mac版.zip", "25.zip", TestbucketId, new StoreFileCallback() {
            @Override
            public void onBegin(long fileSize) {
                System.out.println("Upload started");
            }
            @Override
            public void onProgress(float progress) {
                System.out.printf("Upload progress: %.1f%%\n", progress * 100);
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

        uploader.join();
    }

    public void testUploadCancel() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        Uploader uploader = api.storeFile(false, "/Users/dingyi/Downloads/下载器苹果电脑Mac版.zip", "26.zip", TestbucketId, new StoreFileCallback() {
            @Override
            public void onBegin(long fileSize) {
                System.out.println("Upload started");
            }
            @Override
            public void onProgress(float progress) {
                System.out.printf("Upload progress: %.1f%%\n", progress * 100);
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

        Thread.sleep(5000);
        uploader.cancel();
        uploader.join();
    }

    public void testUploadParallel() throws Exception {
        Genaro api = new Genaro(TestBridgeUrl, V3JSON, "111111");

        List<Uploader> uploaders = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            Uploader uploader = api.storeFile(false, "/Users/dingyi/test/spam.txt", "spam15" + i + ".txt", TestbucketId, new StoreFileCallback() {
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

            uploaders.add(uploader);
        }

        uploaders.stream().forEach(uploader -> uploader.join());
    }
}
