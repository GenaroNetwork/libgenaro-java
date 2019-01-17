package network.genaro.storage;

import org.bouncycastle.util.encoders.Hex;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import network.genaro.storage.GenaroCallback.GetBucketsCallback;
import network.genaro.storage.GenaroCallback.DeleteBucketCallback;
import network.genaro.storage.GenaroCallback.RenameBucketCallback;
import network.genaro.storage.GenaroCallback.ListFilesCallback;
import network.genaro.storage.GenaroCallback.ListMirrorsCallback;
import network.genaro.storage.GenaroCallback.DeleteFileCallback;
import network.genaro.storage.GenaroCallback.ResolveFileCallback;
import network.genaro.storage.GenaroCallback.StoreFileCallback;

@Test()
public final class TestGenaro {
    private static final String V3JSON = "{\"version\":3,\"id\":\"b3d00298-275f-4f09-96d0-2da6000f2a04\",\"address\":\"aaad65391d2d2eafda9b27326d1e80002a6a3dc8\",\"crypto\":{\"ciphertext\":\"c362de15e57e1fd0ca66b6c2483292ed260000000065164e875eebece257702e\",\"cipherparams\":{\"iv\":\"934b7985f4c60000000f97f89a101ee7\"},\"cipher\":\"aes-128-ctr\",\"kdf\":\"scrypt\",\"kdfparams\":{\"dklen\":32,\"salt\":\"f5e2b5075600000003c66191656e03cfb19b5e537dcb117ad4fbc1fda46f61c5\",\"n\":262144,\"r\":8,\"p\":1},\"mac\":\"0b8c0000000b9e9de24357bbe74b68baf576ac31e9bfebe3f3d48c5474703df9\"},\"name\":\"Wallet 0\"}";

//    private static String testBridgeUrl = "http://118.31.61.119:8080";
//    private static String testBridgeUrl = "http://127.0.0.1:8080";
    private static final String testBridgeUrl = "http://120.77.247.10:8080";
//    private static String testBridgeUrl = "http://47.100.33.60:8080";
//    private static final String testBucketId = "5c0e433cdaa4e03fe1b5b287";
//    private static final String testBucketId = "b5e9bd5fd6f571beee9b035f";
    private static final String testBucketId = "5ba341402e49103d8787e52d";
//    private static final String testBucketId = "5c1b3c70a100262b970883a0";

    public void testGetInfo() {
        Genaro api = new Genaro(testBridgeUrl);
        String info = api.getInfo();
        System.out.println(info);
    }

    public void testGetBuckets() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

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
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

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
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

        CompletableFuture<Void> fu = api.renameBucket(testBucketId, "呵呵", new RenameBucketCallback() {
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
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

        Bucket b = api.getBucket(null, testBucketId);

        if(b == null) {
            System.out.println("Get Bucket failed.");
        } else {
            System.out.println(b);
        }
    }

    public void testListFiles() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

        CompletableFuture<Void> fu = api.listFiles(testBucketId, new ListFilesCallback() {
            @Override
            public void onFinish(GenaroFile[] files) {
                if(files.length == 0) {
                    System.out.println("No files.");
                } else {
                    for (GenaroFile b : files) {
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

    public void testListMirrors() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

        CompletableFuture<Void> fu = api.listMirrors(testBucketId, "5c1b3d59926e422b70d1a4ea", new ListMirrorsCallback() {
            @Override
            public void onFinish(String text) {
                System.out.println(text);
            }
            @Override
            public void onFail(String error) {
                System.out.println("List mirrors failed, reason: " + error + ".");
            }
        });

        fu.join();
    }

    public void testIsFileExist() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

        String fileName = "spam.txt";
        String encryptedFileName = CryptoUtil.encryptMetaHmacSha512(BasicUtil.string2Bytes(fileName), api.getPrivateKey(), Hex.decode(testBucketId));

        boolean exist = api.isFileExist(null, testBucketId, encryptedFileName);
        if(exist) {
            System.out.println("File exists.");
        } else {
            System.out.println("File not exists.");
        }
    }

    public void testDeleteFile() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

        CompletableFuture<Void> fu = api.deleteFile(testBucketId, "5c10ee10bbdd6f2d157de097", new DeleteFileCallback() {
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
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");
        GenaroFile file;

        try {
            file = api.getFileInfo(null, testBucketId, "5c2de5e5926e422b70d1ce00");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        System.out.println(file);
    }

    public void testGetPointers() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

        List<Pointer> psa;

        try {
            psa = api.requestPointers(null, testBucketId, "f40da862c00494bb0430e012");
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
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

        Frame frame;
        try {
            frame = api.requestNewFrame(null);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        System.out.println(frame);
    }

    public void testResolveFile() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

        // Downloader downloader = api.resolveFile(testBucketId, "5c3c5d38926e422b70d1fb09", "/Users/dingyi/Genaro/test/download/500m3.data", true, new ResolveFileCallback() {
        // Downloader downloader = api.resolveFile(testBucketId, "5c2ded72926e422b70d1cfd8", "/Users/dingyi/Genaro/test/download/genaroNetwork-windows.zip", true, new ResolveFileCallback() {
        Downloader downloader = api.resolveFile(testBucketId, "5bf7c98165390d21283c15f5", "/Users/dingyi/Genaro/test/download/spam.txt", true, new GenaroCallback.ResolveFileCallback() {
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

    public void testResolveFileCancel() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

//            Downloader downloader = api.resolveFile(testBucketId, "5c0a3006bbdd6f2d157dcedb", "/Users/dingyi/Genaro/test/download/cpor-genaro", new ResolveFileCallback() {
        Downloader downloader = api.resolveFile(testBucketId, "5c08d01c963d402a1f3ede80", "/Users/dingyi/Genaro/test/download/r.zip", true, new ResolveFileCallback() {
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

    public void testResolveFileParallel() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

        List<Downloader> downloaders = new ArrayList<>();

        try {
            for(int i = 0; i < 5; i++) {
                Downloader downloader = api.resolveFile(testBucketId, "5bf7c98165390d21283c15f5", "/Users/dingyi/Genaro/test/download/spam" + i + ".txt", true, new ResolveFileCallback() {
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

    public void testStoreFile() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

//        Uploader uploader = api.storeFile(true, "/Users/dingyi/Downloads/513m.data", "513m.data", testBucketId, new StoreFileCallback() {
//        Uploader uploader = api.storeFile(false, "/Users/dingyi/Downloads/500m.data", "500m2.data", testBucketId, new StoreFileCallback() {

//        Uploader uploader = api.storeFile(true, "/Users/dingyi/Downloads/1.txt", "1a.txt", testBucketId, new StoreFileCallback() {
        Uploader uploader = api.storeFile(true, "/Users/dingyi/Downloads/spam.txt", "spam212.txt", testBucketId, new StoreFileCallback() {
//        Uploader uploader = api.storeFile(true, "/Users/dingyi/Downloads/2097153.data", "2097153.data", testBucketId, new StoreFileCallback() {
//        Uploader uploader = api.storeFile(true, "/Users/dingyi/Downloads/下载器苹果电脑Mac版.zip", "25.zip", testBucketId, new StoreFileCallback() {
//        Uploader uploader = api.storeFile(true, "/Users/dingyi/Downloads/genaro.tar", "1.tar", testBucketId, new StoreFileCallback() {
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

    public void testStoreFileCancel() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

        Uploader uploader = api.storeFile(true, "/Users/dingyi/Downloads/下载器苹果电脑Mac版.zip", "27.zip", testBucketId, new StoreFileCallback() {
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

    public void testStoreFileParallel() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, V3JSON, "111111");

        List<Uploader> uploaders = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            Uploader uploader = api.storeFile(true, "/Users/dingyi/test/spam.txt", "spam2" + i + ".txt", testBucketId, new StoreFileCallback() {
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
