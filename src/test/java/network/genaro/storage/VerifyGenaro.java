package network.genaro.storage;

import org.testng.Assert;
import org.testng.annotations.Test;

import network.genaro.storage.GenaroCallback.GetBucketsCallback;
import network.genaro.storage.GenaroCallback.DeleteBucketCallback;
import network.genaro.storage.GenaroCallback.ListFilesCallback;
import network.genaro.storage.GenaroCallback.ListMirrorsCallback;
import network.genaro.storage.GenaroCallback.DeleteFileCallback;
import network.genaro.storage.GenaroCallback.ResolveFileCallback;
import network.genaro.storage.GenaroCallback.StoreFileCallback;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Test()
public final class VerifyGenaro {
    private static final String testPrivKey = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    private static final String testBridgeUrl = "http://localhost:8091";
    private static final String testIndexStr = "d2891da46d9c3bf42ad619ceddc1b6621f83e6cb74e6b6b6bc96bdbfaefb8692";

    private static final String testBucketId = "368be0816766b28fd5f43af5";
    private static final String testFileId = "998960317b6725a3f8080c2b";

    private String tempDir = System.getProperty("java.io.tmpdir");
    private static final String testUploadFileName = "genaro-test-upload.data";
    private static final String testDownloadFileName = "genaro-test-download.data";

    public void verifyGetInfo() {
        Genaro api = new Genaro(testBridgeUrl);
        String info = api.getInfo();
        Assert.assertNotNull(info);
    }

    public void verifyGetBuckets() {
        Genaro api = new Genaro(testBridgeUrl, testPrivKey);

        CompletableFuture<Void> fu = api.getBuckets(new GetBucketsCallback() {
            @Override
            public void onFinish(Bucket[] buckets) {
                Assert.assertEquals(buckets.length, 3);
                Assert.assertEquals(buckets[0].getId(), "e3eca45f4d294132c07b49f4");
                Assert.assertEquals(buckets[1].getId(), "ef466257f5e10aa4755d411a");
                Assert.assertEquals(buckets[2].getId(), "eaf27b3a4daec36fb45f8b57");
            }
            @Override
            public void onFail(String error) {
                Assert.fail("List buckets failed, reason: " + error + ".");
            }
        });

        fu.join();
    }

    public void verifyDeleteBucket() {
        Genaro api = new Genaro(testBridgeUrl, testPrivKey);

        CompletableFuture<Void> fu = api.deleteBucket(testBucketId, new DeleteBucketCallback() {
            @Override
            public void onFail(String error) {
                Assert.fail("Delete bucket failed, reason: " + error + ".");
            }
        });

        fu.join();
    }

    public void verifyGetBucket() throws Exception {
        Genaro api = new Genaro(testBridgeUrl, testPrivKey);

        Bucket b = api.getBucket(null, testBucketId);

        if(b == null) {
            Assert.fail("Get Bucket failed.");
        }

        Assert.assertEquals(b.getId(), "368be0816766b28fd5f43af5");
    }

    public void verifyListFiles() {
        Genaro api = new Genaro(testBridgeUrl, testPrivKey);

        CompletableFuture<Void> fu = api.listFiles(testBucketId, new ListFilesCallback() {
            @Override
            public void onFinish(GenaroFile[] files) {
                Assert.assertEquals(files.length, 2);
                Assert.assertEquals(files[0].getId(), "f18b5ca437b1ca3daa14969f");
                Assert.assertEquals(files[1].getId(), "85fb0ed00de1196dc22e0f6d");
            }
            @Override
            public void onFail(String error) {
                Assert.fail("List files failed, reason: " + error + ".");
            }
        });

        fu.join();
    }

    public void verifyListMirrors() {
        Genaro api = new Genaro(testBridgeUrl, testPrivKey);

        CompletableFuture<Void> fu = api.listMirrors(testBucketId, testFileId, new ListMirrorsCallback() {
            @Override
            public void onFinish(String text) {
                Assert.assertNotNull(text);
            }
            @Override
            public void onFail(String error) {
                Assert.fail("List mirrors failed, reason: " + error + ".");
            }
        });

        fu.join();
    }

    public void verifyDeleteFile() {
        Genaro api = new Genaro(testBridgeUrl, testPrivKey);

        CompletableFuture<Void> fu = api.deleteFile(testBucketId, testFileId, new DeleteFileCallback() {
            @Override
            public void onFail(String error) {
                Assert.fail("Delete file failed, reason: " + error + ".");
            }
        });

        fu.join();
    }

    public void verifyGetFileInfo() {
        Genaro api = new Genaro(testBridgeUrl, testPrivKey);

        GenaroFile file;
        try {
            file = api.getFileInfo(null, testBucketId, testFileId);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
            return;
        }

        Assert.assertEquals(file.getId(), "998960317b6725a3f8080c2b");
    }

    public void verifyGetPointers() {
        Genaro api = new Genaro(testBridgeUrl, testPrivKey);

        List<Pointer> psa;
        try {
            psa = api.requestPointers(null, testBucketId, testFileId);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
            return;
        }

        Assert.assertEquals(psa.size(), 18);
        Assert.assertEquals(psa.get(0).getHash(), "269e72f24703be80bbb10499c91dc9b2022c4dc3");
    }

    public void verifyRequestFrameId() {
        Genaro api = new Genaro(testBridgeUrl, testPrivKey);

        Frame frame;
        try {
            frame = api.requestNewFrame(null);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
            return;
        }

        Assert.assertEquals(frame.getId(), "d6367831f7f1b117ffdd0015");
    }

    public void verifyStoreFile() {
        Genaro api = new Genaro(testBridgeUrl, testPrivKey);
        api.setIndexStr(testIndexStr);

        if (!tempDir.endsWith("/")) {
            tempDir += "/";
        }
        Uploader uploader = api.storeFile(true, tempDir + testUploadFileName, testUploadFileName, testBucketId, new StoreFileCallback() {
            @Override
            public void onFinish(String fileId) {
                Assert.assertEquals(fileId, "85fb0ed00de1196dc22e0f6d");
            }
            @Override
            public void onFail(String error) {
                Assert.fail("Upload failed, reason: " + (error != null ? error : "Unknown") + ".");
            }
        });
        uploader.join();
    }

    public void verifyResolveFile() {
        Genaro api = new Genaro(testBridgeUrl, testPrivKey);

        if (!tempDir.endsWith("/")) {
            tempDir += "/";
        }
        Downloader downloader = api.resolveFile(testBucketId, testFileId, tempDir + testDownloadFileName, true, new ResolveFileCallback() {
            @Override
            public void onFinish() {
                // check the sha256 of the file is: 5b2eb5f37cc1bfaaf73670cafac5ab7ce247ca06e973e7de0dae940d3af6784b
            }
            @Override
            public void onFail(String error) {
                Assert.fail("Download failed, reason: " + (error != null ? error : "Unknown"));
            }
        });

        downloader.join();
    }
}
