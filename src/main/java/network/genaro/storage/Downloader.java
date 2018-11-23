package network.genaro.storage;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.util.encoders.Hex;

import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import java.util.Arrays;
import java.util.List;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static javax.crypto.Cipher.DECRYPT_MODE;

import static network.genaro.storage.Parameters.*;

public class Downloader {
    private static final Logger logger = LogManager.getLogger(Genaro.class);

    private String path;
    private String tempPath;
    private Genaro bridge;
    private String bucketId;
    private String fileId;
    private Progress progress;

    private long shardSize;
    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
            .writeTimeout(OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
            .readTimeout(OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS)
            .build();

    private static final ExecutorService shardExecutor = Executors.newCachedThreadPool();

    public Downloader(final Genaro bridge, final String path, final String bucketId, final String fileId, Progress progress) {
        this.path = path;
        this.tempPath = path + ".temp";
        this.bridge = bridge;
        this.fileId = fileId;
        this.bucketId = bucketId;
        this.progress = progress;
    }

    public Downloader(final Genaro bridge, final String path, final String bucketId, final String fileId) {
        this(bridge, path, bucketId, fileId, new Progress(){
            @Override
            public void onBegin() { }
            @Override
            public void onEnd() { }
            @Override
            public void onError() { }
            @Override
            public void onProgress(float progress, String message) { }
        });
    }

    private CompletableFuture<ByteBuffer> downloadShardByPointer(Pointer p) {
        return CompletableFuture.supplyAsync(() -> {
            Farmer f = p.getFarmer();
            String url = String.format("http://%s:%s/shards/%s?token=%s", f.getAddress(), f.getPort(), p.getHash(), p.getToken());
            Request request = new Request.Builder().url(url).build();

            try (Response response = client.newCall(request).execute()) {
                if (response.code() != 200) {
                    System.out.println("cannot fetch shard");
                }
                byte[] body = response.body().bytes();
                return ByteBuffer.wrap(body);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }, shardExecutor);
    }

    public void start() throws IOException, TimeoutException, ExecutionException, InterruptedException, InvalidAlgorithmParameterException, InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException {
        this.progress.onBegin();
        FileChannel fileChannel = FileChannel.open(Paths.get(tempPath), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);
        // request info and pointers
        File f = bridge.getFileInfo(bucketId, fileId).get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
        List<Pointer> pointers = bridge.getPointers(bucketId, fileId).get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
        // set shard size to the first shard
        if (pointers.size() > 0) {
            shardSize = pointers.get(0).getSize();
        }
        long fileSize = pointers.stream().filter(p -> !p.isParity()).mapToLong(Pointer::getSize).sum();
        // TODO: check for replace pointer

        // downloading
        AtomicLong downloadedBytes = new AtomicLong();
        CompletableFuture[] downFutures = pointers
                .stream()
                .filter(p -> !p.isParity())
                .map(p -> {
                    CompletableFuture<ByteBuffer> fu = downloadShardByPointer(p);
                    progress.onProgress(0f, "begin downloading " + p);
                    fu.thenAcceptAsync(bf -> {
                        long thisSize = p.getSize();
                        downloadedBytes.addAndGet(thisSize);
                        try {
                            fileChannel.write(bf, shardSize * p.getIndex());
                            progress.onProgress(downloadedBytes.floatValue() / fileSize, "done: " + p);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                    return fu;
                })
                .toArray(CompletableFuture[]::new);
        CompletableFuture futureAll = CompletableFuture.allOf(downFutures);
        futureAll.get(); // all done
        fileChannel.truncate(fileSize);

        // decryption:
        progress.onProgress(1f, "download complete");
        progress.onProgress(1f, "begin decryption");
        byte[] index   = Hex.decode(f.getIndex());
        byte[] fileKey = CryptoUtil.generateFileKey(bridge.getPrivateKey(), Hex.decode(f.getBucket()), index);
        byte[] ivBytes = Arrays.copyOf(index, 16);
        SecretKeySpec keySpec = new SecretKeySpec(fileKey, "AES");
        IvParameterSpec iv = new IvParameterSpec(ivBytes);
        javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
        cipher.init(DECRYPT_MODE, keySpec, iv);

        try (InputStream in = Channels.newInputStream(fileChannel);
             InputStream cypherIn = new CipherInputStream(in, cipher)) {
            Files.copy(cypherIn, Paths.get(this.path));
        }
        fileChannel.close();
        this.progress.onEnd();
    }
}
