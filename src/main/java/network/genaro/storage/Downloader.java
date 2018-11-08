package network.genaro.storage;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.bouncycastle.util.encoders.Hex;

import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static javax.crypto.Cipher.DECRYPT_MODE;

class KeyIv {
    private byte[] key;
    private byte[] iv;

    public KeyIv(byte[] key, byte[] iv) {
        this.key = key;
        this.iv = iv;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getIv() {
        return iv;
    }
}
public class Downloader {

    private String path;
    private BridgeApi bridge;
    private String bucketId;
    private String fileId;

    private long shardSize;
    private FileChannel fileChannel;
    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(100, TimeUnit.SECONDS)
            .writeTimeout(100, TimeUnit.SECONDS)
            .readTimeout(300, TimeUnit.SECONDS)
            .build();

    private static final ExecutorService shardExecutor = Executors.newCachedThreadPool();

    public Downloader(final BridgeApi bridge, final String path, final String bucketId, final String fileId) throws IOException {
        this.path = path;
        this.bridge = bridge;
        this.fileId = fileId;
        this.bucketId = bucketId;
        // init file channel
        this.fileChannel = FileChannel.open(Paths.get(path), CREATE, WRITE, READ);
        //
    }

    private KeyIv determineDecryptionKey(File f, byte[] privateKey) {
        // todo AES key
        byte[] fileKey = CryptoUtil.generateFileKey(privateKey, Hex.decode(f.getBucket()), Hex.decode(f.getIndex()));
        byte[] index   = Hex.decode(f.getIndex());
        byte[] iv = Arrays.copyOf(index, 16);
        return new KeyIv(fileKey, iv);
    }

    private void requestShards() {

    }

    private void recoverShards() {

    }

    private Future<?> downloadShardByPointer(Pointer p) {
        return shardExecutor.submit(() -> {
            Farmer f = p.getFarmer();
            String url = String.format("http://%s:%s/shards/%s?token=%s", f.getAddress(), f.getPort(), p.getHash(), p.getToken());
            Request request = new Request.Builder().url(url).build();

            System.out.println("downloading pointer: " + p);
            try (Response response = client.newCall(request).execute()) {
                if (response.code() != 200) {
                    System.out.println("cannot fetch shard");
                }
                System.out.println("downloaded pointer: " + p);

                byte[] body = response.body().bytes();
                ByteBuffer bbf = ByteBuffer.wrap(body);
                fileChannel.write(bbf, shardSize * p.getIndex());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void start() throws Exception {
        // request info
        File f = bridge.getFileInfo(bucketId, fileId).get();
        KeyIv kiv = determineDecryptionKey(f, bridge.getPrivateKey());

        List<Pointer> pointers = bridge.getPointers(bucketId, fileId).get();
        // set shard size to the first shard
        if (pointers.size() > 0) {
            shardSize = pointers.get(0).getSize();
        }
        long fileSize = pointers.stream().filter(p -> !p.isParity()).mapToLong(Pointer::getSize).sum();
        // TODO: check for replace pointer

        List<Future<?>> downFuture = pointers.stream().filter(p -> !p.isParity()).map(this::downloadShardByPointer).collect(Collectors.toList());
        for (Future p : downFuture) {
            p.get();
            System.out.println("done.");
        }

        SecretKeySpec keySpec = new SecretKeySpec(kiv.getKey(), "AES");
        IvParameterSpec iv = new IvParameterSpec(kiv.getIv());

        //create cipher instance
        javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
        cipher.init(DECRYPT_MODE, keySpec, iv);
        // shard downloaded
        fileChannel.truncate(fileSize);

        fileChannel.close();

        try (InputStream in = Files.newInputStream(Paths.get(this.path))) {
            Files.copy(new CipherInputStream(in, cipher), Paths.get(this.path + "hahaha.jpg"));
        }
    }


    public void cancel() {

    }
}
