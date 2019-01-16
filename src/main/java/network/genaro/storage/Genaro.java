package network.genaro.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.MediaType;
import okhttp3.RequestBody;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketTimeoutException;
import java.net.URLEncoder;
import java.nio.channels.FileChannel;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.web3j.crypto.CipherException;

import org.bouncycastle.util.encoders.Hex;

import static network.genaro.storage.CryptoUtil.*;
import static network.genaro.storage.Parameters.*;
import static network.genaro.storage.Pointer.PointerStatus.*;
import network.genaro.storage.GenaroCallback.GetBucketsCallback;
import network.genaro.storage.GenaroCallback.DeleteBucketCallback;
import network.genaro.storage.GenaroCallback.RenameBucketCallback;
import network.genaro.storage.GenaroCallback.ListFilesCallback;
import network.genaro.storage.GenaroCallback.ListMirrorsCallback;
import network.genaro.storage.GenaroCallback.DeleteFileCallback;
import network.genaro.storage.GenaroCallback.ResolveFileCallback;
import network.genaro.storage.GenaroCallback.StoreFileCallback;

final class ShardMeta {
    private String hash;
    private byte[][] challenges;    // [GENARO_SHARD_CHALLENGES][32]
    private String[] challengesAsStr;  // [GENARO_SHARD_CHALLENGES]

    // Merkle Tree leaves. Each leaf is size of RIPEMD160 hash
    private String[] tree;  // [GENARO_SHARD_CHALLENGES]
    private int index;
    private boolean isParity;
    private long size;

    ShardMeta(int index) { this.setIndex(index); }

    String getHash() {
        return hash;
    }

    void setHash(String hash) {
        this.hash = hash;
    }

    byte[][] getChallenges() { return challenges; }

    void setChallenges(byte[][] challenges) {
        this.challenges = challenges;
    }

    String[] getChallengesAsStr() {
        return challengesAsStr;
    }

    void setChallengesAsStr(String[] challengesAsStr) {
        this.challengesAsStr = challengesAsStr;
    }

    String[] getTree() {
        return tree;
    }

    void setTree(String[] tree) {
        this.tree = tree;
    }

    int getIndex() {
        return index;
    }

    void setIndex(int index) {
        this.index = index;
    }

    boolean getParity() {
        return isParity;
    }

    void setParity(boolean parity) {
        isParity = parity;
    }

    long getSize() {
        return size;
    }

    void setSize(long size) {
        this.size = size;
    }
}

// do not delete the methods that are not called explicitly, the set methods is used when method readValue of ObjectMapper is called
@JsonIgnoreProperties(ignoreUnknown = true)
final class Farmer {
    private String userAgent;
    private String protocol;
    private String address;
    private String port;
    private String nodeID;
    private String lastSeen;

    void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    String getAddress() {
        return address;
    }

    void setAddress(String address) {
        this.address = address;
    }

    String getPort() {
        return port;
    }

    void setPort(String port) {
        this.port = port;
    }

    String getNodeID() { return nodeID; }

    void setNodeID(String nodeID) {
        this.nodeID = nodeID;
    }

    void setLastSeen(String lastSeen) {
        this.lastSeen = lastSeen;
    }

    @Override
    public String toString() {
        return "Farmer{" +
                "userAgent='" + userAgent + '\'' +
                ", protocol='" + protocol + '\'' +
                ", address='" + address + '\'' +
                ", port='" + port + '\'' +
                ", nodeID='" + nodeID + '\'' +
                ", lastSeen='" + lastSeen + '\'' +
                '}';
    }

    String toBriefString() {
        return "Farmer{" +
                "address='" + address + '\'' +
                ", port='" + port + '\'' +
                '}';
    }
}

// do not delete the methods that are not called explicitly, the set methods is used when method readValue of ObjectMapper is called
@JsonIgnoreProperties(ignoreUnknown = true)
final class Frame {
    private String user;
    private long storageSize;
    private long size;
    private boolean locked;
    private String created;
    private String id;

    void setUser(String user) {
        this.user = user;
    }

    void setStorageSize(long storageSize) {
        this.storageSize = storageSize;
    }

    void setSize(long size) {
        this.size = size;
    }

    void setLocked(boolean locked) {
        this.locked = locked;
    }

    void setCreated(String created) {
        this.created = created;
    }

    String getId() {
        return id;
    }

    void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Frame{" +
                "user='" + user + '\'' +
                ", storageSize=" + storageSize +
                ", size=" + size +
                ", locked=" + locked +
                ", created='" + created + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}

final class GenaroExchangeReport {
    private long start;
    private long end;
    private int code;
    private String message;

    long getStart() {
        return start;
    }

    void setStart(long start) {
        this.start = start;
    }

    long getEnd() {
        return end;
    }

    void setEnd(long end) {
        this.end = end;
    }

    int getCode() {
        return code;
    }

    void setCode(int code) {
        this.code = code;
    }

    String getMessage() {
        return message;
    }

    void setMessage(String message) {
        this.message = message;
    }
}

/** @brief A structure that represents a pointer to a shard
 *
 * A shard is an encrypted piece of a file, a pointer holds all necessary
 * information to retrieve a shard from a farmer, including the IP address
 * and port of the farmer, as well as a token indicating a transfer has been
 * authorized. Other necessary information such as the expected hash of the
 * data, and the index position in the file is also included.
 *
 * The data can be replaced with new farmer contact, in case of failure, and the
 * total number of replacements can be tracked.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
final class Pointer {
    /** @brief Enumerable that defines that status of a pointer
     *
     * A pointer will begin as created, and move forward until an error
     * occurs, in which case it will start moving backwards from the error
     * state until it has been replaced and reset back to created. This process
     * can continue until success.
     */
    enum PointerStatus
    {
        POINTER_ERROR_REPORTED,
        POINTER_ERROR,
        POINTER_MISSING,
        POINTER_REPLACED
    }

    private int index;
    private String hash;
    private long size;
    private boolean parity;

    // token & operation & Farmer will be null if no farmer found
    private String token;
    private String operation;
    private Farmer farmer;
    private long downloadedSize;

    private boolean isReplaced = false;
    private PointerStatus status;

    // the count of requestShard
    private int requestCount;
    // the count of requestReplacePointer
    private int replaceCount;

    // exchange report with bridge
    private GenaroExchangeReport report;

    PointerStatus getStatus() {
        return status;
    }

    void setStatus(PointerStatus status) {
        this.status = status;
    }

    boolean isReplaced() {
        return isReplaced;
    }

    void setReplaced(boolean replaced) {
        isReplaced = replaced;
    }

    String getToken() {
        return token;
    }

    void setToken(String token) {
        this.token = token;
    }

    String getOperation() {
        return operation;
    }

    void setOperation(String operation) {
        this.operation = operation;
    }

    Farmer getFarmer() {
        return farmer;
    }

    void setFarmer(Farmer farmer) {
        this.farmer = farmer;
    }

    String getHash() {
        return hash;
    }

    void setHash(String hash) {
        this.hash = hash;
    }

    long getSize() {
        return size;
    }

    void setSize(long size) {
        this.size = size;
    }

    int getIndex() {
        return index;
    }

    void setIndex(int index) {
        this.index = index;
    }

    boolean isParity() {
        return parity;
    }

    void setParity(boolean parity) {
        this.parity = parity;
    }

    int getRequestCount() {
        return requestCount;
    }

    void setRequestCount(int requestCount) {
        this.requestCount = requestCount;
    }

    int getReplaceCount() {
        return replaceCount;
    }

    void setReplaceCount(int replaceCount) {
        this.replaceCount = replaceCount;
    }

    GenaroExchangeReport getReport() {
        return report;
    }

    void setReport(GenaroExchangeReport report) {
        this.report = report;
    }

    @Override
    public String toString() {
        return "Pointer{" +
                "index=" + index +
                ", hash='" + hash + '\'' +
                ", size=" + size +
                ", parity=" + parity +
                ", token='" + token + '\'' +
                ", operation='" + operation + '\'' +
                ", farmer=" + farmer +
                '}';
    }

    String toBriefString() {
        return "Pointer{" +
                "index=" + index +
                ", size=" + size +
                ", parity=" + parity +
                ", farmer=" + ((farmer != null) ? farmer.toBriefString() : "null") +
                '}';
    }

    long getDownloadedSize() {
        return downloadedSize;
    }

    void setDownloadedSize(long downloadedSize) {
        this.downloadedSize = downloadedSize;
    }
}

// do not delete the methods that are not called explicitly, the set methods is used when method readValue of ObjectMapper is called
@JsonIgnoreProperties(ignoreUnknown = true)
final class FarmerPointer {
    private String token;
    private Farmer farmer;

    String getToken() {
        return token;
    }

    void setToken(String token) {
        this.token = token;
    }

    Farmer getFarmer() {
        return farmer;
    }

    void setFarmer(Farmer farmer) {
        this.farmer = farmer;
    }
}

final class ShardTracker {
    enum ShardStatus
    {
        SHARD_PUSH_SUCCESS
    }

    //    int progress;
    private int index;
    private FarmerPointer pointer;
    private ShardMeta meta;
    private long uploadedSize;

    private FileChannel shardChannel;

    // exchange report with bridge
    private GenaroExchangeReport report;

    // the count of pushShard
    private int pushCount;
    // whether the last pushShard call tried to push the shard
    private boolean hasTriedToPush = false;
    private ShardStatus status;

    int getIndex() {
        return index;
    }

    void setIndex(int index) {
        this.index = index;
    }

    FarmerPointer getPointer() {
        return pointer;
    }

    void setPointer(FarmerPointer pointer) {
        this.pointer = pointer;
    }

    ShardMeta getMeta() {
        return meta;
    }

    void setMeta(ShardMeta meta) {
        this.meta = meta;
    }

    long getUploadedSize() {
        return uploadedSize;
    }

    void setUploadedSize(long uploadedSize) {
        this.uploadedSize = uploadedSize;
    }

    GenaroExchangeReport getReport() {
        return report;
    }

    void setReport(GenaroExchangeReport report) {
        this.report = report;
    }

    int getPushCount() {
        return pushCount;
    }

    void setPushCount(int pushCount) {
        this.pushCount = pushCount;
    }

    boolean getHasTriedToPush() {
        return hasTriedToPush;
    }

    void setHasTriedToPush(boolean hasTriedToPush) {
        this.hasTriedToPush = hasTriedToPush;
    }

    ShardStatus getStatus() {
        return status;
    }

    void setStatus(ShardStatus status) {
        this.status = status;
    }

    public FileChannel getShardChannel() {
        return shardChannel;
    }

    public void setShardChannel(FileChannel shardChannel) {
        this.shardChannel = shardChannel;
    }
}

public final class Genaro {
    static final Logger logger = LogManager.getLogger(Genaro.class);
    private static final int POINT_PAGE_COUNT = 3;

    private String bridgeUrl;
    private GenaroWallet wallet;

    // only for test
    private boolean test = false;
    // only for test
    private String privKeyForTest;

    private OkHttpClient genaroHttpClient;

    private String proxyAddr;
    private int proxyPort;

    // the index for uploading
    private String indexStr;

    public Genaro(final String bridgeUrl) {
        init(bridgeUrl);
    }

    // default, not public
    Genaro(final String bridgeUrl, final String privKeyForTest) {
        init(bridgeUrl);
        this.privKeyForTest = privKeyForTest;
        test = true;
    }

    public Genaro(final String bridgeUrl, final String proxyAddr, final int proxyPort) {
        init(bridgeUrl, proxyAddr, proxyPort);
    }

    public Genaro(final String bridgeUrl, final String privKey, final String passwd) throws CipherException, IOException {
        init(bridgeUrl, privKey, passwd);
    }

    public Genaro(final String bridgeUrl, final String privKey, final String passwd, final int logLevel) throws CipherException, IOException {
        init(bridgeUrl, privKey, passwd, logLevel);
    }

    public Genaro(final String bridgeUrl, final String privKey, final String passwd, final int logLevel, final String proxyAddr, final int proxyPort) throws CipherException, IOException {
        init(bridgeUrl, privKey, passwd, logLevel, proxyAddr, proxyPort);
    }

    public void init(final String bridgeUrl) {
        this.bridgeUrl = bridgeUrl;

        genaroHttpClient = new OkHttpClient.Builder()
                .connectTimeout(GENARO_OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
                .writeTimeout(GENARO_OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
                .readTimeout(GENARO_OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS)
                .build();
    }

    public void init(final String bridgeUrl, final String proxyAddr, final int proxyPort) {
        this.bridgeUrl = bridgeUrl;

        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .connectTimeout(GENARO_OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
                .writeTimeout(GENARO_OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
                .readTimeout(GENARO_OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS);

        // set proxy server
        if (proxyAddr != null && !proxyAddr.trim().isEmpty() && proxyPort > 0 && proxyPort <= 65535) {
            this.proxyAddr = proxyAddr;
            this.proxyPort = proxyPort;
            builder = builder.proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyAddr, proxyPort)));
        }

        genaroHttpClient = builder.build();
    }

    public void init(final String bridgeUrl, final String privKey, final String passwd) throws CipherException, IOException {
        this.init(bridgeUrl);
        this.wallet = new GenaroWallet(privKey, passwd);
    }

    public void init(final String bridgeUrl, final String privKey, final String passwd, final int logLevel) throws CipherException, IOException {
        this.init(bridgeUrl, privKey, passwd);
        initLog(logLevel);
    }

    public void init(final String bridgeUrl, final String privKey, final String passwd, final int logLevel, final String proxyAddr, final int proxyPort) throws CipherException, IOException {
        this.init(bridgeUrl, proxyAddr, proxyPort);
        this.wallet = new GenaroWallet(privKey, passwd);
        initLog(logLevel);
    }

    public String getIndexStr() {
        return indexStr;
    }

    public void setIndexStr(String indexStr) {
        this.indexStr = indexStr;
    }

    private void verifyInit(final boolean checkWallet) {
        if (!test && (bridgeUrl == null || (checkWallet && wallet == null))) {
            throw new GenaroRuntimeException("Bridge url or wallet has not been initialized!");
        }
    }

    // init log level fo log4j2
    static void initLog(final int logLevel) {
        Level level;
        switch (logLevel) {
            case 4:
                level = Level.DEBUG;
                break;
            case 3:
                level = Level.INFO;
                break;
            case 2:
                level = Level.WARN;
                break;
            case 1:
                level = Level.ERROR;
                break;
            default:
                level = Level.OFF;
                break;
        }

        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.add(builder.newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.DENY)
                .addAttribute("level", level));

        AppenderComponentBuilder appenderBuilder = builder.newAppender("Stdout", "Console").addAttribute("target",
                ConsoleAppender.Target.SYSTEM_OUT);
        appenderBuilder.add(builder.newLayout("PatternLayout")
                .addAttribute("pattern", "[%d{HH:mm:ss:SSS}] [%p] - %l - %m%n"));
        builder.add(appenderBuilder);
        builder.add(builder.newRootLogger(level).add(builder.newAppenderRef("Stdout")));
        Configurator.initialize(builder.build());

//        final LoggerContext context = LoggerContext.getContext(false);
//        final Configuration config = context.getConfiguration();
//        final PatternLayout layout = PatternLayout.newBuilder().withPattern("%-5level %c{-4} - %msg%n").build();
//        Appender appender = ConsoleAppender.newBuilder().setFollow(true).setTarget(ConsoleAppender.Target.SYSTEM_OUT)
//                .withName("Genaro").withLayout(layout).build();
//        appender.start();
//        config.addAppender(appender);
//        config.getRootLogger().addAppender(appender, level, null);
    }

    public String getBridgeUrl() {
        return bridgeUrl;
    }

    public String getProxyAddr() {
        return proxyAddr;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    static String genaroStrError(final int error_code)
    {
        switch(error_code) {
            case GENARO_BRIDGE_REQUEST_ERROR:
                return "Bridge request error";
            case GENARO_BRIDGE_AUTH_ERROR:
                return "Bridge request authorization error";
            case GENARO_BRIDGE_TOKEN_ERROR:
                return "Bridge request token error";
            case GENARO_BRIDGE_POINTER_ERROR:
                return "Bridge request pointer error";
            case GENARO_BRIDGE_REPOINTER_ERROR:
                return "Bridge request replace pointer error";
            case GENARO_BRIDGE_TIMEOUT_ERROR:
                return "Bridge request timeout error";
            case GENARO_BRIDGE_INTERNAL_ERROR:
                return "Bridge request internal error";
            case GENARO_BRIDGE_RATE_ERROR:
                return "Bridge rate limit error";
            case GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR:
                return "Bucket is not found";
            case GENARO_BRIDGE_FILE_NOTFOUND_ERROR:
                return "File is not found";
            case GENARO_BRIDGE_BUCKET_FILE_EXISTS:
                return "File already exists";
            case GENARO_BRIDGE_OFFER_ERROR:
                return "Unable to receive storage offer";
            case GENARO_BRIDGE_JSON_ERROR:
                return "Unexpected JSON response";
            case GENARO_BRIDGE_FILEINFO_ERROR:
                return "Bridge file info error";
            case GENARO_BRIDGE_DECRYPTION_KEY_ERROR:
                return "Bridge request decryption key error";
            case GENARO_FARMER_REQUEST_ERROR:
                return "Farmer request error";
            case GENARO_FARMER_EXHAUSTED_ERROR:
                return "Farmer exhausted error";
            case GENARO_FARMER_TIMEOUT_ERROR:
                return "Farmer request timeout error";
            case GENARO_FARMER_AUTH_ERROR:
                return "Farmer request authorization error";
            case GENARO_FARMER_INTEGRITY_ERROR:
                return "Farmer request integrity error";
            case GENARO_FILE_INTEGRITY_ERROR:
                return "File integrity error";
            case GENARO_FILE_READ_ERROR:
                return "File read error";
            case GENARO_FILE_WRITE_ERROR:
                return "File write error";
            case GENARO_BRIDGE_FRAME_ERROR:
                return "Bridge frame request error";
            case GENARO_FILE_ENCRYPTION_ERROR:
                return "File encryption error";
            case GENARO_FILE_SIZE_ERROR:
                return "File size error";
            case GENARO_FILE_DECRYPTION_ERROR:
                return "File decryption error";
            case GENARO_FILE_GENERATE_HMAC_ERROR:
                return "File hmac generation error";
            case GENARO_FILE_SHARD_MISSING_ERROR:
                return "File missing shard error";
            case GENARO_FILE_RECOVER_ERROR:
                return "File recover error";
            case GENARO_FILE_RESIZE_ERROR:
                return "File resize error";
            case GENARO_FILE_UNSUPPORTED_ERASURE:
                return "File unsupported erasure code error";
            case GENARO_FILE_PARITY_ERROR:
                return "File create parity error";
            case GENARO_TRANSFER_CANCELED:
                return "File transfer canceled";
            case GENARO_TRANSFER_OK:
                return "No errors";
            case GENARO_ALGORITHM_ERROR:
                return "Algorithm error";
            case GENARO_OUTOFMEMORY_ERROR:
                return "Out of memory error";
            case GENARO_RS_FILE_SIZE_ERROR:
                return "File too large to use Forward Error Correction(Reed-Solomon)";
            case GENARO_UNKNOWN_ERROR:
                // fall through
            default:
                return "Unknown error";
        }
    }

    byte[] getPrivateKey() {
        if (!test) {
            return wallet.getPrivateKey();
        } else {
            return BasicUtil.string2Bytes(privKeyForTest);
        }
    }

    String getPublicKeyHexString() {
        if (!test) {
            return wallet.getPublicKeyHexString();
        } else {
            return "";
        }
    }

    String signRequest(final String method, final String path, final String body) throws NoSuchAlgorithmException {
        if (!test) {
            String msg = method + "\n" + path + "\n" + body;
            return wallet.signMessage(msg);
        } else {
            return "";
        }
    }

    public String getInfo() {
        CompletableFuture<String> fu = CompletableFuture.supplyAsync(() -> {
            verifyInit(false);
            Request request = new Request.Builder()
                    .url(bridgeUrl)
                    .get()
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                String responseBody = response.body().string();

                if (!response.isSuccessful()) throw new GenaroRuntimeException("Unexpected code " + response);

                ObjectMapper om = new ObjectMapper();

                JsonNode bodyNode = om.readTree(responseBody);
                JsonNode infoNode = bodyNode.get("info");

                String title = infoNode.get("title").asText();
                String description = infoNode.get("description").asText();
                String version = infoNode.get("version").asText();
                String host = bodyNode.get("host").asText();

                return "Title:       " + title + "\n" +
                        "Description: " + description + "\n" +
                        "Version:     " + version + "\n" +
                        "Host:        " + host + "\n";
            } catch (IOException e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
            }
        });

        String info;
        try {
            info = fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
        }  catch (Exception e) {
            return null;
        }
        return info;
    }

    CompletableFuture<Bucket> getBucketFuture(final Uploader uploader, final String bucketId) {
        return CompletableFuture.supplyAsync(() -> {
            verifyInit(true);
            String signature;
            try {
                signature = signRequest("GET", "/buckets/" + bucketId, "");
            } catch (NoSuchAlgorithmException e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("getBucket")
                    .url(bridgeUrl + "/buckets/" + bucketId)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            OkHttpClient okHttpClient;
            if(uploader != null) {
                okHttpClient = uploader.getUpHttpClient();
            } else {
                okHttpClient = genaroHttpClient;
            }

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if (code == 404 || code == 400) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR));
                } else if (code != 200) {
                    throw new GenaroRuntimeException("Request failed with status code: " + code);
                }

                ObjectMapper om = new ObjectMapper();
                Bucket bucket = om.readValue(responseBody, Bucket.class);
                return bucket;
            } catch (IOException e) {
                if (uploader.isCanceled()) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    Bucket getBucket(final Uploader uploader, final String bucketId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Bucket> fu = getBucketFuture(uploader, bucketId);
        if(uploader != null) {
            uploader.setFutureGetBucket(fu);
        }
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    /**
     * @brief List available buckets for a user.
     *
     * @param[in] callback The callback when complete
     * @return A CompletableFuture.
     */
    public CompletableFuture<Void> getBuckets(final GetBucketsCallback callback) {
        return CompletableFuture.supplyAsync(() -> {
            verifyInit(true);
            String signature;
            try {
                signature = signRequest("GET", "/buckets", "");
            } catch (NoSuchAlgorithmException e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets")
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if (code == 401) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                    return null;
                } else if (code != 200 && code != 304) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    return null;
                }

                ObjectMapper om = new ObjectMapper();
                Bucket[] buckets = om.readValue(responseBody, Bucket[].class);

                try {
                    // decrypt
                    for (Bucket b : buckets) {
                        if (b.getNameIsEncrypted()) {
                            b.setName(CryptoUtil.decryptMetaHmacSha512(b.getName(), getPrivateKey(), BUCKET_NAME_MAGIC));
                            b.setNameIsEncrypted(false);
                        }
                    }
                } catch (Exception e) {
                    callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                    return null;
                }

                callback.onFinish(buckets);
            } catch (SocketTimeoutException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                return null;
            } catch (IOException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                return null;
            }

            return null;
        });
    }

    /**
     * @brief Delete a bucket.
     *
     * @param[in] bucketId The bucket id
     * @param[in] callback The callback when complete
     * @return A CompletableFuture.
     */
    public CompletableFuture<Void> deleteBucket(final String bucketId, final DeleteBucketCallback callback) {
        return CompletableFuture.supplyAsync(() -> {
            verifyInit(true);
            String signature;
            try {
                signature = signRequest("DELETE", "/buckets/" + bucketId, "");
            } catch (Exception e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets/" + bucketId)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .delete()
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                int code = response.code();
                response.close();

                if (code == 401) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                    return null;
                } else if (code == 404) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR));
                    return null;
                } else if (code != 200 && code != 204) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    return null;
                }

                callback.onFinish();
            } catch (SocketTimeoutException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                return null;
            } catch (IOException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                return null;
            }

            return null;
        });
    }

    /**
     * @brief Rename a bucket.
     *
     * @param[in] bucketId The bucket id
     * @param[in] callback The callback when complete
     * @return A CompletableFuture.
     */
    public CompletableFuture<Void> renameBucket(final String bucketId, final String name, final RenameBucketCallback callback) {
        return CompletableFuture.supplyAsync(() -> {
            verifyInit(true);
            String encryptedName;
            try {
                encryptedName = CryptoUtil.encryptMetaHmacSha512(BasicUtil.string2Bytes(name), getPrivateKey(), BUCKET_NAME_MAGIC);
            } catch (Exception e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }
            String jsonStrBody = String.format("{\"name\": \"%s\", \"nameIsEncrypted\": true}", encryptedName);

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(JSON, jsonStrBody);
            String signature;
            try {
                signature = signRequest("POST", "/buckets/" + bucketId, jsonStrBody);
            } catch (Exception e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets/" + bucketId)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .post(body)
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                int code = response.code();
                response.close();

                if (code == 401) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                    return null;
                } else if (code == 404) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR));
                    return null;
                } else if (code != 200) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    return null;
                }

                callback.onFinish();
            } catch (SocketTimeoutException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                return null;
            } catch (IOException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                return null;
            }

            return null;
        });
    }

    CompletableFuture<GenaroFile> getFileInfoFuture(final Downloader downloader, final String bucketId, final String fileId) {
        return CompletableFuture.supplyAsync(() -> {
            verifyInit(true);
            String path = String.format("/buckets/%s/files/%s/info", bucketId, fileId);
            String signature;
            try {
                signature = signRequest("GET", path, "");
            } catch (Exception e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("getFileInfo")
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            OkHttpClient okHttpClient;
            if(downloader != null) {
                okHttpClient = downloader.getDownHttpClient();
            } else {
                okHttpClient = genaroHttpClient;
            }

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if(code == 403 || code == 401) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                } else if (code == 404 || code == 400) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_FILE_NOTFOUND_ERROR));
                } else if(code == 500) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_INTERNAL_ERROR));
                } else if (code != 200 && code != 304){
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }

                ObjectMapper om = new ObjectMapper();

                GenaroFile file;
                try {
                    file = om.readValue(responseBody, GenaroFile.class);
                } catch (Exception e) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_FILEINFO_ERROR));
                }

                String realName = file.getFilename();
                try {
                    realName = CryptoUtil.decryptMetaHmacSha512(realName, getPrivateKey(), Hex.decode(bucketId));
                    file.setFilename(realName);
                } catch (Exception e) {
                    // do nothing
                }

                String erasureType = file.getErasure().getType();
                if (erasureType != null) {
                    if (erasureType.equals("reedsolomon")) {
                        file.setRs(true);
                    } else {
                        throw new GenaroRuntimeException(genaroStrError(GENARO_FILE_UNSUPPORTED_ERASURE));
                    }
                }

                return file;
            } catch (IOException e) {
                if (downloader.isCanceled()) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    GenaroFile getFileInfo(final Downloader downloader, final String bucketId, final String fileId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<GenaroFile> fu = getFileInfoFuture(downloader, bucketId, fileId);
        if(downloader != null) {
            downloader.setFutureGetFileInfo(fu);
        }
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public CompletableFuture<Void> listMirrors(final String bucketId, final String fileId, final ListMirrorsCallback callback) {
        return CompletableFuture.supplyAsync(() -> {
            verifyInit(true);
            String path = String.format("/buckets/%s/files/%s/mirrors", bucketId, fileId);
            String signature;
            try {
                signature = signRequest("GET", path, "");
            } catch (Exception e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if(code == 403 || code == 401) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                    return null;
                } else if (code == 404 || code == 400) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_FILE_NOTFOUND_ERROR));
                    return null;
                } else if(code == 500) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_INTERNAL_ERROR));
                    return null;
                } else if (code != 200 && code != 304){
                    callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    return null;
                }

                String retText = "";

                ObjectMapper om = new ObjectMapper();
                JsonNode bodyNode = om.readTree(responseBody);

                int i = 0;
                for (JsonNode itemNode: bodyNode) {
                    JsonNode establishedNode = itemNode.get("established");

                    int j = 0;
                    for (JsonNode subNode: establishedNode) {
                        if (j == 0) {
                            retText += String.format("Shard %d: %s\n", i, subNode.get("shardHash").asText());
                        }

                        retText += String.format("\tnodeID: %s\n", subNode.get("contract").get("farmer_id").asText());
                        j++;
                    }
                    i++;
                }

                if (retText.isEmpty()) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_JSON_ERROR));
                } else {
                    callback.onFinish(retText);
                }
            } catch (SocketTimeoutException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                return null;
            } catch (IOException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                return null;
            }

            return null;
        });
    }

    /**
     * @brief Get a list of all files in a bucket.
     *
     * @param[in] bucketId The bucket id
     * @param[in] callback The callback when complete
     * @return A CompletableFuture.
     */
    public CompletableFuture<Void> listFiles(final String bucketId, final ListFilesCallback callback) {
        return CompletableFuture.supplyAsync(() -> {
            verifyInit(true);
            String path = String.format("/buckets/%s/files", bucketId);
            String signature;
            try {
                signature = signRequest("GET", path, "");
            } catch (Exception e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }

            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();
                ObjectMapper om = new ObjectMapper();

                if (code == 404) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR));
                    return null;
                } else if (code == 400) {
                    callback.onFail("Bucket id [" + bucketId + "] is invalid");
                    return null;
                } else if (code == 401) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                    return null;
                } else if (code != 200) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    return null;
                }

                GenaroFile[] files = om.readValue(responseBody, GenaroFile[].class);

                // decrypt
                for (GenaroFile f : files) {
                    String realName = f.getFilename();
                    try {
                        realName = CryptoUtil.decryptMetaHmacSha512(realName, getPrivateKey(), Hex.decode(bucketId));
                        f.setFilename(realName);
                    } catch (Exception e) {
                        // do nothing
                    }
                }

                callback.onFinish(files);
            } catch (SocketTimeoutException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                return null;
            } catch (IOException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                return null;
            }

            return null;
        });
    }

    /**
     * @brief Delete a file in a bucket.
     *
     * @param[in] bucketId The bucket id
     * @param[in] fileId The file id
     * @param[in] callback The callback when complete
     * @return A CompletableFuture.
     */
    public CompletableFuture<Void> deleteFile(final String bucketId, final String fileId, final DeleteFileCallback callback) {
        return CompletableFuture.supplyAsync(() -> {
            verifyInit(true);
            String path = String.format("/buckets/%s/files/%s", bucketId, fileId);
            String signature;
            try {
                signature = signRequest("DELETE", path, "");
            } catch (Exception e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .delete()
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                int code = response.code();
                response.close();

                if (code == 401) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                    return null;
                } else if (code == 404) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_FILE_NOTFOUND_ERROR));
                    return null;
                } else if (code != 200 && code != 204){
                    callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    return null;
                }

                callback.onFinish();
            } catch (SocketTimeoutException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                return null;
            } catch (IOException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                return null;
            }

            return null;
        });
    }

    CompletableFuture<List<Pointer>> requestPointersFuture(final Downloader downloader, final String bucketId, final String fileId) {
        return CompletableFuture.supplyAsync(() -> {
            verifyInit(true);
            List<Pointer> ps= new ArrayList<>();

            int skipCount = 0;
            while (true) {
                logger.info("Requesting next set of pointers, total pointers: " + skipCount);

                List<Pointer> psr;
                try {
                    psr = this.requestPointersRaw(downloader, bucketId, fileId, POINT_PAGE_COUNT, skipCount);
                } catch (Exception e) {
                    if(e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                        throw new GenaroRuntimeException(e.getCause().getMessage());
                    } else {
                        throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    }
                }

                if(psr.size() == 0) {
                    logger.info("Finished requesting pointers");
                    break;
                }

                skipCount += psr.size();
                ps.addAll(psr);
            }

            if (ps.size() == 0) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
            }

            return ps;
        });
    }

    List<Pointer> requestPointers(final Downloader downloader, final String bucketId, final String fileId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<List<Pointer>> fu = requestPointersFuture(downloader, bucketId, fileId);
        if(downloader != null) {
            downloader.setFutureGetPointers(fu);
        }

        // wait it double seconds
        return fu.get(2 * GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    private CompletableFuture<List<Pointer>> requestPointersRawFuture(final Downloader downloader, final String bucketId, final String fileId, final int limit, final int skipCount) {
        return CompletableFuture.supplyAsync(() -> {
            verifyInit(true);
            String queryArgs = String.format("limit=%d&skip=%d", limit, skipCount);
            String url = String.format("/buckets/%s/files/%s", bucketId, fileId);
            String path = String.format("%s?%s", url, queryArgs);
            String signature;
            try {
                signature = signRequest("GET", url, queryArgs);
            } catch (Exception e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("requestPointersRaw")
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            OkHttpClient okHttpClient;
            if(downloader != null) {
                okHttpClient = downloader.getDownHttpClient();
            } else {
                okHttpClient = genaroHttpClient;
            }

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();
                ObjectMapper om = new ObjectMapper();
                JsonNode bodyNode = om.readTree(responseBody);

                logger.info(String.format("Finished request pointers - JSON Response %s", responseBody));

                if (code == 429 || code == 420) {
                    if (bodyNode.has("error")) {
                        logger.error(bodyNode.get("error").asText());
                    }
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_RATE_ERROR));
                } else if (code != 200) {
                    if (bodyNode.has("error")) {
                        logger.error(bodyNode.get("error").asText());
                    }
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_POINTER_ERROR));
                }

                List<Pointer> pointers = om.readValue(responseBody, new TypeReference<List<Pointer>>(){});
                pointers.stream().forEach(pointer -> {
                    if (pointer.getToken() == null || pointer.getFarmer() == null) {
                        // Update status so that it will be retried, do not set to POINTER_MISSING, because it can be replaced
                        pointer.setStatus(POINTER_ERROR_REPORTED);
                    }

                    pointer.setReport(new GenaroExchangeReport());
                });

                return pointers;
            } catch (IOException e) {
                if (downloader.isCanceled()) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    private List<Pointer> requestPointersRaw(final Downloader downloader, final String bucketId, final String fileId, final int limit, final int skipCount)
            throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<List<Pointer>> fu = requestPointersRawFuture(downloader, bucketId, fileId, limit, skipCount);
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    CompletableFuture<Boolean> isFileExistFuture(final Uploader uploader, final String bucketId, final String encryptedFileName) {
        return CompletableFuture.supplyAsync(() -> {
            verifyInit(true);
            String escapedName;
            String path;
            String signature;
            try {
                escapedName = URLEncoder.encode(encryptedFileName, "UTF-8");
                path = String.format("/buckets/%s/file-ids/%s", bucketId, escapedName);
                signature = signRequest("GET", path, "");
            } catch (Exception e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
            }

            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("isFileExist")
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            OkHttpClient okHttpClient;
            if(uploader != null) {
                okHttpClient = uploader.getUpHttpClient();
            } else {
                okHttpClient = genaroHttpClient;
            }

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                response.close();

                if (code == 404) {
                    return false;
                } else if (code == 200) {
                    return true;
                } else {
                    throw new GenaroRuntimeException("Request file-ids failed");
                }
            } catch (IOException e) {
                if (uploader.isCanceled()) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    boolean isFileExist(final Uploader uploader, final String bucketId, final String encryptedFileName) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> fu = isFileExistFuture(uploader, bucketId, encryptedFileName);
        if(uploader != null) {
            uploader.setFutureIsFileExists(fu);
        }
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    CompletableFuture<Frame> requestNewFrameFuture(final Uploader uploader) {
        return CompletableFuture.supplyAsync(() -> {
            verifyInit(true);
            String jsonStrBody = "{}";

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(JSON, jsonStrBody);
            String signature;
            try {
                signature = signRequest("POST", "/frames", jsonStrBody);
            } catch (Exception e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("requestNewFrame")
                    .url(bridgeUrl + "/frames")
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .post(body)
                    .build();

            OkHttpClient okHttpClient;
            if(uploader != null) {
                okHttpClient = uploader.getUpHttpClient();
            } else {
                okHttpClient = genaroHttpClient;
            }

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if (code == 429 || code == 420) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_RATE_ERROR));
                } else if (code == 200) {
                    Frame frame = new ObjectMapper().readValue(responseBody, Frame.class);
                    if (frame.getId() != null) {
                        return frame;
                    } else {
                        throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_FRAME_ERROR));
                    }
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_FRAME_ERROR));
                }
            } catch (IOException e) {
                if (uploader.isCanceled()) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    Frame requestNewFrame(final Uploader uploader) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Frame> fu = requestNewFrameFuture(uploader);
        if(uploader != null) {
            uploader.setFutureRequestNewFrame(fu);
        }
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    /**
     * @brief Download a file
     *
     * @param[in] bucketId The bucket id
     * @param[in] fileId The file id
     * @param[in] filePath The file path
     * @param[in] overwrite Whether to overwrite if exists
     * @param[in] callback The callback on progress or when complete
     * @return A Downloader.
     */
    public Downloader resolveFile(final String bucketId, final String fileId, final String filePath, final boolean overwrite, final ResolveFileCallback callback) {
        Downloader downloader = new Downloader(this, bucketId, fileId, filePath, overwrite, callback);
        CompletableFuture<Void> fu = CompletableFuture.runAsync(downloader);
        downloader.setFutureBelongsTo(fu);

        return downloader;
    }

    /**
     * @brief Upload a file
     *
     * @param[in] rs Whether to use Reed-Solomon to generate parity shards
     * @param[in] filePath The file path
     * @param[in] fileName The file name
     * @param[in] bucketId The bucket id
     * @param[in] callback The callback on progress or when complete
     * @return A Uploader.
     */
    public Uploader storeFile(final boolean rs, final String filePath, final String fileName, final String bucketId, final StoreFileCallback callback) {
        Uploader uploader = new Uploader(this, rs, filePath, fileName, bucketId, callback);
        CompletableFuture<Void> fu = CompletableFuture.runAsync(uploader);
        uploader.setFutureBelongsTo(fu);

        return uploader;
    }
}
