package network.genaro.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.bouncycastle.util.encoders.Hex;

import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static javax.crypto.Cipher.ENCRYPT_MODE;

import org.xbill.DNS.utils.base16;

import static network.genaro.storage.CryptoUtil.*;
import static network.genaro.storage.Parameters.*;

import javax.crypto.Mac;

import static network.genaro.storage.Genaro.GenaroStrError;

interface UploadProgress {
    default void onBegin(long fileSize) { System.out.println("Upload started"); }

    default void onFinish(String error, String fileId) {
        if(error != null) {
            System.out.println("Upload failed: " + error);
        } else {
            System.out.println("Upload finished, fileId: " + fileId);
        }
    }

    /**
     * called when progress update
     * @param progress range from 0 to 1
     */
    default void onProgress(float progress) { }
}

class ShardMeta {
    private String hash;
    private byte[][] challenges;    // [GENARO_SHARD_CHALLENGES][32]
    private String[] challengesAsStr;  // [GENARO_SHARD_CHALLENGES]

    // Merkle Tree leaves. Each leaf is size of RIPEMD160 hash
    private String[] tree;  // [GENARO_SHARD_CHALLENGES]
    private int index;
    private boolean isParity;
    private long size;

    public ShardMeta(int index) { this.setIndex(index); }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public byte[][] getChallenges() { return challenges; }

    public void setChallenges(byte[][] challenges) {
        this.challenges = challenges;
    }

    public String[] getChallengesAsStr() {
        return challengesAsStr;
    }

    public void setChallengesAsStr(String[] challengesAsStr) {
        this.challengesAsStr = challengesAsStr;
    }

    public String[] getTree() {
        return tree;
    }

    public void setTree(String[] tree) {
        this.tree = tree;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean getParity() {
        return isParity;
    }

    public void setParity(boolean parity) {
        isParity = parity;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class FarmerPointer {
    private String token;
    private Farmer farmer;

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Farmer getFarmer() {
        return farmer;
    }

    public void setFarmer(Farmer farmer) {
        this.farmer = farmer;
    }
}

class ShardTracker {
//    int progress;
    private int index;
    private FarmerPointer pointer;
    private ShardMeta meta;
    private long uploadedSize;
    private String shardFile;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public FarmerPointer getPointer() {
        return pointer;
    }

    public void setPointer(FarmerPointer pointer) {
        this.pointer = pointer;
    }

    public ShardMeta getMeta() {
        return meta;
    }

    public void setMeta(ShardMeta meta) {
        this.meta = meta;
    }

    public long getUploadedSize() {
        return uploadedSize;
    }

    public void setUploadedSize(long uploadedSize) {
        this.uploadedSize = uploadedSize;
    }

    public String getShardFile() { return shardFile; }

    public void setShardFile(String shardFile) { this.shardFile = shardFile; }
}

public class Uploader {
    private static final Logger logger = LogManager.getLogger(Genaro.class);

    private String originPath;
    private String fileName;
    private String encryptedFileName;

    // the .crypt file path
    private String cryptFilePath;

    private Genaro bridge;
    private String bucketId;
    private UploadProgress progress;
    private File originFile;
    private boolean rs;

    private int totalDataShards;
    private int totalParityShards;
    private int totalShards;

    private long shardSize;

    private String frameId;

    private byte[] index;
    private byte[] fileKey;

    private String hmacId;

    private int errorStatus;

    private String fileId;

    private static Random random = new Random();
    private static long MAX_SHARD_SIZE = 4294967296L; // 4Gb
    private static long MIN_SHARD_SIZE = 2097152L; // 2Mb
    private static int SHARD_MULTIPLES_BACK = 4;
    private static int GENARO_SHARD_CHALLENGES = 4;

    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(GENARO_OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
            .writeTimeout(GENARO_OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
            .readTimeout(GENARO_OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS)
            .build();

    public Uploader(final Genaro bridge, final boolean rs, final String filePath, final String fileName, final String bucketId, final UploadProgress progress) {
        this.bridge = bridge;
        this.originPath = filePath;
        this.fileName = fileName;
        this.originFile = new File(filePath);
        this.bucketId = bucketId;
        this.progress = progress;
    }

    private static long shardSize(final int hops) {
        return (long) (MIN_SHARD_SIZE * Math.pow(2, hops));
    }

    private static long determineShardSize(final long fileSize, int accumulator) {
        if (fileSize <= 0) {
            return 0;
        }

        accumulator = accumulator > 0 ? accumulator : 0;

        // Determine hops back by accumulator
        int hops = ((accumulator - SHARD_MULTIPLES_BACK) < 0 ) ? 0 : accumulator - SHARD_MULTIPLES_BACK;

        long byteMultiple = shardSize(accumulator);
        double check = (double) fileSize / byteMultiple;

        // Determine if bytemultiple is highest bytemultiple that is still <= size
        if (check > 0 && check <= 1) {
            while (hops > 0 && shardSize(hops) > MAX_SHARD_SIZE) {
                hops = hops - 1 <= 0 ? 0 : hops - 1;
            }
            return shardSize(hops);
        }

        // Maximum of 2 ^ 41 * 8 * 1024 * 1024
        if (accumulator > 41) {
            return 0;
        }

        return determineShardSize(fileSize, ++accumulator);
    }

    private static byte[] randomBuff(int len) {
        byte[] buff = new byte[len];
        random.nextBytes(buff);
        return buff;
    }

    private String createTmpName(String encryptedFileName, String extension) {
        String tmpFolder = System.getProperty("java.io.tmpdir");
        byte[] bytesEncoded = sha256((BasicUtil.string2Bytes(encryptedFileName)));

        return String.format("%s/%s%s", tmpFolder, base16.toString(bytesEncoded).toLowerCase(), extension);
    }

    private String getBucketEntryHmac(byte[] fileKey, List<ShardTracker> shards) throws NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec secretKeySpec = new SecretKeySpec(fileKey, "HmacSHA512");
        Mac mac = Mac.getInstance("HmacSHA512");
        mac.init(secretKeySpec);

        for(ShardTracker shard: shards) {
            byte[] hash = base16.fromString(shard.getMeta().getHash());
            mac.update(hash);
        }

        byte[] digestRaw = mac.doFinal();
        return base16.toString(digestRaw).toLowerCase();
    }

    private boolean createEncryptedFile() {
        index = randomBuff(32);
//        byte[] index = Hex.decode("1ffb37c2ac31231363a5996215e840ab75fc288f98ea77d9bee62b87f6e5852f");
        fileKey = CryptoUtil.generateFileKey(bridge.getPrivateKey(), Hex.decode(bucketId), index);

        byte[] ivBytes = Arrays.copyOf(index, 16);
        SecretKeySpec keySpec = new SecretKeySpec(fileKey, "AES");
        IvParameterSpec iv = new IvParameterSpec(ivBytes);

        javax.crypto.Cipher cipher;
        try {
            cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
            cipher.init(ENCRYPT_MODE, keySpec, iv);
        } catch (Exception e) {
            System.out.println("AES init Error!");
            return false;
        }

        try (InputStream in = new FileInputStream(originPath);
             InputStream cypherIn = new CipherInputStream(in, cipher)) {
            cryptFilePath = createTmpName(encryptedFileName, ".crypt");
            Files.copy(cypherIn, Paths.get(cryptFilePath), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.out.println("file read error or create encrypted file error!");
            return false;
        }

        return true;
    }

    private ShardTracker prepareFrame(ShardTracker shard) {
        ShardMeta shardMeta = shard.getMeta();
        shardMeta.setChallenges(new byte[GENARO_SHARD_CHALLENGES][]);
        shardMeta.setChallengesAsStr(new String[GENARO_SHARD_CHALLENGES]);
        for (int i = 0; i < GENARO_SHARD_CHALLENGES; i ++) {
            byte[] challenge = randomBuff(32);
//                        byte[] challenge ={99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
//                                99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
//                                99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
//                                99, 99};
            shardMeta.getChallenges()[i] = challenge;
            shardMeta.getChallengesAsStr()[i] = base16.toString(challenge).toLowerCase();
        }

        logger.info(String.format("Creating frame for shard index %d...", shardMeta.getIndex()));

        // Initialize context for sha256 of encrypted data
        MessageDigest shardHashMd;
        try {
            shardHashMd = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new GenaroRuntimeException(e.getMessage());
        }

        // Calculate the merkle tree with challenges
        MessageDigest[] firstSha256ForLeaf = new MessageDigest[GENARO_SHARD_CHALLENGES];
        for (int i = 0; i < GENARO_SHARD_CHALLENGES; i++ ) {
            try {
                firstSha256ForLeaf[i] = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                progress.onFinish(e.getMessage(), null);
                System.exit(1);
            }
            firstSha256ForLeaf[i].update(shardMeta.getChallenges()[i]);
        }

        // TODO: the shard's index is a bit different with shardMeta's index, need check.
        if (shard.getIndex() + 1 > totalDataShards) {
            // TODO: Reed-solomn is not implemented yet
            shard.setShardFile("xxxxxx");
        } else {
            shard.setShardFile(cryptFilePath);
        }

        // TODO: the shard's index is a bit different with shardMeta's index, need check.
        // Reset shard index when using parity shards
        int shardMetaIndex = shardMeta.getIndex();
        shardMetaIndex = (shardMetaIndex + 1 > totalDataShards) ? shardMetaIndex - totalDataShards: shardMetaIndex;
        shardMeta.setIndex(shardMetaIndex);

        try (FileInputStream fin = new FileInputStream(cryptFilePath)) {
            int readBytes;
            final int BYTES = AES_BLOCK_SIZE * 256;

            long totalRead = 0;

            // TODO: the shard's index is a bit different with shardMeta's index, need check.
            long position = shardMeta.getIndex() * shardSize;

            byte[] readData = new byte[BYTES];
            do {
                // set file position
                fin.getChannel().position(position);

                readBytes = fin.read(readData, 0, BYTES);

                // end of file
                if(readBytes == -1) {
                    break;
                }

                totalRead += readBytes;
                position += readBytes;

                shardHashMd.update(readData, 0, readBytes);

                for (int i = 0; i < GENARO_SHARD_CHALLENGES; i++ ) {
                    firstSha256ForLeaf[i].update(readData, 0, readBytes);
                }
            } while (totalRead < shardSize);

            shardMeta.setSize(totalRead);
        } catch (IOException e) {
            throw new GenaroRuntimeException(e.getMessage());
        }

        byte[] prehashSha256 = shardHashMd.digest();
        byte[] prehashRipemd160 = CryptoUtil.ripemd160(prehashSha256);

        shardMeta.setHash(base16.toString(prehashRipemd160).toLowerCase());
        byte[] preleafSha256;
        byte[] preleafRipemd160;

        shardMeta.setTree(new String[GENARO_SHARD_CHALLENGES]);
        for (int i = 0; i < GENARO_SHARD_CHALLENGES; i++) {
            // finish first sha256 for leaf
            preleafSha256 = firstSha256ForLeaf[i].digest();

            // ripemd160 result of sha256
            preleafRipemd160 = CryptoUtil.ripemd160(preleafSha256);

            // sha256 and ripemd160 again
            shardMeta.getTree()[i] = CryptoUtil.ripemd160Sha256HexString(preleafRipemd160);
        }

        logger.info(String.format("Finished create frame for shard index %d", shardMeta.getIndex()));

        return shard;
    }

    private ShardTracker pushFrame(ShardTracker shard) {
        //                  if(shard.getPointer().getToken() == null) {
//                      logger.error("Token error");
//                      System.exit(1);
//                  }

        ShardMeta shardMeta = shard.getMeta();

        boolean parityShard;

        // TODO: the shard's index is a bit different with shardMeta's index, need check.
        parityShard = shardMeta.getIndex() + 1 > totalDataShards;

        String[] challengesAsStr = shardMeta.getChallengesAsStr();
        String[] tree = shardMeta.getTree();

        // TODO: exclude is empty for now
        // TODO: the shard's index is a bit different with shardMeta's index, need check.
        ObjectMapper om = new ObjectMapper();
        String challengesJsonStr = null, treeJsonStr = null;
        try {
            challengesJsonStr = om.writeValueAsString(challengesAsStr);
            treeJsonStr = om.writeValueAsString(tree);
        } catch (JsonProcessingException e) {
            throw new GenaroRuntimeException(e.getMessage());
        }
        String jsonStrBody = String.format("{\"hash\":\"%s\",\"size\":%d,\"index\":%d,\"parity\":%b," +
                        "\"challenges\":%s,\"tree\":%s,\"exclude\":[]}",
                shardMeta.getHash(), shardMeta.getSize(), shardMeta.getIndex(), parityShard, challengesJsonStr, treeJsonStr);

        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(JSON, jsonStrBody);
        String path = "/frames/" + frameId;
        String signature = bridge.signRequest("PUT", path, jsonStrBody);
        String pubKey = bridge.getPublicKeyHexString();
        Request request = new Request.Builder()
                .url(bridge.getBridgeUrl() + path)
                .header("x-signature", signature)
                .header("x-pubkey", pubKey)
                .put(body)
                .build();

        // TODO: the shard's index is a bit different with shardMeta's index, need check.
        logger.info(String.format("Pushing frame for shard index %s - JSON body: %s", shardMeta.getIndex(), jsonStrBody));
        try (Response response = client.newCall(request).execute()) {
            String responseBody = response.body().string();

            // TODO: the shard's index is a bit different with shardMeta's index, need check.
            logger.info(String.format("Push frame finished for shard index %s - JSON Response: %s", shardMeta.getIndex(), responseBody));

            int code = response.code();

            if (code == 429 || code == 420) {
                throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_RATE_ERROR));
            } else if (code != 200 && code != 201) {
                throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_OFFER_ERROR));
            }

            FarmerPointer fp = om.readValue(responseBody, FarmerPointer.class);
            shard.setPointer(fp);
        } catch (Exception e) {
            throw new GenaroRuntimeException(e.getMessage());
        }

        return shard;
    }

    private void pushShard(ShardTracker shard) {
        //                  // Reset shard index when using parity shards
//                  req->shard_index = (index + 1 > state->total_data_shards) ? index - state->total_data_shards: index;
//
//                  // make sure we switch between parity and data shards files.
//                  // When using Reed solomon must also read from encrypted file
//                  // rather than the original file for the data
//                  if (index + 1 > state->total_data_shards) {
//                      req->shard_file = state->parity_file;
//                  } else if (state->rs) {
//                      req->shard_file = state->encrypted_file;
//                  } else {
//                      req->shard_file = state->original_file;
//                  }

        ShardMeta shardMeta = shard.getMeta();

        // TODO: progress_put_shard


        // TODO: the shard's index is a bit different with shardMeta's index, need check.
        logger.info(String.format("Transferring Shard index %d...", shardMeta.getIndex()));

        Farmer farmer = shard.getPointer().getFarmer();
        String farmerNodeId = farmer.getNodeID();
        String farmerAddress = farmer.getAddress();
        String farmerPort = farmer.getPort();
        String metaHash = shard.getMeta().getHash();
        long metaSize = shard.getMeta().getSize();
        String shardFileStr = shard.getShardFile();

        // TODO: shard.getIndex() or shard.getMeta().getIndex()
        long filePosition = shard.getIndex() * shardSize;
        String token = shard.getPointer().getToken();

        File shardFile = new File(shardFileStr);
        final byte[] mBlock;
        try {
            mBlock = FileUtils.getBlock(filePosition, shardFile, (int)metaSize);
        } catch (IOException e) {
            throw new GenaroRuntimeException(GenaroStrError(GENARO_FILE_READ_ERROR));
        }

        if(mBlock == null) {
            throw new GenaroRuntimeException(GenaroStrError(GENARO_FILE_READ_ERROR));
        }

        RequestBody requestBody = RequestBody.create(MediaType.parse("application/octet-stream; charset=utf-8"), mBlock);

        String url = String.format("http://%s:%s/shards/%s?token=%s", farmerAddress, farmerPort, metaHash, token);
        Request request = new Request.Builder()
                .url(url)
                .header("x-storj-node-id", farmerNodeId)
                .post(requestBody)
                .build();

        try (Response response = client.newCall(request).execute()) {
            int code = response.code();

            if (code == 200 || code == 201 || code == 304) {
                logger.info(String.format("Successfully transferred shard index %d", shardMeta.getIndex()));
            } else {
                // TODO: the shard's index is a bit different with shardMeta's index, need check.
                logger.error(String.format("Failed to push shard %d", shardMeta.getIndex()));
                throw new GenaroRuntimeException(GenaroStrError(GENARO_FARMER_REQUEST_ERROR));
            }
        } catch (Exception e) {
            throw new GenaroRuntimeException(e.getMessage());
        }
    }

    private void createBucketEntry(List<ShardTracker> shards) {
        try {
            hmacId = getBucketEntryHmac(fileKey, shards);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new GenaroRuntimeException(GenaroStrError(GENARO_FILE_GENERATE_HMAC_ERROR));
        }

        logger.info(String.format("[%s] Creating bucket entry... ", fileName));

        String jsonStrBody;
        if (!rs) {
            jsonStrBody = String.format("{\"frame\": \"%s\", \"filename\": \"%s\", \"index\": \"%s\", \"hmac\": {\"type\": \"sha512\", \"value\": \"%s\"}}",
                    frameId, encryptedFileName, Hex.toHexString(index), hmacId);
        } else {
            // TODO: ReedSolomn is not completed.
            jsonStrBody = "";
            //                  if (state->rs) {
            //                      struct json_object *erasure = json_object_new_object();
            //                      json_object *erasure_type = json_object_new_string("reedsolomon");
            //                      json_object_object_add(erasure, "type", erasure_type);
            //                      json_object_object_add(body, "erasure", erasure);
            //                  }
        }

        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(JSON, jsonStrBody);
        String path = "/buckets/" + bucketId + "/files";
        String signature = bridge.signRequest("POST", path, jsonStrBody);
        String pubKey = bridge.getPublicKeyHexString();
        Request request = new Request.Builder()
                .url(bridge.getBridgeUrl() + path)
                .header("x-signature", signature)
                .header("x-pubkey", pubKey)
                .post(body)
                .build();

        logger.info(String.format("Create bucket entry - JSON body: %s", jsonStrBody));
        try (Response response = client.newCall(request).execute()) {
            ObjectMapper om = new ObjectMapper();
            String responseBody = response.body().string();
            JsonNode bodyNode = om.readTree(responseBody);

            // TODO: the shard's index is a bit different with shardMeta's index, need check.
            logger.info(String.format("Create bucket entry - JSON Response: %s", responseBody));

            int code = response.code();

            if (code != 200 && code != 201) {
                String error = bodyNode.get("error").asText();
                logger.error(error);
                throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
            }

            logger.info("Successfully Added bucket entry");

            fileId = bodyNode.get("id").asText();
        } catch (Exception e) {
            throw new GenaroRuntimeException(e.getMessage());
        }
    }

    public void start() /*throws Exception*/ {
        if(!Files.exists(Paths.get(originPath))) {
            progress.onFinish("File not found!", null);
            return;
        }

        // calculate shard size and count
        long fileSize = originFile.length();
        shardSize = determineShardSize(fileSize, 0);
        if(shardSize <= 0) {
            errorStatus = GENARO_FILE_SIZE_ERROR;
            progress.onFinish(GenaroStrError(errorStatus), null);
            return;
        }

        progress.onBegin(fileSize);

        totalDataShards = (int)Math.ceil((double)fileSize / shardSize);
        totalParityShards = rs ? (int)Math.ceil((double)totalDataShards * 2.0 / 3.0) : 0;
        totalShards = totalDataShards + totalParityShards;

        // verify bucket id
        try {
            bridge.getBucket(bucketId);
        } catch (Exception e) {
//            errorStatus = GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR;
            progress.onFinish(e.getCause().getMessage(), null);
            return;
        }

        // verify file name
        encryptedFileName = CryptoUtil.encryptMetaHmacSha512(BasicUtil.string2Bytes(fileName), bridge.getPrivateKey(), Hex.decode(bucketId));
        boolean exist;
        try {
            exist = bridge.isFileExist(bucketId, encryptedFileName);
        } catch (Exception e) {
            progress.onFinish(e.getCause().getMessage(), null);
            return;
        }
        if (exist) {
            progress.onFinish("file already exists!", null);
            return;
        }

        if(!createEncryptedFile()) {
            progress.onFinish("create encrypted file error!", null);
            return;
        }

        // request frame id
        logger.info("Request frame id");
        Frame frame;
        try {
            frame = bridge.requestNewFrame();
        } catch (Exception e) {
            progress.onFinish(e.getCause().getMessage(), null);
            return;
        }
        frameId = frame.getId();

        logger.info(String.format("Request frame id success, frame id: %s", frameId));

        List<ShardTracker> shards = new ArrayList<>(totalShards);
        for (int i = 0; i < totalShards; i++) {
            ShardTracker shardTracker = new ShardTracker();
            shardTracker.setIndex(i);
            shardTracker.setPointer(new FarmerPointer());
            shardTracker.setMeta(new ShardMeta(i));
            shardTracker.getMeta().setParity(i + 1 > totalDataShards);
            //TODO: no effect yet
            shardTracker.setUploadedSize(0);
            shards.add(shardTracker);
        }

        // TODO: not here
        progress.onProgress(0.0f);

        try {
            shards.parallelStream()
                  .map(this::prepareFrame)
                  .map(this::pushFrame)
                  .forEach(this::pushShard);
        } catch (Exception e) {
            progress.onFinish(e.getCause().getMessage(), null);
            return;
        }

        try {
            createBucketEntry(shards);
        } catch (Exception e) {
            progress.onFinish(e.getCause().getMessage(), null);
            return;
        }

        progress.onProgress(1.0f);
        progress.onFinish(null, fileId);

        // send exchange report
        //
    }
}
