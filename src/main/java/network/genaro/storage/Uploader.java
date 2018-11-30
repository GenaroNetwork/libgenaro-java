package network.genaro.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static javax.crypto.Cipher.ENCRYPT_MODE;

import org.xbill.DNS.utils.base16;

import static network.genaro.storage.CryptoUtil.*;
import static network.genaro.storage.Parameters.*;

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
//    int push_frame_request_count;
//    int push_shard_request_count;
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

    // the .crypt file
    private String cryptFilePath;
    private Genaro bridge;
    private String bucketId;
    private Progress progress;
    private File originFile;
    private boolean rs;

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

    public Uploader(final Genaro bridge, final boolean rs, final String filePath, final String fileName, final String bucketId, final Progress progress) {
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

    private static long determinShardSize(final long fileSize, int accumulator) {
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

        return determinShardSize(fileSize, ++accumulator);
    }

    private static byte[] randomBuff(int len) {
        byte[] buff = new byte[len];
        random.nextBytes(buff);
        return buff;
    }

    private String createTmpName(String encryptedFileName, String extension) {
        String tmpFolder = System.getProperty("java.io.tmpdir");
        byte[] bytesEncoded = sha256((string2Bytes(encryptedFileName)));

        return String.format("%s/%s%s", tmpFolder, base16.toString(bytesEncoded).toLowerCase(), extension);
    }

    public void start() /*throws Exception*/ {
        if(!Files.exists(Paths.get(this.originPath))) {
            System.out.println("File is not found!");
            return;
        }

        long shardSize = determinShardSize(originFile.length(), 0);
        long fileSize = originFile.length();

//        int totalDataShards = (int)(fileSize / shardSize + (fileSize % shardSize == 0 ? 0 : 1));
        int totalDataShards = (int)Math.ceil((double)fileSize / shardSize);
        int totalParityShards = rs ? (int)Math.ceil((double)totalDataShards * 2.0 / 3.0) : 0;
        int totalShards = totalDataShards + totalParityShards;

        // queue_verify_bucket_id
        try {
            bridge.getBucket(bucketId).get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            System.out.println("connect to bridge time out!");
            return;
        } catch (InterruptedException e) {
            System.out.println("InterruptedException!");
            return;
        } catch (ExecutionException e) {
//            System.out.println(e.getCause().getMessage());
            System.out.println("bucket not exists!");
            return;
        }

        // queue_verify_file_name
        encryptedFileName = CryptoUtil.encryptMetaHmacSha512(string2Bytes(fileName), bridge.getPrivateKey(), Hex.decode(bucketId));
        try {
            if (bridge.isFileExist(bucketId, encryptedFileName).get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS)) {
                System.out.println("file already exists!");
                return;
            }
        } catch (UnsupportedEncodingException e) {
            System.out.println("Unsupported Encoding Exception!");
            return;
        } catch (TimeoutException e) {
            System.out.println("connect to bridge time out!");
            return;
        } catch (InterruptedException e) {
            System.out.println("InterruptedException!");
            return;
        } catch (ExecutionException e) {
            System.out.println(e.getCause().getMessage());
            return;
        }

        // queue_create_encrypted_file
//        byte[] index = randomBuff(32);
        byte[] index = Hex.decode("1ffb37c2ac31231363a5996215e840ab75fc288f98ea77d9bee62b87f6e5852f");
        byte[] fileKey = CryptoUtil.generateFileKey(bridge.getPrivateKey(), Hex.decode(bucketId), index);

        byte[] ivBytes = Arrays.copyOf(index, 16);
        SecretKeySpec keySpec = new SecretKeySpec(fileKey, "AES");
        IvParameterSpec iv = new IvParameterSpec(ivBytes);

        javax.crypto.Cipher cipher;
        try {
            cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
            cipher.init(ENCRYPT_MODE, keySpec, iv);
        } catch (Exception e) {
            System.out.println("AES init Error!");
            return;
        }

        // create encrypted file
        try (InputStream in = new FileInputStream(this.originPath);
             InputStream cypherIn = new CipherInputStream(in, cipher)) {
            cryptFilePath = createTmpName(encryptedFileName, ".crypt");
            Files.copy(cypherIn, Paths.get(cryptFilePath), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.out.println("file read error or create encrypted file error!");
            return;
        }

        // queue_request_frame_id
        Frame frame;
        try {
            frame = bridge.requestNewFrame().get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            System.out.println("connect to bridge time out!");
            return;
        } catch (InterruptedException e) {
            System.out.println("InterruptedException!");
            return;
        } catch (ExecutionException e) {
            System.out.println(e.getCause().getMessage());
            return;
        }

        String frameId = frame.getId();

        List<ShardTracker> shards = new ArrayList<>(totalShards);
        for (int i = 0; i < totalShards; i++) {
            ShardTracker shardTracker = new ShardTracker();
            shardTracker.setIndex(i);
            shardTracker.setPointer(new FarmerPointer());
            shardTracker.setMeta(new ShardMeta(i));
            shardTracker.getMeta().setParity(i + 1 > totalDataShards);
            shardTracker.setUploadedSize(0);
            shards.add(shardTracker);
        }

        shards.parallelStream()
              .map(shard -> {
                    // prepare_frame
                    ShardMeta shardMeta = shard.getMeta();
                    shardMeta.setChallenges(new byte[GENARO_SHARD_CHALLENGES][]);
                    shardMeta.setChallengesAsStr(new String[GENARO_SHARD_CHALLENGES]);
                    for (int i = 0; i < GENARO_SHARD_CHALLENGES; i ++) {
//                        byte[] challenge = randomBuff(32);
                        byte[] challenge ={99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
                                99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
                                99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
                                99, 99};
                        shardMeta.getChallenges()[i] = challenge;
                        shardMeta.getChallengesAsStr()[i] = base16.toString(challenge).toLowerCase();
                    }

                    logger.info("Creating frame for shard index %d", shardMeta.getIndex());

                    // Initialize context for sha256 of encrypted data
                    MessageDigest shardHashMd = null;
                    try {
                        shardHashMd = MessageDigest.getInstance("SHA-256");
                    } catch (NoSuchAlgorithmException e) {
                        logger.error("NoSuchAlgorithmException");
                        System.exit(1);
                    }

                    // Calculate the merkle tree with challenges
                    MessageDigest[] firstSha256ForLeaf = new MessageDigest[GENARO_SHARD_CHALLENGES];
                    for (int i = 0; i < GENARO_SHARD_CHALLENGES; i++ ) {
                        try {
                            firstSha256ForLeaf[i] = MessageDigest.getInstance("SHA-256");
                        } catch (NoSuchAlgorithmException e) {
                            System.exit(1);
                        }
                        firstSha256ForLeaf[i].update(shardMeta.getChallenges()[i]);
                    }

                    // TODO: the shard's index is a bit different with shardMeta's index, need check.
                    if (shard.getIndex() + 1 > totalDataShards) {
                        // TODO: Reed-solomn is not implemented yet
                        shard.setShardFile("xxxxxx");
                    } else {
                        shard.setShardFile(this.cryptFilePath);
                    }

                    // TODO: the shard's index is a bit different with shardMeta's index, need check.
                    // Reset shard index when using parity shards
                    int shardMetaIndex = shardMeta.getIndex();
                    shardMetaIndex = (shardMetaIndex + 1 > totalDataShards) ? shardMetaIndex - totalDataShards: shardMetaIndex;
                    shardMeta.setIndex(shardMetaIndex);

                    try (FileInputStream fin = new FileInputStream(this.cryptFilePath)) {
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
                        logger.error("IOException");
                        System.exit(1);
                    } catch (Exception e) {
                        logger.error("Exception: " + e.getMessage());
                        System.exit(1);
                    }

                    byte[] prehash_sha256 = shardHashMd.digest();
                    byte[] prehash_ripemd160 = CryptoUtil.ripemd160(prehash_sha256);

                    shardMeta.setHash(base16.toString(prehash_ripemd160).toLowerCase());
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

                    return shard;
              })
              // push frame
              .map(shard -> {
//                  if(shard.getPointer().getToken() == null) {
//                      logger.error("Token error");
//                      System.exit(1);
//                  }

                  ShardMeta shardMeta = shard.getMeta();

                  // TODO: the shard's index is a bit different with shardMeta's index, need check.
                  logger.info("Pushing frame for shard index %d", shardMeta.getIndex());

                  boolean parityShard;

                  // TODO: the shard's index is a bit different with shardMeta's index, need check.
                  parityShard = shardMeta.getIndex() + 1 > totalDataShards;

                  String[] challengesAsStr = shardMeta.getChallengesAsStr();
                  String[] tree = shardMeta.getTree();

                  // TODO: exclude is empty for now
                  // TODO: the shard's index is a bit different with shardMeta's index, need check.
                  // TODO: when number of challenges is not 4, it doesn't work! need to modify
                  String jsonStrBody = String.format("{\"hash\": \"%s\", \"size\": %d, \"index\": %d, \"parity\": %b, " +
                                  "\"challenges\": [\"%s\", \"%s\", \"%s\", \"%s\"], \"tree\": [\"%s\", \"%s\", \"%s\", \"%s\"], \"exclude\": \"\"}",
                          shardMeta.getHash(), shardMeta.getSize(), shardMeta.getIndex(), parityShard,
                          challengesAsStr[0], challengesAsStr[1] ,challengesAsStr[2] ,challengesAsStr[3],
                          tree[0], tree[1], tree[2], tree[3]);

//                  String jsonStrBody;
//                  if(shardMeta.getIndex() == 1) {
//                      jsonStrBody = "{\"hash\": \"d68a98f5b9fef76e5c33c2baab04fc41a20f21c7\", \"size\": 1024, \"index\": 1, \"parity\": false, \"challenges\": [\"6363636363636363636363636363636363636363636363636363636363636363\",\"6363636363636363636363636363636363636363636363636363636363636363\",\"6363636363636363636363636363636363636363636363636363636363636363\",\"6363636363636363636363636363636363636363636363636363636363636363\"], \"tree\": [\"ab109cdd6243496fc9c77ab69037f1e6c054ad19\",\"ab109cdd6243496fc9c77ab69037f1e6c054ad19\",\"ab109cdd6243496fc9c77ab69037f1e6c054ad19\",\"ab109cdd6243496fc9c77ab69037f1e6c054ad19\"], \"exclude\": \"\"}";
//                  } else {
//                      jsonStrBody = "{\"hash\": \"d68a98f5b9fef76e5c33c2baab04fc41a20f21c7\", \"size\": 1024, \"index\": 0, \"parity\": false, \"challenges\": [\"6363636363636363636363636363636363636363636363636363636363636363\",\"6363636363636363636363636363636363636363636363636363636363636363\",\"6363636363636363636363636363636363636363636363636363636363636363\",\"6363636363636363636363636363636363636363636363636363636363636363\"], \"tree\": [\"ab109cdd6243496fc9c77ab69037f1e6c054ad19\",\"ab109cdd6243496fc9c77ab69037f1e6c054ad19\",\"ab109cdd6243496fc9c77ab69037f1e6c054ad19\",\"ab109cdd6243496fc9c77ab69037f1e6c054ad19\"], \"exclude\": \"\"}";
//                  }

                  MediaType JSON = MediaType.parse("application/json; charset=utf-8");
                  RequestBody body = RequestBody.create(JSON, jsonStrBody);
                  String signature = bridge.signRequest("PUT", "/frames/" + frameId, jsonStrBody);
                  String pubKey = bridge.getPublicKeyHexString();
                  Request request = new Request.Builder()
                          .url(bridge.getBridgeUrl() + "/frames/" + frameId)
                          .header("x-signature", signature)
                          .header("x-pubkey", pubKey)
                          .put(body)
                          .build();

                  logger.info("fn[push_frame] - JSON body: %s", jsonStrBody);
                  try (Response response = client.newCall(request).execute()) {
                      int code = response.code();

                      if (code == 429 || code == 420) {
                          throw new GenaroRuntimeException(Genaro.GenaroStrError(GENARO_BRIDGE_RATE_ERROR));
                      } else if (code != 200 && code != 201) {
                          throw new GenaroRuntimeException(Genaro.GenaroStrError(GENARO_BRIDGE_OFFER_ERROR));
                      }

                      ObjectMapper om = new ObjectMapper();
                      String responseBody = response.body().string();
                      FarmerPointer fp = om.readValue(responseBody, FarmerPointer.class);
                      shard.setPointer(fp);

                  } catch (Exception e) {
                      logger.error(e.getMessage());
                      System.exit(1);
                  }

                  return shard;
              })
              // push_shard
              .map(shard -> {
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
                  logger.info("Transferring Shard index %d...", shardMeta.getIndex());

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

//                  MultipartBody.Builder builder = new MultipartBody.Builder()
//                          .setType(MultipartBody.FORM);
//
//                  File shardFile = new File(shardFileStr);
//                  final byte[] mBlock = FileUtils.getBlock(filePosition, shardFile, (int)metaSize);
////                  RequestBody requestBody = RequestBody.create(MediaType.parse("application/octet-stream; charset=utf-8"), mBlock);
//                  RequestBody requestBody = RequestBody.create(MediaType.parse("multipart/form-data; charset=utf-8"), mBlock);
//
//                  // TODO: what's the meaning of name, filename?
//                  builder.addFormDataPart("abc", "def", requestBody);

//                  String url = String.format("http://%s:%s/shards/%s?token=%s", farmerAddress, farmerPort, metaHash, token);
//                  Request request = new Request.Builder()
//                          .url(url)
//                          .header("x-storj-node-id", farmerNodeId)
//                          .post(builder.build())
//                          .build();


                  File shardFile = new File(shardFileStr);
                  final byte[] mBlock;
                  try {
                      mBlock = FileUtils.getBlock(filePosition, shardFile, (int) metaSize);
                  } catch (IOException e) {
                      throw new GenaroRuntimeException(Genaro.GenaroStrError(GENARO_FILE_READ_ERROR));
                  }

                  if(mBlock == null) {
                      throw new GenaroRuntimeException(Genaro.GenaroStrError(GENARO_FILE_READ_ERROR));
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
                          logger.info("Successfully transferred shard index %d", shardMeta.getIndex());
                      } else {
                          // TODO: the shard's index is a bit different with shardMeta's index, need check.
                          logger.error("Failed to push shard %d", shardMeta.getIndex());
                          throw new GenaroRuntimeException(Genaro.GenaroStrError(GENARO_FARMER_REQUEST_ERROR));
                      }
                  } catch (Exception e) {
                      logger.error(e.getMessage());
                      System.exit(1);
                  }

                  return shard;
              })
              .collect(Collectors.toList());

        // queue_create_bucket_entry

        // send report
    }
}
