package network.genaro.storage;

final class Parameters {
    public static final int GENARO_OKHTTP_CONNECT_TIMEOUT = 40;
    public static final int GENARO_OKHTTP_WRITE_TIMEOUT = 40;
    public static final int GENARO_OKHTTP_READ_TIMEOUT = 80;

    public static final int GENARO_HTTP_TIMEOUT = 40;

    // File transfer success
    public static final int GENARO_TRANSFER_OK = 0;
    public static final int GENARO_TRANSFER_CANCELED = 1;

    // Bridge related errors 1000 to 1999
    public static final int GENARO_BRIDGE_REQUEST_ERROR = 1000;
    public static final int GENARO_BRIDGE_AUTH_ERROR = 1001;
    public static final int GENARO_BRIDGE_TOKEN_ERROR = 1002;
    public static final int GENARO_BRIDGE_TIMEOUT_ERROR = 1003;
    public static final int GENARO_BRIDGE_INTERNAL_ERROR = 1004;
    public static final int GENARO_BRIDGE_RATE_ERROR = 1005;
    public static final int GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR = 1006;
    public static final int GENARO_BRIDGE_FILE_NOTFOUND_ERROR = 1007;
    public static final int GENARO_BRIDGE_JSON_ERROR = 1008;
    public static final int GENARO_BRIDGE_FRAME_ERROR = 1009;
    public static final int GENARO_BRIDGE_POINTER_ERROR = 1010;
    public static final int GENARO_BRIDGE_REPOINTER_ERROR = 1011;
    public static final int GENARO_BRIDGE_FILEINFO_ERROR = 1012;
    public static final int GENARO_BRIDGE_BUCKET_FILE_EXISTS = 1013;
    public static final int GENARO_BRIDGE_OFFER_ERROR = 1014;
    public static final int GENARO_BRIDGE_DECRYPTION_KEY_ERROR = 1015;

    // Farmer related errors 2000 to 2999
    public static final int GENARO_FARMER_REQUEST_ERROR = 2000;
    public static final int GENARO_FARMER_TIMEOUT_ERROR = 2001;
    public static final int GENARO_FARMER_AUTH_ERROR = 2002;
    public static final int GENARO_FARMER_EXHAUSTED_ERROR = 2003;
    public static final int GENARO_FARMER_INTEGRITY_ERROR = 2004;

    // File related errors 3000 to 3999
    public static final int GENARO_FILE_INTEGRITY_ERROR = 3000;
    public static final int GENARO_FILE_WRITE_ERROR = 3001;
    public static final int GENARO_FILE_ENCRYPTION_ERROR = 3002;
    public static final int GENARO_FILE_SIZE_ERROR = 3003;
    public static final int GENARO_FILE_DECRYPTION_ERROR = 3004;
    public static final int GENARO_FILE_GENERATE_HMAC_ERROR = 3005;
    public static final int GENARO_FILE_READ_ERROR = 3006;
    public static final int GENARO_FILE_SHARD_MISSING_ERROR = 3007;
    public static final int GENARO_FILE_RECOVER_ERROR = 3008;
    public static final int GENARO_FILE_RESIZE_ERROR = 3009;
    public static final int GENARO_FILE_UNSUPPORTED_ERASURE = 3010;
    public static final int GENARO_FILE_PARITY_ERROR = 3011;

    // algorithm error
    public static final int GENARO_ALGORITHM_ERROR = 4000;

    // unknown error
    public static final int GENARO_UNKNOWN_ERROR = 9000;

    // Exchange report codes
    public static final int GENARO_REPORT_SUCCESS = 1000;
    public static final int GENARO_REPORT_FAILURE = 1100;

    // Exchange report messages
    public static final String GENARO_REPORT_FAILED_INTEGRITY = "FAILED_INTEGRITY";
    public static final String GENARO_REPORT_SHARD_DOWNLOADED = "SHARD_DOWNLOADED";
    public static final String GENARO_REPORT_SHARD_UPLOADED = "SHARD_UPLOADED";
    public static final String GENARO_REPORT_DOWNLOAD_ERROR = "DOWNLOAD_ERROR";
    public static final String GENARO_REPORT_UPLOAD_ERROR = "TRANSFER_FAILED";
}
