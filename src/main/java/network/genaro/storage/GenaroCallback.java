package network.genaro.storage;

public interface GenaroCallback {
    interface GetBucketsCallback {
        default void onFinish(Bucket[] buckets) { }
        default void onFail(String error) { }
    }

    interface DeleteBucketCallback {
        default void onFinish() { }
        default void onFail(String error) { }
    }

    interface RenameBucketCallback {
        default void onFinish() { }
        default void onFail(String error) { }
    }

    interface ListFilesCallback {
        default void onFinish(GenaroFile[] files) { }
        default void onFail(String error) { }
    }

    interface DeleteFileCallback {
        default void onFinish() { }
        default void onFail(String error) { }
    }

    interface ListMirrorsCallback {
        default void onFinish(String text) { }
        default void onFail(String error) { }
    }

    interface ResolveFileCallback {
        default void onBegin() { }

        /**
         * called when download finish
         *
         * @param fileBytes the file size
         * @param sha256 sha256 of the downloaded file
         */
        default void onFinish(long fileBytes, byte[] sha256) { }

        default void onFail(String error) { }

        default void onCancel() { }

        /**
         * called when progress update
         *
         * @param progress range from 0 to 1
         */
        default void onProgress(float progress) { }
    }

    interface StoreFileCallback {
        default void onBegin(long fileSize) { }

        /**
         * called when upload finish
         *
         * @param fileId file id
         * @param sha256OfEncrypted sha256 of the encrypted file(not include the parity shards)
         */
        default void onFinish(String fileId, byte[] sha256OfEncrypted) { }

        default void onFail(String error) { }

        default void onCancel() { }

        /**
         * called when progress update
         *
         * @param progress range from 0 to 1
         */
        default void onProgress(float progress) { }
    }
}
