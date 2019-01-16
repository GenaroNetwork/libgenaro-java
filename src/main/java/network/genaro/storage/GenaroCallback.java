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

        default void onFinish() { }

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

        default void onFinish(String fileId) { }

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
