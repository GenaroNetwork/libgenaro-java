package network.genaro.storage;

public interface GenaroCallback {
    interface GetBucketsCallback {
        void onFinish(Bucket[] buckets);
        void onFail(String error);
    }

    interface DeleteBucketCallback {
        void onFinish();
        void onFail(String error);
    }

    interface RenameBucketCallback {
        void onFinish();
        void onFail(String error);
    }

    interface ListFilesCallback {
        void onFinish(GenaroFile[] files);
        void onFail(String error);
    }

    interface DeleteFileCallback {
        void onFinish();
        void onFail(String error);
    }

    interface ResolveFileCallback {
        default void onBegin() {
            //        System.out.println("Download started");
        }

        default void onFinish() {
            //        System.out.println("Download finished");
        }

        default void onFail(String error) {
            //        System.out.println("Download failed, reason: " + error != null ? error : "Unknown");
        }

        // after Uploader::cancel is called
        default void onCancel() { }

        /**
         * called when progress update
         *
         * @param progress range from 0 to 1
         */
        default void onProgress(float progress) { }
    }

    interface StoreFileCallback {
        default void onBegin(long fileSize) {
            //        System.out.println("Upload started");
        }

        default void onFinish(String fileId) {
            //        System.out.println("Upload finished, fileId: " + fileId);
        }

        default void onFail(String error) {
            //        System.out.println("Upload failed, reason: " + error != null ? error : "Unknown");
        }

        // after Uploader::cancel is called
        default void onCancel() { }

        /**
         * called when progress update
         *
         * @param progress range from 0 to 1
         */
        default void onProgress(float progress) { }
    }
}
