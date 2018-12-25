package network.genaro.storage;

import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.internal.Util;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;

import java.io.IOException;
import java.io.InputStream;

final class UploadRequestBody extends RequestBody {
    public interface ProgressListener {
        void transferred(long delta);
    }

    private static final int SEGMENT_SIZE = 2 * 1024;

    private InputStream input;
    private ProgressListener listener;
    private String contentType;

    public UploadRequestBody(InputStream input, String contentType, ProgressListener listener) {
        this.input = input;
        this.contentType = contentType;
        this.listener = listener;
    }

    @Override
    public long contentLength() throws IOException {
        long length = input.available();
        return length;
    }

    @Override
    public MediaType contentType() {
        return MediaType.parse(contentType);
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        Source source = null;
        try {
            source = Okio.source(input);
            long delta;

            while ((delta = source.read(sink.buffer(), SEGMENT_SIZE)) != -1) {
                sink.flush();
                if(listener != null) {
                    listener.transferred(delta);
                }
            }
        } finally {
            Util.closeQuietly(source);
        }
    }
}
