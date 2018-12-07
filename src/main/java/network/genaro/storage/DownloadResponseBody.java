package network.genaro.storage;

import okhttp3.MediaType;
import okhttp3.ResponseBody;
import okio.*;

import java.io.IOException;

public class DownloadResponseBody extends ResponseBody {

    public interface ProgressListener {
        void transferred(long delta);
    }

    private ResponseBody responseBody;
    private ProgressListener listener;
    private BufferedSource bufferedSource;

    public DownloadResponseBody(ResponseBody responseBody, ProgressListener listener){
        this.responseBody = responseBody;
        this.listener = listener;
    }

    @Override
    public MediaType contentType() {
        return responseBody.contentType();
    }

    @Override
    public long contentLength() {
        return responseBody.contentLength();
    }

    @Override public BufferedSource source() {
        if (bufferedSource == null) {
            bufferedSource = Okio.buffer(source(responseBody.source()));
        }

        return bufferedSource;
    }

    private Source source(Source source) {
        return new ForwardingSource(source) {
            @Override public long read(Buffer sink, long byteCount) throws IOException {
                // read() returns the number of bytes read, or -1 if this source is exhausted.
                long delta = super.read(sink, byteCount);

                if (listener != null && delta != -1) {
                    listener.transferred(delta);
                }

                return delta;
            }
        };
    }
}
