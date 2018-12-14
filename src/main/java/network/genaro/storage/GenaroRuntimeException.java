package network.genaro.storage;

class GenaroRuntimeException extends RuntimeException {
    public GenaroRuntimeException(){
        super();
    }

    public GenaroRuntimeException(String message){
        super(message);
    }

//    public GenaroRuntimeException(String message, Throwable cause){
//        super(message,cause);
//    }
//
//    public GenaroRuntimeException(Throwable cause) {
//        super(cause);
//    }
}
