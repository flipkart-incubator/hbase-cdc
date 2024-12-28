package com.flipkart.yak.sep.commons.exceptions;

public class SepRuntimeException extends RuntimeException {
    public SepRuntimeException() {
    }

    public SepRuntimeException(String message) {
        super(message);
    }

    public SepRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public SepRuntimeException(Throwable cause) {
        super(cause);
    }
}
