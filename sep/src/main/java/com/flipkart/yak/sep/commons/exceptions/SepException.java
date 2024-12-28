package com.flipkart.yak.sep.commons.exceptions;

public class SepException extends Exception {

    public SepException() {
    }

    public SepException(String message) {
        super(message);
    }

    public SepException(Throwable cause) {
        super(cause);
    }
}
