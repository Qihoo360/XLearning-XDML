package net.qihoo.xitong.xdml.utils;

public class XDMLException extends RuntimeException{
    private static final long serialVersionUID = 1L;

    public XDMLException() {
    }

    public XDMLException(String message) {
        super(message);
    }

    public XDMLException(String message, Throwable cause) {
        super(message, cause);
    }

    public XDMLException(Throwable cause) {
        super(cause);
    }
}
