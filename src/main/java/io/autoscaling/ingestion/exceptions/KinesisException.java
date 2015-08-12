package io.autoscaling.ingestion.exceptions;

/**
 * Created by saschamoellering on 06/08/15.
 */
public class KinesisException extends Exception {

    public KinesisException() {
        super();    //To change body of overridden methods use File | Settings | File Templates.
    }

    public KinesisException(String message) {
        super(message);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public KinesisException(String message, Throwable cause) {
        super(message, cause);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public KinesisException(Throwable cause) {
        super(cause);    //To change body of overridden methods use File | Settings | File Templates.
    }

    protected KinesisException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);    //To change body of overridden methods use File | Settings | File Templates.
    }
}
