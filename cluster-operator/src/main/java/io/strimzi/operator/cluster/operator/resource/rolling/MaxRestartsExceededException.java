package io.strimzi.operator.cluster.operator.resource.rolling;

public class MaxRestartsExceededException extends RuntimeException {
    public MaxRestartsExceededException(String message) {
        super(message);
    }
}
