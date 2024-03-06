/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

class UnrestartableNodesException extends RuntimeException {

    /**
     * This exception indicates that a node cannot be attempted to restart which could be
     * because of not satisfying the safety conditions or the maximum number of retry attempt has been reached.
     * @param message
     */
    public UnrestartableNodesException(String message) {
        super(message);
    }
}