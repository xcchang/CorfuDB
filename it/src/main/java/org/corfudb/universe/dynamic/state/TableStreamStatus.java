package org.corfudb.universe.dynamic.state;

import lombok.Getter;

/**
 * Possible status of a table stream in corfu.
 *
 * Created by edilmo on 11/06/18.
 */
public enum TableStreamStatus {
    /**
     * The data retrieved from corfu is equal to the data expected.
     */
    CORRECT(0),

    /**
     * The data retrieved from corfu for one snapshot is not equal to the data expected.
     */
    PARTIALLY_CORRECT(1),

    /**
     * The data retrieved from corfu is not equal to the data expected.
     */
    INCORRECT(2),

    /**
     * The data is not available.
     */
    UNAVAILABLE(3);

    @Getter
    final int statusValue;

    TableStreamStatus(int statusValue) {
        this.statusValue = statusValue;
    }
}
