package org.corfudb.infrastructure;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.runtime.view.Layout;

/**
 * Phase2 data consists of rank and the proposed layout.
 * The container class provides a convenience to persist and retrieve
 * these two pieces of data together.
 */
@Getter
@ToString
@AllArgsConstructor
public class Phase2Data {
    private final Rank rank;
    private final Layout layout;
}