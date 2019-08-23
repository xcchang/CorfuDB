package org.corfudb.runtime.collections;

import lombok.Getter;
import org.corfudb.runtime.collections.CorfuTable.IndexRegistry;
import lombok.Builder;

/**
 * Created by zlokhandwala on 2019-08-09.
 */
@Builder
public class TableOptions<K, V> {
    private final IndexRegistry<K, V> indexRegistry;

    @Getter
    @Builder.Default
    private final String diskPath = "";
}
