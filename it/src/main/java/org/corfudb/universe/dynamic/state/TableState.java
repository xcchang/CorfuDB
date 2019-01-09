package org.corfudb.universe.dynamic.state;

import lombok.Data;
import lombok.ToString;
import org.corfudb.universe.dynamic.Stats;
import org.corfudb.universe.dynamic.events.CorfuTableDataGenerationFunction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the state of a table stream in corfu.
 *
 * The state includes all the data put in the table stream during
 * the whole history.
 * In order to have a efficient representation of the data that allows to test
 * with a payload arbitrary large, no data at all is saved, just
 * a instance of a class that describe how to produce the data.
 *
 * Created by edilmo on 11/06/18.
 */
@ToString
@Data
public class TableState implements Cloneable {

    /**
     * Name of the table stream in corfu.
     */
    private final String tableStreamName;

    /**
     * Concrete status of the stream after a GetData event.
     * The idea is simple:
     *      - Put data: always forced this status to CORRECT.
     *      - Get data: mark the status as INCORRECT if and only if
     *                  the retrieved data is not equal to the data
     *                  in this state.
     *      - Get data snapshot: mark the status as PARTIALLY_CORRECT if and only if
     *                           the retrieved snapshot data is not equal to
     *                           the data in this state.
     *      - All: mark the status as UNAVAILABLE if the operation failed.
     */
    private TableStreamStatus status = TableStreamStatus.CORRECT;

    /**
     * Sequence of values put in the stream during the time.
     * The idea of this is to give support to the snapshot feature of corfu.
     */
    public final List<CorfuTableDataGenerationFunction> dataTableStream = new ArrayList<>();

    public Object clone() {
        TableState clone = new TableState(this.tableStreamName);
        clone.status = this.status;
        clone.dataTableStream.addAll(this.dataTableStream);
        return clone;
    }

    /**
     * Computes the count of corfu table streams for each possible table status.
     *
     * @return Map with {@link TableStreamStatus} as key and the count of corfu tables in that status as value.
     */
    public static LinkedHashMap<String, Object> getStatsForCollection(List<TableState> collection) {
        Map<TableStreamStatus, Long> aggregation = collection.stream().collect(
                Collectors.groupingBy(TableState::getStatus, Collectors.counting()));
        LinkedHashMap<String, Object> sortedAggregations = new LinkedHashMap<>();
        if(!aggregation.containsKey(TableStreamStatus.CORRECT))
            sortedAggregations.put(Stats.CORRECT_TABLES, 0L);
        else
            sortedAggregations.put(Stats.CORRECT_TABLES, aggregation.get(TableStreamStatus.CORRECT));
        if(!aggregation.containsKey(TableStreamStatus.INCORRECT))
            sortedAggregations.put(Stats.INCORRECT_TABLES, 0L);
        else
            sortedAggregations.put(Stats.INCORRECT_TABLES, aggregation.get(TableStreamStatus.INCORRECT));
        if(!aggregation.containsKey(TableStreamStatus.PARTIALLY_CORRECT))
            sortedAggregations.put(Stats.PARTIALLY_CORRECT_TABLES, 0L);
        else
            sortedAggregations.put(Stats.PARTIALLY_CORRECT_TABLES, aggregation.get(TableStreamStatus.PARTIALLY_CORRECT));
        if(!aggregation.containsKey(TableStreamStatus.UNAVAILABLE))
            sortedAggregations.put(Stats.UNAVAILABLE_TABLES, 0L);
        else
            sortedAggregations.put(Stats.UNAVAILABLE_TABLES, aggregation.get(TableStreamStatus.UNAVAILABLE));
        return sortedAggregations;

    }
}
