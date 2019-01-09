package org.corfudb.universe;

import java.util.List;

public interface LongevityListener {
    String getId();
    void setReportColumns(List<String> reportColumns, List<List<String>> data);
    void reportIteration(List<String> report);
    void finish();
}
