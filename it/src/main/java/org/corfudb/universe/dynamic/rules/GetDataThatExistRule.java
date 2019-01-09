package org.corfudb.universe.dynamic.rules;

import org.corfudb.universe.dynamic.events.GetDataEvent;
import org.corfudb.universe.dynamic.events.PutDataEvent;
import org.corfudb.universe.dynamic.events.UniverseEvent;
import org.corfudb.universe.dynamic.events.UniverseEventOperator;
import org.corfudb.universe.dynamic.state.State;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Rule to ensure that:
 *      - A {@link GetDataEvent} is executed if and only if some data exist for
 *      its respective corfu table stream and is not being modified in this same composite event.
 *      - If the data does not exist previously, the rule check whether the current
 *      event is a sequential composite event that is creating the data itself before to read it.
 *      - If the data is being modified, the rule check whether the current
 *      event is a sequential composite event that is modifying the data after to read it.
 *
 * Created by edilmo on 11/06/18.
 */
public class GetDataThatExistRule extends UniverseRule {

    /**
     * Check if the composite event, desire state and real state, are all compliant
     * with the rule defined.
     *
     * @param compositeEvent    Composite event that is happening.
     * @param desireState       Current desire state of the universe.
     * @param realState         Current real state of the universe.
     * @return                  Whether the composite event, desire state and real state,
     *                          are all compliant with the rule.
     */
    @Override
    public boolean check(UniverseEventOperator compositeEvent, State desireState, State realState) {
        boolean pass = true;
        //Lets check if there is a invalid get data (over a empty corfu table).
        List<UniverseEvent> events = compositeEvent.getSimpleEventsToCompose();
        HashMap<String, Integer> getDataEvents = new HashMap<>();
        HashMap<Integer, GetDataEvent> invalidGetDataEvents = new HashMap<>();
        for(int i = 0; i < events.size(); i++){
            UniverseEvent event = events.get(i);
            if(event instanceof GetDataEvent){
                GetDataEvent getDataEvent = (GetDataEvent)event;
                getDataEvents.put(getDataEvent.getTableStreamName(), i);
                String tableStreamName = getDataEvent.getTableStreamName();
                if(!desireState.hasDataInTableStream(tableStreamName)){
                    invalidGetDataEvents.put(i, getDataEvent);
                }
            }
        }
        HashMap<Integer, PutDataEvent> putDataEvents = new HashMap<>();
        HashMap<String, Integer> invalidPutDataEvents = new HashMap<>();
        for(int i = 0; i < events.size(); i++){
            UniverseEvent event = events.get(i);
            if(event instanceof PutDataEvent){
                PutDataEvent putDataEvent = (PutDataEvent)event;
                putDataEvents.put(i, putDataEvent);
                if(getDataEvents.containsKey(putDataEvent.getTableStreamName())){
                    invalidPutDataEvents.put(putDataEvent.getTableStreamName(), i);
                }
            }
        }
        if((!invalidGetDataEvents.isEmpty() || !invalidPutDataEvents.isEmpty()) && compositeEvent instanceof UniverseEventOperator.Sequential){
            //lets check if the previous invalid gets are executed over data that is being put
            //by the composite event itself
            for(Map.Entry<Integer, GetDataEvent> getDataEntry: invalidGetDataEvents.entrySet()){
                List<Map.Entry<Integer, PutDataEvent>> filteredPutDataEvents = putDataEvents.entrySet().stream().
                        filter(pde -> pde.getValue().getTableStreamName().equals(getDataEntry.getValue().getTableStreamName())).
                        collect(Collectors.toList());
                for(Map.Entry<Integer, PutDataEvent> putDataEntry: filteredPutDataEvents){
                    if(putDataEntry.getKey() < getDataEntry.getKey()){
                        invalidGetDataEvents.remove(getDataEntry.getKey());
                    }
                }
            }
            //lets check if the put data is after the get data
            for(Map.Entry<String, Integer> putDataEntry: invalidPutDataEvents.entrySet()){
                if(getDataEvents.containsKey(putDataEntry.getKey()) && getDataEvents.get(putDataEntry.getKey()) < putDataEntry.getValue()){
                    invalidPutDataEvents.remove(putDataEntry.getKey());
                }
            }
        }
        return invalidGetDataEvents.isEmpty() && invalidPutDataEvents.isEmpty();
    }

    public GetDataThatExistRule(){
    }
}
