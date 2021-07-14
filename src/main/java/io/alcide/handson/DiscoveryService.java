package io.alcide.handson;

import io.alcide.handson.model.Event;
import io.alcide.handson.model.Relation;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DiscoveryService {

    List<Relation> relations;
    List<Event> events;
    long maxTsMs;

    public List<Relation> getRelations() {
        return relations;
    }

    public void consume(List<Event> events) {
        this.events = events;
        relations = events.stream().map(e -> new Relation(e.srcIP, e.destIp)).distinct().collect(Collectors.toList());
        maxTsMs=(events.size()>0)?events.stream().max((o1, o2) -> (int) (o1.tsMs-o2.tsMs)).get().tsMs:0;
    }

    public Set<String> getRecentActiveIPs(int n, TimeUnit unit) {
        if (events.size()==0){
            return new HashSet<>();
        }
        long maxTsMs=events.stream().max((o1, o2) -> (int) (o1.tsMs-o2.tsMs)).get().tsMs;
        return events.stream().filter(event -> event.tsMs>maxTsMs-unit.toMillis(n))
                .map(event -> new HashSet<>(Arrays.asList(event.srcIP,event.destIp))).flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    public Set<String> getIpWithHighestOutboundTraffic(int n, TimeUnit unit){
        if (events.size()==0){
            return new HashSet<>();
        }
        Map<String,Long> IPsToOutboundTraffic = events.stream().filter(event -> event.tsMs>maxTsMs-unit.toMillis(n))
                .collect(Collectors.groupingBy(event -> event.srcIP,Collectors.summingLong(value -> value.byteSent)));
        long maxOutbound = IPsToOutboundTraffic.entrySet().stream().max((o1, o2) -> (int) (o1.getValue()-o2.getValue())).get().getValue();
        Set<String> IPsWithMaxOutbound= new HashSet<>();
        for (Map.Entry<String,Long> entry : IPsToOutboundTraffic.entrySet()){
            if (entry.getValue()==maxOutbound){
                IPsWithMaxOutbound.add(entry.getKey());
            }
        }
        return IPsWithMaxOutbound;

    }


}