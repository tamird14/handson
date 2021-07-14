package io.alcide.handson;

import io.alcide.handson.model.Event;
import io.alcide.handson.model.Relation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class TestTopologyDiscovery {

    Generator generator;
    List<Relation> relations;
    Stream<Event> eventStream;

    DiscoveryService discoveryService;

    @Before
    public void before() {
        generator = new Generator();
        List<String> ips = generator.generateIps(10);
        relations = generator.generateRelations(ips);
        eventStream = generator.createEventStream(relations);

        discoveryService = new DiscoveryService();
    }

    @After
    public void after() throws InterruptedException {
        generator.stop();
    }

    @Test
    public void testTopologyDiscovery() {
        List<Event> events = eventStream
                .limit(1000)
                .collect(Collectors.toList());

        //please implement consume() to pass this test
        discoveryService.consume(events);

        assertEquals(relations, discoveryService.getRelations());
    }

    @Test
    public void testIpsByRecentEvents() {
        //simulate events
        List<Event> events = eventStream
                .limit(1000)
                .collect(Collectors.toList());

        discoveryService.consume(events);

        /** please implement and test your code to return (efficiently) IPs related to the most recent events
         *  within a time frame (defined by the user. e.g from UI)
         *
         * Should be something like this:
         *  discoveryService.getRecentActiveIPs(4, TimeUnit.HOURS)
         */

//        Set<String> recentIPs = events.stream().filter(event -> event.tsMs>996).map(event -> new HashSet<>(Arrays.asList(event.srcIP,event.destIp))).flatMap(Collection::stream)
//                .collect(Collectors.toSet());
        Event e1 = events.get(999);
        Event e2 = events.get(998);
        Event e3 = events.get(997);
        Event e4 = events.get(996);
        Set<String> recentIPs = new HashSet<>(Arrays.asList(e1.destIp, e1.srcIP, e2.destIp, e2.srcIP, e3.destIp, e3.srcIP, e4.destIp, e4.srcIP));

        assertEquals(recentIPs, discoveryService.getRecentActiveIPs(4, TimeUnit.MILLISECONDS));

        Set<String> allIPs = events.stream().map(event -> new HashSet<>(Arrays.asList(event.srcIP, event.destIp))).flatMap(Collection::stream)
                .collect(Collectors.toSet());
        assertEquals(allIPs, discoveryService.getRecentActiveIPs(1, TimeUnit.SECONDS));
    }


    @Test
    public void testIpsByHighestOutboundTraffic() {
        //simulate events
        List<Event> events = eventStream
                .limit(1000)
                .collect(Collectors.toList());

        discoveryService.consume(events);

        /** please implement and test your code to return (efficiently) the IPs with highest amount of outbound traffic
         *  within a time frame (defined by the user. e.g from UI)
         *
         * Should be something like this:
         *  discoveryService.getRecentActiveIPs(4, TimeUnit.HOURS)
         */

        Event e1 = events.get(999);
        Event e2 = events.get(998);
        Event e3 = events.get(997);
        Event e4 = events.get(996);
        Set<Event> recentEvents = new HashSet<>(Arrays.asList(e1,e2,e3,e4));
        Map<String,Long> IPsToTraffic =recentEvents.stream().collect(Collectors.groupingBy(event -> event.srcIP,Collectors.summingLong(value -> value.byteSent)));
        long maxTraffic=0;
        for (String IP : IPsToTraffic.keySet()){
            if (IPsToTraffic.get(IP)>maxTraffic){
                maxTraffic=IPsToTraffic.get(IP);
            }
        }
        Set<String> IPsWithMaxTraffic = new HashSet<>();
        for (String IP : IPsToTraffic.keySet()){
            if (IPsToTraffic.get(IP)==maxTraffic){
                IPsWithMaxTraffic.add(IP);
            }
        }
        assertEquals(IPsWithMaxTraffic,discoveryService.getIpWithHighestOutboundTraffic(4,TimeUnit.MILLISECONDS));

        IPsToTraffic =events.stream().collect(Collectors.groupingBy(event -> event.srcIP,Collectors.summingLong(value -> value.byteSent)));
        maxTraffic=0;
        for (String IP : IPsToTraffic.keySet()){
            if (IPsToTraffic.get(IP)>maxTraffic){
                maxTraffic=IPsToTraffic.get(IP);
            }
        }
        IPsWithMaxTraffic = new HashSet<>();
        for (String IP : IPsToTraffic.keySet()){
            if (IPsToTraffic.get(IP)==maxTraffic){
                IPsWithMaxTraffic.add(IP);
            }
        }
        assertEquals(IPsWithMaxTraffic,discoveryService.getIpWithHighestOutboundTraffic(1,TimeUnit.SECONDS));
    }
}
