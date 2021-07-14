package io.alcide.handson;

import com.google.gson.Gson;
import io.alcide.handson.model.Event;
import io.alcide.handson.model.Relation;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Generator {

    private final String outDir;
    ExecutorService execService = Executors.newSingleThreadExecutor();

    public Generator() {
        File outDir = new File("output" + File.separator + System.currentTimeMillis());
        outDir.mkdirs();

        this.outDir = outDir.getAbsolutePath();
    }

    public void stop() throws InterruptedException {
        execService.shutdown();
        execService.awaitTermination(5, TimeUnit.SECONDS);
    }

    public Stream<Event> createEventStream(List<Relation> relations) {
        Relation[] relationsArr = relations.toArray(new Relation[]{});
        Stream<Relation> randomStream = 
                new Random().ints(1000, 0, relations.size())
                .mapToObj(i -> relationsArr[i]);

        AtomicLong fakeTs = new AtomicLong();

        return Stream.concat(relations.stream(), randomStream)
                .map(r -> new Event(r.srcIp, r.destIp,
                            (long)(Math.random() * 100),
                            fakeTs.incrementAndGet()))
                .peek(event -> writeToFile(event, "events"));
    }

    public List<String> generateIps(int numOfEndPoints) {
        return new Random().ints(numOfEndPoints)
                .mapToObj(ip -> String.format("%d.%d.%d.%d", (ip & 0xff), (ip >> 8 & 0xff), (ip >> 16 & 0xff), (ip >> 24 & 0xff)))
                .peek(ip -> writeToFile(ip, "ips"))
                .collect(Collectors.toList());
    }

    private void writeToFile(Object obj, String targetFile) {
        Path path = Paths.get(outDir, targetFile);
        execService.submit(() -> {
            try {
                String str = new Gson().toJson(obj) + "\n";
                Files.write(path, str.getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public List<Relation> generateRelations(List<String> ips) {
        List<Relation> relations = ips.stream()
                .flatMap(ip -> randRelations(ip, ips).stream())
                .peek(rel -> writeToFile(rel, "relations"))
                .collect(Collectors.toList());

        return relations;
    }

    private List<Relation> randRelations(String ip, List<String> ips) {
        final double relatedPercentage = Math.random();
        return ips.stream()
                .filter(dontCare -> Math.random() < relatedPercentage)
                .map(otherIp -> new Relation(ip, otherIp))
                .collect(Collectors.toList());
    }
}
