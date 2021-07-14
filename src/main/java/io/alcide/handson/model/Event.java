package io.alcide.handson.model;

public class Event {

    public final String srcIP;
    public final String destIp;
    public final long byteSent;
    public final long tsMs;

    public Event(String srcIP,
            String destIp,
            long byteSent,
            long tsMs) {
        this.srcIP = srcIP;
        this.destIp = destIp;
        this.byteSent = byteSent;
        this.tsMs = tsMs;
    }
}
