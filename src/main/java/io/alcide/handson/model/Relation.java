package io.alcide.handson.model;

import java.util.Objects;

public class Relation {
    public final String srcIp;
    public final String destIp;

    public Relation(String srcIp, String destIp) {
        this.srcIp = srcIp;
        this.destIp = destIp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Relation relation = (Relation) o;
        return srcIp.equals(relation.srcIp) && destIp.equals(relation.destIp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcIp, destIp);
    }
}