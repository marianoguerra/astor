package org.marianoguerra.astor;

import java.util.Map;
import java.util.TreeMap;

public class Index<T> {
    private TreeMap<Long, T> index;

    public Index() {
        this.index = new TreeMap<Long, T>();
    }

    public void set(final long seqnum, final T value) {
        this.index.put(seqnum, value);
    }

    public Map.Entry<Long, T> closest(final long seqnum) {
        return this.index.floorEntry(seqnum);
    }

    public T remove(final long seqnum) {
        return this.index.remove(seqnum);
    }
}
