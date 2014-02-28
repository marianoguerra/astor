package org.marianoguerra.astor;

import java.util.List;
import java.io.IOException;

public interface EventStore {
    Event write(long timestamp, byte[] data) throws IOException;
    List<Event> readLast(int count) throws IOException;
    List<Event> read(long seqnum) throws IOException;
    List<Event> read(long seqnum, int count) throws IOException;
}
