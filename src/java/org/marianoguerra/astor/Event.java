package org.marianoguerra.astor;

import java.nio.ByteBuffer;

public class Event {
    public static final int HEADER_SEQNUM_SIZE = 8;
    public static final int HEADER_TIMESTAMP_SIZE = 8;
    public static final int HEADER_CRC_SIZE = 8;
    public static final int HEADER_DATA_LENGTH_SIZE = 4;
    public static final int HEADER_SIZE = HEADER_SEQNUM_SIZE +
        HEADER_TIMESTAMP_SIZE + HEADER_CRC_SIZE + HEADER_DATA_LENGTH_SIZE;
    public final long seqnum;
    public final long timestamp;
    public final long crc;
    public final int size;
    public final byte[] data;

    public Event(ByteBuffer buffer) {
        this.seqnum = buffer.getLong();
        this.timestamp = buffer.getLong();
        this.crc = buffer.getLong();
        this.size = buffer.getInt();
        byte[] data = new byte[this.size];
        buffer.get(data);
        this.data = data;
    }

    public Event newWithBody(final ByteBuffer buffer) {
        return new Event(this.seqnum, this.timestamp, this.crc, buffer.array());
    }

    public Event(long seqnum, long timestamp, long crc, int size) {
        this.seqnum = seqnum;
        this.timestamp = timestamp;
        this.crc = crc;
        this.size = size;
        this.data = null;
    }

    public Event(long seqnum, long timestamp, long crc, byte[] data) {
        this.seqnum = seqnum;
        this.timestamp = timestamp;
        this.crc = crc;
        this.size = data.length;
        this.data = data;
    }

    public static Event readOnlyHeader(ByteBuffer buffer) {
        long seqnum = buffer.getLong();
        long timestamp = buffer.getLong();
        long crc = buffer.getLong();
        int size = buffer.getInt();

        return new Event(seqnum, timestamp, crc, size);
    }

    public ByteBuffer toByteBuffer(int paddingAfter) {
        int bufferSize = HEADER_SIZE + this.size + paddingAfter;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

        buffer.putLong(this.seqnum);
        buffer.putLong(this.timestamp);
        buffer.putLong(this.crc);
        buffer.putInt(this.size);
        buffer.put(this.data);

        return buffer;
    }

    public byte[] toBytes() {
        return this.toBytes(0);
    }

    public byte[] toBytes(int paddingAfter) {
        return this.toByteBuffer(0).array();
    }
}
