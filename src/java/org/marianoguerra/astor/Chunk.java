package org.marianoguerra.astor;

import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.AbstractMap;
import java.util.zip.Adler32;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.channels.FileChannel;

public class Chunk implements EventStore, EventHolder {
    private long _baseSeqNum;
    private long _oldest;

    private long _lastSeqNum;
    private long _newest;

    private ByteBuffer _headerBuffer;
    private Path path;
    private FileChannel _fileChannel;
    private Index<Long> _index;

    public Chunk(Path path) throws FileNotFoundException {
        this._baseSeqNum = -1;
        this._lastSeqNum = -1;
        this._oldest = -1;
        this._newest = -1;

        this.path = path;
    }

    protected FileChannel getFileChannel() throws FileNotFoundException {
        if (this._fileChannel == null) {
            RandomAccessFile raf = new RandomAccessFile(this.path.toFile(), "rw");
            this._fileChannel = raf.getChannel();
        }

        return this._fileChannel;
    }

    protected Index<Long> getIndex() {
        if (this._index == null) {
            this._index = new Index<Long>();
        }

        return this._index;
    }

    protected Map.Entry<Long, Long> getClosestPosition(final long seqnum) {
        Map.Entry<Long, Long> entry = this.getIndex().closest(seqnum);

        if (entry == null) {
            return new AbstractMap.SimpleImmutableEntry<Long, Long>(0l, 0l);
        } else {
            return entry;
        }
    }

    protected void indexEvent(final long seqnum, final long position) {
        this.getIndex().set(seqnum, position);
    }

    protected long calculateChecksum(final byte[] data) {
        Adler32 adler = new Adler32();
        adler.update(data);
        return adler.getValue();
    }

    public long lastSeqNum() throws IOException {
        if (this._lastSeqNum == -1) {
            this.readLastStats();
        }

        return this._lastSeqNum;
    }

    public long baseSeqNum() throws IOException {
        if (this._baseSeqNum == -1) {
            this.readFirstStats();
        }

        return this._baseSeqNum;
    }

    protected ByteBuffer getHeaderBuffer() {
        if (this._headerBuffer == null) {
            this._headerBuffer = ByteBuffer.allocate(Event.HEADER_SIZE);
        }

        this._headerBuffer.position(0);
        return this._headerBuffer;
    }

    protected void readLastStats() throws IOException {
        FileChannel handle = this.getFileChannel();
        long size = handle.size();
        long lengthFieldSize = Event.HEADER_DATA_LENGTH_SIZE;

        if (size == 0) {
            this._lastSeqNum = 0;
            this._baseSeqNum = 0;
            this._oldest = 0;
            this._newest = 0;
        } else {
            // TODO: read a big chunk (like 8K) and then if size is less than
            // we already have the entry and don't have to read again
            // also: buffer pool
            ByteBuffer buffer = this.getHeaderBuffer();
            handle.position(size - lengthFieldSize);
            int bytesRead = handle.read(buffer);

            if (bytesRead >= lengthFieldSize) {
                buffer.position(0);
                int dataSize = buffer.getInt();
                
                // TODO: buffer pool
                int entryBufferSize = Event.HEADER_SIZE + dataSize;

                long entryPosition =  size - entryBufferSize - lengthFieldSize;
                handle.position(entryPosition);
                // TODO: pass size hint
                Event lastEvent = this.readNext();

                this._lastSeqNum = lastEvent.seqnum;
                this.indexEvent(lastEvent.seqnum, entryPosition);
                this._newest = lastEvent.timestamp;
            } else {
                throw new IOException(
                        "Read less bytes than expected: expected " +
                        lengthFieldSize + " but got " + bytesRead);
            }

        }
    }

    protected void readFirstStats() throws IOException {
        FileChannel handle = this.getFileChannel();
        ByteBuffer buffer = this.getHeaderBuffer();
        long size = handle.size();

        if (size == 0) {
            this._lastSeqNum = 0;
            this._baseSeqNum = 0;
            this._oldest = 0;
            this._newest = 0;
        } else {
            int bytesRead = handle.read(buffer, 0);
            if (bytesRead < Event.HEADER_SIZE) {
                throw new IOException(
                        "Read less bytes than expected: expected " +
                        Event.HEADER_SIZE + " but got " + bytesRead);
            } else {
                buffer.position(0);
                Event event = Event.readOnlyHeader(buffer);

                this._oldest = event.timestamp;
                this._baseSeqNum = event.seqnum;
                this.indexEvent(event.seqnum, 0);

                // if only one event we initialize the last fields too
                if (size == Event.HEADER_SIZE + event.size +
                        Event.HEADER_DATA_LENGTH_SIZE) {
                    this._newest = this._oldest;
                    this._lastSeqNum = this._baseSeqNum;
                }
            }
        }
    }

    protected long incrementLastSeqNum() throws IOException {
        this._lastSeqNum = this.lastSeqNum() + 1;
        return this._lastSeqNum;
    }

    public long newest() throws IOException {
        if (this._newest == -1) {
            this.readLastStats();
        }

        return this._newest;
    }

    protected long updateNewest(long timestamp) {
        this._newest = timestamp;
        return this._newest;
    }

    public Event write(final long timestamp, final byte[] data)
            throws IOException {
        long endPosition;
        long lastSeqNum;
        FileChannel handle = this.getFileChannel();
        long oldSize = handle.size();

        if (oldSize == 0) {
            lastSeqNum = 0;
            this._lastSeqNum = 0;
        } else {
            lastSeqNum = this.lastSeqNum();
        }

        long seqnum = lastSeqNum + 1;

        long crc = this.calculateChecksum(data);

        Event event = new Event(seqnum, timestamp, crc, data);
        ByteBuffer eventBytes = event.toByteBuffer(Event.HEADER_DATA_LENGTH_SIZE);
        eventBytes.putInt(event.size);
        eventBytes.position(0);
        endPosition = handle.size();
        handle.write(eventBytes, endPosition);
        
        if (oldSize == 0) {
            this._oldest = event.timestamp;
            this._baseSeqNum = event.seqnum;
        }

        this.indexEvent(event.seqnum, endPosition);
        this.incrementLastSeqNum();
        this.updateNewest(timestamp);

        return event;
    }

    protected Event readNext() throws IOException {
        return readNext(false);
    }

    protected Event readNext(final boolean skipBody) throws IOException {
        final FileChannel handle = this.getFileChannel();
        final ByteBuffer headerBuffer = this.getHeaderBuffer();
        final int headerBytesRead = handle.read(headerBuffer);
        headerBuffer.position(0);

        if (headerBytesRead < Event.HEADER_SIZE) {
            throw new IOException(
                    "Read less bytes than expected: expected " +
                    Event.HEADER_SIZE + " but got " + headerBytesRead);
        }

        Event eventHeader = Event.readOnlyHeader(headerBuffer);

        if (skipBody) {
            handle.position(handle.position() + eventHeader.size + 
                    Event.HEADER_DATA_LENGTH_SIZE);
            return eventHeader;
        }

        final int entryBufferSize = eventHeader.size;
        ByteBuffer entryBuffer = ByteBuffer.allocate(entryBufferSize);
        final int entryBytesRead = handle.read(entryBuffer);

        if (entryBytesRead != entryBufferSize) {
            throw new IOException(
                    "Read less bytes than expected: expected " +
                    entryBufferSize + " but got " + entryBytesRead);
        } else {
            entryBuffer.rewind();
            final Event event = eventHeader.newWithBody(entryBuffer);
            // advance the length field after the event
            handle.position(handle.position() + Event.HEADER_DATA_LENGTH_SIZE);
            return event;
        }
    }

    public List<Event> readLast(final int count) throws IOException {
        final long lastSeqNum = this.lastSeqNum();
        long fromSeqNum = lastSeqNum - count;
        long finalCountLong;
        int finalCount;

        if (fromSeqNum < 0) {
            finalCountLong = count + fromSeqNum;
            fromSeqNum = 0;
        } else {
            finalCountLong = count;
        }

        if (finalCountLong > Integer.MAX_VALUE) {
            throw new RuntimeException("count overflow on readLast: " +
                    finalCountLong);
        } else {
            finalCount = (int)finalCountLong;
        }

        return read(fromSeqNum, finalCount);
    }

    public List<Event> read(final long seqnum) throws IOException {
        return read(seqnum, 1);
    }

    public List<Event> read(final long seqnum, final int count) throws IOException {
        List<Event> result = new LinkedList<Event>();

        if (seqnum < 0) {
            return result;
        }

        final Map.Entry<Long, Long> indexEntry = this.getClosestPosition(seqnum);
        final FileChannel handle = this.getFileChannel();
        
        if (handle.size() > 0) {
            handle.position(indexEntry.getValue());
            final long firstSeqNum = indexEntry.getKey();
            final long lastSeqNum = seqnum + count;

            int i;
            long currentSeqNum;
            for (currentSeqNum = firstSeqNum, i = 0;
                    i < count && currentSeqNum < lastSeqNum;
                    i += 1) {
                final long eventStartPosition = handle.position();

                final boolean skip = currentSeqNum < firstSeqNum;
                final Event event = this.readNext(skip);

                if (event == null) {
                    break;
                }

                if (!skip) {
                    result.add(event);
                }

                this.indexEvent(event.seqnum, eventStartPosition);
            }
        }

        return result;
    }

    public long size() throws IOException {
        return this.getFileChannel().size();
    }

    public long count() throws IOException {
        long lastSeqNum = this.lastSeqNum();
        long baseSeqNum = this.baseSeqNum();

        if (lastSeqNum == 0) {
            return 0;
        } else {
            return lastSeqNum - baseSeqNum + 1;
        }
    }

    public long oldest() throws IOException {
        if (this._oldest == -1) {
            this.readFirstStats();
        }

        return this._oldest;
    }
}
