package net.mbl.demo.hcfsjar;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;

public class JarEntryInputStream extends InputStream implements Seekable, PositionedReadable {

    private final InputStream inputStream;

    public JarEntryInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public int read() throws IOException {
        return inputStream.read();
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        return inputStream.read(buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        inputStream.read(buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        inputStream.read(buffer);
    }

    @Override
    public void seek(long pos) throws IOException {
        skip(pos);
    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public int available() throws IOException {
        return inputStream.available();
    }
}
