package org.apache.spark.scheduler;

import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.storage.BlockManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class MergeReader {
    private int shuffleId;
    private long mapId;
    private File dataFile;
    private File indexFile;
    private ByteBuffer byteBuffer;
    private FileInputStream dataFileInputStream;
    private FileChannel dataFileChannel;
    private FileInputStream indexFileInputStream;
    private FileChannel indexFileChannel;
    private boolean isReadComplete;

    public MergeReader(int shuffleId, long mapId, int capacity) throws FileNotFoundException {
        this.shuffleId= shuffleId;
        this.mapId = mapId;
        BlockManager blockManager = SparkEnv.get().blockManager();
        IndexShuffleBlockResolver blockResolver = new IndexShuffleBlockResolver(SparkEnv.get().conf(), blockManager);
        dataFile = blockResolver.getDataFile(shuffleId, mapId);
        indexFile = blockResolver.getIndexFile(shuffleId, mapId);
        allocateBuffer(capacity);
        dataFileInputStream = openStream(dataFile);
        dataFileChannel = openChannel(dataFileInputStream);
        indexFileInputStream = openStream(indexFile);
        indexFileChannel = openChannel(indexFileInputStream);
    }

    private FileInputStream openStream(File file) throws FileNotFoundException {
        return new FileInputStream(file);
    }

    private FileChannel openChannel(FileInputStream fileInputStream) throws FileNotFoundException {
        return fileInputStream.getChannel();
    }

    public void allocateBuffer(int capacity){
        byteBuffer = ByteBuffer.allocate(capacity);
    }

    public ByteBuffer readDatafile() throws IOException {
        byteBuffer.clear();
        int count = dataFileChannel.read(byteBuffer);
        if((count <= 0)){
            isReadComplete = true;
        }
        return byteBuffer;
    }

    public ByteBuffer getIndexFile() throws IOException {
        ByteBuffer indexByteBuffer = ByteBuffer.allocate(1024*2000);
        indexFileChannel.read(indexByteBuffer);
        return indexByteBuffer;
    }

    public boolean isReadComplete(){
        return isReadComplete;
    }

    public void closeChannel() throws IOException {
        dataFileInputStream.close();
        indexFileInputStream.close();
    }

    public void closeFileInputStream() throws IOException {
        dataFileInputStream.close();
        indexFileChannel.close();
    }
}
