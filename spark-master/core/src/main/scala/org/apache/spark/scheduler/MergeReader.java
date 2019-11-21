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
    private FileInputStream fileInputStream;
    private FileChannel fileChannel;
    private boolean isReadComplete;

    public MergeReader(int shuffleId, long mapId, int capacity) throws FileNotFoundException {
        this.shuffleId= shuffleId;
        this.mapId = mapId;
        BlockManager blockManager = SparkEnv.get().blockManager();
        IndexShuffleBlockResolver blockResolver = new IndexShuffleBlockResolver(SparkEnv.get().conf(), blockManager);
        dataFile = blockResolver.getDataFile(shuffleId, mapId);
        indexFile = blockResolver.getDataFile(shuffleId, mapId);
        allocateBuffer(capacity);
        openStream(dataFile);
        openChannel();
    }

    private void openStream(File file) throws FileNotFoundException {
        fileInputStream =  new FileInputStream(file);
    }

    private void openChannel() {
        fileChannel = fileInputStream.getChannel();
    }

    public void allocateBuffer(int capacity){
        byteBuffer = ByteBuffer.allocate(capacity);
    }

    public ByteBuffer readDatafile() throws IOException {
        byteBuffer.clear();
        int count = fileChannel.read(byteBuffer);
        if((count <= 0)){
            isReadComplete = true;
        }
        return byteBuffer;
    }

    public boolean isReadComplete(){
        return isReadComplete;
    }

    public void closeChannel() throws IOException {
        fileChannel.close();
    }

    public void closeFileInputStream() throws IOException {
        fileInputStream.close();
    }
}
