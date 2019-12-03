package org.apache.spark.scheduler

import java.io.{BufferedOutputStream, DataOutputStream, File, FileOutputStream, IOException}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.{BlockManager, ShuffleIndexBlockId}
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._
import scala.collection.mutable

private[spark] class MergeWriterScala (
       shuffleId: Int,
       mapId: Long) {

//  private var dataFile = null
//  private var indexFile = null
//  private var dataFileOutputStream = null
//  private var dataFileChannel = null
//  private var indexFileOutputStream = null
//  private var indexFileChannel = null
  val blockManager: BlockManager = SparkEnv.get.blockManager
  val blockResolver: IndexShuffleBlockResolver = new IndexShuffleBlockResolver(SparkEnv.get.conf, blockManager)

  def dataFile: File = blockResolver.getDataFile(shuffleId, mapId)
  def indexFile: File = blockResolver.getIndexFile(shuffleId, mapId)

  private def openStream(file: File) = new FileOutputStream(file, true)

  private def openChannel(fileInputStream: FileOutputStream) = fileInputStream.getChannel

  def dataFileOutputStream: FileOutputStream = openStream(dataFile)
  def dataFileChannel: FileChannel = openChannel(dataFileOutputStream)
  def indexFileOutputStream: FileOutputStream = openStream(indexFile)
  def indexFileChannel: FileChannel = openChannel(indexFileOutputStream)


  def writeDataFile(dataFileBuffer: ByteBuffer): Unit = {
    dataFileBuffer.flip
    dataFileChannel.write(dataFileBuffer)
  }

  def writeIndexFile(indexFileBuffer: ByteBuffer): Unit = {
    indexFileBuffer.flip
    indexFileChannel.write(indexFileBuffer)
  }

  def closeChannel(): Unit = {
    dataFileChannel.close()
    indexFileChannel.close()
  }

  def closeFileOutputStream(): Unit = {
    dataFileOutputStream.close()
    indexFileOutputStream.close()
  }



  def merge(readers: Array[MergeReader]): Array[Long] ={
    var ifDone = readers.zipWithIndex.map(ele => ele._2).toSet
    var lengths = Array[Long]()
    for (r <- readers){
      val rl: Array[java.lang.Long] = r.getLengths.asScala.toArray
      var idx = 0
      for(v  <- rl){
        if(lengths.isEmpty || lengths.size <= idx){
          lengths :+= v.asInstanceOf[Long]
        }else{
          lengths(idx) += v.asInstanceOf[Long]
        }
        idx +=1
      }
    }

    while(ifDone.nonEmpty){
      var done : Set[Int] = Set()
      for (ifDoneidx: Int <- ifDone){
        this.writeDataFile(readers(ifDoneidx).readDatafile())
        if(readers(ifDoneidx).isReadComplete) {
          done += ifDoneidx
        }
      }
      ifDone = ifDone.diff(done)
    }


    val indexFile = blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
    val indexTmp = Utils.tempFileWith(indexFile)

    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
      // We take in lengths of each block, need to convert it to offsets.
      var offset = 0L
      out.writeLong(offset)
      for (length <- lengths) {
        offset += length
        out.writeLong(offset)
    }
    {
      out.close()
    }

    if (indexFile.exists()) {
      indexFile.delete()
    }
    if (dataFile.exists()) {
      dataFile.delete()
    }
    if (!indexTmp.renameTo(indexFile)) {
      throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
    }

    lengths

  }
}
