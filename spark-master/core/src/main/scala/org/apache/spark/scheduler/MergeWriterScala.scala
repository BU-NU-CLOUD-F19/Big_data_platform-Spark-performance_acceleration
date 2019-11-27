package org.apache.spark.scheduler

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.BlockManager

import scala.collection.mutable

private[spark] class MergeWriterScala(
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

  def dataFile = blockResolver.getDataFile(shuffleId, mapId)
  def indexFile = blockResolver.getIndexFile(shuffleId, mapId)

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


  def write(readers: Seq[MergeReader], idx: Seq[Int]): Unit ={
    val ifDone = readers.zipWithIndex.map(ele => ele._2).toSet
    while(ifDone.nonEmpty){
      var done : Set[Int] = Set()
      for (idx: Int <- ifDone){
        this.writeDataFile(readers(idx).readDatafile())
        if(readers(idx).isReadComplete) {
          done += idx
        }
      }
      ifDone -- done
    }
    // TODO: write index file
  }
}
