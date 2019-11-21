package org.apache.spark.scheduler

import java.io.FileOutputStream
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.BlockManager
import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext}


private[spark] class MergeTask(
                                     stageId: Int,
                                     mapId : Int,
                                     stageAttemptId: Int,
                                     taskBinary: Broadcast[Array[Byte]],
                                     partition: Partition,
                                     @transient private var locs: Seq[TaskLocation],
                                     localProperties: Properties,
                                     serializedTaskMetrics: Array[Byte],
                                     jobId: Option[Int] = None,
                                     appId: Option[String] = None,
                                     appAttemptId: Option[String] = None,
                                     isBarrier: Boolean = false)
  extends Task[Unit](stageId, stageAttemptId, partition.index, localProperties,
    serializedTaskMetrics, jobId, appId, appAttemptId, isBarrier)
    with Logging {
 //  var dep : ShuffleDependency[_, _, _];

//  def this(partitionId: Int) {
//    this(0, 0, null, new Partition { override def index: Int = 0 }, null, new Properties, null)
//  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): Unit ={
    logInfo("we are here in merrge.............")
   // MergerReader(Seq[] block )
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTimeNs = System.nanoTime()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val rddAndDep = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTimeNs = System.nanoTime() - deserializeStartTimeNs
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L
    val dep  = rddAndDep._2;
    val blockManager: BlockManager = SparkEnv.get.blockManager;
    val blockResolver = new IndexShuffleBlockResolver(SparkEnv.get.conf, blockManager);
    val file2 = blockResolver.getDataFile(dep.shuffleId,context.taskAttemptId());
//    val indexFile = blockResolver.getIndexFile( dep.shuffleId,context.taskAttemptId()-1);
//    val channel = Files.newByteChannel(indexFile.toPath)
//    val ina = new DataInputStream(Channels.newInputStream(channel))
//    try {
//      val offset = ina.readLong()
//      val nextOffset = ina.readLong()
//      val actualPosition = channel.position()
//      val transportConf = SparkTransportConf.fromSparkConf(SparkEnv.get.conf, "shuffle")
//      val buf: FileSegmentManagedBuffer = new FileSegmentManagedBuffer(
//        transportConf,
//        file,
//        offset,
//        nextOffset - offset)
//      buf.convertToNetty()
//    } finally {
//      ina.close()
//    }


    val out = new FileOutputStream(file2, true)
    val outChannel: FileChannel = out.asInstanceOf[FileOutputStream].getChannel()
    val mergeReader: MergeReader = new MergeReader(dep.shuffleId, context.taskAttemptId()-1, 1024*1000);
    while(!mergeReader.isReadComplete)
    {
      outChannel.write(mergeReader.readDatafile().flip().asInstanceOf[ByteBuffer])
    }
    mergeReader.closeChannel();
    mergeReader.closeFileInputStream();
    outChannel.close();

    "something"
  }
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "MergeTask(%d, %d)".format(stageId, partitionId)


}

