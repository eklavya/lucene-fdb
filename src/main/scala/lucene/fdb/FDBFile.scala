package lucene.fdb

import java.util.concurrent.atomic.AtomicLong

import com.apple.foundationdb.directory.DirectorySubspace
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.{Transaction, TransactionContext}

import scala.collection.JavaConverters._

class FDBFile(val name: String, val ctx: TransactionContext, val dir: DirectorySubspace) {
  private val blob = dir.createOrOpen(ctx, List(name).asJava).join()

  /*
  We can cache the length since the file objects are reused and we are tracking the state here.
  Saves a round trip from db.
   */
  private val curLen = new AtomicLong(-1L)

  def getLength(tr: Transaction): Long =
    if (curLen.get == -1) {
      curLen.set(updateLength(tr))
      curLen.get
    } else curLen.get

  private def updateLength(tr: Transaction): Long = {
    val v = tr.get(blob.pack(Tuple.from("length"))).join()
    if (v == null) 0L
    else Tuple.fromBytes(v).getLong(0)
  }

  def readByte(tr: Transaction, pos: Int): Byte = {
    val segmentNum: Integer = (pos / 10240) + 1
    val blockPos = pos % 10240
    val key = blob.subspace(Tuple.from(segmentNum)).pack()
    val kv = tr.get(key).join()
    if (kv != null) {
      kv(blockPos)
    } else throw new IndexOutOfBoundsException
  }

  private def getSegment(tr: Transaction, segmentNum: Integer) = {
    val key = blob.subspace(Tuple.from(segmentNum)).pack()
    val kv = tr.get(key).join()
    kv
  }

  def readBytes(tr: Transaction, pos: Long, b: Array[Byte], offset: Int, len: Int): Unit = {
    val sn = (pos / 10240) + 1
    val data = getSegment(tr, sn.toInt)
    val blockPos = (pos % 10240).toInt
    val readLength = if (len > (10240 - blockPos)) 10240 - blockPos else len
    System.arraycopy(data, blockPos.toInt, b, offset, readLength)
    if (readLength != len) {
      readBytes(tr, pos + readLength, b, offset + readLength, len - readLength)
    }
  }

  def writeByte(tr: Transaction, pos: Long, b: Byte): Unit = {
    val l = getLength(tr)
    val segmentNum: Integer = ((pos / 10240) + 1).toInt
    val blockPos = (pos % 10240).toInt
    val key = blob.subspace(Tuple.from(segmentNum)).pack()
    val kv = tr.get(key).join()
    if (kv != null) {
      kv(blockPos) = b
      tr.set(key, kv)
    } else {
      val v = new Array[Byte](10240)
      v(blockPos) = b
      tr.set(blob.subspace(Tuple.from(segmentNum)).pack(), v)
    }
    curLen.getAndIncrement()
    tr.set(blob.pack(Tuple.from("length")), Tuple.from((l + 1).asInstanceOf[Object]).pack)
  }

  def writeBytes(tr: Transaction, pos: Long, b: Array[Byte], offset: Int, len: Int): Unit = {
    val segmentNum: Integer = ((pos / 10240) + 1).toInt
    if (pos > curLen.get) {
      //We are writing at the end and don't need to make a db query since this block is new
      writeNewBlock(tr, pos, b, offset, len, segmentNum)
    } else {
      val key = blob.subspace(Tuple.from(segmentNum)).pack()
      val kv = tr.get(key).join()
      if (kv != null) {
        val blockPos = (pos % 10240).toInt
        val writeLength =
          if (len > (10240 - blockPos)) 10240 - blockPos else len
        System.arraycopy(b, offset, kv, blockPos, writeLength)
        tr.set(key, kv)
        val l = getLength(tr)
        curLen.getAndAdd(writeLength)
        tr.set(blob.pack(Tuple.from("length")),
               Tuple.from((l + writeLength).asInstanceOf[Object]).pack)
        if (writeLength != len) {
          writeBytes(tr, pos + writeLength, b, offset + writeLength, len - writeLength)
        }
      } else {
        writeNewBlock(tr, pos, b, offset, len, segmentNum)
      }
    }
  }

  private def writeNewBlock(tr: Transaction,
                            pos: Long,
                            b: Array[Byte],
                            offset: Int,
                            len: Int,
                            segmentNum: Integer) = {
    val writeLength = if (len > 10240) 10240 else len
    val kv = new Array[Byte](10240)
    System.arraycopy(b, offset, kv, 0, writeLength)
    tr.set(blob.subspace(Tuple.from(segmentNum)).pack(), kv)
    val l = getLength(tr)
    curLen.getAndAdd(writeLength)
    tr.set(blob.pack(Tuple.from("length")), Tuple.from((l + writeLength).asInstanceOf[Object]).pack)
    if (writeLength != len) {
      writeBytes(tr, pos + writeLength, b, offset + writeLength, len - writeLength)
    }
  }
}
