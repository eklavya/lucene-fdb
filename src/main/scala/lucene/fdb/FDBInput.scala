package lucene.fdb

import java.util.concurrent.atomic.AtomicLong

import com.apple.foundationdb.TransactionContext
import com.apple.foundationdb.directory.DirectorySubspace
import org.apache.lucene.store.BufferedIndexInput

class FDBInput(file: FDBFile, ctx: TransactionContext, bufferSize: Int)
    extends BufferedIndexInput(file.name, bufferSize) {
  // keeps track of the current position in the file where next read will begin
  private val position = new AtomicLong(0L)

  override def readInternal(b: Array[Byte], offset: Int, length: Int): Unit =
    ctx.run { tr =>
      file.readBytes(tr, position.get, b, offset, length)
    }

  override def seekInternal(pos: Long): Unit =
    position.set(pos)

  override def close(): Unit = ()

  override def length(): Long =
    ctx.run(file.getLength)
}
