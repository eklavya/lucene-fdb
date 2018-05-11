package lucene.fdb

import java.io.OutputStream
import java.util.concurrent.atomic.AtomicLong

import com.apple.foundationdb.TransactionContext
import com.apple.foundationdb.directory.DirectorySubspace

class FDBFileOutput(file: FDBFile, ctx: TransactionContext, dir: DirectorySubspace)
    extends OutputStream {
  private val position = new AtomicLong(0L)

  override def write(b: Int): Unit = ctx.run { tr =>
    file.writeByte(tr, position.get, b.toByte)
    position.getAndIncrement()
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = ctx.run { tr =>
    file.writeBytes(tr, position.get, b, off, len)
    position.getAndAdd(len)
  }
}
