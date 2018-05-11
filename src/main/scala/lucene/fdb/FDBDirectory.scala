package lucene.fdb

import java.io.OutputStream
import java.util
import java.util.concurrent.ConcurrentHashMap

import com.apple.foundationdb.TransactionContext
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import org.apache.lucene.store._

import scala.collection.JavaConverters._

class FDBDirectory(name: String, ctx: TransactionContext) extends Directory {
  private val openFiles = new ConcurrentHashMap[String, FDBFile]()
  private val dirLayer = new DirectoryLayer()

  private val dir = dirLayer.createOrOpen(ctx, List(name).asJava).join()

  override def listAll(): Array[String] = {
    val l: util.List[String] = dir.list(ctx).join()
    l.asScala.toArray
  }

  override def deleteFile(name: String): Unit = {
    dir.removeIfExists(ctx, List(name).asJava)
    openFiles.remove(name)
  }

  override def fileLength(name: String): Long = ctx.run { tr =>
    val f = openFiles.computeIfAbsent(name, _ => new FDBFile(name, ctx, dir))
    f.getLength(tr)
  }

  override def createOutput(name: String, context: IOContext): IndexOutput = {
    val f = openFiles.computeIfAbsent(name, _ => new FDBFile(name, ctx, dir))
    new FDBOutput(name, new FDBFileOutput(f, ctx, dir).asInstanceOf[OutputStream])
  }

  override def createTempOutput(prefix: String, suffix: String, context: IOContext): IndexOutput = {
    val name = s"$prefix-$suffix.tmp"
    val f = openFiles.computeIfAbsent(name, _ => new FDBFile(name, ctx, dir))
    new FDBOutput(name,
                  new FDBFileOutput(f, ctx, dir)
                    .asInstanceOf[OutputStream])
  }

  override def sync(names: util.Collection[String]): Unit = ()

  override def rename(source: String, dest: String): Unit = {
    openFiles.remove(source)
    dir.move(ctx, List(source).asJava, List(dest).asJava).join()
  }

  override def syncMetaData(): Unit = ()

  override def openInput(name: String, context: IOContext): IndexInput = {
    val f = openFiles.computeIfAbsent(name, _ => new FDBFile(name, ctx, dir))
    new FDBInput(f, ctx, dir)
  }

  override def obtainLock(name: String): Lock =
    ctx.run { tr =>
      val v = tr.get(Tuple.from(name).pack).join()
      val isLocked = (v != null) && Tuple.fromBytes(v).getBoolean(0)
      if (isLocked) throw new LockObtainFailedException("could not obtain lock")
      else {
        tr.set(Tuple.from(name).pack, Tuple.from(true.asInstanceOf[Object]).pack)
        new FDBLock(name, ctx)
      }
    }

  override def close(): Unit =
    openFiles.clear()
}
