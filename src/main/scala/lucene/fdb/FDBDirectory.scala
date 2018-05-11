package lucene.fdb

import java.io.OutputStream
import java.util
import java.util.concurrent.ConcurrentHashMap

import com.apple.foundationdb.TransactionContext
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import org.apache.lucene.store._

import scala.collection.JavaConverters._

/**
  * A lucene store [[org.apache.lucene.store.Directory]] backed by FoundationDB
  *
  * @param name            name of the directory to open, will create/open a [[com.apple.foundationdb.directory.Directory]] of the same name
  * @param ctx
  * @param readBufferSize  size of the buffer while reading files, keep it as big as possible, since it will save the db round trips
  *                        and will make a HUGE difference in performance, by default it's 10 MB
  * @param writeBufferSize size of the buffer while writing files, keep it as big as possible, since it will save the db round trips
  *                        and will make a HUGE difference in performance, it can't be more than 10 MB since that's the
  *                        default per transaction byte limit in FoundationDB. Default value is 9 MB.
  */
class FDBDirectory(name: String,
                   ctx: TransactionContext,
                   readBufferSize: Int = 10 * 1024 * 1024,
                   writeBufferSize: Int = 9 * 1024 * 1024)
    extends Directory {
  /*
  All open files are tracked here, this is to enable reuse of the FDBFile objects so that
  we don't make db queries for things that can be cached for ex: length.
   */
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
    new FDBOutput(name, new FDBFileOutput(f, ctx).asInstanceOf[OutputStream], writeBufferSize)
  }

  override def createTempOutput(prefix: String, suffix: String, context: IOContext): IndexOutput = {
    val name = s"$prefix-$suffix.tmp"
    val f = openFiles.computeIfAbsent(name, _ => new FDBFile(name, ctx, dir))
    new FDBOutput(name,
                  new FDBFileOutput(f, ctx)
                    .asInstanceOf[OutputStream],
                  writeBufferSize)
  }

  override def sync(names: util.Collection[String]): Unit = ()

  override def rename(source: String, dest: String): Unit = {
    openFiles.remove(source)
    dir.move(ctx, List(source).asJava, List(dest).asJava).join()
  }

  override def syncMetaData(): Unit = ()

  override def openInput(name: String, context: IOContext): IndexInput = {
    val f = openFiles.computeIfAbsent(name, _ => new FDBFile(name, ctx, dir))
    new FDBInput(f, ctx, readBufferSize)
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
