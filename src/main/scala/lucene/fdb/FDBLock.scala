package lucene.fdb

import com.apple.foundationdb.TransactionContext
import com.apple.foundationdb.tuple.Tuple
import org.apache.lucene.store.Lock

class FDBLock(name: String, ctx: TransactionContext) extends Lock {
  override def close(): Unit = ctx.run { tr =>
    tr.set(Tuple.from(name).pack, Tuple.from(false.asInstanceOf[Object]).pack)
  }

  override def ensureValid(): Unit = ()
}
