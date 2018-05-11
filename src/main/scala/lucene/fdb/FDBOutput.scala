package lucene.fdb

import java.io.OutputStream

import org.apache.lucene.store.OutputStreamIndexOutput

class FDBOutput(name: String, out: OutputStream)
    extends OutputStreamIndexOutput(name, name, out, 9 * 1024 * 1024)
