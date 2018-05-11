package lucene.fdb

import java.io.OutputStream

import org.apache.lucene.store.OutputStreamIndexOutput

class FDBOutput(name: String, out: OutputStream, bufferSize: Int)
    extends OutputStreamIndexOutput(name, name, out, bufferSize)
