import org.apache.lucene.document.{Document, Field}
import org.apache.lucene.queryparser.classic.QueryParser
import com.apple.foundationdb.FDB
import lucene.fdb.FDBDirectory
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.TextField
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.search.IndexSearcher

import scala.collection.JavaConverters._
import scala.util.Random

object Main {

  private val fdb = FDB.selectAPIVersion(510)
  private val db = fdb.open()

  def main(args: Array[String]): Unit = {
    val analyzer = new StandardAnalyzer

    val words = io.Source
      .fromInputStream(this.getClass.getResourceAsStream("words_alpha.txt"))
      .getLines()
      .toArray

    val directory = new FDBDirectory("lucene", db)
    val config = new IndexWriterConfig(analyzer)
    val iWriter = new IndexWriter(directory, config)

    (0 until 10000).foreach { i =>
      val doc = new Document
      val text = (0 until 100)
        .map { _ =>
          words(Random.nextInt(words.length))
        }
        .mkString(" ")
      doc.add(new Field("fieldname", text, TextField.TYPE_NOT_STORED))
      doc.add(new Field("id", s"$i", TextField.TYPE_STORED))
      iWriter.addDocument(doc)
    }
    iWriter.close()

    val iReader = DirectoryReader.open(directory)
    val iSearcher = new IndexSearcher(iReader)
    val parser = new QueryParser("fieldname", analyzer)
    val query = parser.parse("text")
    val hits = iSearcher.search(query, 1000).scoreDocs

    hits.foreach { sd =>
      val hitDoc = iSearcher.doc(sd.doc)
      println(s"${hitDoc.get("id")}")

    }

    iReader.close()
  }
}
