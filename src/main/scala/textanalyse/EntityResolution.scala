package textanalyse

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class EntityResolution(sc: SparkContext, dat1: String, dat2: String, stopwordsFile: String, goldStandardFile: String) {

  val amazonRDD: RDD[(String, String)] = Utils.getData(dat1, sc)
  val googleRDD: RDD[(String, String)] = Utils.getData(dat2, sc)
  val stopWords: Set[String] = Utils.getStopWords(stopwordsFile)
  val goldStandard: RDD[(String, String)] = Utils.getGoldStandard(goldStandardFile, sc)

  var amazonTokens: RDD[(String, List[String])] = _
  var googleTokens: RDD[(String, List[String])] = _
  var corpusRDD: RDD[(String, List[String])] = _
  var idfDict: Map[String, Double] = _

  var idfBroadcast: Broadcast[Map[String, Double]] = _
  var similarities: RDD[(String, String, Double)] = _

  def getTokens(data: RDD[(String, String)]): RDD[(String, List[String])] = {

    /*
     * getTokens soll die Funktion tokenize auf das gesamte RDD anwenden
     * und aus allen Produktdaten eines RDDs die Tokens extrahieren.
     */

    // data (after collect()): Tuple[]: 0: _1 "b0006z" _2 "clickart 9500...", 1: ...
    val _stopWords = stopWords
    val temp = data.map(x => (x._1, EntityResolution.tokenize(x._2, _stopWords)))
    //val checkresult = temp.collect()
    temp
  }

  def countTokens(data: RDD[(String, List[String])]): Long = {

    /*
     * Zählt alle Tokens innerhalb eines RDDs
     * Duplikate sollen dabei nicht eliminiert werden
     */

    //    deterministic
    //    var test = 0
    //    val _data = data.collect()
    //    val temp0 = _data.map(x => {
    //      test += x._2.size
    //      x
    //    })
    //    temp0

    //    with accu (not serializable)
    //    val _data = data
    //    val _sc = sc
    //    val list = _data.map(x => x._2.size)
    //    val counteraccu = _sc.accumulator(0)
    //
    //    def count(element:Int, counter:Accumulator[Int]):Unit={
    //      counter += element
    //    }
    //
    //    val temp1 = list.foreach(count(_,counteraccu))
    //    temp1

    val temp2 = data.map(x => x._2.size)
    val temp3 = temp2.sum()
    temp3.toInt
  }

  def findBiggestRecord(data: RDD[(String, List[String])]): (String, List[String]) = {

    /*
     * Findet den Datensatz mit den meisten Tokens
     */
    val temp0 = data.map(x => (x._1, x._2, x._2.size))
    val temp1 = data.map(x => x._2.size).max()
    val temp2 = temp0.filter(x => x._3 == temp1)
    val temp3 = temp2.map(x => (x._1, x._2))
    temp3.first()

//    /**/
    // data.sortBy(data => -data._2.size).first
/**/
  }

  def createCorpus = {

    /*
     * Erstellt die Tokenmenge für die Amazon und die Google-Produkte
     * (amazonRDD und googleRDD), vereinigt diese und speichert das
     * Ergebnis in corpusRDD
     */

    corpusRDD = getTokens(amazonRDD).union(getTokens(googleRDD))

  }

  def calculateIDF = {

    /*
     * Berechnung des IDF-Dictionaries auf Basis des erzeugten Korpus
     * Speichern des Dictionaries in die Variable idfDict
     */

//        val size = corpusRDD.count()
//        val wordHowManyTimesHappenInDoc1 = allWordsWithoutDubplicates.groupBy(x => x)
//        val wordHowManyTimesHappen2 = wordHowManyTimesHappen1.map(x => (x._1, x._2.size))
//        val result = wordHowManyTimesHappen2.map(x => (x._1, x._2.toDouble / size.toDouble))
//        val checkresult = wordHowManyTimesHappen1.collect()
//        val checkresult2 = wordHowManyTimesHappen2.collect()
//        val checkresult3 = result.collect()
//        idfDict = result.collect().toMap

    val corp = this.corpusRDD
    val size = this.corpusRDD.count()

    val temp0 = this.corpusRDD.map(x => x._2.distinct)
    val temp1 = temp0.flatMap(x => x)
    val temp2 = temp1.groupBy(identity)
    val temp3 = temp2.mapValues(_.size)
    val temp4 = temp3.collect().toMap.map(x => (x._1, size.toDouble / x._2.toDouble))

    val checkResult1 = temp1.collect()
    val checkResult2 = temp2.collect()
    val checkResult3 = temp3.collect()
    val checkResult4 = temp4

    idfDict = temp4

  }


  def simpleSimimilarityCalculation: RDD[(String, String, Double)] = {

    /*
     * Berechnung der Document-Similarity für alle möglichen 
     * Produktkombinationen aus dem amazonRDD und dem googleRDD
     * Ergebnis ist ein RDD aus Tripeln bei dem an erster Stelle die AmazonID
     * steht, an zweiter die GoogleID und an dritter der Wert
     */
    // amazonRDD: RDD of (b000jz4hqo,clickart 950 000 - premier image pack (dvd-rom)  "broderbund")
    // googleRDD: RDD of (http://www.google.com/base/feeds/snippets/11448761432933644608,spanish vocabulary builder "expand your vocabulary! contains fun lessons that both teach and entertain you'll quickly find yourself mastering new terms. includes games and more!" )
    // cartesianRDD: RDD of ((b000jz4hqo,clickart 950 000 - premier image pack (dvd-rom)  "broderbund"),(http://www.google.com/base/feeds/snippets/11448761432933644608,spanish vocabulary builder "expand your vocabulary! contains fun lessons that both teach and entertain you'll quickly find yourself mastering new terms. includes games and more!" ))
    val _amazonRDD = amazonRDD
    val _googleRDD = googleRDD
    val _computeSimilarity = EntityResolution.computeSimilarity(_: ((String, String), (String, String)), _: Map[String, Double], _: Set[String])
    val _stopWords = stopWords
    val _idfDict = idfDict
    val cartesianRDD = _amazonRDD.cartesian(_googleRDD)

    val result = cartesianRDD.map(x => {
      _computeSimilarity(x, _idfDict, _stopWords)
    })

    //    val temp = result.collect()
    result
  }

  def findSimilarity(vendorID1: String, vendorID2: String, sim: RDD[(String, String, Double)]): Double = {

    /*
     * Funktion zum Finden des Similarity-Werts für zwei ProduktIDs
     */

    val temp = sim.filter(x => {
      x._1 == vendorID1 && x._2 == vendorID2
    })

    temp.first()._3
  }

  def simpleSimimilarityCalculationWithBroadcast: RDD[(String, String, Double)] = {

    // amazonRDD: RDD of (b000jz4hqo,clickart 950 000 - premier image pack (dvd-rom)  "broderbund")
    // googleRDD: RDD of (http://www.google.com/base/feeds/snippets/11448761432933644608,spanish vocabulary builder "expand your vocabulary! contains fun lessons that both teach and entertain you'll quickly find yourself mastering new terms. includes games and more!" )
    // cartesianRDD: RDD of ((b000jz4hqo,clickart 950 000 - premier image pack (dvd-rom)  "broderbund"),(http://www.google.com/base/feeds/snippets/11448761432933644608,spanish vocabulary builder "expand your vocabulary! contains fun lessons that both teach and entertain you'll quickly find yourself mastering new terms. includes games and more!" ))
    val _sc = sc
    val _amazonRDD = amazonRDD
    val _googleRDD = googleRDD
    val _computeSimilarity = EntityResolution.computeSimilarityWithBroadcast(_: ((String, String), (String, String)), _: Broadcast[Map[String, Double]], _: Set[String])
    val _stopWords = stopWords
    val _idfDict = idfDict
    val idfBroadcast = sc.broadcast(_idfDict)
    val cartesianRDD = _amazonRDD.cartesian(_googleRDD)

    val result = cartesianRDD.map(x => {
      _computeSimilarity(x, idfBroadcast, _stopWords)
    })

    //        val temp = result.collect()
    result

  }

  /*
   *
   * 	Gold Standard Evaluation
   */

  //  def evaluateModel(goldStandard: RDD[(String, String)]): (Long, Double, Double) = {
  //
  //    /*
  //     * Berechnen Sie die folgenden Kennzahlen:
  //     *
  //     * Anzahl der Duplikate im Sample
  //     * Durchschnittliche Consinus Similaritaet der Duplikate
  //     * Durchschnittliche Consinus Similaritaet der Nicht-Duplikate
  //     *
  //     *
  //     * Ergebnis-Tripel:
  //     * (AnzDuplikate, avgCosinus-SimilaritätDuplikate,avgCosinus-SimilaritätNicht-Duplikate)
  //     */
  //
  //    // goldStandard: RDD (1300 elements) of (b00004tkvy http://www.google.com/base/feeds/snippets/18441110047404795849,gold) // without comma, one string
  //
  //    val rddGoldSplitted = goldStandard.map(x => {
  //      val e = x._1.split(" ")
  //      (e(0), e(1))
  //    })
  //
  //    val _amazonRDD = amazonRDD
  //    val _googleRDD = googleRDD
  //    val cartesianRDD = _amazonRDD.cartesian(_googleRDD)
  //    val cartesianRDD2 = cartesianRDD.map(x => (x._1._1, x._2._1))
  //
  //    val temp1 = cartesianRDD2.subtract(goldStandard).collect()
  //???
  //  }

  def evaluateModel(goldStandard: RDD[(String, String)]): (Long, Double, Double) = { //fare anche qua con cartesisches produkt spark

    /*
     * Berechnen Sie die folgenden Kennzahlen:
     *
     * Anzahl der Duplikate im Sample
     * Durchschnittliche Consinus Similaritaet der Duplikate
     * Durchschnittliche Consinus Similaritaet der Nicht-Duplikate
     *
     *
     * Ergebnis-Tripel:
     * (AnzDuplikate, avgCosinus-SimilaritätDuplikate,avgCosinus-SimilaritätNicht-Duplikate)
     */

    val joinedDocumentsSimilarities = simpleSimimilarityCalculation.map(x => (x._1 + " " + x._2, x._3))
      .leftOuterJoin(goldStandard)
//
//    //ACCUMULATORS:
//    val plagiarismSim = sc.doubleAccumulator("plagSim")
//    val plagiarismCount = sc.longAccumulator("plagCount")
//
//    val notPlagiarismSim = sc.doubleAccumulator("notPlagSim")
//    val notPlagiarismCount = sc.doubleAccumulator("notPlagCount")
//
//    joinedDocumentsSimilarities.foreach(x =>
//      x match {
//        case (x, (y, Some(z))) => plagiarismSim.add(y); plagiarismCount.add(1)
//        case (x, y) => notPlagiarismSim.add(y._1); notPlagiarismCount.add(1)
//      })
//
//    val res = (plagiarismCount.value.toLong, plagiarismSim.value / plagiarismCount.value, notPlagiarismSim.value / notPlagiarismCount.value)
//    res //(146, 0.22603425561949433, 0.0012149319829381333)

    val dups = joinedDocumentsSimilarities.filter(x => {
      x._2._2 match {
        case None => false
        case _=> true
      }
    })

    val noDups = joinedDocumentsSimilarities.filter(x => {
      x._2._2 match {
        case None => true
        case _=> false
      }
    })

    val res2 = (dups.count(), dups.map(x => x._2._1).mean(), noDups.map(x => x._2._1).mean())

    res2 //(146, 0.22603425561949433, 0.0012149319829381333)
  }
}

object EntityResolution {

  def tokenize(s: String, stopws: Set[String]): List[String] = {
    /*
   	* Tokenize splittet einen String in die einzelnen Wörter auf
   	* und entfernt dabei alle Stopwords.
   	* Verwenden Sie zum Aufsplitten die Methode Utils.tokenizeString
   	*/

    val temp0 = Utils.tokenizeString(s)
    val temp1 = temp0.filter(x => !stopws.contains(x))
    temp1
  }

  def getTermFrequencies(tokens: List[String]): Map[String, Double] = {

    /*
     * Berechnet die Relative Haeufigkeit eine Wortes in Bezug zur
     * Menge aller Wörter innerhalb eines Dokuments 
     */

    // tokens: List[String]: "this", "is", "test", "test"...
    // result: Map("test" -> 0.4, "this" -> 0.2, "is" -> 0.2, "another" -> 0.1, "and" -> 0.1)
    val size = tokens.size

    val temp0 = tokens
      .groupBy(x => x) // (test,List(test, test, test, test))
      .map(x => (x._1, x._2.length))
      .map(x => (x._1, x._2.toDouble / size.toDouble))

    temp0

  }

  def computeSimilarity(record: ((String, String), (String, String)), idfDictionary: Map[String, Double], stopWords: Set[String]): (String, String, Double) = {

    /*
     * Bererechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem 
     * Sie die erforderlichen Parameter extrahieren
     */

    //      val one = calculateDocumentSimilarity(record._1._1, record._1._2, idfDictionary, stopWords)
    //      val two = calculateDocumentSimilarity(record._2._1, record._2._2, idfDictionary, stopWords)
    //      if (one >= two) {
    //        (record._1._1, record._1._2, one)
    //      } else {
    //        (record._2._1, record._2._2, two)
    //      }

    // amazonRDD: RDD of (b000jz4hqo,clickart 950 000 - premier image pack (dvd-rom)  "broderbund")
    // googleRDD: RDD of (http://www.google.com/base/feeds/snippets/11448761432933644608,spanish vocabulary builder "expand your vocabulary! contains fun lessons that both teach and entertain you'll quickly find yourself mastering new terms. includes games and more!" )
    // Hint: you dont need to tokenize it here it will done in calculateDocumentSimilarity function
    val result = (record._1._1, record._2._1, calculateDocumentSimilarity(record._1._2, record._2._2, idfDictionary, stopWords))
    result
  }

  def calculateTF_IDF(terms: List[String], idfDictionary: Map[String, Double]): Map[String, Double] = {

    /* 
     * Berechnung von TF-IDF Wert für eine Liste von Wörtern
     * Ergebnis ist eine Mapm die auf jedes Wort den zugehörigen TF-IDF-Wert mapped
     */
    //
    //    val size = terms.size
    //    val temp0 = terms
    //      .groupBy(x => x) // (test,List(test, test, test, test))
    //      .map(x => (x._1, x._2.length))
    //      .map(x => (x._1, x._2.toDouble / size.toDouble))
    //
    //    temp0

    // erste lösung
//    val tf = getTermFrequencies(terms).toList // List: (customizing,0.16666666666666666), (2007,0.16666666666666666), ...
//    val tfwords = tf.map(x => x._1).toList // List: "customizing", "2007", ...
//    // idfDictionary: HashMap: (serious,400.0), (boutiques,400.0), (breaks,400.0)
//
//    val temp = idfDictionary.map(x => if (tfwords.contains(x._1)) (x._1, x._2 * tf.find(z => z._1 == x._1).get._2) else (x._1, x._2 * 0)) // HashMap: (customizing,16.666666666666664), (2007,3.5087719298245617), ...
//    val result = temp.filter(_._2 != 0)
//    result // HashMap: (customizing,16.666666666666664), (2007,3.5087719298245617), (interface,3.0303030303030303)...

  // zweite lösung
    getTermFrequencies(terms).map(tf => (tf._1, tf._2 * idfDictionary(tf._1)))
  }

  def calculateDotProduct(v1: Map[String, Double], v2: Map[String, Double]): Double = {

    /*
     * Berechnung des Dot-Products von zwei Vectoren
     */

    // erste Lösung
//    val keys = v1.keys.toSet.union(v2.keys.toSet)
//    val products = keys.map(k => v1.getOrElse(k, 0.0) * v2.getOrElse(k, 0.0))
//    products.sum

    // zweite lösung
//    v1.keys.toSet.intersect(v2.keys.toSet).map(key => v1(key) * v2(key)).sum

    // dritte 2
      v1.map { case (k, v) => (k, v * v2.getOrElse(k, .0)) }.values.sum
  }

  def calculateNorm(vec: Map[String, Double]): Double = {

    /*
     * Berechnung der Norm eines Vectors
     */

    Math.sqrt(((for (el <- vec.values) yield el * el).sum))
  }

  def calculateCosinusSimilarity(doc1: Map[String, Double], doc2: Map[String, Double]): Double = {

    /* 
     * Berechnung der Cosinus-Similarity für zwei Vectoren
     */

    val dotProd = calculateDotProduct(doc1, doc2)
    val multi = calculateNorm(doc1) * calculateNorm(doc2)
    dotProd / multi
  }

  def calculateDocumentSimilarity(doc1: String, doc2: String, idfDictionary: Map[String, Double], stopWords: Set[String]): Double = {
    /*
     * Berechnung der Document-Similarity für ein Dokument
     */

    val doc1Tokenized = tokenize(doc1, stopWords)
    val doc2Tokenized = tokenize(doc2, stopWords)

    val doc1IDF = calculateTF_IDF(doc1Tokenized, idfDictionary)
    val doc2IDF = calculateTF_IDF(doc2Tokenized, idfDictionary)

    val cos = calculateCosinusSimilarity(doc1IDF, doc2IDF)
    cos

  }

  def computeSimilarityWithBroadcast(record: ((String, String), (String, String)), idfBroadcast: Broadcast[Map[String, Double]], stopWords: Set[String]): (String, String, Double) = {

    /*
     * Bererechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem 
     * Sie die erforderlichen Parameter extrahieren
     * Verwenden Sie die Broadcast-Variable.
     */

    // idfDictionary: Map[String, Double]
    val result = (record._1._1, record._2._1, calculateDocumentSimilarity(record._1._2, record._2._2, idfBroadcast.value, stopWords))
    result

  }
}
