package similarity

import org.apache.spark.rdd.RDD

object minhash {

  def shingle_line(stList: List[String], shingleSize: Int): Shingled_Record = {
    if (stList.isEmpty)
      throw new IllegalArgumentException("stList cannot be empty")

    val id = stList(0)

    val (count, records) = stList.slice(1,stList.length).sliding(shingleSize).foldLeft((0, Set[Int]()))((op, shingle) => {
      (op._1 + 1, op._2 +  utils.hash_string_lst(shingle))
    })

    val returnRecords = if (records.size > 0) Option[Set[Int]](records) else None

    Shingled_Record(id, count, returnRecords)
  }

  /**
   * Given a list of hash functions, return a list of minimum hashes such that
   * element i is the hash_function return with the lowest hash
   * @param r
   * @param hashFuncs
   * @return
   */
  def minhash_record(r: Shingled_Record, hashFuncs: List[Hash_Func]): Min_Hash_Record = {
    r.shingles match {
      case Some(shingles) =>
        val minHashes:Vector[Int] = hashFuncs.foldLeft(Vector[Int]())((op, el) => {
          op :+ (shingles.map(el).min)
        })

        Min_Hash_Record(r.id, minHashes)
      case _ => Min_Hash_Record(r.id, Vector[Int]())
    }
  }

  def compute_jaccard_pair(a: Shingled_Record, b: Shingled_Record): Similarity = {
    (a.shingles, b.shingles) match {
      case (Some(records_a), Some(records_b)) => Similarity(
        a.id,
        b.id,
        records_a.intersect(records_b).size.toFloat / records_a.union(records_b).size
      )
      case _ => Similarity(a.id, b.id, 0)
    }
  }

  def find_jaccard_matches(records: RDD[Shingled_Record], minSimilarity: Double): Matches = {
    val matches = records.cartesian(records).filter(el => el._1 != el._2).aggregate(Array[Similarity]())(
      (accumulator, el) => {
        val score = compute_jaccard_pair(el._1, el._2)
        if (score.sim > minSimilarity) accumulator :+ score else accumulator
      },
      (l1,l2) => l1++l2
    )

    filter_duplicates(matches)
  }

  def min_hash_similarity(a: Min_Hash_Record, b: Min_Hash_Record): Similarity = {
    val sameEls = a.minHashes.zip(b.minHashes).foldLeft(0)((op, pair) =>
        if (pair._1 == pair._2) op + 1 else op
      )
    val similarity:Float = sameEls.toFloat / a.minHashes.length
    Similarity(a.id, b.id, similarity)
  }

  def filter_duplicates(records: Matches): Matches = {
    // filter out double pairs. I.e. if we have (a,b), don't include (b,a)

    val (retMatches, _) = records.foldLeft((Array[Similarity](), Set[(String, String)]()))((op, el) => {
      if (op._2.contains((el.idA, el.idB)) || op._2.contains((el.idB, el.idA)))
        op
      else
        (op._1 :+ el, op._2 + ((el.idA, el.idB)))
    })

    retMatches
  }

  def find_minhash_matches(records: RDD[Min_Hash_Record], minSimilarity: Double): Matches = {
    val matches = records.cartesian(records).filter(el => el._1 != el._2).aggregate(Array[Similarity]())(
      (accumulator, el) => {
        if (el._1.id == "t1088" && el._2.id == "t5015")
          println("here")
        val similarity = min_hash_similarity(el._1, el._2)
        if (similarity.sim > minSimilarity)
          accumulator :+ similarity
        else
          accumulator
      },
      (l1, l2) => l1++l2
    )

    filter_duplicates(matches)
  }

  def find_lsh_matches(minHashes: RDD[Min_Hash_Record], minSimilarity: Double, bandSize: Int): Matches = {x
    Array[Similarity]()
  }
}

