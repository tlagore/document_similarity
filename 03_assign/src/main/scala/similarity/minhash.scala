package similarity

import org.apache.spark.rdd.RDD

/**
 * Perform Jaccard, minhash, and lsh hash similarity measures for documents
 *
 * @author Tyrone Lagore (V00995698)
 */
object minhash {

  /**
   * Given a line, create a shingle out of it
   * @param stList list of tokens, including a prepended id
   * @param shingleSize Size of the shingle we want to use for similarity matching
   * @return
   */
  def shingle_line(stList: List[String], shingleSize: Int): Shingled_Record = {
    if (stList.isEmpty)
      throw new IllegalArgumentException("stList cannot be empty")

    val id = stList.head

    val (count, records) = stList.slice(1,stList.length).sliding(shingleSize).foldLeft((0, Set[Int]()))((op, shingle) => {
      (op._1 + 1, op._2 +  utils.hash_string_lst(shingle))
    })

    val returnRecords = if (records.nonEmpty) Option[Set[Int]](records) else None

    Shingled_Record(id, count, returnRecords)
  }

  /**
   * Given a list of hash functions and a shingled record, return a list of minimum hashes of each shingle such that
   * element i is min(hashFuncs(shingle_i))
   * @param r Shingled_Record to hash
   * @param hashFuncs list of hash functions to use for minhash
   * @return Min_Hash_Record describing the hashed shingle
   */
  def minhash_record(r: Shingled_Record, hashFuncs: List[Hash_Func]): Min_Hash_Record = {
    r.shingles match {
      case Some(shingles) =>
        val minHashes:Vector[Int] = hashFuncs.foldLeft(Vector[Int]())((op, el) => {
          op :+ shingles.map(el).min
        })

        Min_Hash_Record(r.id, minHashes)
      case _ => Min_Hash_Record(r.id, Vector[Int]())
    }
  }

  /**
   * Compute a Jaccard pair's similarity
   * @param a Shingled_Record to compare
   * @param b Shingled_Record to compare
   * @return Similarity score
   */
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

  /**
   * Find all Jaccard matches that meet minSimilarity threshold given RDD of Shingled Records
   * @param records Shingled records to check for similarity
   * @param minSimilarity minimum similarity to be considered a match
   * @return Matches for Jaccard similarity
   */
  def find_jaccard_matches(records: RDD[Shingled_Record], minSimilarity: Double): Matches = {
    val cartesianFiltered = records.cartesian(records).filter(el => el._1.id < el._2.id)

    cartesianFiltered.aggregate(Array[Similarity]())(
      (accumulator, el) => {
        val score = compute_jaccard_pair(el._1, el._2)
        if (score.sim >= minSimilarity) accumulator :+ score else accumulator
      },
      (l1,l2) => l1++l2
    )
  }

  /**
   * Calculate min hash similarity score
   * @param a Min_Hash_Record to compare
   * @param b Min_Hash_Record to compare
   * @return Similarity score
   */
  def min_hash_similarity(a: Min_Hash_Record, b: Min_Hash_Record): Similarity = {
    val sameEls = a.minHashes.zip(b.minHashes).foldLeft(0)((op, pair) =>
        if (pair._1 == pair._2) op + 1 else op
      )
    val similarity = sameEls.toDouble / a.minHashes.length.toDouble
    Similarity(a.id, b.id, similarity)
  }

  /**
   * Given a set of Min_Hash_Records, find matches for a minimum similarity
   * @param records RDD of Min_Hash_Records to check similarity against
   * @param minSimilarity minimum _similarity to be considered a match
   * @return Matches for minhash similarity
   */
  def find_minhash_matches(records: RDD[Min_Hash_Record], minSimilarity: Double): Matches = {
    val cartesianFiltered = records.cartesian(records).filter(el => el._1.id < el._2.id)

    cartesianFiltered.aggregate(Array[Similarity]())(
      (accumulator, el) => {
        val similarity = min_hash_similarity(el._1, el._2)
        if (similarity.sim >= minSimilarity)
          accumulator :+ similarity
        else
          accumulator
      },
      (l1, l2) => l1++l2
    )
  }

  /**
   * Creates an lsh hash record from a Min_Hash_Record by sliding a window of size bandSize
   * across the min_hashes and hashing these into a single integer for each.
   *
   * The result is a much smaller list of hashed hashes
   * @param minHashRecord the Min_Hash_Record of the shingle
   * @param bandSize the number of hashed shingles we want to hash into a single slot
   * @return Min_Hash_Record with reduced number of hashes
   */
  def create_lsh_record(minHashRecord: Min_Hash_Record, bandSize: Int): Min_Hash_Record = {
    val lshHashes = minHashRecord.minHashes.sliding(bandSize, bandSize).foldLeft(Vector[Int]())(
      (op, el) => {
        op :+ utils.hash_int_lst(el.toList)
      }
    )
    Min_Hash_Record(minHashRecord.id, lshHashes)
  }

  /**
   * Given an RDD of Min_Hash_Record, convert them to LSH records and compute similarity
   * @param minHashes minHashes to compute similarity matches for
   * @param minSimilarity minimum similarity to be considered a match
   * @param bandSize number of hashed shingles we want to hash into a single slot for the lsh record
   * @return Matches for lsh similarity
   */
  def find_lsh_matches(minHashes: RDD[Min_Hash_Record], minSimilarity: Double, bandSize: Int): Matches = {
    find_minhash_matches(
      minHashes.map(record => create_lsh_record(record, bandSize)).persist(),
      minSimilarity
    )
  }
}

