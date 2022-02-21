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

  def minhash_record(r: Shingled_Record, hashFuncs: List[Hash_Func]): Min_Hash_Record = {
    Min_Hash_Record("id", Vector[Int]())
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
    records.cartesian(records).filter((el) => el._1 != el._2).aggregate(Array[Similarity]())(
      (accumulator, el) => {
        val score = compute_jaccard_pair(el._1, el._2)
        if (score.sim > minSimilarity) accumulator :+ score else accumulator
      },
      (l1,l2) => l1++l2
    )
  }

  def find_minhash_matches(records: RDD[Min_Hash_Record], minSimilarity: Double): Matches = {
    new Array[Similarity](1)
  }

  def find_lsh_matches(minHashes: RDD[Min_Hash_Record], minSimilarity: Double, bandSize: Int): Matches = {
    new Array[Similarity](1)
  }
}
