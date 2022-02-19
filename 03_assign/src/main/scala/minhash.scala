package similarity 

/*
Your student name
*/

import org.apache.spark.rdd.RDD

object minhash {

  def shingle_line(stList: List[String], shingleSize:Int):Shingled_Record = ???

  def minhash_record(r: Shingled_Record, hashFuncs: List[Hash_Func]): Min_Hash_Record = ???

  def compute_jaccard_pair(a:Shingled_Record, b: Shingled_Record): Similarity = ???

  def find_jaccard_matches(records: RDD[Shingled_Record], minSimilarity:Double): Matches = ???

  def find_minhash_matches(records:RDD[Min_Hash_Record], minSimilarity:Double): Matches = ???

  def find_lsh_matches(minHashes: RDD[Min_Hash_Record], minSimilarity: Double, bandSize: Int): Matches = ???

}


