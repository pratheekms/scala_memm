import MEMM.MEMM

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD

import scala.io.Source
import java.io.{FileNotFoundException, IOException}
import scala.collection.mutable.ArrayBuffer

import util.Random

import org.apache.spark.mllib.optimization.SquaredL2Updater
import scala.collection.immutable.Set
import scala.util.{Try,Success,Failure}
import scala.util.control.Breaks._

object MEMMTester{

	val conf = new SparkConf().setAppName("Simple Application")
	val sc = new SparkContext(conf)
	val no_of_features : Int= 14

	var output_label_set = collection.mutable.Set[String]()

	def RDDFromText(fileName : String) : (RDD[Vector[String]], collection.mutable.Set[String], ArrayBuffer[Vector[String]]) = {
		var training_pairs = ArrayBuffer[Vector[String]]()
		// 
		//
		//
		//

		try {
			val bufferedSource = Source.fromFile(fileName)
			var word_count = 0
			var previous_word : String = null
			var previous_previous_word : String = null
			var next_word : String = null
			var next_next_word : String = null

			val lines = bufferedSource.getLines
			for (line <- bufferedSource.getLines) {
				var words = line.split(" ")
				
				var previous_tag :String = "*";
				var previous_previous_tag : String = "*";
				var word_index_in_sentence = 0
				val sentence_length = words.length

				for (word_tag <- words){
					val wt = word_tag.split("__")
					val word = wt(0)
					val tag = wt(1)

					output_label_set += tag

					if(word_index_in_sentence> 0){
						previous_word = words(word_index_in_sentence- 1).split("__")(0)
					}
					else{
						previous_word = "*"
					}
					if(word_index_in_sentence> 1){
						previous_previous_word = words(word_index_in_sentence- 2).split("__")(0)
					}
					else{
						previous_previous_word = "*"
					}

					if(word_index_in_sentence< sentence_length - 1){
						next_word = words(word_index_in_sentence+ 1).split("__")(0)
					}
					else{
						next_word = "*"
					}

					if(word_index_in_sentence< sentence_length - 2){
						next_next_word = words(word_index_in_sentence+ 2).split("__")(0)
					}
					else{
						next_next_word = "*"
					}
					
					val training_pair_x = Vector(word_count.toString, word_index_in_sentence.toString, previous_previous_word, previous_word, word, next_word, next_next_word, previous_tag, previous_previous_tag, tag)
					// println(training_pair_x.getClass)
					val training_pair_y = tag
					val temp = (tag, training_pair_x)

					training_pairs += training_pair_x
					previous_previous_tag = previous_tag
					previous_tag = tag
					word_index_in_sentence += 1
					word_count += 1

					// println(temp.getClass)

				}

			}
		}
		catch {
			case e: FileNotFoundException => println("Couldn't find that file.")
			case e: IOException => println("Got an IOException!")
		}
		val rdd = sc.parallelize(training_pairs)
		// println(rdd.getClass)


		return (rdd, output_label_set, training_pairs)
	}

	def get_features(label : String, context : Vector[String]) : Vector[Double] = {
		var features = new Array[Double](no_of_features)

// 	# check if first letter capital and current tag is NNP
	if(Character.isUpperCase(context(4)(0)) && (context(9)=="NNP" || context(9)=="NNPS")) {
		features(0) = 1.0
	}

// 	# check if first letter capital and previous tag is NNP
	if(Character.isUpperCase(context(4)(0)) && (context(7) == "NNP")){
		features(1) = 1.0
	}

// 	# previos tag is adjective and current tag is noun
	val adverb_tags = Set("JJ","JJR","JJS")
	val noun_tags = Set("NN","NNP","NNPS","NNS")
	if(adverb_tags.contains(context(7)) && noun_tags.contains(context(9))){
		features(2) = 1.0
	} 

// 	# previos tag is adverb and current tag is verb
	val verb_tags = Set("VBD","VBG","VBN","VBP","VBZ")
	if(adverb_tags.contains(context(7)) && verb_tags.contains(context(9))){
		features(3) = 1.0
	}

// 	prepositions = ["about","above","across","after","against","along","among","around","at","before","behind","below","beneath","beside","between","by","down","during","except","for","from","in","inside","instead of","into","like","near","of","off","on","onto","out of","outside","over","past","since","through","to","toward","under","underneath","until","up","upon","with","within","without"]
	val prepositions = Set("about","above","across","after","against","along","among","around","at","before","behind","below","beneath","beside","between","by","down","during","except","for","from","in","inside","instead of","into","like","near","of","off","on","onto","out of","outside","over","past","since","through","to","toward","under","underneath","until","up","upon","with","within","without")
	if(prepositions.contains(context(4).toLowerCase) && (context(9) == "IN")){
		features(4) = 1.0
	}

// 	# wh questions
	val wh_words = Set("WDT","WP","WRB")
	if(context(4)(0).toLower=='w' && context(4)(1).toLower=='h' && wh_words.contains(context(9))){
		features(5) = 1.0
	}

// conjunctions
	val conjunctions = Set("and","or","but","nor","so","for", "yet")
	if(conjunctions.contains(context(4).toLowerCase) && (context(9) == "CC")){
		features(6) = 1.0
	}

// 	# check if no
	try{
		context(4).toInt
		features(7) = 0.0
	}
	catch{
	  case _: Throwable => features(7) = 0.0
	}

// 	# check for determiners
	val determiners = Set("a","an","the","this","that","these","those","my","your","her","his","its","our","their")
	if(determiners.contains(context(4).toLowerCase) && context(9)=="DT"){
		features(8) = 1.0
	}

// 	# check for existential "there"
// 	# current word is "there" and next word is the one in list ["is","was","were","has"]
	val existential_there = Set("is","was","were","has")
	if(context(4).toLowerCase == "there" && existential_there.contains(context(5))){
		features(9) = 1.0
	}

// 	# personal pronoun
	val personal_pronoun = Set("i", "you", "he", "she", "it", "they", "we")
	if(personal_pronoun.contains(context(4)) && context(9) == "PRP"){
		features(10) = 1.0
	}

// word 'to'
	if(context(4).toLowerCase == "to" && context(9) == "TO"){
		features(11) = 1.0
	}

// 	# check if ascii, FW means foreign word
	// var flag = false
	// val punctuations = Set('.',',',';',':','?','\'','"')
	// for(i <- 0 to context(4).length-1){
	// 	val c = context(4)(i)
	// 	if(c.toLower <= 'z' && c.toLower >= 'a') {
	// 		flag = false
	// 	}
	// 	else{
	// 		if(!punctuations.contains(c)){
	// 		flag = true
	// 		}
	// 		// println(c)
	// 	}
	// }
	var flag = false
	for(i <- 0 to context(4).length-1){
		val c = context(4)(i)
		if(c > 127) {
			flag = true
		}
	}
	if(flag){
		features(12) = 1.0
	}
	

// 	# modal verb
 	val modal = Set("can", "could", "may", "might", "must", "ought", "shall", "should", "will", "would")
 	if(modal.contains(context(4)) && context(9) == "MD"){
 		features(13) = 1.0
 	}

	return features.toVector
	}


	def main(args: Array[String]) {
	val (data, label_set, data_raw) = RDDFromText("wsj_small.data")
	MEMM.train(get_features, data, data_raw, no_of_features, label_set, new SquaredL2Updater())
// Updaters are for reqularization. there are a couple of types.

	}

}	
