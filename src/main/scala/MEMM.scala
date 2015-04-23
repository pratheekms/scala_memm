package MEMM;

import org.apache.spark.rdd.RDD

import scala.math
import util.Random

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS}
import breeze.math.MutableInnerProductModule

import org.apache.spark.Logging
// import org.apache.spark.mllib.linalg.{Vector, Vectors}
// import MEMM.BLAS
import org.apache.spark.mllib.optimization.Updater
import scala.collection.mutable.{Map => MMap}


class Gradient(get_features: (String, Vector[String]) => Array[Double], 
			training : RDD[(String,Vector[String])],
			no_of_features : Int,
			label_set : collection.mutable.Set[String]) {

    def compute(data: RDD[Vector[Any]], label: String, weights: Vector[Double], 
    	cumGradient: Vector[Double]): Double = {

    	return Random.nextDouble()
    }

    def get_cost(param : Vector[Double]) : Double = {
    	return Random.nextInt(500)
    }

    def get_gradient(param : Vector[Double]) : Vector[Double] = {
    	var grad = new Array[Double](no_of_features)
		for (i <- 1 to no_of_features){
			grad(i) = Random.nextDouble()
		}
		return grad.toVector
    }

}


object HelperFunctions{
	def helper_cost(context : Vector[String]) : (Double, Double) = {

		return (Random.nextDouble(), Random.nextDouble())
		}
	
	val lambda_func_sum = (a:(Double,Double), b:(Double, Double)) => (a._1+b._1, a._2+b._2)

	var all_input_features = ArrayBuffer[Vector[Double]]()
	var all_input_tag_combination_features =  MMap[String,ArrayBuffer[Vector[Double]]]().withDefaultValue(new ArrayBuffer[Vector[Double]]())

	def gradient_preprocess(data_raw : ArrayBuffer[Vector[String]], 
		label_set : collection.mutable.Set[String],
		get_features: (String, Vector[String]) => Vector[Double]) : Unit = {
		data_raw.foreach{x =>
			println(x)
			label_set.iterator.foreach{y =>
				var features = all_input_tag_combination_features(y)
				val current_input_tag_features = get_features(y, x)
				features += current_input_tag_features
				all_input_tag_combination_features(y) = features
				if(x(9) == y){
					all_input_features += current_input_tag_features
					println(current_input_tag_features)
					println("---")
				}
			}

		}

	}

}

// def gradient_preprocess1(input_data,tags):
// 	print 'Preprocessing for gradient'
// 	global all_input_features, all_input_tag_combination_features
// 	all_input_tag_combination_features = {}

// 	for i,x in enumerate(input_data):
// 		for y in tags:
// 			t = (x['i'],x['index'],x['t_minus_one'],x['t_minus_two'],x['tag'],x['wn'])
// 			features = all_input_tag_combination_features.get(y, [])
// 			current_input_tag_features = get_features(t, y)
// 			features.append(current_input_tag_features)
// 			all_input_tag_combination_features[y] = features
// 			if (x['tag'] == y):
// 				all_input_features.append(current_input_tag_features)
	
// 	for k, v in all_input_tag_combination_features.items():
// 		all_input_tag_combination_features[k] = numpy.array(v)

// 	all_input_features_broadcast = sc.broadcast(numpy.array(all_input_features))
// 	all_input_tag_combination_features = sc.broadcast(all_input_tag_combination_features)
// 	print 'Done'
// 	return
class CostGrad(dataset : RDD[Vector[String]]) extends DiffFunction[BDV[Double]] {

	override def calculate(param: BDV[Double]): (Double, BDV[Double]) = {
		
		var features = new Array[Double](10)

		val (expected, emperical) = dataset.map(HelperFunctions.helper_cost).reduce(HelperFunctions.lambda_func_sum)
		
		(expected - emperical, BDV(features))
	}
}


class MEMM(get_features: (String, Vector[String]) => Vector[Double], 
	dataset : RDD[Vector[String]],
	data_raw : ArrayBuffer[Vector[String]],
	no_of_features : Int) {
		
		var parameters : Array[Int] = new Array[Int](no_of_features);
		var input_sequence = dataset
		// var output_labels = tags
		// var output_label_set  = input_sequence.countApproxDistinctByKey(0.5)

			
}

object MEMM {
		  def train(get_features: (String, Vector[String]) => Vector[Double], 
			training : RDD[Vector[String]],
			data_raw : ArrayBuffer[Vector[String]],
			no_of_features : Int,
			label_set : collection.mutable.Set[String],
			updater : Updater): Unit = {
		
		val memm = new MEMM(get_features, training, data_raw, no_of_features)
		// println(memm.output_label_set)
    	// val test = Vectors.fromBreeze(weights)
		HelperFunctions.gradient_preprocess(data_raw, label_set, get_features)
		// println (HelperFunctions.all_input_features)
		val numCorrections = 10
		val convergenceTol = 1e-4
		val maxNumIterations = 20
		val regParam = 0.1
		val initialWeights = new Array[Double](no_of_features)

		val lbfgs = new BreezeLBFGS[BDV[Double]](maxIter=100, m=3, tolerance=convergenceTol)
		
		val param = new Array[Double](10)
		lbfgs.minimize(new CostGrad(training), BDV(param))
		// (space : MutableInnerProductModule[Vector[Double], Double])

		// println(CostGrad)
		
		}
	}
