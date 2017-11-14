import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import scala.math._
import scala.collection.mutable.PriorityQueue


object purandare_himanshu_clustering {
  def createDataMap(data:String): scala.collection.mutable.Map[(Float, Float, Float, Float), String] = {
	 val array = data.split(",")
	 val dataMap = scala.collection.mutable.Map[(Float, Float, Float, Float), String]((array(0).toFloat, array(1).toFloat, array(2).toFloat, array(3).toFloat) -> array(4).toString())
	return dataMap
	}
  def ret_dist(tup1:(Float, Float, Float, Float),tup2:(Float, Float, Float, Float)):Float = {
    return (Math.sqrt(Math.pow((tup1._1 - tup2._1 ),2).toFloat + Math.pow((tup1._2 - tup2._2),2).toFloat + Math.pow((tup1._3 - tup2._3 ),2).toFloat + Math.pow((tup1._4 - tup2._4 ),2).toFloat).toFloat)
  }
  
  def getCentroid(new_set:Set[(Float,Float,Float,Float)]):(Float,Float,Float,Float) = {
    //var Centroid = (0.0.toFloat,0.0.toFloat,0.0.toFloat,0.0.toFloat)
    var sz = new_set.size
    var cent_1 = 0.0.toFloat
    var cent_2 = 0.0.toFloat
    var cent_3 = 0.0.toFloat
    var cent_4 = 0.0.toFloat
    
    for (ii <- new_set) {
      cent_1 += ii._1
      cent_2 += ii._2
      cent_3 += ii._3
      cent_4 += ii._4
    }
    
    return ((cent_1/sz),(cent_2/sz),(cent_3/sz),(cent_4/sz)) 
  }
  
  
  def main(args: Array[String]): Unit = {
    var clustering_data = "/home/test/SampleApp/clustering_data.dat"
    var k = 3
    if (args.length != 2) {
      clustering_data = "/home/test/SampleApp/clustering_data.dat"
      k = 3
    }
    else {
      clustering_data = args(0)
      k = (args(1)).toInt
    }
    
    val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
      val sc = new SparkContext(conf)
      
      var dataLines = sc.textFile(clustering_data , 4).cache()
      var c_Data_ungrouped = dataLines.flatMap(x => createDataMap(x))
      var names_map = c_Data_ungrouped.collect.toMap
      var dup_list = scala.collection.mutable.ListBuffer[Set[(Float,Float,Float,Float)]]()
      var dup_count_map = scala.collection.mutable.Map[Set[(Float,Float,Float,Float)],Int]()
      var c_Data = c_Data_ungrouped.groupByKey
      //c_Data.foreach(f=>println(f))
      var c_Data_Collect = c_Data.collect
      c_Data_Collect.foreach(f=>
        if (f._2 .size>1) {
          dup_list += Set(f._1 )
          dup_count_map += (Set(f._1) -> (f._2.size - 1))
        }
      )
      var collected_c_data = c_Data.keys.collect
      var points = scala.collection.mutable.Set(collected_c_data:_*)
      var cluster = scala.collection.mutable.Map[(Float,Float,Float,Float),Set[(Float,Float,Float,Float)]]()
      for (ii <- points) {
        cluster += ii -> Set(ii) 
      }
    
      //def donutOrder(d: ((Float, Float, Float, Float), (Float, Float, Float, Float), Float)) = d._3
      def diff(t2: (((Float,Float,Float,Float),(Float,Float,Float,Float)),Float)) = t2._2
      //atta sathi
      var collected_data = c_Data.collect
      var dist_map=scala.collection.mutable.Map[((Float,Float,Float,Float),(Float,Float,Float,Float)),Float]()
      var diff_data_PQ : PriorityQueue[(((Float,Float,Float,Float),(Float,Float,Float,Float)),Float)]=PriorityQueue()(Ordering.by(diff).reverse)
      //var dist_map:scala.collection.mutable.Map[((Float,Float,Float,Float),(Float,Float,Float,Float)),Float]()
      for (ii <- 0 until (collected_c_data.size - 1)){
        for (jj <- (ii+1) until collected_c_data.size){
          var distance = ret_dist(collected_c_data(ii), collected_c_data(jj))
          diff_data_PQ += ((collected_c_data(ii), collected_c_data(jj))-> distance)
          dist_map += (collected_c_data(ii),collected_c_data(jj))->distance
        }
      }
    //collected_c_data.foreach(f=>println(f))
      //println(dist_map.size)
      var ct:Int = 1
      //main wala while loop
      var DS = scala.collection.mutable.Set[(Float,Float,Float,Float)]()
      while (cluster.size > k) {
        var dq_value = diff_data_PQ.dequeue
        var dq_set = Set[(Float, Float, Float, Float)](dq_value._1._1  ,dq_value._1 ._2 )
        while (!DS.intersect(dq_set).isEmpty) {
          dq_value = diff_data_PQ.dequeue
          dq_set = Set[(Float, Float, Float, Float)](dq_value._1._1  ,dq_value._1 ._2 )
        }
        DS += dq_value._1 ._1 
        DS += dq_value._1 ._2 
        
        var removed_1 = cluster.getOrElse(dq_value._1 ._1 ,Set[(Float, Float, Float, Float)]())
        var removed_2 = cluster.getOrElse(dq_value._1 ._2 ,Set[(Float, Float, Float, Float)]())
        
        cluster.remove(dq_value._1 ._1 )
        cluster.remove(dq_value._1 ._2 )
        var newSetToAdd =  removed_1.union(removed_2)
        var newCentroid = getCentroid(newSetToAdd)
        
        var newClusterKeys = cluster.keys
        for (ii <- newClusterKeys) {
          diff_data_PQ += (ii,newCentroid)-> ret_dist(ii,newCentroid)
        }
        cluster += newCentroid -> newSetToAdd
        
        
      }//While ends here
      var cluster_list = scala.collection.mutable.Map[(Float,Float,Float,Float),scala.collection.mutable.ListBuffer[(Float,Float,Float,Float)]]()
      for (xx<-cluster) {
        cluster_list += xx._1 -> xx._2 .to[scala.collection.mutable.ListBuffer] 
      }
      
      cluster.foreach(f=>{
        for (ii <- dup_list) {
          if(f._2 .intersect(ii).equals(ii)) {
            for (cnt <- 0 until dup_count_map(ii)){
              cluster_list(f._1 ) += ii.toList(0)
            }
          }
        }
      })
      //println(cluster_list.size)
      //cluster_list.foreach(f=>println(f._2 .size, f))
      
      var wrong_assigned = 0
      var FileName = "himanshu_purandare_cluster_"+k+".txt"
      val pw = new PrintWriter(new File(FileName))
      for (xx <- cluster_list.keys) {
        var findName = scala.collection.mutable.Map[String,Int]()
        for (ii <- cluster_list(xx)) {
          var Name = names_map.getOrElse(ii, "Some")
          findName(Name) = (findName.getOrElse(Name, 0) + 1) 
        }
        var maxString = findName.maxBy(_._2 )._1 
        for (dd<-findName.keys){
          if (!(dd == maxString)) {
            wrong_assigned += findName(dd)
          }
        }
        pw.write("cluster:"+maxString+"\n")
        for (ii<-cluster_list(xx)) {
          pw.write("[" + ii._1 + ", " + ii._2 + ", " + ii._3 + ", " + ii._4 + ", " + names_map.getOrElse(ii, "Some") + "]"+"\n")
        }
        pw.write("Number of points in this cluster:"+cluster_list(xx).size+"\n")
        pw.write("\n")
      }
      pw.write("Number of points wrongly assigned:"+wrong_assigned)
      pw.close 
  }

}