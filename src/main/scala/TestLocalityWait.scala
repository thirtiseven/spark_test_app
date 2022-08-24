/* SimpleApp.scala */
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}
import org.apache.spark.sql.SparkSession

//../../projects/apache/spark/bin/spark-submit  --class "TestLocalityWait" --master spark://localhost:7077 --conf spark.executor.instances=4 --conf spark.executor.cores=2 target/scala-2.12/simple-project_2.12-1.0.jar
object TestLocalityWait {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Test Locality Wait Application").getOrCreate()

    //executor_[hostname]_[executorid]
    val preferredLocations = Seq("executor_localhost_0")
    new PreferenceAwareRDD(spark.sparkContext, Seq.empty, preferredLocations, 1000, 100)
      .mapPartitions(s => {
        Thread.sleep(1000)
        s
      }).count()
    spark.stop()
  }
}

class PreferenceAwareRDD(sparkContext: SparkContext, deps:Seq[Dependency[String]], preferredLocations: Seq[String], numPartitions: Int, numRecordsPerPartition: Int)
  extends RDD[String](sparkContext, deps) {

  @DeveloperApi
  override def compute(partition: Partition, context: TaskContext): Iterator[String] = {

    List.range(0, numRecordsPerPartition).map(i => {
      s"partition${partition.index}:records${i}"
    }).iterator
  }

  override protected def getPartitions: Array[Partition] = {
    List.range(0, numPartitions).map(i => {
      val x: Partition = DefaultPartition(i)
      x
    }).toArray
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = preferredLocations

  case class DefaultPartition(indx: Int) extends Partition {
    override def index: Int = indx
  }
}
