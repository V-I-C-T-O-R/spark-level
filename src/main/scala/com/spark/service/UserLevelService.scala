package com.spark.service

import javax.persistence.EntityManager

import com.spark.entity.DataLevelQueryEntity
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

/**
 * Created by Victor on 17-6-2.
 */
@Service
class UserLevelService @Autowired()(val entityManager: EntityManager) {
    val logger = LoggerFactory.getLogger(this.getClass())

    @Value("${spark.sqlStringLevel}")
    private val sql: String = null
    @Value("${level.numClusters}")
    private val levelNumClusters: Int = 0
    @Value("${level.numIterations}")
    private val levelNumIterations: Int = 0
    @Value("${level.runTimes}")
    private val levelRunTimes: Int = 0

    @Transactional
    def schedulCluster(): Unit = {
        queryData
    }

    def queryData() {
        logger.info("=======start to query data for user level kmeans =======")
        val queryList = JavaConversions.asScalaBuffer(entityManager.createNativeQuery(sql, classOf[DataLevelQueryEntity]).getResultList).toArray.asInstanceOf[Array[AnyRef]]
        doSomething(queryList)
        logger.info("Spark MLlib K-means clustering finished.")
    }

    def doSomething(queryList: Array[AnyRef]): Unit = {
        val realArray = new ArrayBuffer[DataLevelQueryEntity]
        queryList.foreach(e => {
            val query = e.asInstanceOf[DataLevelQueryEntity]
            realArray +=query
        })
        doKmeans(realArray.toArray)
    }

    def doKmeans(queryDatas: Array[DataLevelQueryEntity]) {
        val conf = new SparkConf().setAppName("K-Means Clustering").setMaster("local").set("spark.executor.memory", "1g") .set("spark.driver.allowMultipleContexts","true")
        val sc = new SparkContext(conf)
        logger.info("config spark context")
        val rddOrg = new ArrayBuffer[org.apache.spark.mllib.linalg.Vector]
        queryDatas.foreach(e => {
            val a = new ArrayBuffer[Double]
            a += e.getRValue
            a += e.getFValue
            a += e.getMValue
            rddOrg.append(Vectors.dense(a.toArray))
        })
        logger.info("make spark RDD")
        val parsedTrainingData = sc.makeRDD(rddOrg)

        var clusterIndex: Int = 0
        val clusters: KMeansModel =
            KMeans.train(parsedTrainingData, levelNumClusters, levelNumIterations, levelRunTimes)

        logger.info("Cluster Number:" + clusters.clusterCenters.length)
        logger.info("Cluster Centers Information Overview:")
        clusters.clusterCenters.foreach(
            x => {
                logger.info("Center Point of Cluster " + clusterIndex + ":")
                clusterIndex += 1
            })

        val resultMap = scala.collection.mutable.Map(0 -> Map("num" -> 0.0, "r" -> 0.0, "f" -> 0.0, "m" -> 0.0))
        for (i <- 1 until levelNumClusters) {
            resultMap += (i -> Map("num" -> 0.0, "r" -> 0.0, "f" -> 0.0, "m" -> 0.0))
        }

        val storeMap = scala.collection.mutable.Map(0 -> ArrayBuffer[Long]())
        for (i <- 1 until levelNumClusters) {
            storeMap += (i -> ArrayBuffer[Long]())
        }

        val preOrg = new ArrayBuffer[org.apache.spark.mllib.linalg.Vector]
        queryDatas.foreach(e => {
            val a = new ArrayBuffer[Double]
            a += e.getId
            a += e.getRValue
            a += e.getFValue
            a += e.getMValue
            preOrg.append(Vectors.dense(a.toArray))
        })
        val preData = sc.makeRDD(preOrg)

        preData.collect().foreach(testDataLine => {
            val testArr = testDataLine.toArray
            val a = new ArrayBuffer[Double]
            a += testArr(1)
            a += testArr(2)
            a += testArr(3)
            val predictedClusterIndex: Int = clusters.predict(Vectors.dense(a.toArray))

            storeMap(predictedClusterIndex) += testArr(0).toLong
            resultMap(predictedClusterIndex) +=(
                    "num" -> (resultMap(predictedClusterIndex)("num") + 1),
                    "r" -> (resultMap(predictedClusterIndex)("r") + testDataLine.toArray(1)),
                    "f" -> (resultMap(predictedClusterIndex)("f") + testDataLine.toArray(2)), "m" -> (resultMap(predictedClusterIndex)("m") + testDataLine.toArray(3)))
            logger.info("The data " + testDataLine.toString + " belongs to cluster " + predictedClusterIndex)
        })

        var rTotal = 0.0
        var fTotal = 0.0
        var mTotal = 0.0
        for ((x, y) <- resultMap) {
            logger.info("cluster index [" + x + "]", "  nodes count" + "[" + y("num") + "]", "  avg A rate [" + y("r") / y("num") + "]", "  avg F rate [" + y("f") / y("num") + "]", "  avg M rate [" + y("m") / y("num") + "]")
            rTotal += y("r") / y("num")
            fTotal += y("f") / y("num")
            mTotal += y("m") / y("num")
        }

        val statisticMap = scala.collection.mutable.Map(0 -> Map("sum" -> 0.0, "r" -> 0.0, "f" -> 0.0, "m" -> 0.0, "num" -> 0.0))
        for (i <- 1 until levelNumClusters) {
            statisticMap += (i -> Map("sum" -> 0.0, "r" -> 0.0, "f" -> 0.0, "m" -> 0.0, "num" -> 0.0))
        }

        resultMap.foreach(e => {
            statisticMap(e._1) +=(
                    "num" -> e._2("num"),
                    "r" -> e._2("r") / e._2("num"),
                    "f" -> e._2("f") / e._2("num"),
                    "m" -> e._2("m") / e._2("num"),
                    "sum" -> (e._2("r") / (e._2("num") * rTotal) + e._2("f") / (e._2("num") * fTotal) + e._2("m") / (e._2("num") * mTotal))
                    )
        })

        statisticMap.toList.sortWith(_._2("sum")>_._2("sum")).foreach(e=>{
            println("cluster index [" + e._1 + "]", "  nodes count" + "[" + e._2("num") + "]", " sum rate [" + e._2("sum") + "]", "  statistic R rate [" + e._2("r") + "]",
                "  statistic  F rate [" + e._2("f") + "]", "   statistic M rate [" + e._2("m") + "]")
        })

        for ((x, y) <- storeMap) {
            println("cluster index [" + x + "] include storeIds : ")
            var flag = false
            y.foreach(e => {
                if (!flag) {
                    flag = true
                } else {
                    print(",")
                }
                print(e)
            })
            println()
        }

        println("Spark MLlib K-means clustering test finished.")
        sc.stop()
    }
}
