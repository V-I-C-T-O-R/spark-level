package com.spark.util

/**
 * Created by Victor on 17-6-8.
 */
object Tool {
    def doCriteria(x: Double, max: Double, min: Double): Double = {
        (x - min) / (max - min)
    }
}
