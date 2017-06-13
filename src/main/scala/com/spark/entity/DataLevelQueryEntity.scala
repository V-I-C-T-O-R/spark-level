package com.spark.entity

import javax.persistence._

/**
 * Created by Victor  on 17-6-2.
 */
@Entity
class DataLevelQueryEntity extends BaseEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id")
    private var id: Long = 0L
    @Column(name = "r")
    private var rValue: Double = 0.0
    @Column(name = "f")
    private var fValue: Double = 0.0
    @Column(name = "m")
    private var mValue: Double = 0.0

    def getRValue = this.rValue

    def getId = this.id

    def getFValue = this.fValue

    def getMValue = this.mValue


    def setRValue(rValue: Double): Unit = {
        this.rValue = rValue
    }

    def setFValue(fValue: Double): Unit = {
        this.fValue = fValue
    }

    def setMValue(mValue: Double): Unit = {
        this.mValue = mValue
    }

    def setId(id: Long): Unit = {
        this.id = id
    }

    def getMaxValue(): Double = {
        var max = this.getRValue
        if (max <= this.getFValue) {
            max = this.getFValue
        }
        if (max <= this.getMValue) {
            max = this.getMValue
        }
        max
    }

    def getMinValue(): Double = {
        var min = this.getRValue
        if (min >= this.getFValue) {
            min = this.getFValue
        }
        if (min >= this.getMValue) {
            min = this.getMValue
        }
        min
    }
}
