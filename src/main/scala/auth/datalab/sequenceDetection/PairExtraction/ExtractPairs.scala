package auth.datalab.sequenceDetection.PairExtraction

import auth.datalab.sequenceDetection.Structs
import org.apache.spark.rdd.RDD

trait ExtractPairs {
    def extract(data:RDD[Structs.Sequence]):RDD[Structs.EventIdTimeLists]
}
