(ns lumprj.funcs.realstream

  (:import
           (java.io  BufferedReader InputStreamReader)
           (edu.iris.miniseedutils.steim GenericMiniSeedRecord)
           (java.lang.Math)
           (java.text SimpleDateFormat)
           (java.util StringTokenizer))
  (:require
            [lumprj.funcs.conmmon :as conmmon]
            [taoensso.timbre :as timbre]
            [lumprj.models.db :as db]
            )
  )


;;解码minidata
(defn decodeminirtdata  [x bis buf recLen]
  (.read bis buf 0 recLen)
  (let [
         gmsRec (GenericMiniSeedRecord/buildMiniSeedRecord buf)
         updata (make-array Integer/TYPE (.getNumSamples gmsRec))]
    (if(.decompress gmsRec updata)()())
    ;;(println (.getNanos (.getStartTime gmsRec)))
    ;;(println (.toString (.getStartTime gmsRec)))
    {:stationname (str (.getNetwork gmsRec) "/" (.getStation gmsRec) "/" (.getChannel gmsRec))
     :data (into [] updata)
     :time (.getStartTime gmsRec)
     :rate (int (.getSampleRate gmsRec))
     :edtime (.getEndTime gmsRec)
     }
    )

  )

(defn running-station [station]
  (db/has-suspend-station station)
  )



(defn suspend-station [station time]
  (let [ df   (new SimpleDateFormat "yyyy-MM-dd HH:mm:ss.SSS")
         ]
    (println (str station "断记11111"))
    (when (= (count (db/is-suspend-station  station)) 0)(db/new-suspend-station station  (.format df time)))
    )

  )
;;解码minidata
(defn decodeminirtbufdata  [x  buf]
  (let [
         gmsRec (GenericMiniSeedRecord/buildMiniSeedRecord buf)
         updata (make-array Integer/TYPE (.getNumSamples gmsRec))]

    (if(.decompress gmsRec updata)()(suspend-station (.getStation gmsRec)))     ;running-station (.getStation gmsRec)
    ;(println (.getNanos (.getStartTime gmsRec)))
    ;(timbre/info (str (.getStartTime gmsRec) ": " (.getStation gmsRec)))
    {:stationname (str (.getNetwork gmsRec) "/" (.getStation gmsRec) "/" (.getChannel gmsRec))
     :data (into [] updata)
     :channel (.getChannel gmsRec)
     :name   (.getStation gmsRec)
     :time (.getStartTime gmsRec)
     :rate (int (.getSampleRate gmsRec))
     :edtime (.getEndTime gmsRec)
     }
    )

  )


;;相关分析
(defn correlation-analysis [reallist realbegin samplelist samplebegin len ]
  ;(println reallist)
  (let [aw1arr (drop realbegin (take (+ len realbegin) reallist))
        aw2arr (drop samplebegin (take (+ len samplebegin) samplelist))
        aw1 (conmmon/average aw1arr)
        aw2 (conmmon/average aw2arr )
        sw1 (conmmon/sum (map #(Math/pow (- (* %) aw1) 2) aw1arr))
        sw2 (conmmon/sum (map #(Math/pow (- (* %) aw2) 2) aw2arr))
        sw12 (conmmon/sum (for [i (range 0 (count aw1arr))
                   :let [x (nth aw1arr i)
                         y (nth aw2arr i)
                         ]
                   ]
               (* (- (* y) aw2) (- (* x) aw1))
               ) )
        ]
    ;;(println sw12)
    ;;(println (Math/sqrt (* sw1 sw2)))
    (/ sw12 (Math/sqrt (* sw1 sw2)))
    )
  )