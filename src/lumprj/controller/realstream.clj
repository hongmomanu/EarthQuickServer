(ns lumprj.controller.realstream

  (:import (cn.org.gddsn.liss.client LissClient LissException LissTransferType)
           (edu.iris.miniseedutils.steim GenericMiniSeedRecord GenericMiniSeedRecordOutput)
           (cn.org.gddsn.liss.util LissClientReader)
           (java.io FileInputStream BufferedInputStream PushbackInputStream File DataInputStream)
           (java.util HashSet Date Calendar)
           (java.sql Timestamp)
           (java.text SimpleDateFormat)
           (java.lang.Math)
           (lumprj.java AmqClojure)
           (lumprj.java Test)
           (lumprj.java.eqim EqimConnectorTip)
           ;(cn.org.gddsn.jopens.pod.util PodUtil)
           (cn.org.gddsn.jopens.pod.amq AmqEarService)
           (org.springframework.core.io FileSystemResource)
           (org.springframework.beans.factory.xml XmlBeanFactory)
           ;;(edu.iris.timeutils TimeStamp)
           (cn.org.gddsn.seis.evtformat.seed SeedVolume SeedVolumeNativePlugin)
           (cn.org.gddsn.jopens.entity.seed Dataless)
           (cn.org.gddsn.jopens.client SeedVolumeImporter Migration)


           )
  (:use compojure.core  org.httpkit.server)
  (:require [lumprj.models.db :as db]
            [lumprj.funcs.realstream :as realstream]
            [lumprj.funcs.conmmon :as conmmon]
            [lumprj.funcs.websocket :as websocket]
            [me.raynes.fs :as fs]
            [clojure.data.json :as json]
            [noir.response :as resp]
            [taoensso.timbre :as timbre]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.triggers :as t]
            [clojurewerkz.quartzite.jobs :as j]
            [clojurewerkz.quartzite.jobs :refer [defjob]]
            [clojurewerkz.quartzite.schedule.calendar-interval :refer [schedule with-interval-in-days with-interval-in-minutes with-interval-in-seconds]]
            ;[clojurewerkz.quartzite.schedule.simple :refer [schedule with-repeat-count with-interval-in-seconds with-interval-in-milliseconds]]
            [clj-time.coerce :as clj]
            [clj-time.local :as l]
            [lumprj.funcs.system :as system]
            )
  )

(declare getrealstreams readrealstreamfromcache readsamplestreamcache readsamplestreamcache-less
  make-milltime-data make-milltime-data-cross readrealstreamfromcacheall-filter  readrealstreamfromcacheall-filter-new
  get-epicenter-sampledata-less readsamplestreamcache-less readsamplestreamcache-less-name send-rts-info rts-relation-begin
  get-epicenter-sampledata-less-name get-streamcacheall-data-new readrealstreamfromcache-now
  dataprocess-new dataprocess-new-caculate make-milltime-data-nocross readrealstreamfromcacheall-filter-last
  get-streamcacheall-data-last get-streamcacheall-data readrealstreamcache-less-name  get-epicenter-realdata-less-name
  caculate-zerocross-num
  )

(def REAL_STREAM_CLIENT (atom {
                               }))



(def realstream-hub (atom {}))

(def avarage-cross (atom {}))

(defn updatestreamserver []
  (let [rtsserver (:rts (conmmon/get-config-prop))
         ]
    (swap! REAL_STREAM_CLIENT assoc "ip" (:ip rtsserver))
    (swap! REAL_STREAM_CLIENT assoc "port" (:port rtsserver))
    (swap! REAL_STREAM_CLIENT assoc "timelong" (:timelong rtsserver))
    (swap! REAL_STREAM_CLIENT assoc "user" (:user rtsserver))
    (swap! REAL_STREAM_CLIENT assoc "pass" (:pass rtsserver))
    (swap! REAL_STREAM_CLIENT assoc "stations" (map #(str (:networkcode %) "/" (:stationcode %)) (db/stationcode-list)))
    (swap! REAL_STREAM_CLIENT assoc "cachelong" (:cachelong rtsserver))

  ))

(defn stationcheck [stationname btime etime dataaddr gatewayaddr]


   (resp/json {:dataping (if (nil? dataaddr) false (system/ping dataaddr))
               :gateway (if (nil? gatewayaddr) false (system/ping gatewayaddr))   :results (db/stationcheck stationname btime etime)})
  )

(defn make-average-type [type data]
  (conmmon/average(map #(:zerocrossnum %) (filter (fn [x]
                                            (> (.indexOf (:stationname x) type) 0))
                                    data
                                    )))

  )

(defn make-average-no-type [data]
  (conmmon/average(map #(:zerocrossnum %) data))
  )

 ;;断记统计
(defn getsuspend-station [station]
  (let [ stationcode   (:stationcode station)
         results (db/get-streamcacheall-data stationcode)]
    (if (= (count results) 0)(realstream/suspend-station stationcode)(realstream/running-station stationcode))

  )  )


(defn getstreamzerocross-fn [station timelong]

  (let [
         df   (new SimpleDateFormat "yyyy-MM-dd HH:mm:ss.SSS")

         cal (Calendar/getInstance)
         caldo  (do (.setTimeInMillis cal (.getTime (new Date))) (.add cal Calendar/MILLISECOND   (- 0 timelong)))
         rtime (.format df (.getTime (.getTime cal)))
         ;alldata-bhe (get-streamcacheall-data (:stationcode station) 1)
         alldata-bhn (db/get-streamcacheall-data  (str (:networkcode station) "/" (:stationcode station) "%HN") 1)
         alldata-bhe (db/get-streamcacheall-data  (str (:networkcode station) "/" (:stationcode station) "%HE") 1)
         alldata-bhz (db/get-streamcacheall-data  (str (:networkcode station) "/" (:stationcode station) "%HZ") 1)

         bhelast (first alldata-bhe)
         bhnlast (first alldata-bhn)
         bhzlast (first alldata-bhz)

         bhetime (get-epicenter-realdata-less-name rtime  (str (:networkcode station) "/" (:stationcode station) "/" (:channel bhelast)))
         bhetime-n (get-epicenter-realdata-less-name rtime  (str (:networkcode station) "/" (:stationcode station) "/" (:channel bhnlast)))
         bhetime-z (get-epicenter-realdata-less-name rtime  (str (:networkcode station) "/" (:stationcode station) "/" (:channel bhzlast)))
         alldata (into []  (apply concat (map #(:data %)
                                              bhetime)))
         alldata-n (into []  (apply concat (map #(:data %)
                                              bhetime-n)))
         alldata-z (into []  (apply concat (map #(:data %)
                                              bhetime-z)))


        ispasue  (or (nil? bhelast)(> (- (clj/to-long   (l/local-now))  (.getTime (:time bhelast) )) 60000))
        pausetime (.getTime (if (nil? bhelast) (new Timestamp (.getTime (new Date))) (new Timestamp (.getTime (:time bhelast) ))))
        pausedo (if ispasue (realstream/suspend-station (:stationcode station) pausetime)(realstream/running-station (:stationcode station)))

        bhesub  (get @avarage-cross (:stationcode station))


         ;bhe (if ispasue 0 (:zerocrossnum bhelast))
        during     (if (> (count bhetime) 0) (- (.getTime (:edtime (last bhetime))) (.getTime (:time (first bhetime)) ) ) timelong)
         bhe (if ispasue 0 (int (* (/ (caculate-zerocross-num  alldata) during) timelong) ) )
         bhn (if ispasue 0 (int (* (/ (caculate-zerocross-num  alldata-n) during) timelong) ) )
         bhz (if ispasue 0 (int (* (/ (caculate-zerocross-num  alldata-z) during) timelong) ) )
        avbhe (if (nil? bhesub)  [bhe bhn bhz 0] bhesub)
        test (swap! avarage-cross assoc (:stationcode station)  [(/(+ (* (first avbhe) (last avbhe))  bhe) (+ (last avbhe) 1) )
                                                                 (/(+ (* (second avbhe) (last avbhe))  bhn) (+ (last avbhe) 1) )
                                                                 (/(+ (* (avbhe 2) (last avbhe))  bhz) (+ (last avbhe) 1) )
                                                                 (+ (last avbhe) 1) ])
        ]

                          {
                            :crossnowbhe  bhe
                            :crossavgbhe (first avbhe)
                            :crossnowbhn  bhn
                            :crossavgbhn (second avbhe)
                            :crossnowbhz  bhz
                            :crossavgbhz (avbhe 2)
                            :stationname (:stationname station)
                            :stationcode (:stationcode station)
                            :connecttype (:connecttype station)
                            :dataaddr (:dataaddr station)
                            :gatewayaddr (:gatewayaddr station)
                            :contact (:contact station)
                            :phone (:phone station)
                            :stationid (:id station)
                            :crossnums (:crossnums station)
                            :geom (:geom station)
                            :ispasue ispasue
                            :time  (if ispasue (.format df (.getTime (new Date))) (.format df (:time bhelast)))
                           }

    )
  )
;;读取头文件
(defn dataless []
  (let [in (new FileInputStream "/home/jack/test/ZJ.201402130341.0002.seed" )
        dl (new Dataless)
        ]
    (.readSeedVolumeDataless dl (new PushbackInputStream in 4096))
    (.close in)
    (let [
           station (-> (.getSeedConfig dl) (.getStation)(.get 0))
           channel (-> (.getChannel station)(.get 0))

           ]
      ;(println (-> (.getResponse channel)(.get 0) (.getBlockette061)))
      )

    )

  )
;根据震中获取样本数据
(defn get-epicenter-sampledata [ time station type]
  ;;(println (db/get))
  ;;(readrealstreamfromcache)
  (readsamplestreamcache time station type)
  )

(defn get-epicenter-sampledata-less [ time station type]
  (readsamplestreamcache-less time station type)
  )

(defn get-epicenter-sampledata-less-name [ time station name]
  (readsamplestreamcache-less-name time station name)
  )

(defn get-epicenter-realdata-less-name [ time station]
  (readrealstreamcache-less-name time station)
  )



(defn max-one-fn [data max]

  (map #(/ % 1) data)
  )
;;相关分析业务
(defn realstreamrelations [rtime rstaton stime sstation second move]

  (let [ sample  (get-epicenter-sampledata-less  stime sstation 0)
         rate (/ -1000 (:rate (first sample)))
         df   (new SimpleDateFormat "yyyy-MM-dd HH:mm:ss.SSS")
         dft   (new SimpleDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS")
         fstime (let [time (:time (first sample))

                      ]
                  (.format df time)

                  )
         fstimet (let [time (:time (first sample))

                      ]
                  (.format dft time)

                  )
         stimet (clojure.string/replace stime #" " "T")
         sampledata1  (into []  (apply concat (map #(:data %) sample)))

         stimespan (- (clj/to-long fstimet) (clj/to-long stimet))


         sampledata (if (> stimespan 0)(sampledata1)(drop (/ stimespan rate) sampledata1))
         samplemax (apply max (map #(Math/abs %) sampledata ))
         realstream (get-epicenter-sampledata-less   rtime rstaton 1)
         frtime (let [time (:time (first realstream))
                      ]
                  (.format df time)

                  )
         frtimet (let [time (:time (first realstream))
                      ]
                  (.format dft time)

                  )

         rtimet (clojure.string/replace rtime #" " "T")
         ;realstreamdata (map #(:data %) realstream)
         realstreamdata1 (into [] (apply concat (map #(:data %) realstream)))
         rtimespan (- (clj/to-long frtimet) (clj/to-long rtimet))
         ;;test1 (println frtime rtime rtimet rtimespan "rtimspan" (clj/to-long frtimet) (clj/to-long rtimet))
         ;;test2 (println fstime stimespan "stimspan" (clj/to-long fstimet) (clj/to-long stimet))
         realstreamdata (if (> rtimespan 0)realstreamdata1 (drop (/ rtimespan rate) realstreamdata1))
         realmax (apply max (map #(Math/abs %) realstreamdata ))




        ]

    (resp/json {
                 :success true
                 :sstation sstation
                 :stime (if (> stimespan 0)fstime stime)
                 :rtime (if (> rtimespan 0) frtime rtime)
                 :rate rate
                 :rstation rstaton
                 :relations (map #(realstream/correlation-analysis
                                    (max-one-fn realstreamdata realmax)
                                    %
                                    (max-one-fn sampledata samplemax)
                                    0 (* second 100)) (range 0 move))
                })

    )


  )


;;相关分析业务实时
(defn realstreamrelationsrts [name rtime rstaton stime sstation second move]

  (let [ sample  (get-epicenter-sampledata-less-name  stime sstation name)
         rate (/ -1000 (:rate (first sample)))
         df   (new SimpleDateFormat "yyyy-MM-dd HH:mm:ss.SSS")
         dft   (new SimpleDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS")
         fstime (let [time (:time (first sample))

                      ]
                  (.format df time)

                  )
         fstimet (let [time (:time (first sample))

                       ]
                   (.format dft time)

                   )
         stimet (clojure.string/replace stime #" " "T")
         sampledata1  (into []  (apply concat (map #(:data %) sample)))

         stimespan (- (clj/to-long fstimet) (clj/to-long stimet))


         sampledata (if (> stimespan 0)sampledata1(drop (/ stimespan rate) sampledata1))
         samplemax (apply max (map #(Math/abs %) sampledata ))
         realstream (get-epicenter-realdata-less-name rtime rstaton);(readrealstreamfromcache-now rtime rstaton (- 0 rate));(readrealstreamfromcache  rtime rstaton )

         frtime (let [time (:time (first realstream))
                      ]
                  (.format df time)

                  )
         frtimet (let [time (:time (first realstream))
                       ]
                   (.format dft time)

                   )

         rtimet (clojure.string/replace rtime #" " "T")
         ;realstreamdata (map #(:data %) realstream)
         realstreamdata1 (into []  (apply concat (map #(:data %) realstream))) ;(map #(:data %) realstream)
         rtimespan (- (clj/to-long frtimet) (clj/to-long rtimet))
         ;;test1 (println frtime rtime rtimet rtimespan "rtimspan" (clj/to-long frtimet) (clj/to-long rtimet))
         ;;test2 (println fstime stimespan "stimspan" (clj/to-long fstimet) (clj/to-long stimet))
         ;;test1 (println (:time (first realstream) ) (/ rtimespan rate))
         realstreamdata (if (> rtimespan 0)realstreamdata1 (drop (/ rtimespan rate) realstreamdata1))
         realmax (apply max (map #(Math/abs %) realstreamdata ))




         ]
    ;(println (count sampledata1))
    (resp/json {
                 :success true
                 :sstation sstation
                 :stime (if (> stimespan 0)fstime stime)
                 :rtime (if (> rtimespan 0) frtime rtime)
                 :rate rate
                 :rstation rstaton
                 :relations (map #(realstream/correlation-analysis
                                    (max-one-fn realstreamdata realmax)
                                    %
                                    (max-one-fn sampledata samplemax)
                                    0 (* second 100)) (range 0 move))
                 })

    )


  )


(defn getstreamzerocross [timelong]
  ;;(println (db/stationcode-list))
  (resp/json {:success true
              :results  (map  #(getstreamzerocross-fn % timelong) (db/stationcode-list) )
              }   )
  )
;单元测试
(defn java-clojure-test [name]
  ;(println "ok111")
  (str "hello" name)
  )
;eqim 推送自动报警
(defn send-eqim-info [sp net ip]
  ;(println @websocket/channel-hub)
  (let [df (new SimpleDateFormat "yyyy-MM-dd HH:mm:ss")
        ]
    (doseq [channel (keys @websocket/channel-hub)]
      ;;(println "ok")
      (send! channel (json/write-str
                       {:location (.Location_cname sp)
                        :lat (.Lat sp) :lon (.Lon sp) :depth (.Depth sp)
                        :stime (.format df (.Save_time sp))
                        :eqtype (.Eq_type sp)  :time (.format df (.O_Time  sp))
                        :M (.M sp) :Ml (.Ml sp) :Ms (.Ms sp) :sname (.Sname net) :cname (.Cname net)
                        :code (.Code net)
                        :ip ip
                        :type "eqim"
                        }
                       )
        false)
      )

    )


  )
;rts 地震监测
(defn send-rts-info [eventid]
  (let [infoitem (first (db/get-rts-eventinfo eventid))
        lon (:epi_lon infoitem)
        lat (:epi_lat infoitem)
        id (:id infoitem)
        time (:o_time infoitem)
        eventranges (:eventranges (conmmon/get-config-prop))
        ]
    (doall(map #(when (and
                        (<= lon (nth (:range %) 1))
                        (>= lon (nth (:range %) 0))
                 (<= lat (nth (:range %) 3)) (>= lat (nth (:range %) 2))
                 ) (doseq [channel (keys @websocket/channel-hub)]
                         ;;(println "ok")
                         (send! channel (json/write-str
                                          {:results (rts-relation-begin id)
                                           :type "rts"
                                           :eventid  eventid
                                           :lonlat [lon lat]
                                           :range (:range %)
                                           :name (:sname %)
                                           :time time
                                           }
                                          )
                           false)))  eventranges) )


  )
  )

(defn rts-relation-begin [catalogid]

  (let [stationitems (db/get-rts-contentinfo catalogid )]
    stationitems
    )

  )

(defn eqim-server-init []
  (let [
         eqimservers (:eqimservers (conmmon/get-config-prop))
         ]
    ;(println eqimservers)
    (dorun(pmap #(.receiveAndPublish
                  (proxy [EqimConnectorTip] [(:ip %) (:port %) (:user %) (:pass %)]
                    (clojureeqm [sp net ] (send-eqim-info sp net (:ip %))))) eqimservers))
    )
  )

(defn rts-server-init []
  (let [pamq (proxy [AmqClojure] [] (amqplay [mess] (do (send-rts-info mess)(println "rts happen"))))
        rootpath (str (System/getProperty "user.dir") "/")
        cfgFile (str rootpath "applicationContext-amqEar-jms.xml")
        res (new FileSystemResource cfgFile)
        ac (new XmlBeanFactory res)
        amq (.getBean ac "amqEarService")
        ]
    (.setclojure amq pamq)
    (.runListening amq)
    )

  )
(defn eqim-test []
  (.receiveAndPublish (new EqimConnectorTip "10.33.8.174" 5001 "show" "show"))
  (resp/json {:success true})
  )

(defn rts-test []
  ;(println (str "load" (AmqEarService/cfgFile)))
  ;(let [ res (new FileSystemResource (AmqEarService/cfgFile))
  ;       ac (new XmlBeanFactory res)
  ;       amq (.getBean ac "amqEarService")
  ;       ]
   ; (.runListening amq)

  ;  )
  (resp/json {:success true :results (send-rts-info "ZJ.201404141732.0001")})
  )

(defn readrealstreamfromcacheall-filter-new [stationname step timelong]
  ;(println stationname)
  (get-streamcacheall-data-new stationname step timelong)

  )
(defn readrealstreamfromcacheall-filter-last [stationname step timelong]
  ;(println stationname)
  (get-streamcacheall-data-last stationname step timelong)

  )
(defn readrealstreamfromcacheall-filter [stationname]
  (map #(conj {:time (:time % )}
          {:stationname (:stationname %)}
          {:data (read-string (:data %))}
          {:zerocrossnum (:zerocrossnum %)}
          )
    (db/get-streamcacheall-data stationname))
  )
(defn get-streamcacheall-data [station step]
  (let [data (db/get-streamcacheall-data station step)]
    (apply concat  (map #(let [
                                itemdatas (conj % {:data (read-string (:data %))})
                                items   (dataprocess-new itemdatas)
                                ]
                           items
                           ;{:data (nth data 0) :time (nth data 1) } :zerocrossnum (nth data 2)
                           ) data))

    )


  )
(defn make-datas-to-data [datas time]
  (map #(conj {} {:data % :time time}) datas)
  )

(defn get-streamcacheall-data-last [station step timelong]
  (let [timenow (* (quot (.getTime (new Timestamp (.getTime (new Date))))1000) 1000)
        time-range (range timenow (- timenow timelong)  (- 0 step))
        realdata
        (loop [nCurrent 0 ]
          (if (not (nil? (get @realstream-hub (str (nth time-range nCurrent) station))) )

                [(nth time-range nCurrent)]


            (recur (+ nCurrent 1))))


        ]
    ;(println realdata)

    ;(println @realstream-hub)
    (apply concat  (map #(let [
                                data (get @realstream-hub (str % station))
                                items   (dataprocess-new data)
                                ]
                           items
                           ;{:data (nth data 0) :time (nth data 1) } :zerocrossnum (nth data 2)
                           ) realdata))
    )
  )
(defn get-streamcacheall-data-new [station step timelong]
  (let [timenow (* (quot (.getTime (new Timestamp (.getTime (new Date))))1000) 1000)
        time-range (range (- timenow timelong) timenow step)
        realdata (filter (fn [x]
                           (not (nil? (get @realstream-hub (str x station))) ))
                   time-range
                   )
        ]
    ;(println realdata)

    ;(println @realstream-hub)
    (apply concat  (map #(let [
                        data (get @realstream-hub (str % station))
                        items   (dataprocess-new data)
                        ]
                        items
                        ;{:data (nth data 0) :time (nth data 1) } :zerocrossnum (nth data 2)
                   ) realdata))
    )
  ;(db/get-streamcacheall-data station)
  )

(defn readrealstreamfromcache-now [time station step]


  (let [timenow (* (quot (.getTime (new Timestamp (.getTime (new Date))))1000) 1000)
        timecaculate  (.getTime (Timestamp/valueOf time))
        timebegin (- (* (quot timecaculate 1000) 1000) 60000)
        timebegin-last (if (> (- timenow timebegin) 700000) (- timenow 700000) timebegin)
        time-range (range timebegin-last timenow step)

        realdata (filter (fn [x]
                           (not (nil? (get @realstream-hub (str x station))) ))
                   time-range
                   )
        ]
    ;(println realdata)

    ;(println @realstream-hub)


    (apply concat  (map #(let [
                                data (get @realstream-hub (str % station))
                                items   (dataprocess-new-caculate data timecaculate)
                                ]
                           items
                           ;{:data (nth data 0) :time (nth data 1) } :zerocrossnum (nth data 2)
                           ) realdata))

    )

  )
(defn readrealstreamfromcache [time station]
  (map #(conj {:time (:time % )}
          {:edtime (:edtime % )}
          {:rate (:rate % )}
          {:stationname (:stationname %)}
          {:data (read-string (:data %))}
          {:zerocrossnum (:zerocrossnum %)}
          )
    (db/get-streamcacheall time station))
  )

(defn readrealstreamfromcache-mem [time station]

  (let [

         realstream (get-epicenter-realdata-less-name time station);(readrealstreamfromcache-now rtime rstaton (- 0 rate));(readrealstreamfromcache  rtime rstaton )
         dft   (new SimpleDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS")
         frtimet (let [time (:time (first realstream))
                       ]
                   (.format dft time)

                   )

         rate (/ -1000 (:rate (first realstream)))
         rtimet (clojure.string/replace time #" " "T")
         ;realstreamdata (map #(:data %) realstream)
         realstreamdata1 (into []  (apply concat (map #(:data %) realstream))) ;(map #(:data %) realstream)
         rtimespan (- (clj/to-long frtimet) (clj/to-long rtimet))
         ;;test1 (println frtime rtime rtimet rtimespan "rtimspan" (clj/to-long frtimet) (clj/to-long rtimet))
         ;;test2 (println fstime stimespan "stimspan" (clj/to-long fstimet) (clj/to-long stimet))
         ;;test1 (println (:time (first realstream) ) (/ rtimespan rate))
         realstreamdata (if (> rtimespan 0)realstreamdata1 (drop (/ rtimespan rate) realstreamdata1))
         ]
    (println rate )
    (println realstreamdata1)
    realstreamdata
    )

  )

(defn read-data-fn [row]
  ;(println row)
  {:data (read-string (:data row) ) :time (:time row) :edtime (:edtime row) :rate (:rate row)}
  )
(defn readsamplestreamcache [time station type]
  ;;(println time station)
  ;;(println (db/get-samplecache time station))
  (map #(read-data-fn %) (db/get-samplecache time station type))
  )

(defn readsamplestreamcache-less [time station type]
  ;;(println time station)
  ;;(println (db/get-samplecache time station))
  (map #(read-data-fn %) (db/get-samplecache-less time station type))
  )

(defn readsamplestreamcache-less-name [time station name]
  ;;(println time station)
  ;;(println (db/get-samplecache time station))
  (map #(read-data-fn %) (db/get-sample-less time station name))
  )

(defn readrealstreamcache-less-name [time station ]
  ;;(println time station)
  ;;(println (db/get-samplecache time station))
  (map #(read-data-fn %) (db/get-real-less time station ))
  )



(defn readsamplestreamcache-detail [time station second type]
  (drop 0 (take (* 100 second) (let
                                 [calendar (Calendar/getInstance)
         df   (new SimpleDateFormat "yyyy-MM-dd HH:mm:ss.SSS")
         sample  (db/get-samplecache-bytype-less time station type)
         fstime (let [times (:time (first sample))
                      ]
                  (.format df times)
                  )
         rate (/ -1000 (:rate (first sample)))
         fstimet (clojure.string/replace fstime #" " "T")
         mytime (clojure.string/replace time #" " "T")
         sampledata1  (into []  (apply concat (map #(read-string (:data %)) sample)))

         stimespan (- (clj/to-long fstimet) (clj/to-long mytime))
         ;test1 (println stimespan mytime fstimet (count sampledata1))

         ]
          ;(doall (map #(println  (:time %) (:edtime %) (count (read-string (:data %))) (:stationname %)) sample))
    (if (>= stimespan 0) sampledata1 (drop (/ stimespan rate) sampledata1))
    )))
  ;(drop 0 (take (* 100 second) (map #(read-string (:data %)) (db/get-samplecache-bytype time station type))))
  )
(defn readsamplestreamdata-detail [time station second name]
  (let
                                 [calendar (Calendar/getInstance)
         df   (new SimpleDateFormat "yyyy-MM-dd HH:mm:ss.SSS")
         sample  (db/get-sample-bytype-less time station name)
         fstime (let [times (:time (first sample))
                      ]
                  (.format df times)
                  )
         rate (/ -1000 (:rate (first sample)))
         fstimet (clojure.string/replace fstime #" " "T")
         mytime (clojure.string/replace time #" " "T")
         sampledata1  (into []  (apply concat (map #(read-string (:data %)) sample)))

         stimespan (- (clj/to-long fstimet) (clj/to-long mytime))
         ;test1 (println stimespan mytime fstimet (count sampledata1))

         ]
          ;(doall (map #(println  (:time %) (:edtime %) (count (read-string (:data %))) (:stationname %)) sample))
    (drop 0 (take (* (:rate (first sample)) second) (if (>= stimespan 0) sampledata1 (drop (/ stimespan rate) sampledata1)) ))
    )
  ;(drop 0 (take (* 100 second) (map #(read-string (:data %)) (db/get-samplecache-bytype time station type))))
  )



(defn caculate-zerocross-num [dataall]
  (let [data dataall ;(pmap #(- (dataall (inc %)) (dataall %)) (drop-last(range 0 (count dataall))))
        ]
    (count (for [x (drop-last (range 0 (count data)))
                 :let [y 0]
                 :when (and (> x 0) (< (* (- (nth data x) (nth data (- x 1))) (- (nth data (+ x 1 )) (nth data x ))) 0))]
             y))
    )

  )
;; 实时地震数据
(defn realstream-data-func [data]

    (db/insert-streamcache  data)


  )

(defn realstream-data-update-func [data]
  ;;(println (map #(make-milltime-data-cross data (:time data) %) (range 0 (count (:data data)))))
  (map #(db/update-streamcache %) (map #(make-milltime-data-cross data (:time data) %) (range 0 (count (:data data)))) )
  )

(defn dataprocess-new [data]
  (map #(make-milltime-data-cross data (:time data) %) (range 0 (count (:data  data))))
  )
(defn dataprocess-new-caculate [data time]
  (filter (fn [x]
            (>=  (:time x)  time))
    (map #(make-milltime-data-nocross data (:time data) %) (range 0 (count (:data  data))))
    )

  )
(defn dataprocess-del [step station]
  (let [
         timenow (* (quot (.getTime (new Timestamp (.getTime (new Date))))1000) 1000)
         lists (map #(str (subs station 0 (- (count station) 1) ) %) ["E" "N" "Z"])
        timetenminuts-before  (- timenow (* (get @REAL_STREAM_CLIENT "cachelong") 1000))
        del-range (range (- timetenminuts-before (* (get @REAL_STREAM_CLIENT "cachelong") 1000) ) timetenminuts-before step)
        ]
    ;(println "删除缓存数据 " station step (first del-range) (last del-range))
    (dorun (for [x lists]
      (do (println x) (dorun (map #(swap! realstream-hub dissoc (str % x)) del-range)))))

    ;(dorun (map #(swap! realstream-hub dissoc (str % station)) del-range))
    ;(println @realstream-hub)
    ;(reset! realstream-hub (take (/ (count @realstream-hub) 2)  @realstream-hub ))
    )
  ;(doall (map ))
  )
(defn realstreamcacheJob-child-dataprocess-new [data]

  (let [
        ;timedata (first (map #(dataprocess-new %) data))
        ;onedata (first data)
        ]

    (swap! realstream-hub assoc (str (.getTime (:time data)) (:stationname data)) data)

    ;(dataprocess-del (/ 1000 (:rate data)) (:stationname data))

    (swap! realstream-hub assoc (:name data) [(:channel data) (:rate data)])
    )


  )
(defn realstreamcacheJob-child-dataprocess [data]
  (realstream-data-func data)
  ;(let [result (db/get-streamcache)]
   ; (when (>  (count result) 0) (db/del-streamcache))
   ; )
  )
(defn make-milltime-data [data time n]
  (let [cal (Calendar/getInstance)]
    (.setTimeInMillis cal (.getTime time))
    (.add cal Calendar/MILLISECOND (* 10 n))
    {:time (new Timestamp (->(.getTime cal)(.getTime))) :data (nth (:data data) n)
     :stationname (:stationname data)
     :type (:type data)
     }
    )

  )
(defn make-milltime-data-nocross [data time n]
  (let [cal (Calendar/getInstance)]
    ;(.setTimeInMillis cal (.getTime (Timestamp/valueOf time)))
    ;(.add cal Calendar/MILLISECOND (* 10 n))
    ;(println  (nth (:data data) n))
    (.setTimeInMillis cal (.getTime time))
    (.add cal Calendar/MILLISECOND (* 10 n))
    {:time (.getTime (new Timestamp (->(.getTime cal)(.getTime))))
     :data (nth (:data data) n)
     :stationname (:stationname data)
     :rate (:rate data)
     }
    )

  )
(defn make-milltime-data-cross [data time n]
  (let [cal (Calendar/getInstance)]
    ;(.setTimeInMillis cal (.getTime (Timestamp/valueOf time)))
    ;(.add cal Calendar/MILLISECOND (* 10 n))
    ;(println  (nth (:data data) n))
    (.setTimeInMillis cal (.getTime time))
    (.add cal Calendar/MILLISECOND (* 10 n))
    {:time (.getTime (new Timestamp (->(.getTime cal)(.getTime)))) :data (nth (:data data) n)
     :stationname (:stationname data)
     :rate (:rate data)

     :zerocrossnum (caculate-zerocross-num (:data data))
     }
    )


  )

(defn sampledata-child-process-local [data]
  (db/insert-sample  data)
  )
(defn sampledata-child-process [data]
  (db/del-samplecache data)
  ;(db/insert-samplecache  (map #(make-milltime-data data (:time data) %) (range 0 (count (:data data)))))
  (db/insert-samplecache  data)
  )

(defjob realstreamcacheJob
  [ctx]    ;;realstreamcacheJob
  (println "定时获取实时数据")
  (let [data  (getrealstreams)] ;;(getrealstreams)

    (realstreamcacheJob-child-dataprocess data)

    )
  )



(defn realstream-one [lissClient firstTime buf stationcodename]
  (let [
         realclientstream (.retrieveRealTimeStream lissClient (into-array [stationcodename]))  ;[stationcodename]
         lissInputStream  (new DataInputStream realclientstream)
         ;;lissInputStreamcopy (new DataInputStream realclientstream)
         ;;shortnum (.readShort lissInputStreamcopy)
         ;test (println shortnum)
         ]

    (loop [nCurrent 0 test 1]

      (if (< (get @REAL_STREAM_CLIENT "timelong") nCurrent)
        (
          do
          (timbre/info "读取数据完成")
          (.abortRealTimeStreamTransport lissClient)
          (.quit lissClient)
          )
        (recur (quot (- (.getTime (new Date))  (.getTime firstTime))  1000)
          (
            do
            ;(println "读取数据中...")
            (.readFully lissInputStream buf)
            (GenericMiniSeedRecord/buildMiniSeedRecord buf)
            (realstreamcacheJob-child-dataprocess-new (realstream/decodeminirtbufdata 1 buf))
              ;(realstreamcacheJob-child-dataprocess-new  (doall (map #(realstream/decodeminirtbufdata % buf)  (take 1 (iterate inc 0) ) )))
            ;(println "poooo")
            ;(realstreamcacheJob-child-dataprocess
            ;  (doall (map #(realstream/decodeminirtbufdata % buf)  (take 1 (iterate inc 0) ) )))
            )  )))

    )

  )
(defn realstream-indirect [stationcodename]
  (try
    (let [
           lissClient (new LissClient (get @REAL_STREAM_CLIENT "ip")  (get @REAL_STREAM_CLIENT "port") )
           buf  (byte-array 512)
           firstTime (new Date)
           stations (get @REAL_STREAM_CLIENT "stations")
           ]

      (.login lissClient (get @REAL_STREAM_CLIENT "user")  (get @REAL_STREAM_CLIENT "pass") )

      (.setType lissClient LissTransferType/BINARY)
      (.setRtServerPassiveMode lissClient false)
      (timbre/info (str "Retrieving  MiniSeed data from " (get @REAL_STREAM_CLIENT "ip") ))

      (let [
             realclientstream (.retrieveRealTimeStream lissClient (into-array stations))  ;[stationcodename] stations
             lissInputStream  (new DataInputStream realclientstream)
             timelong   (get @REAL_STREAM_CLIENT "timelong")
             ;;lissInputStreamcopy (new DataInputStream realclientstream)
             ;;shortnum (.readShort lissInputStreamcopy)
             ;test (println shortnum)
             ]

        (loop [nCurrent 0 test 1]

          (if (<  timelong nCurrent)
            (
              do
              (timbre/info "读取数据完成")
              (.abortRealTimeStreamTransport lissClient)
              (.quit lissClient)
              )
            (recur (quot (- (.getTime (new Date))  (.getTime firstTime))  1000)
              (
                do
                ;(timbre/info "读取数据中..." nCurrent "/" timelong)
                (.readFully lissInputStream buf)
                (GenericMiniSeedRecord/buildMiniSeedRecord buf)
                ;(realstreamcacheJob-child-dataprocess-new (realstream/decodeminirtbufdata 1 buf ) )
                (realstreamcacheJob-child-dataprocess (realstream/decodeminirtbufdata 1 buf ) )
                ;(realstreamcacheJob-child-dataprocess-new  (pmap #(realstream/decodeminirtbufdata % buf)  (take 1 (iterate inc 0) ) ))
                ;(Thread/sleep 10)
                ;(println "poooo")
                ;(realstreamcacheJob-child-dataprocess
                ;  (doall (map #(realstream/decodeminirtbufdata % buf)  (take 1 (iterate inc 0) ) )))
                )  )))

        )

      ;(doall (pmap #(realstream-one rtsserver lissClient firstTime buf %) stations))
      )
    true
    (catch Exception e (do  (timbre/info "重新获取数据") (realstream-indirect "*")false))
    )



  )

(defn get-stations-realstream []

  (doall (pmap #(realstream-indirect %) (map #(:stationcode %) (db/stationcode-list)))  )
    )

(defjob realstream-del-Job [ctx]
  (let [stations (get @REAL_STREAM_CLIENT "stations")
        ;rangelist (range 0 (count stations) 7)
        num  (:counts (first  (db/get-streancache-num)))
        ]
    (timbre/info "实时数据数量......：" ctx)
    (timbre/info "实时数据数量......：" num)
    (when (= num 0) (do (timbre/info "realstrema again") (realstream-indirect "*")))
    ;(dataprocess-del (/ 1000 (:rate data)) (:stationname data))
    ;(dorun (map #(dataprocess-del (/ 1000 (second (get @realstream-hub %))) (str % "/" (first (get @realstream-hub %)) )) stations))
    (db/del-streamcache-all)
    )

  )
(defjob realstream-nofile-Job [ctx]
  (let [stations (get @REAL_STREAM_CLIENT "stations")
        ;rangelist (range 0 (count stations) 7)

        ]
    ;(dorun (pmap #(realstream-indirect (drop %  (take  (+ 7 %) stations))) rangelist))
    (dorun (map #(realstream-indirect [%]) stations))
    )

  ;(realstream-indirect (map #(:stationcode %) (db/stationcode-list)));(map #(:stationcode %) (db/stationcode-list)) "XAJ" "QIU" "NIB" "FY" "QIY" "XP" "WXJ"
  ;(dorun (map #(realstream-indirect [(:stationcode %)]) (db/stationcode-list)))
  )
(defn realstream-del []
  (qs/initialize)
  (qs/start)
  (let [job (j/build
              (j/of-type  realstream-del-Job);realstream-nofile-Job realstreamcacheJob
              (j/with-identity (j/key "jobs.noop.1")))
        trigger (t/build
                  (t/with-identity (t/key "triggers.1"))
                  (t/start-now)
                  (t/with-schedule (schedule
                                     ;;(with-repeat-count 10)
                                     ;(with-interval-in-minutes 1)
                                     (with-interval-in-seconds (get @REAL_STREAM_CLIENT "cachelong"))  ;(- (get @REAL_STREAM_CLIENT "timelong") 3)
                                     )))]
    (qs/schedule job trigger))


  )
(defn makerealstreamcache []



  (qs/initialize)
  (qs/start)
  (let [job (j/build
              (j/of-type  realstream-nofile-Job);realstream-nofile-Job realstreamcacheJob
              (j/with-identity (j/key "jobs.noop.1")))
        trigger (t/build
                  (t/with-identity (t/key "triggers.1"))
                  (t/start-now)
                  (t/with-schedule (schedule
                                     ;;(with-repeat-count 10)
                                     ;(with-interval-in-minutes 1)
                                     (with-interval-in-seconds (get @REAL_STREAM_CLIENT "timelong"))  ;(- (get @REAL_STREAM_CLIENT "timelong") 3)
                                     )))]
    (qs/schedule job trigger))

  )

(defn del-sampledata-cache [name]

  (db/del-sample name)
  )
(defn make-sampledata [paths  name]

  (let [path paths
        nums (count (db/get-sample-byname name))
        ]
    ()
    (if (> nums 0) "已存在" (map #(let [seedplugin (new SeedVolumeNativePlugin)
                                     ]
                                 (.setFile  seedplugin (new File %))                  ;/home/jack/test/ZJ.201402130341.0002.seed
                                 (loop [gmsRec (.getNextMiniSeedData seedplugin) test 1]
                                   (if (nil? gmsRec)
                                     (println "解码完成la")
                                     (recur (.getNextMiniSeedData seedplugin)
                                       (
                                         do

                                         (sampledata-child-process-local {:stationname (str (.getNetwork gmsRec) "/" (.getStation gmsRec) "/"  (.getChannel gmsRec))
                                                                    :data (into [] (.getData gmsRec))
                                                                    :time (new Timestamp (* (.getStartTime gmsRec) 1000))
                                                                    :edtime (new Timestamp (* (.getEndTime gmsRec) 1000))
                                                                    :rate (int (.getSampleRate gmsRec))
                                                                    :name name
                                                                    }   )
                                         ))) )

                                 ) path))


    )


  )

(defn make-sampledata-cache [paths type name]
  (when (= type 1) (db/del-samplecache-type type))
  (when (= type 0) (do (db/del-samplecache-type 1) (db/del-samplecache-type 0)))
  ;(println (count (db/get-samplecache-type 1)) (count (db/get-samplecache-type 0)))

  (let [path paths]
    (map #(let [seedplugin (new SeedVolumeNativePlugin)
                ]
            (.setFile  seedplugin (new File %))                  ;/home/jack/test/ZJ.201402130341.0002.seed
            (loop [gmsRec (.getNextMiniSeedData seedplugin) test 1]
              (if (nil? gmsRec)
                (println "解码完成la")
                (recur (.getNextMiniSeedData seedplugin)
                  (
                    do

                     (sampledata-child-process {:stationname (str (.getNetwork gmsRec) "/" (.getStation gmsRec) "/"  (.getChannel gmsRec))
                                             :data (into [] (.getData gmsRec))
                                             :time (new Timestamp (* (.getStartTime gmsRec) 1000))
                                             :edtime (new Timestamp (* (.getEndTime gmsRec) 1000))
                                             :type type
                                             :rate (int (.getSampleRate gmsRec))
                                             }   )
                    ))) )

            ) path)

    )


  )

(defn makerealstreamfile []
  (.start (new Thread (new LissClientReader "10.33.5.103" "rts" "rts"
                                            "/home/jack/test/testnew11111.HAZ" 30 "HAZ"
                                            "testsS")))
  )

(defn makerealstream-nofile [ip  user pass timelong stnCodes]
  (try
    (let [lissClient (new LissClient ip 5000)
          lissInputStream  (new DataInputStream (.retrieveRealTimeStream lissClient stnCodes))
          buf  (byte-array 512)
          firstTime (new Date)
          ]
      (.login lissClient user pass)
      (timbre/info (str "Logged into Server: " ip))
      (.setType lissClient LissTransferType/BINARY)
      (.setRtServerPassiveMode lissClient false)
      (timbre/info "Enter the passive transport mod")
      (timbre/info (str "Retrieving  MiniSeed data from " ip))

      (loop [nCurrent 0 test 1]
        (if (< timelong nCurrent)
          (do
            (timbre/info "读取数据完成")
            (.abortRealTimeStreamTransport lissClient)
            (.quit lissClient)
            )
          (recur (quot (- (.getTime (new Date))  (.getTime firstTime))  1000)
            (
                do
                (.readFully lissInputStream buf)
                (GenericMiniSeedRecord/buildMiniSeedRecord buf)
              (doall (map #(realstream/decodeminirtdata % buf (byte-array 512) 512)  (take 1 (iterate inc 0) ) ))

            )  )))

      )
    true
    (catch Exception e false)
    )


  )

(defn relation-tool [filename]
  (try


    {:success true :msg (read-string (slurp filename)) }
    (catch Exception e {:success false :msg (.getMessage e)})
    )

  )
(defn readsiglefilestream [filepath]

  (let [
         ;path "/home/jack/Downloads/ZJ_HAZ_BHZ_2.mseed"
         path filepath
         bis (new BufferedInputStream (new FileInputStream path))
         buf  (byte-array 512)
         ;;now_ms (.getTime (new Date))
         ]
    (.skip bis  (long 0))
    ;;(println (/ (fs/size path) 512))
    (doall (map #(realstream/decodeminirtdata % bis buf 512)  (take (/ (fs/size path) 512) (iterate inc 0) ) ))
    )



  )


(defn getrealstreams []
  (let [paths ["/home/jack/test/ZJ_HAZ_BHE_1.mseed"
               "/home/jack/test/ZJ_HAZ_BHN_0.mseed"
               "/home/jack/test/ZJ_HAZ_BHZ_2.mseed"
               ]]
    (concat (readsiglefilestream (nth paths 0)) (readsiglefilestream (nth paths 1)) (readsiglefilestream (nth paths 2)))

    )
  )
(defn readrealstream []


  ;(.start (new Thread (new LissClientReader "10.33.5.103" "rts" "rts"
  ;                                 "/home/jack/test/testnew.BJT" 10 "HAZ"
  ;                                 "")))
  ;(let [bis (new BufferedInputStream (new FileInputStream "/home/jack/test/test.BJT") 6665536)]
  ;  (println bis)
  ;  (.printMiniSeedRecordContents (new GenericMiniSeedRecordOutput) bis System/err)
  ;  )
  (let [paths [
               "/home/jack/test/ZJ_HAZ_BHE_1.mseed"
               "/home/jack/test/ZJ_HAZ_BHN_0.mseed"
               "/home/jack/test/ZJ_HAZ_BHZ_2.mseed"
               ]]
    (map #(into [] (readsiglefilestream %)) paths)

    )

  ;;(GenericMiniSeedRecord/testDecompress "/home/jack/test/test.BJT")

  ;(println (.parseMiniSeedWaveforms (new GenericMiniSeedRecordOutput) "/home/jack/test/testnw.BJT"))

  ;;(slurp "/home/jack/test/test")
  ;(let [lissClient (new LissClient "127.0.0.1" 5000)]

  ;  (.login lissClient "jack" "shayu626")
  ;  )

  ;;(resp/json (dboracle/oracltest))
  )