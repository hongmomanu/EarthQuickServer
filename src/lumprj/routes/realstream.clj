(ns lumprj.routes.realstream
  (:use compojure.core)

  (:require [lumprj.controller.realstream :as realstreammanager]
            [lumprj.funcs.websocket :as ws]
            [noir.response :as resp]
            [clojure.data.json :as json]
            )

  )

(defroutes realstream-routes

  (GET "/readrealstreambyfilename" []


    (resp/json {:success true
                :msg  "测试实时miniSeed"
                :result (realstreammanager/readrealstream)})

    )

  (GET "/readrealstreamfromcache" [time station]

    (let [data (realstreammanager/readrealstreamfromcache time station)]
      (resp/json {:success true :result  data})
      )
    )
  (GET "/realstream/realdata" [time station step]
    (resp/json (realstreammanager/readrealstreamfromcache-now time station (read-string step)))
    )
  (GET "/realstream/datatest" [station step]
    ;(resp/json {:results (realstreammanager/get-streamcacheall-data-new station (read-string step) 600000)})
    (resp/json {:results (realstreammanager/get-streamcacheall-data station (read-string step))})
    )

  (GET "/readrealstream" []

    (realstreammanager/makerealstreamfile)
    (resp/json {:success true})

    )

  (GET "/realstream/getrealstream" [timelong]
    (realstreammanager/getstreamzerocross (read-string timelong))
    )

  (GET "/realstream/eqimtest" []
    (realstreammanager/eqim-test)
    )

  (GET "/realstream/rtstest" []
    (realstreammanager/rts-test)
    )


  (GET "/realstream/stationcheck" [stationname btime etime dataaddr gatewayaddr]
       (realstreammanager/stationcheck stationname btime etime dataaddr gatewayaddr)
       )


  (GET "/realstream/realstreamrelations" [rtime rstation stime sstation second move]
    (realstreammanager/realstreamrelations rtime rstation stime sstation (read-string second) (read-string move))
    )

  (GET "/realstream/rtsstreamrelations" [name rtime rstation stime sstation second move]
    (realstreammanager/realstreamrelationsrts name rtime rstation stime sstation (read-string second) (read-string move))
    )

  (GET "/realstream/samplescache" [time station]
    (resp/json {:success true :result (realstreammanager/readsamplestreamcache time station)})
    )

  (GET "/realstream/samples" [time station name]
    (resp/json {:success true :result (realstreammanager/get-epicenter-sampledata-less-name time station name )})
    )
  (GET "/realstream/samplescachedetail" [time station second type timesample]
    (let [result (realstreammanager/readsamplestreamcache-detail time station (read-string second) type)
          result1 (realstreammanager/readsamplestreamcache-detail timesample station (read-string second) 0)
          ]

      (resp/json {:success true :result  result
      :result1 result1
                  })
      )
    )
  (GET "/realstream/samplescachedetaillocal" [name time station second type timesample rate]
    (let [result1 (realstreammanager/readsamplestreamdata-detail timesample station (read-string second) name)

          ;result (map #(:data %) (drop 0 (take (* (read-string rate) (read-string second)) (realstreammanager/readrealstreamfromcache time station))))
          result  (drop 0 (take (* (read-string rate) (read-string second)) (realstreammanager/readrealstreamfromcache-mem time station) ))
          ]

      (resp/json {:success true :result  result
      :result1 result1
                  })
      )

    )
  (GET "/realstream/makesamplescache" [paths type name]

    (resp/json {:success true :result  (realstreammanager/make-sampledata-cache (clojure.string/split paths #",") (read-string  type) name)})
    )



  (GET "/realstream/makesampleslocal" [paths  name]

    (resp/json {:success true :result  (realstreammanager/make-sampledata (clojure.string/split paths #",")  name)})
    )
  (GET "/realstream/delsampleslocal" [name]

    (resp/json {:success true :result  (realstreammanager/del-sampledata-cache  name)})
    )

  (GET "/realstream/websockettest" [eventid]
    ;(realstreammanager/send-eqim-info)
    (realstreammanager/send-rts-info eventid)
    (resp/json {:success true})
    )

  (POST "/realstream/toolconfig" [filename]
    (resp/json (realstreammanager/relation-tool filename))
    )




)


