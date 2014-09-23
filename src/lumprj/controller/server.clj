(ns lumprj.controller.server
  (:use compojure.core)
  (:require [lumprj.models.db :as db]
            [lumprj.funcs.system :as system]
            [noir.response :as resp]
            [lumprj.funcs.conmmon :as conmmon]
            )
  )

(declare mem-cpu-filter)

(def SSH_SHOW_LIST (atom {}))

(defn update-ssh-list []
  (let [results (db/serverlist 0 10000)]
    (dorun (map #(swap! SSH_SHOW_LIST conj (system/get-ssh-connect (:servervalue %) (:username %) (:password %))) results)
      )
    )
  )

(defn saveserver [servername servervalue id username password machinecss]

  (db/update-server
    {:servername servername :servervalue servervalue
     :username username :password password :machinecss machinecss}
     id
    )
  (resp/json {:success true})
  )
(defn delserver [serverid]

  (db/del-server serverid)
  (resp/json {:success true :msg "删除成功"})
  )
(defn addserver [servername servervalue parentid type]
  (let [servercount (if (> (read-string parentid) 0) (:counts (first (db/has-server servername servervalue)))
                      (:counts (first (db/has-system servervalue))))]
    (if (> servercount 0)
      (resp/json {:success false :msg "服务端口已存在"})
      (resp/json {:success true :msg (db/create-server
                                       {:servername servername :servervalue servervalue
                                        :parentid parentid :type type})})
      )
    )

  )

(defn addsystemlog [systemlogs]

  (db/add-systemlog
    systemlogs)
  )


(defn systemmachines [parentid]

  (resp/json (db/servertree parentid))
  )

(defn serverport [serverid ip]
  (let [results (db/serverport serverid)]

    (resp/json (map #(conj % {:isconnect (system/checkport ip (:servervalue %))}) results))
    )

  )
(defn serverport-app-check [serverid ip]
  (let [results (db/serverport serverid)]
    (map #(conj % {:isconnect (if (> (:type %) 0)  (system/checkappname ip (:servervalue %) @SSH_SHOW_LIST) (system/checkport ip (:servervalue %)))}) results)
    )
  )
(defn serverport-app [serverid ip]
  (let [results (serverport-app-check serverid ip)]
    results
    )
  )

(defn server-cpu [ip]

  (let [results (system/getCpuRatioByIpCommon ip @SSH_SHOW_LIST)
        version (system/getunix-version ip @SSH_SHOW_LIST)
        ]
    (if (nil? (first results)) "" (* 100 ( / (read-string (re-find #"\d+.\d+" (first results))) (if(= version "linux")
                                                                                                  (let [results (system/getLinuxCpu-num ip @SSH_SHOW_LIST)]
                                                                                                    (read-string (first results))
                                                                                                    )(let [results (system/getBsdCpu-num ip @SSH_SHOW_LIST)]
                                                                                                       (read-string (first results))
                                                                                                       )) )))

    )
  ;(if (= (system/getunix-version ip @SSH_SHOW_LIST) "linux")
  ;  (let [results (system/getCpuRatioByIp ip @SSH_SHOW_LIST)]
  ;    (if (> (count results) 0) (re-find #"\d+.\d+" (first results)) "")


  ;    )
  ;  (let [results (system/getCpuRatioByIpBsd ip @SSH_SHOW_LIST)]
  ;    (if
  ;      (> (count results) 0)
  ;      (

  ;        let [ memlist (map #(mem-cpu-filter % 2) results)]
  ;        (conmmon/sum memlist)

   ;       ) "")

   ;   )
   ; )

  )

(defn server-disk [ip]
  (let [results (system/getDiskRatioByIp ip @SSH_SHOW_LIST)]
    (if
      (> (count results) 0)
      (
        map #(conj {} {:value (re-find #"\d+%" %) :name (last (clojure.string/split % #" "))}) results
        ) [])
    )
  )

(defn mem-cpu-filter [item num]

  (let [itemlist (clojure.string/split item #"\s+")
        mem   (read-string (nth  itemlist num) )
        ]
    ;(println itemlist)
    (if ( >  mem 100) 0 mem)
    )
  )
(defn server-mem [ip]
  (if (= (system/getunix-version ip @SSH_SHOW_LIST) "linux")
    (let [results (system/getMemRationLinux ip @SSH_SHOW_LIST)]

      (if (> (count results) 0) (

                                  /
                                  (read-string (re-find #"\d+"  (re-find #"\w+ free" (first results))))
                                  (read-string (re-find #"\d+"  (re-find #"\w+ total" (first results))))
                                  ) "")


      )
    (let [results (system/getMemRatioByIpBsd ip @SSH_SHOW_LIST)]
      (if
        (> (count results) 0)
        (
          let [ memlist (map #(mem-cpu-filter % 3) results)]

          (/ (- 100 (conmmon/sum memlist)) 100)

          ;/
          ;(read-string (first (clojure.string/split (re-find #"\d+ free" (first results)) #"free")))


          ;(read-string (first (clojure.string/split (re-find #"\d+ total" (first results)) #"total")))

          ) "")

      )
    )


  )

(defn serverlist [key start limit]
  (let [results (db/serverlist start limit)]
    ;;(println (server-cpu "192.168.2.112"))
    (resp/json {:results (map #(conj %
                                 (let [isping (system/ping (:servervalue %))]
                                   {:isping isping
                                    :cpu (if(true? isping) (server-cpu (:servervalue %)) "")
                                    :mem (if(true? isping) (server-mem (:servervalue %)) "")
                                    :disk (if(true? isping) (server-disk (:servervalue %))  "")
                                    :apps (if(true? isping) (serverport-app (:key %) (:servervalue %)) [])}
                                   )
                                 ) results)
                :totalCount (:counts (first (db/servercount)))})
    )

  )





(defn cputimenow []
  (let [cpulistlist (map-indexed (fn [idx itm] {:name (inc idx) :value itm}) (system/getCpuRatioByIp "192.168.2.112" @SSH_SHOW_LIST))
        cpusmap (reduce (fn [initstr item] (conj initstr
                                             (read-string (str "{:cpu" (:name item) " " (* (read-string (:value item)) 100) "}"))))
                  {} cpulistlist)
        ]
    (conj {:time (System/currentTimeMillis)} cpusmap)
    )

  )
(defn memorytimenow []
  (let [memorymap (system/getMemoryRatio)
        ]
    memorymap
    )
  )
;;cpu info list
(defn getcpuratio []

  (resp/json [(cputimenow)])

  )

(defn getmemoryratio []
  (resp/json (memorytimenow))
  )



