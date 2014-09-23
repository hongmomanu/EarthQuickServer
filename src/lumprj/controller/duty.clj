(ns lumprj.controller.duty
  (:import
           (java.util Calendar)
           (java.sql Timestamp)
           (java.text SimpleDateFormat)
           )
  (:use compojure.core)
  (:require [lumprj.models.db :as db]
            [noir.response :as resp]
            [me.raynes.fs :as fs]
            [clj-http.client :as client]
            [clj-time.format :as f]
            [clj-time.local :as l]
            )
  )

(defn dutylist []
  (resp/json (db/duty-list))
  )
(def custom-formatter (f/formatter "yyyyMMdd"))

(defn getcurrentduty [day date]
  (resp/json (db/duty-query-day day date))
  )

(defn getmissions []
  (resp/json (db/mission-query))
  )
(defn savemission [request]
  (let [form-params (:form-params request)]
    (db/update-mission form-params (get form-params "id"))
    (resp/json {:success true})
    )
  )

(defn delmission [missionid]
  (db/del-mission missionid)
  (resp/json {:success true})
  )

(defn getdutymissions [day]
  (resp/json (db/mission-today-list day))
  )

(defn insertnewmission [userid]
  (db/mission-history-insert (let [missions (db/mission-query)]
    (map #(conj {:missionid (:id (select-keys % [:id]))} {:userid userid :missionstatus 0}) missions)
    ))
  (resp/json {:success true})
  )

(defn getworkmanagerevents [startDate endDate]
  (let [data (db/getworkmanagerevents startDate (str endDate "T"))]
    (resp/json {:success true :data (map #(conj {:ad true} %) data)})
    )

  )

(defn addworkmanagerevents [cid start end]
  (resp/json {:success true :data [{:cid cid :start start :end end
                                    :id (first (vals (db/addworkmanagerevents cid start end)))}]
              :msg "添加成功"} )
  )

(defn saveworkmanagerevents [id cid start end]
  (db/saveworkmanagerevents id {:userid cid :start start :end end})
  (resp/json {:success true :data [{:cid cid :start start :end end
                                    :id id}]
              :msg "保存成功"} )
  )

(defn deleteworkmanagerevents [id]
  (db/deleteworkmanagerevents id)
  (resp/json {:success true :data []
              :msg "删除成功"} )
  )

(defn getcalendars []
  (let [results (db/user-list)]
    (resp/json {:results (map #(conj {:color (if(even? (:id %)) (- 32 (:id %))(+ 0 (:id %) )) } %) results)})
    )

    )

(defn getstationinfo [stationcode]
  (resp/json (db/get-station-code stationcode))
  )
(defn getstations[keyword start limit]
  (resp/json
    {
      :results (db/stations-list keyword start limit )
      :totalCount (:counts (first (db/stations-count keyword start limit)))
      }
    )

  )
(defn addnewstation [values]

  (if (> (first (vals (db/savenewstation values))) 0) (resp/json {:success true})
    (resp/json {:success false :msg "插入数据失败"})
    )
  (resp/json {:success true})
  )
(defn savestation [values]
  (db/savestation values)
  (resp/json {:success true})

  )
(defn delstation [sid]
  (db/delstation sid)
  (resp/json {:success true})
  )
(defn mysqlalert []
  (resp/json (db/mysqlalert))
  )
(defn recordcheck [today yestoday]
  (let [results (db/is-suspend today yestoday)]
    (resp/json {:success (> (count results) 0) :result results})
    )
  )
(defn maketodaymission [day userid]
  (if (= (:counts (first (db/mission-history-query day userid))) 0)
    (insertnewmission userid)(resp/json {:success false}))
  )
(defn insertduty [day userid]
  (if (> (count (db/duty-query-day day)) 0) (resp/json {:success false :msg "已存在"})
    (if (> (first (vals (db/duty-insert day userid))) 0) (resp/json {:success true})
      (resp/json {:success false :msg "插入数据失败"})
      )
    )

  )
(defn insertmission [missionname missiontime missioninterval]
  (if (> (first (vals (db/mission-insert missionname missiontime missioninterval))) 0) (resp/json {:success true})
    (resp/json {:success false :msg "插入数据失败"})
    )
  )

(defn senddutylogs [dutylogs]
  (db/add-dutylog
    dutylogs)
  )

(defn copywavefile [sourcedir targetdir]
  (fs/copy-dir sourcedir targetdir)
  (resp/json {:success (fs/exists? sourcedir)})

  )

(defn checkevents [sourcedir  archiveminsize]

  (let [
         cal (Calendar/getInstance)
         df   (new SimpleDateFormat "yyyyMMdd")
         dfm (new SimpleDateFormat "M")
         datastr2 (do (.add cal Calendar/DATE -1) (.format df (new Timestamp (->(.getTime cal)(.getTime)))))
         mstr  (do (.add cal Calendar/DATE -1) (.format dfm (new Timestamp (->(.getTime cal)(.getTime)))))
         results1 (doall (fs/find-files
                    (str sourcedir "seed/" mstr "/")  ; The root directory to search
                           (re-pattern (str "(.*)" datastr2 "(.*)" ))))
         results2 (doall (fs/find-files
                    (str sourcedir "evt/" mstr "/")  ; The root directory to search
                           (re-pattern (str "(.*)" datastr2 "(.*)" ))))
         allresults (into results1 results2)
         ]
    (resp/json {:success (empty? allresults) :results (map #(fs/base-name %) allresults)  })  ;(/ (count allresults) 2)
    )
  )
;;波形文件监测
(defn checkarchive [sourcedir  archiveminsize]
  (let [earthplatformlist (db/stationcode-list)
        networklist (db/network-list)
        ;datestr (f/unparse custom-formatter (l/local-now))
        cal (Calendar/getInstance)
        df   (new SimpleDateFormat "yyyyMMdd")
        datastr2 (do (.add cal Calendar/DATE -1) (.format df (new Timestamp (->(.getTime cal)(.getTime)))))
        hourrange (map #(if (< % 10) (str 0 %) %) (range 0 24))
        earthplatformliststr (map #(conj {:str (str datastr2 "." (:networkcode %) "." (:stationcode %) ".seed" )} %) earthplatformlist)

        ]

    (let [
           netresultsstr (for [x networklist
                            y hourrange
                            ]
                        (str datastr2 y "." (:networkcode x) ".seed" ))

           netresults (filter
                            #(false? (and (fs/child-of? sourcedir (str sourcedir %))
                                       (>= (fs/size (str sourcedir %)) (* (read-string archiveminsize) 1024)))) netresultsstr)


           results (filter
                    #(false? (and (fs/child-of? sourcedir (str sourcedir (:str %)))
                               (>= (fs/size (str sourcedir (:str %))) (* (read-string archiveminsize) 1024)))) earthplatformliststr)

           allresults (into (map #(:str %) results) netresults)
           ]
      (resp/json {:success (empty? allresults) :results allresults})
      )

    )


  )


(defn sendsms [tel msg telpart]
  (db/insertmsm tel msg telpart)
  (resp/json {:success true})
  )
(defn completeduty [id dutylog]
  (db/completedutymission id dutylog)
  (resp/json {:success true})
  )
(defn eqimcheck [id username password url securl]
  (let [my-cs (clj-http.cookies/cookie-store)]
    (println url securl)
    (println  username password)

    (try
      (client/post url {:form-params {:name username :pwd password}
                        :socket-timeout 5000
                        :conn-timeout 5000
                        :cookie-store my-cs})
      (resp/json {:success true :msg (:body (client/get securl {:cookie-store my-cs
                                                                :as :auto}))})

      (catch Exception e (resp/json {:success false}))
      )

    )


  )

(defn newrecord [params]
  (try
    (client/post (:url params) {:form-params params  :socket-timeout 1000
                                :conn-timeout 1000})
    (resp/json {:success true :msg "ok"})
    (catch Exception e (resp/json {:success false}))
    )

  )

(defn eqimcheck-nologin [url]
  (let [my-cs (client/get url {:as :auto  :socket-timeout 1000
                               :conn-timeout 1000})]

    (try
      (println (:body my-cs))
      (resp/json {:success true :msg (:body my-cs)})

      (catch Exception e (resp/json {:success false}))
      )

    )


  )


(defn delenumbyid [request]
  (let [form-params (:form-params request)]
    (db/duty-del-byids (get form-params "enumids"))
    )
  )

