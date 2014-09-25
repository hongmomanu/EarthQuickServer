(ns lumprj.models.db
  (:use korma.core
        [korma.db :only [defdb with-db]])
  (:require [lumprj.models.schema :as schema]))

(defdb db schema/db-spec-sqlite)
(defdb dbmysql schema/db-mysql)
(defdb memdb schema/db-h2-mem)
(defdb h2db schema/db-spec)
(defdb orcldb schema/db-oracle)
(defdb orcldbstation schema/db-oraclestation)


(declare users dutyenum dutymission dutymissionhistory suspend)


(defentity streamcache
  (database memdb)
  )
(defentity samplecache
  (database memdb)
  )
(defentity sample
  (database h2db)
)



(defentity dutyenum
  (database db)
  (belongs-to users {:fk :userid})
  )
(defentity users
  (database db)
  ;;(has-one dutyenum {:fk :userid})
  )
(defentity dutymission
  (database db)
  )
(defentity dutymissionhistory
  (database db)
  (belongs-to dutymission {:fk :missionid})
  )


(defentity servers
  (database db)
  )

(defentity systemwatchlog
  (database db)
  (belongs-to servers {:fk :serverid})
  (belongs-to users {:fk :userid})
  )

(defentity dutylog
  (database db)
  (belongs-to users {:fk :userid})
  )

(defentity sendmessage
  (database db)
  )
(defentity messagetel
  (database db)
  )

(defentity stations
  (database db)
  )

(defentity suspend
  (database db)
  )

(defn is-suspend [today yestday]
  (select suspend
    (fields :station :begin_time :end_time)
    (where (or (and
             {:begin_time [>= yestday] :end_time [= nil]}
             {:begin_time [< today]})

             (and
               {:end_time [not= nil]} ;:begin_time [>= yestday]
               {:end_time [>= yestday]}
               {:end_time [< today]}
               {:begin_time [< today]} {:end_time [> (sqlfn DATETIME :begin_time "+10 minutes")]})

             )
      )
    )
  )
(defn has-suspend-station [station]
  (update suspend
    (set-fields {:end_time (sqlfn datetime "now" "localtime")})
    (where {:station station :end_time [= nil]})
    )
  )

(defn is-suspend-station [station]
  (select suspend
    ;(set-fields {:end_time (sqlfn datetime "now" "localtime")})
    (where {:station station :end_time [= nil]})
    )
  )

(defn new-suspend-station [station time]
  (insert suspend
    (values {:station station :begin_time time})
    )
  )
(defn insert-streamcache [caches]
  (insert streamcache
    (values caches)
    )
  )
(defn del-samplecache [caches]
  (delete samplecache
    (where (and
             ;{:time [>= (:time caches)]} {:time [< (:edtime caches)]}
             {:time [= (:time caches)]}
             {:stationname (:stationname caches)} {:type (:type caches)}) )
    )
  )
(defn del-samplecache-type [typeid]
  (delete samplecache
    (where {:type typeid} )
    )
  )

(defn del-sample [name]
  (delete sample
    (where {:name name})
    )
  )
(defn get-sample-byname [name]
  (select sample
    (where {:name name})
    )
  )
(defn get-samplecache-type [typeid]
  (select samplecache
    (where {:type typeid} )
    )
  )
(defn insert-samplecache [caches]
  (insert samplecache
    (values  caches )
    )
  )

(defn insert-sample [caches]
  (insert sample
    (values  caches )
    )
  )

(defn update-streamcache [caches]
  (update streamcache
    (set-fields caches)
    (where {:time (:time caches) :stationname (:stationname caches)}
      )
    )
  )

(defn del-streamcache [name]

  (delete streamcache
    (where {:time [< (sqlfn DATEADD "MINUTE"  -10 (sqlfn now) )]
            :stationname name
            })
    )
  )

(defn del-streamcache-all []

  (delete streamcache
    (where {:time [< (sqlfn DATEADD "MINUTE"  -10 (sqlfn now) )]
            })
    )
  )

(defn get-streancache-num []
  (select streamcache
    (aggregate (count :stationname) :counts)
    )
  )

(defn has-streamcache [time stationname]

  (select streamcache
    (fields :stationname)
    (where {:time time :stationname stationname}
             )
    )
  )
(defn get-streamcache []
  (select streamcache
    (fields :stationname :time)
    (where (and {:time [<= (sqlfn now)]}
             {:time [>= (sqlfn DATEADD "MINUTE"  -3 (sqlfn now) )]}
             ))
    (order :time :ASC)
    )
  )


(defn get-streamcacheall-data [station step]
  (select streamcache
    (where
      {:stationname  [like (str  station "%")]}
      )
    (limit step)
    (order :time :DESC)
    )

  )

(defn get-streamcacheall[starttime station]
  (select streamcache
    (where
      {:stationname station  :time [>= starttime]}
      )
    (order :time :ASC)
    )
  )
(defn get-samplecache [starttime station type]
  ;(println starttime station)
  (select samplecache
    (fields  [(sqlfn FORMATDATETIME :time "yyyy-MM-dd hh:mm:ss.SS" "local" "GMT") :time]
             :stationname :data)
    (where
             {:stationname station :time [>= starttime] :type type}
    )
    (order :time :ASC)
    )

  )
(defn get-samplecache-less [starttime station type]
  ;(println starttime station)
  (select samplecache
    (fields   :time ;;[(sqlfn FORMATDATETIME :time "yyyy-MM-dd hh:mm:ss.SS" "local" "GMT") :time]
             :stationname :data :rate)
    (where
      (or {:stationname station :time [>= starttime] :type type}
        {:stationname station :time [< starttime] :edtime [> starttime] :type type}
        )

    )
    (order :time :ASC)
    )

  )

(defn get-sample-less [starttime station name]
  ;(println starttime station)
  ;(println starttime station name)
  (select sample
    (fields   :time ;;[(sqlfn FORMATDATETIME :time "yyyy-MM-dd hh:mm:ss.SS" "local" "GMT") :time]
             :stationname :data :rate)
    (where
      (or {:stationname station :time [>= starttime] :name name}
        {:stationname station :time [< starttime] :edtime [> starttime] :name name}
        )

    )
    (order :time :ASC)
    )

  )
(defn get-real-less [starttime station]
  ;(println starttime station)
  ;(println starttime station name)
  (select streamcache
    (fields   :time ;;[(sqlfn FORMATDATETIME :time "yyyy-MM-dd hh:mm:ss.SS" "local" "GMT") :time]
             :stationname :data :rate :edtime)
    (where
      (or {:stationname station :time [>= starttime] }
        {:stationname station :time [< starttime] :edtime [> starttime]}
        )

    )
    (order :time :ASC)
    )

  )

(defn get-samplecache-bytype-less [starttime station type]
  ;(println starttime station)
  (select samplecache
    ;(fields  [(sqlfn FORMATDATETIME :time "yyyy-MM-dd hh:mm:ss.SS" "en" "GMT+") :time]
    ;         [(sqlfn FORMATDATETIME :edtime "yyyy-MM-dd hh:mm:ss.SS" "en" "GMT") :edtime]
    ;         :stationname :data)
    (where
      (or {:stationname station :time [>= starttime] :type type}
        {:stationname station :time [< starttime] :edtime [> starttime] :type type}
        )
    )
    (order :time :ASC)
    )

  )
(defn get-sample-bytype-less [starttime station name]
  ;(println starttime station)
  (select sample
    ;(fields  [(sqlfn FORMATDATETIME :time "yyyy-MM-dd hh:mm:ss.SS" "en" "GMT+") :time]
    ;         [(sqlfn FORMATDATETIME :edtime "yyyy-MM-dd hh:mm:ss.SS" "en" "GMT") :edtime]
    ;         :stationname :data)
    (where
      (or {:stationname station :time [>= starttime] :name name}
        {:stationname station :time [< starttime] :edtime [> starttime] :name name}
        )
    )
    (order :time :ASC)
    )



  )
(defn get-samplecache-bytype [starttime station type]
  ;(println starttime station)
  (select samplecache
    ;(fields  [(sqlfn FORMATDATETIME :time "yyyy-MM-dd hh:mm:ss.SS" "en" "GMT+") :time]
    ;         [(sqlfn FORMATDATETIME :edtime "yyyy-MM-dd hh:mm:ss.SS" "en" "GMT") :edtime]
    ;         :stationname :data)
    (where
             {:stationname station :time [>= starttime] :type type}
    )
    (order :time :ASC)
    )

  )
(defn create-user [user]

  (insert users
          (values user))

  )

(defn add-systemlog [systemlog]
  (insert systemwatchlog
    (values systemlog)
    )
  )

(defn add-dutylog [dutylogs]
  ;;(insert dutylog
  (insert systemwatchlog
    (values dutylogs)
    )
  )

(defn create-server [server]

  (insert servers
    (values server))
  )

(defn update-server [data id]
  (update servers
    (set-fields data)
    (where {:id id}))

  )
(defn del-mission [missionid]
  (delete dutymission
    (where {:id missionid})
    )
  )
(defn update-mission [data id]
  (update dutymission
    (set-fields data)
    (where {:id id})
    )
  )
(defn update-user [data]
  (update users
  (set-fields data)
  (where {:id (:id data)})))

(defn del-user [userid]
  (delete users
    (where {:id userid})
    )
  )

(defn get-user [id]
  (first (select users
                 (where {:id id})
                 (limit 1))))

(defn user-list []
  (select users
    (fields [:id :userid]  :id [:displayname :title] :displayname :username :telnum :password :admin)
    ;(limit 1)
    )
  )

(defn log-system-statics [params]
  (select systemwatchlog
    (fields  [(sqlfn strftime "%Y-%m-%d" :time) :date])
    (where (and {:time [> (:bgday params)]}
             {:time [< (:edday params)]}
             ))
    (aggregate (count :id) :counts)
    (group :date)
    )
  )
(defn log-duty-statics [params]
  (select dutylog
    (fields  [(sqlfn strftime "%Y-%m-%d" :time) :date])
    (where (and {:time [> (:bgday params)]}
             {:time [< (:edday params)]}
             ))
    (aggregate (count :id) :counts)
    (group :date)
    )
  )

(defn log-duty-statics-dayinfo [day]
  (select dutylog
    (fields  :statustype)
    (where {:time [like (str day "%")]})
    (aggregate (count :id) :counts)
    (group :statustype)
    )
  )

(defn log-system-statics-dayinfo [day]
  (select systemwatchlog
    (fields  :statustype)
    (where {:time [like (str day "%")]})
    (aggregate (count :id) :counts)
    (group :statustype)
    )
  )
(defn log-del [params]
  (delete systemwatchlog
    (where (and {:time [> (:bgday params)]}
             {:time [< (:edday params)]}
             )
      )
    (where {:logcontent [like (str "%" (:keyword params) "%") ]})
    (where {:statustype [like (str (:statustype params) "%")]})

    )
  )
(defn log-list [params]

    (select systemwatchlog
      (with servers
        (fields :servername :servervalue)
        )
      (with users
        (fields :username :displayname)
        )
      (where (and {:time [> (:bgday params)]}
               {:time [< (:edday params)]}
               )
        )

      (where {:logcontent [like (str "%" (:keyword params) "%") ]})
      (where {:statustype [like (str (:statustype params) "%")]})
      (limit (:limit params))
      (offset (:start params))
      (order :id :DESC)
      )

  )
(defn log-duty-list [params]
  (select dutylog
    (with users
      (fields :username)
      )
    (where (and {:time [> (:bgday params)]}
             {:time [< (:edday params)]}
             ))
    (where {:logcontent [like (str "%" (:keyword params) "%") ]})
    (limit (:limit params))
    (offset (:start params))
    (order :id :DESC)
    )
  )

(defn log-sendmessage-list [params]
  (select sendmessage
    (limit (:limit params))
    (offset (:start params))
    (order :id :DESC)
    )
  )
(defn log-msgusers-list [params]
  (select messagetel
    (limit (:limit params))
    (offset (:start params))
    (order :id :DESC)
    )
  )

(defn log-msgusers-all []
  (select messagetel)
  )

(defn get-station-code [code]
  (select stations
    (fields :stationcode :stationname :networkcode :networkname)
    (where {:stationcode code})
    )
  )
(defn stationcode-list []
  (select stations
    (fields :stationcode :stationname :geom :networkcode :id :dataaddr :gatewayaddr :contact :phone :connecttype :crossnums)
    )
  )
(defn network-list []
  (select stations
    (fields :networkcode)
    (group :networkcode)
    )
  )
(defn stations-list [keyword starts limits]
  (select stations
    (where {:stationname [like (str "%" keyword "%") ]})

    (limit limits)
    (offset starts)
    (order :id :DESC)
    )
  )

(defn stations-count [keyword start limit]
  (select stations
    (where {:stationname [like (str "%" keyword "%") ]})
    (aggregate (count :id) :counts)
    )
  )

(defn savenewstation [key-values]
  (insert stations
    (values  key-values)
    )

  )
(defn addnewsendmessage [key-values]
  (insert sendmessage
    (values  key-values)
    )

  )

(defn delsenduser [id]
  (delete messagetel
    (where {:id id})
    )
  )


(defn addnewsenduser [key-values]
  (insert messagetel
    (values  key-values)
    )

  )
(defn getuserbytel [tel]
  (select messagetel
    (where {:tel tel})
    (aggregate (count :id) :counts)
    )
  )
(defn updatesenduser [key-values]
  (update messagetel
    (set-fields key-values)
    (where {:id  (get key-values "id")}
      )
    )
  )
(defn updatesendmessage [key-values]

  (update sendmessage
    (set-fields key-values)
    (where {:id  (get key-values "id")}
      )
    )
  )

(defn delsendmessage [sid]

  (delete sendmessage
    (where {:id sid})
    )
  )

(defn delstation [sid]
  (delete stations
    (where {:id sid})
    )
  )
(defn savestation [key-values]
  (update stations
    (set-fields key-values)
    (where {:id  (:id key-values)}
      )
    )
  )
(defn log-duty-count [params]
  (select dutylog
    (where (and {:time [> (:bgday params)]}
             {:time [< (:edday params)]}
             ))
    (where {:logcontent [like (str "%" (:keyword params) "%") ]})
    (aggregate (count :id) :counts)
    )
  )
(defn log-sendmessage-count [params]
  (select sendmessage
    (aggregate (count :id) :counts)
    )
  )
(defn log-msgusers-count [params]
  (select messagetel
    (aggregate (count :id) :counts)
    )
  )

(defn log-count [params]
  (select systemwatchlog
    (where (and {:time [>= (:bgday params)]}
                {:time [<= (:edday params)]}
             ))
    (where {:logcontent [like (str "%" (:keyword params) "%") ]})
    (where {:statustype [like (str (:statustype params) "%")]})
    (aggregate (count :id) :counts)
    )
  )

(defn duty-list []
  (select dutyenum

    (fields [:id :enumid] :day :userid)
    (with users
      (fields :username :displayname)
      )
    (order :day)
    )
  )
(defn duty-del-byids [ids]
  (delete dutyenum
    (where {:id (if(instance? String ids)ids [in ids])})
    )
  )
(defn mission-query []
  (select dutymission)
  )

(defn completedutymission [id dutylog]
  (update dutymissionhistory
    (set-fields {:missionstatus 1 :time (sqlfn datetime "now" "localtime") :dutylog dutylog})
    (where {:id id}))
  )
(defn mission-today-list [day]
  (select dutymissionhistory
    (with dutymission
      (fields :missionname :missionlabel :missiontime :missioninterval )
      )
    (where {:time [like (str day "%")]})
    )
  )
(defn mission-history-query [day userid]
  (select dutymissionhistory
    (where {:time [like (str day "%")] }) ;;:userid userid
    (aggregate (count :id) :counts)
    )

  )

(defn addworkmanagerevents [cid start end]
  (insert dutyenum
    (values {:start start :end end :userid cid }))
  )
(defn saveworkmanagerevents [id data]
  (update dutyenum
    (set-fields data)
    (where {:id id})
    )
  )
(defn deleteworkmanagerevents [id]
  (delete dutyenum
    (where {:id id})
    )
  )
(defn getworkmanagerevents [startDate endDate]
  (select dutyenum
    (fields [:id :enumid] :id  :day :userid [:userid :cid] :start :end )
    (with users
      (fields [:displayname :title] :username)
      )
    (where (or (and {:start [>= startDate]}
             {:start [<= endDate]}
             )
             (and {:end [>= startDate]}
               {:end [<= endDate]}
               )
             )

      )
    )
  )

(defn duty-query-day [day date]
  (select dutyenum
    (fields [:id :enumid] :day :userid )
    (with users
      (fields :username :displayname :telnum)
      )
    ;(where {:day day})
    (where (and {(sqlfn strftime "%Y-%m-%d" :start "localtime") [<= date]}
                 {(sqlfn strftime "%Y-%m-%d" :end "localtime") [>= date]}
                 )
      )
    )
  )
(defn duty-insert [day userid]
  (insert dutyenum
    (values {:day day :userid userid}))
  )
(defn mission-history-insert [missionlist]
  (insert dutymissionhistory
    (values missionlist)
    )
  )

(defn mission-insert [missionname missiontime missioninterval]
  (insert dutymission
    (values {:missionname missionname :missiontime missiontime :missioninterval missioninterval})
    )
  )

(defn has-user [username pass]
  (select users
    (where {:username username :password pass})
    ))
  ;;(with-db db
  ;;  (exec-raw ["SELECT * FROM users WHERE username = ? and password=?" [username pass]] :results))
  ;;               )

(defn has-username [username]
  (with-db db
    (exec-raw ["SELECT * FROM users WHERE username = ? " [username]] :results))
  )

(defn del-server [serverid]
  (delete servers
    (where {:id serverid})
    )
  )
(defn has-server [servername servervalue]
  (with-db db
    (select servers (where {:servervalue servervalue :servername servername}) (aggregate (count :id) :counts)))
    ;;(exec-raw ["SELECT * FROM servers WHERE servername = ? and servervalue=?" [servername servervalue]] :results))
  )
(defn has-system [servervalue]
    (with-db db
      (select servers (where {:servervalue servervalue}) (aggregate (count :id) :counts)))

  )
(defn servertree [parentid]
  (select servers
    (where {:parentid parentid})
    (order :id))

  )
(defn serverlist [start limits]


  (select servers
    (fields [:id :key] :servername :servervalue :machinecss :type :parentid :time :username :password)
    (where {:parentid -1})
    (order :id)
    (limit limits)
    (offset start))

    ;;(exec-raw ["SELECT * FROM servers WHERE parentid=-1 limit ? offset ?" [limit start]] :results))
  )


(defn servercount []
  (with-db db
    (select servers (where {:parentid -1}) (aggregate (count :id) :counts)))
    ;;(exec-raw ["SELECT * FROM servers WHERE parentid=-1" []] :results))
 )

(defn serverport [serverid]
  (with-db db
    (exec-raw ["SELECT * FROM servers WHERE parentid=?" [serverid]] :results))
  )

(defn get-rts-contentinfo [catalogid]
  (with-db dbmysql
    (exec-raw ["select  Net_code , Sta_code ,Chn_code , DATE_FORMAT(Phase_time, '%Y-%m-%d %H:%i:%s') as time  from  Phase_A where Catalog_id =?" [catalogid]] :results))

  )
(defn get-rts-eventinfo [eventid]
  (with-db dbmysql
    (exec-raw ["select id,Epi_lat, Epi_lon, DATE_FORMAT(O_time, '%Y-%m-%d %H:%i:%s') as o_time from  Catalog_A where Event_id =?" [eventid]] :results))
  )
(defn mysqlalert []
  (with-db dbmysql
    (exec-raw ["SELECT * FROM rt where MATCH ('\"320902\"')" []] :results))
  )

(defn insertmsm [tel msg telpart]

  (with-db orcldb
    (exec-raw ["insert into m_sms_queue (TELNUMBER,MSGCONTENT,state,telpart) values (?,?,?,?)" [tel msg 0 telpart]])
    )
  )

(defn stationcheck [stationname btime etime]

  (with-db orcldbstation
           (exec-raw ["select ALARMNAME,sdate from valarmrtinfonow where StationName like ? || '%'
           and sdate >= to_date(?,'yyyy-MM-dd') and sdate < to_date(?,'yyyy-MM-dd')  order by sdate desc " [stationname btime etime]] :results)
  )

  )

