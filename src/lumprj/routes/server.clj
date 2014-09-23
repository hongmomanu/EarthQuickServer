(ns lumprj.routes.server

  (:use compojure.core)
  (:require [lumprj.controller.server :as servermanager]
            [noir.response :as resp]
            [clojure.data.json :as json]
            )

  )


(defroutes server-routes

  (POST "/server/addserver" [servername servervalue parentid type]
    (servermanager/addserver servername servervalue parentid type)
    )
  (POST "/server/delserver" [serverid]
    (servermanager/delserver serverid)
    )
  (POST "/server/saveserver" [servername servervalue id username password machinecss]
    (servermanager/saveserver servername servervalue id username password machinecss)
    )
  (POST "/server/sendsystemlogs" [systemlogs]

    (servermanager/addsystemlog (json/read-str systemlogs
                                  :key-fn keyword))
    )

  (GET "/serverlist" [key start limit]
    (servermanager/serverlist key start limit)
    )

  (GET "/server/getsystems" [node]
    (servermanager/systemmachines node)
    )

  (GET "/serverport" [serverid ip]
    (servermanager/serverport serverid ip)
    )
  (GET "/getcpuratio" []
    (servermanager/getcpuratio)
    )

  (GET "/getmemoryratio" []
    (servermanager/getmemoryratio)
    )





)


