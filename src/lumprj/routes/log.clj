(ns lumprj.routes.log
  (:use compojure.core)
  (:require [lumprj.controller.log :as logmanager]
            [noir.response :as resp]
            [clojure.data.json :as json]
            )

  )

(defroutes log-routes

  (GET "/log/getlogsystem" request
    (logmanager/log-system-list (:params  request))

    )
  (GET "/log/getlogduty" request
    (logmanager/log-duty-list (:params  request))

    )
  (GET "/log/getSendMsgConfig" request
    (logmanager/log-sendmessage-list (:params  request))

    )

  (GET "/log/sendtelmsg" [content]
    (logmanager/sendtelmsg content)
    )
  (POST "/log/sendtelmsg" [content]
    (logmanager/sendtelmsg content)
    )

  (GET "/log/getSendMsgUsers" request
    (logmanager/log-msgusers-list (:params  request))

    )

  (GET "/log/imptelusers" [filepath]
     (logmanager/log-imptelusers filepath)
    )

  (POST "/log/insertSendMsgUsers" request
    (logmanager/log-msgusers-insert (:form-params  request))

    )

  (POST "/log/insertSendMsgConfig" request
    (logmanager/log-sendmessage-insert (:form-params  request))

    )
  (POST "/log/updateSendMsgConfig" request
    (logmanager/log-sendmessage-update (:form-params  request))

    )
  (POST "/log/delSendMsgConfig" [id]
    (logmanager/log-sendmessage-del id)

    )




  (POST "/log/deletelogs" request
    (logmanager/log-system-del (:params  request))

    )

  (GET "/log/logsystemstatics" request
    (logmanager/log-system-statics (:params  request))
    )
  (GET "/log/logsystemstaticsinfobyday" [day searchtype]
    (logmanager/log-system-statics-dayinfo day searchtype)
    )
  )