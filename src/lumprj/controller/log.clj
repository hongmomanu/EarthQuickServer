(ns lumprj.controller.log
  (:use compojure.core)
  (:require [lumprj.models.db :as db]
            [noir.response :as resp]
            [clojure.data.json :as json]
            )
  )

(defn log-system-statics [params]
  (let [searchtype (:type params)]
    (if (= searchtype "duty")
      (resp/json (db/log-duty-statics params))
      (resp/json (db/log-system-statics params))
    )
  )
  )
(defn log-system-statics-dayinfo [day searchtype]
  (if (= searchtype "duty")
    (resp/json (db/log-duty-statics-dayinfo day))
    (resp/json (db/log-system-statics-dayinfo day))
    )
  )

(defn log-system-del [params]
  (resp/json (db/log-del params))
  )
(defn log-system-list [params]
     (resp/json
        {
         :results (db/log-list params )
         :totalCount (:counts (first (db/log-count params)))
        }
       )

  )
(defn log-duty-list  [params]

     (resp/json
        {
         :results (db/log-duty-list params)
         :totalCount (:counts (first (db/log-duty-count params)))
        }
       )

  )

(defn log-sendmessage-list [params]

  (resp/json
    {
      :results (db/log-sendmessage-list params)
      :totalCount (:counts (first (db/log-sendmessage-count params)))
      }
    )

  )
(defn log-sendmessage-insert [values]

  (if (> (first (vals (db/addnewsendmessage values))) 0) (resp/json {:success true})
    (resp/json {:success false :msg "插入数据失败"})
    )

  )
(defn log-sendmessage-update [values]

  (let [
         fieldsvalue (if (nil?(get values "sendmethod"))values (assoc values "sendmethod" (json/write-str (get values "sendmethod"))))
         ]

    (db/updatesendmessage fieldsvalue)
    (resp/json {:success true})
    )



  )
(defn log-sendmessage-del [id]

  (db/delsendmessage id)
  (resp/json {:success true})
  )

