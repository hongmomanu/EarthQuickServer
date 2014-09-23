(ns lumprj.controller.log
  (:use compojure.core)
  (:require [lumprj.models.db :as db]
            [noir.response :as resp]
            [clojure.data.json :as json]
            [clojure.xml :as xml]
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

(defn log-imptelusers [filepath]
  (let [data (:content (first (:content (xml/parse filepath))))
        telkey {:134 1 :135 1  :136 1   :137 1   :138 1   :139 1   :150 1   :151 1  :152 1 :157 1 :158 1  :159 1  :182 1  :183 1
             :187 1 :188 1  :130 2  :131 2  :132 2  :155 2  :156 2  :185 2
         :186 2  :133 3  :153 3  :180 3 :189  3}
        ]
    (doall(map #(sendtelmsg (:tel (:attrs %)) (:name (:attrs %)) telkey) data))
    (resp/json {:success true})
    )


  )
(defn sendtelmsg [telnum username telkey]
  (let [
          telpart (get telkey (keyword (subs telnum 0 3)))
          nums (:counts (first (db/getuserbytel telnum)))
         ]
    (if (= nums 0) (db/addnewsenduser {:username username :tel telnum}))

    )

  )
(defn log-msgusers-list [params]
  (resp/json
    {
      :results (db/log-msgusers-list params)
      :totalCount (:counts (first (db/log-msgusers-count params)))
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
(defn log-msgusers-insert [values]

  (if (> (first (vals (db/addnewsenduser values))) 0) (resp/json {:success true})
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

