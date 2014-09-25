(ns lumprj.controller.log
  (:use compojure.core)
  (:require [lumprj.models.db :as db]
            [noir.response :as resp]
            [clojure.data.json :as json]
            [clj-http.client :as client]
            [clojure.xml :as xml]
            ;;[clj-soap.core :as soap]
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

#_(defn log-wlsdtest []

  (let [client (soap/client-fn "http://www.zjdz.gov.cn/webservice/articleapi.asmx")
        data  (client :GetAllCatalogList "ZJDZ" "L9dP2kaB")
        ]

    (resp/json {:success true :msg data})

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
(defn sendtelmsg [content]
  (let [

         users   (db/log-msgusers-all)
         telkey {:134 1 :135 1  :136 1   :137 1   :138 1   :139 1   :150 1   :151 1  :152 1 :157 1 :158 1  :159 1  :182 1  :183 1
                 :187 1 :188 1  :130 2  :131 2  :132 2  :155 2  :156 2  :185 2
                 :186 2  :133 3  :153 3  :180 3 :189  3}

         ]
    (doall (map #(db/insertmsm (:tel %) content (get telkey (keyword (subs (:tel %) 0 3)))) users))
    (resp/json {:success true})
    )

  )
(defn insert-users [telnum username]
  (let [
         nums (:counts (first (db/getuserbytel telnum)))
         ]
    (if (= nums 0) (db/addnewsenduser {:username username :tel telnum}))

    )

  )


(defn log-imptelusers [filepath]
  (let [data (:content (first (:content (xml/parse filepath))))
        ]
    (doall(map #(insert-users (:tel (:attrs %)) (:name (:attrs %)) ) data))
    (resp/json {:success true})
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
  (let [ fieldsvalue (assoc values "sendmethod" (json/write-str (get values "sendmethod")))
         ]

    (if (> (first (vals (db/addnewsendmessage fieldsvalue))) 0) (resp/json {:success true})
      (resp/json {:success false :msg "插入数据失败"})
      )
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

(defn log-sendsoap [url content action]
  (let [
         ;h {"SOAPAction" action}
         content (client/post url {:body content  :content-type  "application/soap+xml; charset=utf-8"   :socket-timeout 10000
                                                       :conn-timeout 10000})       ;:form-params (dissoc query-params "url")
        ]
    (:body content)
    )

  )

(defn log-sendweibo [username password content]
  (let [
         url  "https://api.weibo.com/2/statuses/update.json"
         resp (client/post url {:basic-auth [username password]
                                :form-params {:status content :source "2702428363"}
                                :socket-timeout 10000
                                :conn-timeout 10000})       ;:form-params (dissoc query-params "url")
        ]
    (:body resp)
    )

  )

