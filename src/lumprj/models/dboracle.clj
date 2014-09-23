(ns lumprj.models.dboracle
  (:use korma.core
        [korma.db :only [defdb with-db]])
  (:require [lumprj.models.schema :as schema]))

(defdb dboracle schema/db-oracle)






(defentity t_doorplate
  (database dboracle)
  )

(defn oracltest []
  (with-db dboracle
    ;;(select t_doorplate
      (select t_doorplate (aggregate (count :id) :cnt)))
    ;;(exec-raw ["SELECT count(*) FROM t_doorplate" []] :results))
  )

