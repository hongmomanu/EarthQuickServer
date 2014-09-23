(ns lumprj.routes.home
  (:use compojure.core)
  (:require [lumprj.views.layout :as layout]
            [noir.response :as resp]
            [lumprj.util :as util]))

(defn home-page []
  (resp/json {:success true :msg "启动成功"}
    ))

(defn about-page []
  (layout/render "about.html"))
(comment "(defn home-page []
  (layout/render
    'home.html' {:content (util/md->html '/md/docs.md')})");;(util/md->html "/md/success.md")

(defroutes home-routes

  ;;(GET "/" [] (resp/redirect "index.html"))
  (GET "/" [] (home-page))
  (GET "/about" [] (about-page)))


