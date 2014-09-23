(ns lumprj.handler
  (:require [compojure.core :refer [defroutes]]
            [lumprj.routes.home :refer [home-routes]]
            [lumprj.routes.user :refer [user-routes]]
            [lumprj.routes.server :refer [server-routes]]
            [lumprj.routes.realstream :refer [realstream-routes]]
            [lumprj.routes.duty :refer [duty-routes]]
            [lumprj.routes.log :refer [log-routes]]
            [noir.util.middleware :as middleware]
            [compojure.route :as route]
            [taoensso.timbre :as timbre]
            [lumprj.models.schema :as schema]
            [lumprj.controller.server :as server]
            [lumprj.controller.realstream :as realstream]
            [lumprj.funcs.websocket :as websocket]
            [com.postspectacular.rotor :as rotor]
            [selmer.parser :as parser]
            [me.raynes.fs :as fs]
            [environ.core :refer [env]]))

(defroutes app-routes
  (route/resources "/")
  (route/not-found "Not Found"))

(defn init
  "init will be called once when
   app is deployed as a servlet on
   an app server such as Tomcat
   put any initialization code here"
  []
  (timbre/set-config!
    [:appenders :rotor]
    {:min-level :info
     :enabled? true
     :async? false ; should be always false for rotor
     :max-message-per-msecs nil
     :fn rotor/append})

  (timbre/set-config!
    [:shared-appender-config :rotor]
    {:path "lumprj.log" :max-size (* 512 1024) :backlog 10})



  (if (env :selmer-dev) (parser/cache-off!))

  (if-not (schema/initialized?) (schema/create-tables))
  (websocket/start-server 3001)
  (server/update-ssh-list) ;;更新ssh列表
  (realstream/updatestreamserver);;更新rtsserver配置信息
  (schema/create-streamcache-table) ;;创建数据流缓存表
  (schema/create-samplecache-table) ;;创建样本缓存表
  (timbre/info "创建缓存表结束")
  ;(timbre/info (let [parentpath (str schema/datapath "samplefiles/")]
  ;               (map #(str  parentpath %)(fs/list-dir parentpath))
  ;               ))

  ;(future (realstream/makerealstreamcache));;生成数据缓存
  ;(future (realstream/realstream-indirect ["*"]));;生成数据缓存直接取
  ;(future (realstream/realstream-del)) ;;删除数据  weile ceshi zhushi de shishi shuju
  ;(future (realstream/get-stations-realstream));;生成数据缓存直接取
  ;(future(realstream/make-sampledata-cache (let [parentpath (str schema/datapath "samplefiles/")]
  ;                                           (map #(str  par8080/lumprjentpath %)(fs/list-dir parentpath))
  ;                                           ))) ;;生成样本缓存

  ;(future (realstream/eqim-server-init));eqim 启动服务监听
  ;(future (realstream/rts-server-init));rts 启动服务监听
  (timbre/info "创建缓存结束")
  (timbre/info "lumprj started successfully"))

(defn destroy
  "destroy will be called when your application
   shuts down, put any clean up code here"
  []
  (timbre/info "lumprj is shutting down..."))

(defn template-error-page [handler]
  (if (env :selmer-dev)
    (fn [request]
      (try
        (handler request)
        (catch clojure.lang.ExceptionInfo ex
          (let [{:keys [type error-template] :as data} (ex-data ex)]
            (if (= :selmer-validation-error type)
              {:status 500
               :body (parser/render error-template data)}
              (throw ex))))))
    handler))

(def app (middleware/app-handler
           ;; add your application routes here
           [user-routes server-routes realstream-routes home-routes duty-routes log-routes app-routes]
           ;; add custom middleware here
           :middleware [template-error-page]
           ;; add access rules here
           :access-rules []
           ;; serialize/deserialize the following data formats
           ;; available formats:
           ;; :json :json-kw :yaml :yaml-kw :edn :yaml-in-html :xml
           :formats [:json-kw :edn :json :jsonp]))
