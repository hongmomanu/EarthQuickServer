(ns lumprj.funcs.websocket
  (:use org.httpkit.server)
  (:require
            [clojure.data.json :as json]
            )
)

(def channel-hub (atom {}))

(defn handler [request]
  (with-channel request channel
    ;; Store the channel somewhere, and use it to sent response to client when interesting event happened
    (swap! channel-hub assoc channel request)
    (on-close channel (fn [status]
                        ;; remove from hub when channel get closed
                        (println channel " disconnected. status: " status)
                        (swap! channel-hub dissoc channel)))))




(defn start-server [port]
  (run-server handler {:port port})
  )


;(future (loop []
;          (println (keys @channel-hub))
;          (doseq [channel (keys @channel-hub)]
;            (println "ok")
;            (send! channel (json/write-str
;                                  {:happiness (rand 10)})
;              false)
;            )
;          (Thread/sleep 5000)
;          (recur)))







