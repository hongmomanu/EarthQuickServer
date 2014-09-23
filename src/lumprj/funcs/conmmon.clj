(ns lumprj.funcs.conmmon


  )


(defn sum [arr]
  (apply + arr)
    )
(defn average [coll]
  (if (= (count coll) 0) 0  (let [sum (apply + coll)]
                    (quot sum (count coll))))
  )


(defn get-config-prop []
  (let [filename (str (System/getProperty "user.dir") "/" "server.config")]
    (read-string (slurp filename))
    )
  )