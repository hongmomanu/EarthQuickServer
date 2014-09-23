(ns lumprj.routes.user
  (:use compojure.core)
  (:require [lumprj.controller.user :as user]
            [noir.response :as resp]
            )

  )


(defroutes user-routes

  (POST "/user/adduser" [username password displayname admin telnum departments email]
    (user/adduser username password displayname admin telnum departments email)
    )
  (POST "/user/saveuser" request
    (user/saveuser request)
    )
  (POST "/user/deluser" [userid]
    (user/deluser userid)
    )
  (GET "/users" []
    (user/userlist)
    )
  (POST "/login" [username password]

    (user/login username password)

  )


)


