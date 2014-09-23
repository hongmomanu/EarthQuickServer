(defproject
  lumprj
  "0.1.0-SNAPSHOT"
  :dependencies
  [
   [com.h2database/h2 "1.3.175"]
   [org.xerial/sqlite-jdbc "3.7.15-M1"]
   [com.oracle/ojdbc6 "11.2.0.3"]
   ;;[org.mariadb.jdbc/mariadb-java-client "1.1.5"]
   [mysql/mysql-connector-java "5.1.25"]
   ;;[self/sigar "1.6.4"]
   [org.springframework/spring-beans "3.2.8.RELEASE"]
   [org.springframework/spring-core "3.2.8.RELEASE"]
   [org.springframework/spring-jms "3.2.8.RELEASE"]
   [org.codehaus.castor/castor "1.2"]
   [http-kit "2.1.16"]
   ;[self/lumprj "1.0.6"]
   ;[self/IPPlot "1.0.0"]
   [earthquick/ipplot "1.1.0"]

   ;[self/NetSeisIP "1.0.0"]
   [earthquick/netseisip "1.1.0"]
   ;[self/seiscommon "1.0.0"]
   [earthquick/seiscommon "1.0.0"]
   [clojurewerkz/quartzite "1.1.0"]
   [ch.ethz.ganymed/ganymed-ssh2 "261"]
   [clj-http "0.9.0"]
   [me.raynes/fs "1.4.5"]
   [org.clojure/data.json "0.2.4"]
   [ring-server "0.3.1"]
   [lein-ring "0.8.8"]
   [environ "0.4.0"]
   [com.taoensso/timbre "3.0.0"]
   [markdown-clj "0.9.41"]
   [korma "0.3.0-RC6"]
   [com.taoensso/tower "2.0.1"]
   [selmer "0.6.1"]
   [org.clojure/clojure "1.6.0"]
   [log4j
    "1.2.17"
    :exclusions
    [javax.mail/mail
     javax.jms/jms
     com.sun.jdmk/jmxtools
     com.sun.jmx/jmxri]]
   [compojure "1.1.6"]
   [lib-noir "0.8.1"]
   [com.postspectacular/rotor "0.1.0"]]
  :aot [lumprj.controller.realstream]
  :repl-options {:init-ns lumprj.repl}
  :plugins [[lein-ring "0.8.10"]
            [lein-environ "0.4.0"]
            [ring-refresh "0.1.1"]
            [ring/ring-jetty-adapter "1.2.1"]
            [lein-localrepo "0.5.3"]
            [lein-immutant "1.2.1"]
            ]
  :ring {:handler lumprj.handler/app
         :init    lumprj.handler/init
         :destroy lumprj.handler/destroy}
  :profiles
  {:uberjar {:aot :all}
   :production {:ring {:open-browser? false
                       :stacktraces?  false
                       :auto-reload?  false}}
   :dev {:dependencies [[ring-mock "0.1.5"]
                        [ring/ring-devel "1.2.1"]]
         :env {:dev true}}}
  :repositories [
                  ["java.net" "http://download.java.net/maven/2"]
                  ["nexus" "https://code.lds.org/nexus/content/groups/main-repo"]
                 ["sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                              ;; If a repository contains releases only setting
                              ;; :snapshots to false will speed up dependencies.
                              :snapshots false
                              ;; Disable signing releases deployed to this repo.
                              ;; (Not recommended.)
                              :sign-releases false
                              ;; You can also set the policies for how to handle
                              ;; :checksum failures to :fail, :warn, or :ignore.
                              :checksum :fail
                              ;; How often should this repository be checked for
                              ;; snapshot updates? (:daily, :always, or :never)
                              :update :always
                              ;; You can also apply them to releases only:
                              :releases {:checksum :fail :update :always}}]
                 ;; Repositories named "snapshots" and "releases" automatically
                 ;; have their :snapshots and :releases disabled as appropriate.
                 ;; Credentials for repositories should *not* be stored
                 ;; in project.clj but in ~/.lein/credentials.clj.gpg instead,
                 ;; see `lein help deploying` under "Authentication".
                 ]
  :description
  "FIXME: write description"
    ;;:jvm-opts ["-Djava.library.path=/home/jack/soft/lumprj/target/native"]
  :java-source-paths ["src/lumprj/java" ] ; Java source is stored separately.
  ;;:native-path "native"
  ;;:resource-paths ["lib/*"]
  ;;:library-path "lib"
  :min-lein-version "2.0.0")
