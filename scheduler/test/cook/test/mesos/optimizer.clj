(ns cook.test.mesos.optimizer
  (:use clojure.test)
  (:require [clj-time.core :as t]
            [clojure.core.async :as async]
            [cook.mesos.optimizer :as optimizer]))

;; Tests to make sure data flows and validates properly
(deftest test-optimizer-cycle
  (let [host {:count 1
              :instance-type "small"
              :cpus 1
              :mem 1000}
        host-feed (reify optimizer/HostFeed
                         (get-available-host-info [this]
                           [host]))
        optimizer (reify optimizer/Optimizer
                          (produce-schedule [this queue running available [host-info & host-infos]]
                            {0 {:suggested-matches {host-info (map :job/uuid queue)}}})) 
        queue [{:job/uuid (java.util.UUID/randomUUID)} {:job/uuid (java.util.UUID/randomUUID)}]]
    (optimizer/optimizer-cycle! (fn get-queue [] queue)
                                (fn get-running [] [])
                                (fn get-offers [] [])
                                host-feed
                                optimizer)))
