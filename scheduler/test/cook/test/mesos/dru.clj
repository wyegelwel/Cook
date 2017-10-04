;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(ns cook.test.mesos.dru
 (:use clojure.test)
 (:require [clj-time.core :as t]
           [clj-time.coerce :as tc]
           [cook.mesos.dru :as dru]
           [cook.mesos.share :as share]
           [cook.mesos.util :as util]
           [cook.test.testutil :refer (restore-fresh-database! create-dummy-job create-dummy-instance create-dummy-group)]
           [datomic.api :as d :refer (q db)]
           [plumbing.core :refer [map-vals]]))

(deftest test-compute-task-scored-task-pairs
  (testing "return empty set on input empty set"
    (is (= []
           (dru/compute-task-scored-task-pairs {:mem 25.0 :cpus 25.0} '()))))

  (testing "sort tasks from same user"
    (let [datomic-uri "datomic:mem://test-score-tasks"
          conn (restore-fresh-database! datomic-uri)
          job1 (create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
          job2 (create-dummy-job conn :user "ljin" :memory 5.0  :ncpus 5.0)
          job3 (create-dummy-job conn :user "ljin" :memory 15.0 :ncpus 25.0)
          job4 (create-dummy-job conn :user "ljin" :memory 25.0 :ncpus 15.0)
          task1 (create-dummy-instance conn job1 :instance-status :instance.status/running)
          task2 (create-dummy-instance conn job2 :instance-status :instance.status/running)
          task3 (create-dummy-instance conn job3 :instance-status :instance.status/running)
          task4 (create-dummy-instance conn job4 :instance-status :instance.status/running)
          task-ent1 (d/entity (d/db conn) task1)
          task-ent2 (d/entity (d/db conn) task2)
          task-ent3 (d/entity (d/db conn) task3)
          task-ent4 (d/entity (d/db conn) task4)
          tasks [task-ent1 task-ent2 task-ent3 task-ent4]]
      (let [scored-task1 (dru/->ScoredTask task-ent1 0.4 10.0 10.0)
            scored-task2 (dru/->ScoredTask task-ent2 0.6 5.0 5.0)
            scored-task3 (dru/->ScoredTask task-ent3 1.6 15.0 25.0)
            scored-task4 (dru/->ScoredTask task-ent4 2.2 25.0 15.0)]
        (is (= [[task-ent1 scored-task1]
                [task-ent2 scored-task2]
                [task-ent3 scored-task3]
                [task-ent4 scored-task4]]
               (dru/compute-task-scored-task-pairs {:mem 25.0 :cpus 25.0} tasks)))))))

(deftest test-init-dru-divisors
  (testing "compute dru divisors for users with different shares"
    (let [datomic-uri "datomic:mem://test-init-dru-divisors"
          conn (restore-fresh-database! datomic-uri)
          job1 (create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
          job2 (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0)
          job3 (create-dummy-job conn :user "sunil" :memory 10.0 :ncpus 10.0)
          task1 (create-dummy-instance conn job1 :instance-status :instance.status/running)
          task2 (create-dummy-instance conn job2 :instance-status :instance.status/running)
          db (d/db conn)
          running-task-ents (util/get-running-task-ents db)
          pending-job-ents [(d/entity db job3)]]
      (let [_ (share/set-share! conn "default"
                                "Raising limits for new cluster"
                                :mem 25.0 :cpus 25.0 :gpus 1.0)
            _ (share/set-share! conn "wzhao"
                                "Tends to use too much stuff"
                                :mem 10.0 :cpus 10.0)
            db (d/db conn)]
        (is (= {"ljin" {:mem 25.0 :cpus 25.0 :gpus 1.0}
                "wzhao" {:mem 10.0 :cpus 10.0 :gpus 1.0}
                "sunil" {:mem 25.0 :cpus 25.0 :gpus 1.0}}
               (dru/init-user->dru-divisors db running-task-ents pending-job-ents)))))))

(deftest test-group-max-expected-runtime
  (let [datomic-uri "datomic:mem://test-sorted-task-scored-task-pairs"
        conn (restore-fresh-database! datomic-uri)
        group-id (create-dummy-group conn)
        jobs [(create-dummy-job conn :expected-runtime 100 :group group-id)
              (create-dummy-job conn :expected-runtime 120 :group group-id)
              (create-dummy-job conn :group group-id)]
        create-running-job
        (fn [conn & {:keys [expected-runtime group instance-runtime]}]
          (let [job-id (create-dummy-job conn
                                         :expected-runtime expected-runtime
                                         :job-state :job.state/running
                                         :group group)]
            (create-dummy-instance conn job-id
                                   :instance-status :instance.status/running
                                   :start-time (tc/to-date (t/minus (t/now)
                                                                    (t/millis instance-runtime)))
                                   :end-time (tc/to-date (t/now)))))
        ]
    (is (= 120 (dru/group-max-expected-runtime (d/entity (d/db conn) group-id))))
    (create-running-job conn :expected-runtime 200 :group group-id :instance-runtime 100)
    (is (= 120 (dru/group-max-expected-runtime (d/entity (d/db conn) group-id))))
    (create-running-job conn :expected-runtime 200 :group group-id :instance-runtime 50)
    (is (= 150 (dru/group-max-expected-runtime (d/entity (d/db conn) group-id))))))

(deftest test-sorted-task-scored-task-pairs
  (let [datomic-uri "datomic:mem://test-sorted-task-scored-task-pairs"
        conn (restore-fresh-database! datomic-uri)
        wzhao-group (create-dummy-group conn)
        ljin-group (create-dummy-group conn)
        jobs [(create-dummy-job conn :user "ljin" :memory 10.0 :ncpus 10.0)
              (create-dummy-job conn :user "ljin" :memory 5.0  :ncpus 5.0)
              (create-dummy-job conn :user "ljin" :memory 12.0 :ncpus 12.0
                                :group ljin-group :expected-runtime 200)
              (create-dummy-job conn :user "ljin" :memory 25.0 :ncpus 15.0
                                :group ljin-group :expected-runtime 100)
              (create-dummy-job conn :user "wzhao" :memory 10.0 :ncpus 10.0
                                :expected-runtime 100 :group wzhao-group)
              (create-dummy-job conn :user "sunil" :memory 10.0 :ncpus 10.0)]
        tasks (doseq [job jobs]
                (create-dummy-instance conn job :instance-status :instance.status/running))
        db (d/db conn)
        task-ents (util/get-running-task-ents db)]
    (let [share {:mem 10.0 :cpus 10.0}
          ordered-drus [0.5 1.0 1.0 1.35 1.5 5.2]]
      (testing "dru order correct"
        (is (= ordered-drus
               (map (comp :dru second)
                    (dru/sorted-task-scored-task-pairs
                      {"ljin" share "wzhao" share "sunil" share}
                      (map-vals (partial sort-by identity (util/same-user-task-comparator))
                                (group-by util/task-ent->user task-ents)))))))
      ;; Check that the order of users doesn't affect dru order
      (testing "order of users doesn't affect dru order"
        (is (= (dru/sorted-task-scored-task-pairs
                 {"ljin" share "wzhao" share "sunil" share}
                 (map-vals (partial sort-by identity (util/same-user-task-comparator))
                           (group-by util/task-ent->user task-ents)))
               (dru/sorted-task-scored-task-pairs
                 {"ljin" share "wzhao" share "sunil" share}
                 (->> task-ents
                      (group-by util/task-ent->user)
                      (map-vals (partial sort-by identity (util/same-user-task-comparator)))
                      seq
                      shuffle
                      (into {})))))))))

(deftest test-compute-sorted-task-cumulative-gpu-score-pairs
  (testing "return empty set on input empty set"
    (is (= []
           (dru/compute-sorted-task-cumulative-gpu-score-pairs 25.0 '()))))

  (testing "sort tasks from same user"
    (let [datomic-uri "datomic:mem://test-score-tasks"
          conn (restore-fresh-database! datomic-uri)
          job1 (create-dummy-job conn :user "ljin" :gpus 10.0)
          job2 (create-dummy-job conn :user "ljin" :gpus 5.0)
          job3 (create-dummy-job conn :user "ljin" :gpus 25.0)
          job4 (create-dummy-job conn :user "ljin" :gpus 15.0)
          task1 (create-dummy-instance conn job1 :instance-status :instance.status/running)
          task2 (create-dummy-instance conn job2 :instance-status :instance.status/running)
          task3 (create-dummy-instance conn job3 :instance-status :instance.status/running)
          task4 (create-dummy-instance conn job4 :instance-status :instance.status/running)
          task-ent1 (d/entity (d/db conn) task1)
          task-ent2 (d/entity (d/db conn) task2)
          task-ent3 (d/entity (d/db conn) task3)
          task-ent4 (d/entity (d/db conn) task4)
          tasks [task-ent1 task-ent2 task-ent3 task-ent4]]
      (is (= [[task-ent1 1.0] [task-ent2 1.5] [task-ent3 4.0] [task-ent4 5.5]]
             (dru/compute-sorted-task-cumulative-gpu-score-pairs 10.0 tasks))))))

(deftest test-sorted-task-cumulative-gpu-score-pairs
  (testing "dru order correct"
    (let [datomic-uri "datomic:mem://test-sorted-task-cumulative-gpu-score-pairs"
          conn (restore-fresh-database! datomic-uri)
          jobs [(create-dummy-job conn :user "ljin" :gpus 10.0)
                (create-dummy-job conn :user "ljin" :gpus 5.0)
                (create-dummy-job conn :user "ljin" :gpus 25.0)
                (create-dummy-job conn :user "ljin" :gpus 15.0)
                (create-dummy-job conn :user "wzhao" :gpus 10.0)
                (create-dummy-job conn :user "sunil" :gpus 10.0)]
          tasks (doseq [job jobs]
                  (create-dummy-instance conn job :instance-status :instance.status/running))
          db (d/db conn)
          task-ents (util/get-running-task-ents db)]
      (let [expected-result [["wzhao" 1.0]
                             ["ljin" 2.0]
                             ["ljin" 3.0]
                             ["sunil" 4.0]
                             ["ljin" 8.0]
                             ["ljin" 11.0]]]
        (is (= expected-result
               (map
                 (fn [[task gpu-score]] [(get-in task [:job/_instance :job/user]) gpu-score])
                 (dru/sorted-task-cumulative-gpu-score-pairs
                   {"ljin"  {:gpus 5.0}
                    "wzhao" {:gpus 10.0}
                    "sunil" {:gpus 2.5}}
                   (map-vals (partial sort-by identity (util/same-user-task-comparator))
                             (group-by util/task-ent->user task-ents))))))))))

(comment (run-tests))
