(ns org.replikativ.spindel.ygg-adopt-test
  "A fork opens again from its record after its handle and its child ctx are
   gone, as after a restart. The test holds the Datahike methods of
   `overlay-locator` and `reopen-overlay`: the runtime of spindel has no
   Datahike, and a user of a store gives the methods."
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as dh]
            [org.replikativ.spindel.engine.context :as ectx]
            [org.replikativ.spindel.engine.core :as ec]
            [org.replikativ.spindel.yggdrasil :as ygg]
            [yggdrasil.adapters.datahike :as ydh]
            [yggdrasil.protocols :as yp])
  (:import [yggdrasil.adapters.datahike DatahikeOverlay]))

(defmethod ygg/overlay-locator DatahikeOverlay [v]
  {:type :datahike
   :locator (subs (str (:fork-branch v)) 1)
   :parent-branch (subs (str (:parent-branch v)) 1)})

(defmethod ygg/reopen-overlay :datahike [sid psys {:keys [locator parent-branch]}]
  (let [branch (keyword locator)]
    (when-not (contains? (set (yp/branches psys)) branch)
      (throw (ex-info "the recorded branch is missing" {:type ::missing :system sid})))
    (ydh/->DatahikeOverlay psys (atom (yp/checkout psys branch)) branch
                           (keyword parent-branch) :frozen)))

(def ^:private schema
  [{:db/ident :x/id :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :x/v :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :x/w :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(defn- world
  "A ctx with one Datahike system in a memory store."
  []
  (let [cfg  {:store {:backend :memory :id (random-uuid)}
              :schema-flexibility :write :keep-history? true}
        _    (dh/create-database cfg)
        conn (dh/connect cfg)
        ctx  (ectx/create-execution-context)]
    (dh/transact conn schema)
    (dh/transact conn [{:x/id "r1" :x/v "base" :x/w "base"}])
    (binding [ec/*execution-context* ctx]
      (ygg/register! (ydh/create conn {:system-name "data"})))
    ctx))

(defn- data-conn [ctx]
  (binding [ec/*execution-context* ctx] (:conn (ygg/system "data"))))

(defn- row [ctx]
  (dh/pull @(data-conn ctx) '[:x/v :x/w] [:x/id "r1"]))

(defn- through-edn [record]
  (edn/read-string (pr-str record)))

(deftest a-fork-opens-again-from-its-record-and-merges
  (let [ctx    (world)
        fork   (binding [ec/*execution-context* ctx] (ygg/fork!))
        _      (dh/transact (data-conn (:child-ctx fork)) [{:x/id "r1" :x/v "copy"}])
        record (through-edn (ygg/fork-record fork))]
    (testing "the record names the system by a string locator"
      (is (string? (get-in record [:systems "data" :locator]))))
    (let [h (ygg/adopt-fork! ctx record)]
      (testing "the adopted fork reads what the fork wrote"
        (is (= {:x/v "copy" :x/w "base"} (row (:child-ctx h)))))
      (testing "the adopted handle is open and names the recorded fork"
        (is (ygg/open-fork? h))
        (is (= (:fork/id record) (:fork/adopted-from (ygg/fork-descriptor h)))))
      (testing "the merge brings the write of the fork into the parent"
        (ygg/merge-fork! h)
        (is (= {:x/v "copy" :x/w "base"} (row ctx)))
        (is (not (ygg/open-fork? h)))))))

(deftest a-fork-whose-branch-is-gone-does-not-open
  (let [ctx    (world)
        fork   (binding [ec/*execution-context* ctx] (ygg/fork!))
        record (through-edn (ygg/fork-record fork))]
    (ygg/discard-fork! fork)
    (is (= ::missing
           (try (ygg/adopt-fork! ctx record) nil
                (catch clojure.lang.ExceptionInfo e (:type (ex-data e))))))))
