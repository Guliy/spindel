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

(deftest the-diff-detail-names-its-base
  (let [ctx  (world)
        fork (binding [ec/*execution-context* ctx] (ygg/fork!))]
    (dh/transact (data-conn (:child-ctx fork)) [{:x/id "r1" :x/v "copy"}])
    (dh/transact (data-conn ctx) [{:x/id "r1" :x/w "room"}])
    (let [{:keys [delta base baseless?]} (get (ygg/fork-diff-detail fork) "data")]
      (is (false? baseless?))
      (is (some? base))
      (testing "the delta holds the change of the fork and not the change of the room"
        (is (some #(= "copy" (nth % 3)) (:added delta)))
        (is (not-any? #(= "room" (nth % 3)) (:added delta)))))))

(defn- store
  "A system with the heads \"p\" (parent) and \"f\" (fork). `ancestor` is
   the common ancestor, or :throw. `diff` of a base in `gone` throws, as the
   diff of a base that a GC removed."
  [ancestor gone]
  (reify
    yp/Snapshotable
    (snapshot-id [_] "f")
    yp/Graphable
    (common-ancestor [_ _ _]
      (if (= :throw ancestor) (throw (ex-info "no graph" {})) ancestor))
    yp/Mergeable
    (diff [_ a b]
      (when (contains? gone a) (throw (ex-info "the snapshot is gone" {})))
      {:from a :to b})))

(defn- parent-head []
  (reify yp/Snapshotable (snapshot-id [_] "p")))

(def ^:private diff-detail #'ygg/system-diff-detail)

(deftest the-base-of-a-diff-comes-from-the-graph-then-from-the-record
  (testing "the common ancestor is the base"
    (is (= {:delta {:from "a" :to "f"} :base "a" :baseless? false}
           (diff-detail (store "a" #{}) (parent-head) "r"))))
  (testing "without the graph, the recorded base is the base"
    (is (= {:delta {:from "r" :to "f"} :base "r" :baseless? false}
           (diff-detail (store :throw #{}) (parent-head) "r")))))

(deftest a-diff-whose-base-is-gone-says-so
  (is (= {:delta {:from "p" :to "f"} :base nil :baseless? true}
         (diff-detail (store "a" #{"a" "r"}) (parent-head) "r"))))

(deftest a-fork-whose-branch-is-gone-does-not-open
  (let [ctx    (world)
        fork   (binding [ec/*execution-context* ctx] (ygg/fork!))
        record (through-edn (ygg/fork-record fork))]
    (ygg/discard-fork! fork)
    (is (= ::missing
           (try (ygg/adopt-fork! ctx record) nil
                (catch clojure.lang.ExceptionInfo e (:type (ex-data e))))))))
