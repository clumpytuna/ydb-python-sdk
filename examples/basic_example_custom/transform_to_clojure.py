import clj

txns_path = "txns-short-small-ready"

with open(txns_path, "r") as txns_file:
    transactions = txns_file.read()
    with open(txns_path + "-clojure") as txns_clj_file:
        print(clj.dumps((txns_file.read())))
