(#%require-dylib "libsteel_postgres"
                 (prefix-in postgres/ (only-in client/connect query batch-execute execute)))

; (define client (postgres/client/connect "host=localhost user=postgres password=password"))

; (postgres/batch-execute
;  client
;  "CREATE TABLE IF NOT EXISTS person2 (
;         id      SERIAL PRIMARY KEY,
;         name    TEXT NOT NULL,
;         data    BYTEA
;     )")

; (postgres/execute client "INSERT INTO person2 (name, data) VALUES ($1, $2)" (list "steel-name" void))

; (displayln (postgres/query client "SELECT id, name, data FROM person2"))

;;@doc
;; Create a postgres client with a connection string
(define client/connect postgres/client/connect)

;;@doc
;; Executes a query, with parameters
(define query postgres/query)

;;@doc
;; Batch execute multiple queries
(define batch-execute postgres/batch-execute)

;;@doc
;; Execute a single query
(define execute postgres/execute)
