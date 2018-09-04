/* citus--7.5-7--8.0-1 */
SET search_path = 'pg_catalog';

CREATE OR REPLACE FUNCTION pg_catalog.relation_is_a_known_shard(regclass)
  RETURNS bool
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$relation_is_a_known_shard$$;
COMMENT ON FUNCTION relation_is_a_known_shard(regclass)
    IS 'returns true if the given relation is a known shard';

CREATE OR REPLACE FUNCTION pg_catalog.citus_table_is_visible(oid)
  RETURNS bool
LANGUAGE C STRICT
STABLE
PARALLEL SAFE
AS 'MODULE_PATHNAME', $$citus_table_is_visible$$;
COMMENT ON FUNCTION citus_table_is_visible(oid)
	IS 'wrapper on pg_table_is_visible, filtering out tables (and indexes) that are known to be shards';

-- this is the exact same query with what \d
-- command produces, except pg_table_is_visible
-- is replaced with pg_catalog.relation_is_a_known_shard(c.oid)
CREATE VIEW citus.citus_shards_on_worker AS 
	SELECT n.nspname as "Schema",
	  c.relname as "Name",
	  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'table' END as "Type",
	  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
	FROM pg_catalog.pg_class c
	     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relkind IN ('r','p','v','m','S','f','')
	      AND n.nspname <> 'pg_catalog'
	      AND n.nspname <> 'information_schema'
	      AND n.nspname !~ '^pg_toast'
  		AND pg_catalog.relation_is_a_known_shard(c.oid)
	ORDER BY 1,2;
ALTER VIEW citus.citus_shards_on_worker SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_shards_on_worker TO public;

-- this is the exact same query with what \di
-- command produces, except pg_table_is_visible
-- is replaced with pg_catalog.relation_is_a_known_shard(c.oid)
CREATE VIEW citus.citus_shard_indexes_on_worker AS 
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'table' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
 c2.relname as "Table"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid
WHERE c.relkind IN ('i','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
      AND n.nspname !~ '^pg_toast'
  AND pg_catalog.relation_is_a_known_shard(c.oid)
ORDER BY 1,2;

ALTER VIEW citus.citus_shard_indexes_on_worker SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_shard_indexes_on_worker TO public;

CREATE FUNCTION get_global_active_transactions(OUT database_id oid, OUT process_id int, OUT initiator_node_identifier int4, OUT transaction_number int8, OUT transaction_stamp timestamptz)
  RETURNS SETOF RECORD
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$get_global_active_transactions$$;
 COMMENT ON FUNCTION get_global_active_transactions(OUT database_id oid, OUT process_id int, OUT initiator_node_identifier int4, OUT transaction_number int8, OUT transaction_stamp timestamptz)
     IS 'returns distributed transaction ids of active distributed transactions from each worker of the cluster';

CREATE FUNCTION master_run_from_same_connection(text, integer, text, boolean)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_run_from_same_connection$$;
COMMENT ON FUNCTION master_run_from_same_connection(text, integer, text, boolean)
    IS 'run command from same connection';
    
CREATE OR REPLACE FUNCTION pg_catalog.citus_isolation_test_session_is_blocked(pBlockedPid integer, pInterestingPids integer[])
RETURNS boolean AS $$
  DECLARE
    mBlockedTransactionNum int8;
    workerProcessId integer;
  BEGIN
    IF pg_catalog.old_pg_isolation_test_session_is_blocked(pBlockedPid, pInterestingPids) THEN
      RETURN true;
    END IF;

    -- pg says we're not blocked locally; check whether we're blocked globally.
    IF EXISTS (SELECT 0 FROM pg_class where relname = 'blocking_process_sequence' ) THEN      
      -- Now check it not only from coordinator but also from workers. Since the
      -- process on worker started the transaction causing a lock we need to get
      -- the id of that process. Sequence has been used since uncommitted changes
      -- on the seequnce can be read from other sessions.
      --
      -- Note that, you need to create sequence with the name 'blocking_process_sequence'
      -- to test MX functionalities.
      SELECT nextval('public.blocking_process_sequence') - 1 INTO workerProcessId;

      SELECT transaction_number INTO mBlockedTransactionNum 
        FROM get_global_active_transactions() WHERE process_id = workerProcessId;
    ELSE
      -- Check whether two transactions initiated from the coordinator get locked
      -- on some of the worker node. 
      SELECT transaction_number INTO mBlockedTransactionNum
        FROM get_all_active_transactions() WHERE process_id = pBlockedPid;
    END IF;

    RETURN EXISTS (
      SELECT 1 FROM dump_global_wait_edges()
        WHERE waiting_transaction_num = mBlockedTransactionNum
    );
  END;
$$ LANGUAGE plpgsql;

RESET search_path;
