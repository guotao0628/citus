/* citus--8.0-3--8.0-4 */
SET search_path = 'pg_catalog';

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

CREATE OR REPLACE FUNCTION pg_catalog.citus_blocking_pids(pBlockedPid integer)
RETURNS int4[] AS $$
  DECLARE
    mLocalBlockingPids int4[];
    mRemoteBlockingPids int4[];
    mLocalTransactionNum int8;
    workerProcessId integer;
    coordinatorProcessId integer;
  BEGIN
    SELECT pg_catalog.old_pg_blocking_pids(pBlockedPid) INTO mLocalBlockingPids;

    IF (array_length(mLocalBlockingPids, 1) > 0) THEN
      RETURN mLocalBlockingPids;
    END IF;

    -- pg says we're not blocked locally; check whether we're blocked globally.
    IF EXISTS (SELECT 0 FROM pg_class where relname = 'blocking_process_sequence' ) THEN      
      -- Now check it not only from coordinator but also from workers. Since the
      -- process on worker started the transaction causing a lock we need to get
      -- the id of that process. Sequence has been used since uncommitted changes
      -- on the seequnce can be read from other sessions.
      --
      -- Note that, you need to create sequence with the name 'blocking_process_sequence'
      -- and 'blocking_process_coordinator_sequence' to test MX functionalities.
      SELECT nextval('public.blocking_process_sequence') - 1 INTO workerProcessId;

      SELECT transaction_number INTO mLocalTransactionNum 
        FROM get_global_active_transactions() WHERE process_id = workerProcessId;

       IF EXISTS (SELECT waiting_transaction_num AS txn_num FROM dump_global_wait_edges()
                     WHERE waiting_transaction_num = mLocalTransactionNum) THEN

         SELECT nextval('public.blocking_process_coordinator_sequence') - 1 INTO coordinatorProcessId;
         SELECT array_agg(coordinatorProcessId) INTO mRemoteBlockingPids;

       END IF;

    ELSE
      -- Check whether two transactions initiated from the coordinator get locked
      -- on some of the worker node. 
      SELECT transaction_number INTO mLocalTransactionNum
        FROM get_all_active_transactions() WHERE process_id = pBlockedPid;

      SELECT array_agg(process_id) INTO mRemoteBlockingPids FROM (
      WITH activeTransactions AS (
        SELECT process_id, transaction_number FROM get_all_active_transactions()
      ), blockingTransactions AS (
        SELECT blocking_transaction_num AS txn_num FROM dump_global_wait_edges()
        WHERE waiting_transaction_num = mLocalTransactionNum
      )
      SELECT activeTransactions.process_id FROM activeTransactions, blockingTransactions
        WHERE activeTransactions.transaction_number = blockingTransactions.txn_num
      ) AS sub;

    END IF;
    
    RETURN mRemoteBlockingPids;
  END;
$$ LANGUAGE plpgsql;    
    
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
