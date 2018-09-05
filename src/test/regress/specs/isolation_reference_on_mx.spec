setup
{	
	CREATE SEQUENCE public.blocking_process_sequence;
	SELECT setval('public.blocking_process_sequence', 1);

  	SELECT citus.replace_isolation_tester_func();
  	SELECT citus.refresh_isolation_tester_prepared_statement();

	SELECT start_metadata_sync_to_node('localhost', 57637);
	SELECT start_metadata_sync_to_node('localhost', 57638);
	SET citus.replication_model to streaming;

	CREATE TABLE ref_table(user_id int, value_1 int);
	SELECT create_reference_table('ref_table');
	INSERT INTO ref_table VALUES (1, 11), (2, 21), (3, 31), (4, 41), (5, 51), (6, 61), (7, 71);
}

teardown
{
	DROP TABLE ref_table;
	DROP SEQUENCE public.blocking_process_sequence;
	SELECT citus.restore_isolation_tester_func();
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-update-ref-table"
{
	SELECT master_run_from_same_connection('localhost', 57637, 'BEGIN', false); 
	SELECT master_run_from_same_connection('localhost', 57637, 'UPDATE ref_table SET value_1 = 12 WHERE user_id = 1', false);
}

step "s1-delete-from-ref-table"
{
	SELECT master_run_from_same_connection('localhost', 57637, 'BEGIN', false);
	SELECT master_run_from_same_connection('localhost', 57637, 'DELETE FROM ref_table WHERE user_id = 1', false);
}

step "s1-insert-into-ref-table"
{
	SELECT master_run_from_same_connection('localhost', 57637, 'BEGIN', false);
	SELECT master_run_from_same_connection('localhost', 57637, 'INSERT INTO ref_table VALUES(8,81),(9,91)', false);
}

step "s1-copy-to-ref-table"
{
	SELECT master_run_from_same_connection('localhost', 57637, 'BEGIN', false);
	SELECT master_run_from_same_connection('localhost', 57637, 'COPY ref_table FROM PROGRAM ''echo 10, 101 && echo 11, 111'' WITH CSV', false);
}

step "s1-select-for-update"
{
	SELECT master_run_from_same_connection('localhost', 57637, 'BEGIN', false);
	SELECT master_run_from_same_connection('localhost', 57637, 'SELECT * FROM ref_table FOR UPDATE', false);
}

step "s1-commit-worker"
{
    SELECT master_run_from_same_connection('localhost', 57637, 'COMMIT', true);
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-update-ref-table"
{
	SELECT master_run_from_same_connection('localhost', 57638, 'BEGIN', false);
	SELECT master_run_from_same_connection('localhost', 57638, 'UPDATE ref_table SET value_1 = 12 WHERE user_id = 1', false);
}

step "s2-delete-from-ref-table"
{
	SELECT master_run_from_same_connection('localhost', 57638, 'BEGIN', false);
	SELECT master_run_from_same_connection('localhost', 57638, 'DELETE FROM ref_table WHERE user_id = 2', false);
}

step "s2-insert-into-ref-table"
{
	SELECT master_run_from_same_connection('localhost', 57638, 'BEGIN', false);
	SELECT master_run_from_same_connection('localhost', 57638, 'INSERT INTO ref_table VALUES(8,81),(9,91)', false);
}

step "s2-copy-to-ref-table"
{
	SELECT master_run_from_same_connection('localhost', 57638, 'BEGIN', false);
	SELECT master_run_from_same_connection('localhost', 57638, 'COPY ref_table FROM PROGRAM ''echo 10, 101 && echo 11, 111'' WITH CSV', false);
}

step "s2-commit-worker"
{
    SELECT master_run_from_same_connection('localhost', 57638, 'COMMIT', true);
}

step "s2-commit"
{
    COMMIT;
}

permutation "s1-begin" "s1-update-ref-table" "s2-begin" "s2-update-ref-table" "s1-commit-worker" "s2-commit-worker" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-delete-from-ref-table" "s2-begin" "s2-update-ref-table" "s1-commit-worker" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-insert-into-ref-table" "s2-begin" "s2-update-ref-table" "s1-commit-worker" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-copy-to-ref-table" "s2-begin" "s2-update-ref-table" "s1-commit-worker" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-copy-to-ref-table" "s2-begin" "s2-insert-into-ref-table" "s1-commit-worker" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-copy-to-ref-table" "s2-begin" "s2-copy-to-ref-table" "s1-commit-worker" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-select-for-update" "s2-begin" "s2-update-ref-table" "s1-commit-worker" "s1-commit" "s2-commit"
