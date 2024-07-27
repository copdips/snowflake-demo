import json
import time
from contextlib import contextmanager
from typing import Generator, Tuple

import snowflake.connector as sc
from snowflake.connector.converter_null import SnowflakeNoConverterToPython


@contextmanager
def snowflake_cursor() -> (
    Generator[Tuple[sc.SnowflakeConnection, sc.cursor.SnowflakeCursor], None, None]
):
    # connections.toml
    # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#connecting-using-the-connections-toml-file
    # other connection parameters (auth, retry logic, etc.):
    # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect
    conn = sc.connect(
        # Improving query performance by bypassing data conversion
        # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example#improving-query-performance-by-bypassing-data-conversion
        # converter_class=SnowflakeNoConverterToPython,
        connection_name="dev",
        session_parameters={
            "QUERY_TAG": json.dumps({"user": "xiang"}),
            "STATEMENT_TIMEOUT_IN_SECONDS": 60 * 5,
        },
    )
    cur = conn.cursor()
    try:
        yield conn, cur
    except sc.errors.ProgrammingError as e:
        # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example#handling-errors
        # default error message
        print(e)
        # customer error message
        print("Error {0} ({1}): {2} ({3})".format(e.errno, e.sqlstate, e.msg, e.sfqid))
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()


with sc.connect(connection_name="dev") as conn:
    print("\n====== standard usage with original context manager")
    with conn.cursor() as cur:
        cur.execute("select 1;")
        print(cur.fetchone())
        # cur.execute(
        #     """
        #     select
        #         query_tag,
        #         count(*) as num_executions,
        #         avg(total_elapsed_time/1000) as avg_total_elapsed_time_s
        #     from snowflake.account_usage.query_history
        #     where
        #         start_time > current_date - 7
        #     group by 1
        #     """
        # )
        # print(cur.fetchall())

with snowflake_cursor() as (conn, cur):
    print("\n====== standard usage with custom context manager")
    cur.execute("select 1;")
    print(cur.fetchone())
    cur.execute("SELECT current_version()")
    print(cur.fetchone())


with snowflake_cursor() as (conn, cur):
    print("\n====== return as a list of dict")
    # ! cur.describe can get the column names without executing the query
    result_metadata_list = cur.describe("SELECT CURRENT_ROLE()")
    print(",".join([col.name for col in result_metadata_list]))
    cur.execute("SELECT CURRENT_ROLE()")
    result_meta = cur.description
    results = [dict(zip([col.name for col in result_meta], row)) for row in cur]
    print(results)

with snowflake_cursor() as (conn, cur):
    print("\n====== return as a pandas dataframe")
    cur.execute("SELECT CURRENT_ROLE()")
    print(cur.fetch_pandas_all())

with snowflake_cursor() as (conn, cur):
    print("\n====== return as a pandas dataframe in batch for large data set")
    cur.execute("SELECT CURRENT_ROLE()")
    for batch_df in cur.fetch_pandas_batches():
        print(batch_df.head())


with snowflake_cursor() as (conn, cur):
    print("\n====== multiple SQL statements at once conn")
    sql = """
        select 1;
        SELECT CURRENT_ROLE();
        """
    results = conn.execute_string(sql)
    [print(r.fetchall()) for r in results]


with snowflake_cursor() as (conn, cur):
    print("\n====== multiple SQL statements at once with cur")
    queries = [
        "select 1;",
        "SELECT CURRENT_ROLE();",
    ]
    for query in queries:
        cur.execute(query)
        print(cur.fetchall())

with snowflake_cursor() as (conn, cur):
    print("\n====== use async query")
    cur.execute_async("select 1;")
    query_id = cur.sfqid
    cur.execute_async("SELECT CURRENT_ROLE();")
    # try:
    #     while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
    #         time.sleep(1)
    # except sc.errors.ProgrammingError as err:
    #     print("Programming Error: {0}".format(err))
    # ! conn.get_results_from_sfqid has internal wait mechanism, but not for conn.query_result()
    cur.get_results_from_sfqid(query_id)
    # cur.query_result(query_id)
    results = cur.fetchall()
    print(f"{results[0]}")


with snowflake_cursor() as (conn, cur):
    print("\n====== use async query with long query with query_result")
    query = """
            SELECT
                COUNT(*)
            FROM
                TABLE(GENERATOR(ROWCOUNT => 100000000000));
        """
    start_time = time.time()
    cur.execute_async(query)
    query_id = cur.sfqid
    cur.query_result(query_id)
    print(cur.fetchall())
    print(f"Execution time query_result: {time.time() - start_time} seconds")


with snowflake_cursor() as (conn, cur):
    print("\n====== use async query with long query with get_results_from_sfqid")
    query = """
            SELECT count(*) from table(generator(timeLimit => 5))
        """
    start_time = time.time()
    cur.execute_async(query)
    query_id = cur.sfqid
    cur.get_results_from_sfqid(query_id)
    print(cur.fetchall())
    print(f"Execution time get_results_from_sfqid: {time.time() - start_time} seconds")
