import json
import logging

import psycopg2
import config

def get_allocations_reward():
    data_json = []
    try:
        # Set up the connection parameters
        params = {
            'host': config.db_host,
            'port': config.db_port,
            'database': config.agent_database,
            'user': config.username,
            'password': config.password,
        }

        # Connect to the database
        conn = psycopg2.connect(**params)

        # Set up a cursor to execute the SQL query
        cur = conn.cursor()

        # Execute the SQL query
        query = """
        SELECT allocation_id as "allocateId", value_aggregate / 10^18 as "Fees"
        FROM public.scalar_tap_ravs
        WHERE redeemed_at IS NULL
        UNION ALL
        SELECT lower(ar.allocation) as "allocateId", sum(fees) / 10^18 as "Fees"
        FROM public.allocation_summaries als
        JOIN public.allocation_receipts ar on als.allocation = ar.allocation 
        WHERE als."closedAt" is null
        GROUP BY lower(ar.allocation);
            """
        cur.execute(query)

        # Fetch the results and print them out
        results = cur.fetchall()
        keys = ('allocateId', 'fees')
        rows = [dict(zip(keys, row)) for row in results]
        data_json = rows

        # Close the cursor and connection
        cur.close()
        conn.close()
    except Exception as e:
        print("get_allocations_reward error " + str(e))
        logging.error("get_allocations_reward: " + str(e))

    return data_json

def get_total_pending_reward():
    total_reward = 0
    try:
        # Set up the connection parameters
        params = {
            'host': config.db_host,
            'port': config.db_port,
            'database': config.agent_database,
            'user': config.username,
            'password': config.password,
        }

        # Connect to the database
        conn = psycopg2.connect(**params)

        # Set up a cursor to execute the SQL query
        cur = conn.cursor()

        # Execute the SQL query
        query = """
            SELECT SUM(fees) / 10^18 as total_fees
            FROM public.allocation_summaries als
            JOIN public.allocation_receipts ar on als.allocation = ar.allocation 
            WHERE als."closedAt" is null
            """
        cur.execute(query)

        # Fetch the result
        result = cur.fetchone()
        if result:
            total_reward = result[0]

        # Close the cursor and connection
        cur.close()
        conn.close()
    except Exception as e:
        print("get_total_pending_reward error " + str(e))
        logging.error("get_total_pending_reward: " + str(e))

    return total_reward

def get_allocation_reward(allocateId):
    data_json = []
    try:
        # Set up the connection parameters
        params = {
            'host': config.db_host,
            'port': config.db_port,
            'database': config.agent_database,
            'user': config.username,
            'password': config.password,
        }

        # Connect to the database
        conn = psycopg2.connect(**params)
        # Set up a cursor to execute the SQL query
        cur = conn.cursor()
        # Execute the first SQL query
        query1 = """
        SELECT value_aggregate / 10^18 as "Fees"
        FROM public.scalar_tap_ravs
        WHERE redeemed_at IS NULL AND allocation_id = %s;
        """
        cur.execute(query1, (allocateId,))
        result = cur.fetchone()

        if not result or result[0] is None:
            # If no result from the first query, execute the second query
            query2 = """
            SELECT SUM(fees) / 10^18 as "Fees"
            FROM public.allocation_summaries als
            JOIN public.allocation_receipts ar on als.allocation = ar.allocation 
            WHERE als."closedAt" is null
            and lower(ar.allocation) = lower(%s);
            """
            cur.execute(query2, (allocateId,))
            result = cur.fetchone()

        # Fetch the result
        result = cur.fetchone()
        if result:
            data_json = {"fees": result[0]}
        else:
            data_json = {"fees": 0}

        # Close the cursor and connection
        cur.close()
        conn.close()
    except Exception as e:
        print(f"get_allocation_reward error for allocateId {allocateId}: {str(e)}")
        logging.error(f"get_allocation_reward for allocateId {allocateId}: {str(e)}")

    return data_json
