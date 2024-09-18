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
        WHERE redeemed_at IS NULL;
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
        SELECT SUM(value_aggregate) / 10^18 as "TotalFees"
        FROM public.scalar_tap_ravs
        WHERE redeemed_at IS NULL;
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
