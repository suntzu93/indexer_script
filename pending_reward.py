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
                SELECT lower(ar.allocation) as "allocateId", sum(fees) / 10^18 as "Fees"
                FROM public.allocation_summaries als
                JOIN public.allocation_receipts ar on als.allocation = ar.allocation 
                where als."closedAt" is null
                group by lower(ar.allocation);
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
