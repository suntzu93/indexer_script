import psycopg2
from psycopg2 import sql
import logging
import config
import subprocess
import os
import threading

logging.basicConfig(filename='manage_publication_subscription.log', level=logging.DEBUG)

def get_connection(host):
    return psycopg2.connect(
        host=host,
        port=config.db_port,
        database=config.primary_database,
        user=config.username,
        password=config.password
    )

def create_publication(schema_name):
    try:
        conn = get_connection(config.primary_host_local)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_publication WHERE pubname = %s
                    ) THEN
                        EXECUTE 'CREATE PUBLICATION ' || %s || ' FOR TABLES IN SCHEMA ' || %s;
                        RAISE NOTICE 'Publication {0}_pub has been created.';
                    ELSE
                        RAISE NOTICE 'Publication {0}_pub already exists.';
                    END IF;
                END
                $$;
            """).format(
                sql.Identifier(schema_name)
            ), [f"{schema_name}_pub", f"{schema_name}_pub", schema_name])
            logging.info(f"Publication {schema_name}_pub ensured on primary.")
    except Exception as e:
        logging.error(f"Error creating publication: {e}")
        raise
    finally:
        conn.close()

def drop_publication(schema_name):
    try:
        conn = get_connection(config.primary_host_local)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP PUBLICATION IF EXISTS {};").format(
                sql.Identifier(f"{schema_name}_pub")
            ))
            logging.info(f"Publication {schema_name}_pub dropped from primary.")
    except Exception as e:
        logging.error(f"Error dropping publication: {e}")
        raise
    finally:
        conn.close()

def drop_subscription(schema_name):
    try:
        conn = get_connection(config.replica_host)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP SUBSCRIPTION IF EXISTS {};").format(
                sql.Identifier(f"{schema_name}_sub")
            ))
            logging.info(f"Subscription {schema_name}_sub dropped from replica.")
    except Exception as e:
        logging.error(f"Error dropping subscription: {e}")
        raise
    finally:
        conn.close()

def drop_pub_sub(schema_name):
    try:
        drop_subscription(schema_name)
        drop_publication(schema_name)
        return {"status": "success", "message": "Publication and Subscription dropped successfully."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def drop_schema(schema_name):
    try:
        conn = get_connection(config.replica_host)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE;").format(sql.Identifier(schema_name)))
            logging.info(f"Schema {schema_name} dropped .")
    except Exception as e:
        logging.error(f"Error dropping/creating schema: {e}")
        raise
    finally:
        conn.close()

def dump_and_restore_schema(schema_name):
    try:
        # Set environment variables for both primary and replica connections
        os.environ['PGPASSWORD'] = config.password
        
        dump_command = f"pg_dump -h {config.primary_host_local} -U {config.username} -d {config.primary_database} -n {schema_name} --schema-only"
        restore_command = f"psql -h {config.replica_host} -U {config.username} -d {config.primary_database}"
        
        process = subprocess.Popen(f"{dump_command} | {restore_command}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"Error in dump and restore: {stderr.decode()}")
        
        logging.info(f"Schema {schema_name} dumped from primary and restored on replica.")
        return {"status": "success", "message": f"Schema {schema_name} successfully dumped and restored."}
    except Exception as e:
        logging.error(f"Error dumping/restoring schema: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        # Clear the password from environment variables
        if 'PGPASSWORD' in os.environ:
            del os.environ['PGPASSWORD']

def create_subscription(schema_name, isCopy=True):
    def create_sub_async():
        try:
            conn = get_connection(config.replica_host)
            conn.autocommit = True
            with conn.cursor() as cur:
                subscription_name = f"{schema_name}_sub"
                publication_name = f"{schema_name}_pub"
                connection_string = f"host={config.primary_host} port={config.db_port} user={config.username} password={config.password} dbname={config.primary_database}"
                
                cur.execute(f"DROP SUBSCRIPTION IF EXISTS {subscription_name};")
                logging.info(f"Subscription {subscription_name} dropped from replica.")
                logging.info(f"Creating subscription {subscription_name} on replica.")
                create_sub_query = f"""
                    CREATE SUBSCRIPTION {subscription_name}
                    CONNECTION '{connection_string}'
                    PUBLICATION {publication_name} WITH (copy_data = {isCopy});
                """
                
                cur.execute(create_sub_query)
                
                logging.info(f"Subscription {subscription_name} created on replica.")
        except Exception as e:
            logging.error(f"Error creating subscription: {e}")
        finally:
            if conn:
                conn.close()

    # Start the subscription creation in a separate thread
    thread = threading.Thread(target=create_sub_async)
    thread.start()

    # Wait for a short time to check if the thread has completed
    thread.join(timeout=5)

    if thread.is_alive():
        return {"status": "pending", "message": f"Subscription creation for {schema_name} is in progress. Please check the logs for completion."}
    else:
        return {"status": "success", "message": f"Subscription creation for {schema_name} completed successfully."}

def handle_create_pub_sub(schema_name, isCopy=True):
    try:
        create_publication(schema_name)
        drop_schema(schema_name)

        dump_result = dump_and_restore_schema(schema_name)
        if dump_result["status"] == "error":
            return dump_result

        subscription_result = create_subscription(schema_name, isCopy)
        return subscription_result
    except Exception as e:
        logging.error(f"Error in create_pub_sub: {e}")
        return {"status": "error", "message": str(e)}

def handle_drop_pub_sub(schema_name):
    try:
        result = drop_pub_sub(schema_name)
        return result
    except Exception as e:
        logging.error(f"Error in handle_drop_pub_sub: {e}")
        return {"status": "error", "message": str(e)}

def compare_row_counts(schema_name):
    try:
        primary_conn = get_connection(config.primary_host_local)
        replica_conn = get_connection(config.replica_host)
        
        with primary_conn.cursor() as primary_cur, replica_conn.cursor() as replica_cur:
            # Get tables from primary
            primary_cur.execute(sql.SQL("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE';
            """), [schema_name])
            primary_tables = [row[0] for row in primary_cur.fetchall()]
            
            # Get tables from replica
            replica_cur.execute(sql.SQL("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE';
            """), [schema_name])
            replica_tables = [row[0] for row in replica_cur.fetchall()]
            
            all_tables = set(primary_tables) | set(replica_tables)
            comparison_results = []
            
            for table in all_tables:
                # Get row counts from primary
                primary_cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{};").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table)
                ))
                primary_count = primary_cur.fetchone()[0]
                
                # Get row counts from replica
                if table in replica_tables:
                    replica_cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{};").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(table)
                    ))
                    replica_count = replica_cur.fetchone()[0]
                else:
                    replica_count = 0
                
                difference = primary_count - replica_count
                comparison_results.append({
                    "table": f"{schema_name}.{table}",
                    "primary_row_count": primary_count,
                    "replica_row_count": replica_count,
                    "difference": difference
                })
        
        return {"status": "success", "data": comparison_results}
    except Exception as e:
        logging.error(f"Error in compare_row_counts: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        primary_conn.close()
        replica_conn.close()

def get_publication_stats():
    try:
        conn = get_connection(config.primary_host_local)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT application_name, state, sent_lsn, write_lsn, flush_lsn, replay_lsn, 
                       write_lag, flush_lag, replay_lag, sync_state, reply_time 
                FROM pg_stat_replication;
            """)
            stats = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            publication_stats = []
            for row in stats:
                row_dict = dict(zip(columns, row))
                # Convert timedelta fields to strings
                for key in ['write_lag', 'flush_lag', 'replay_lag']:
                    if row_dict.get(key) is not None:
                        row_dict[key] = str(row_dict[key])
                # Convert reply_time to ISO format if not None
                if row_dict.get('reply_time') is not None:
                    row_dict['reply_time'] = row_dict['reply_time'].isoformat()
                publication_stats.append(row_dict)
        return {"status": "success", "data": publication_stats}
    except Exception as e:
        logging.error(f"Error retrieving publication stats: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()

def get_subscription_stats():
    try:
        conn = get_connection(config.replica_host)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    s.subname AS subscription_name,
                    s.subconninfo AS connection_info,
                    s.subenabled AS enabled,
                    ss.received_lsn,
                    ss.latest_end_lsn,
                    ss.last_msg_send_time,
                    ss.last_msg_receipt_time
                FROM pg_subscription s
                LEFT JOIN pg_stat_subscription ss ON s.subname = ss.subname;
            """)
            stats = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            subscription_stats = [dict(zip(columns, row)) for row in stats]
        return {"status": "success", "data": subscription_stats}
    except Exception as e:
        logging.error(f"Error retrieving subscription stats: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()