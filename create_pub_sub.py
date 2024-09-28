import psycopg2
from psycopg2 import sql
import logging
import config
import subprocess
import os
import threading
import concurrent.futures

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

def drop_subscription(schema_name, replica_host, replica_index):
    try:
        conn = get_connection(replica_host)
        conn.autocommit = True
        with conn.cursor() as cur:
            subscription_name = f"{schema_name}_sub" if replica_index == 0 else f"{schema_name}_sub_{replica_index}"
            cur.execute(sql.SQL("DROP SUBSCRIPTION IF EXISTS {};").format(
                sql.Identifier(subscription_name)
            ))
            logging.info(f"Subscription {subscription_name} dropped from replica {replica_host}.")
    except Exception as e:
        logging.error(f"Error dropping subscription on {replica_host}: {e}")
        raise
    finally:
        conn.close()

def drop_pub_sub(schema_name):
    try:
        for index, replica_host in enumerate(config.replica_hosts):
            drop_subscription(schema_name, replica_host, index)
        drop_publication(schema_name)
        return {"status": "success", "message": "Publication and Subscriptions dropped successfully."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def drop_schema(schema_name, replica_host):
    try:
        conn = get_connection(replica_host)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE;").format(sql.Identifier(schema_name)))
            logging.info(f"Schema {schema_name} dropped on {replica_host}.")
    except Exception as e:
        logging.error(f"Error dropping/creating schema on {replica_host}: {e}")
        raise
    finally:
        conn.close()

def dump_and_restore_schema(schema_name, replica_host):
    try:
        # Set environment variables for both primary and replica connections
        os.environ['PGPASSWORD'] = config.password
        
        dump_command = f"pg_dump -h {config.primary_host_local} -U {config.username} -d {config.primary_database} -n {schema_name} --schema-only"
        restore_command = f"psql -h {replica_host} -U {config.username} -d {config.primary_database}"
        
        process = subprocess.Popen(f"{dump_command} | {restore_command}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"Error in dump and restore: {stderr.decode()}")
        
        logging.info(f"Schema {schema_name} dumped from primary and restored on replica {replica_host}.")
        return {"status": "success", "message": f"Schema {schema_name} successfully dumped and restored on {replica_host}."}
    except Exception as e:
        logging.error(f"Error dumping/restoring schema on {replica_host}: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        # Clear the password from environment variables
        if 'PGPASSWORD' in os.environ:
            del os.environ['PGPASSWORD']

def create_subscription(schema_name, replica_host, replica_index, isCopy=True):
    def subscription_task():
        try:
            conn = get_connection(replica_host)
            conn.autocommit = True
            with conn.cursor() as cur:
                # Create subscription name based on replica index
                subscription_name = f"{schema_name}_sub" if replica_index == 0 else f"{schema_name}_sub_{replica_index}"
                publication_name = f"{schema_name}_pub"
                connection_string = (
                    f"host={config.primary_host} port={config.db_port} "
                    f"user={config.username} password={config.password} "
                    f"dbname={config.primary_database}"
                )
                
                cur.execute(f"DROP SUBSCRIPTION IF EXISTS {subscription_name};")
                logging.info(f"Subscription {subscription_name} dropped from replica {replica_host}.")
                logging.info(f"Creating subscription {subscription_name} on replica {replica_host}.")
                create_sub_query = f"""
                    CREATE SUBSCRIPTION {subscription_name}
                    CONNECTION '{connection_string}'
                    PUBLICATION {publication_name} WITH (copy_data = {isCopy});
                """
                
                cur.execute(create_sub_query)
                
                logging.info(f"Subscription {subscription_name} created on replica {replica_host}.")
        except Exception as e:
            logging.error(f"Error creating subscription on {replica_host}: {e}")
        finally:
            if conn:
                conn.close()
    
    thread = threading.Thread(target=subscription_task)
    thread.start()
    return {"status": "creating", "message": f"Subscription creation started on {replica_host}."}

def handle_create_pub_sub(schema_name, isCopy=True):
    try:
        # Check if schema exists on primary server
        primary_conn = get_connection(config.primary_host_local)
        with primary_conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = %s
                );
            """, (schema_name,))
            schema_exists = cur.fetchone()[0]

        if not schema_exists:
            return {"status": "error", "message": f"Schema '{schema_name}' does not exist on the primary server."}

        create_publication(schema_name)

        # Use ThreadPoolExecutor to parallelize operations on replica servers
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(config.replica_hosts)) as executor:
            # Drop schema and restore on all replicas
            drop_futures = [executor.submit(drop_schema, schema_name, replica_host) for replica_host in config.replica_hosts]
            concurrent.futures.wait(drop_futures)

            dump_futures = [executor.submit(dump_and_restore_schema, schema_name, replica_host) for replica_host in config.replica_hosts]
            dump_results = concurrent.futures.wait(dump_futures)

            if any(future.result()["status"] == "error" for future in dump_results.done):
                return {"status": "error", "message": "Error occurred while dumping and restoring schema on one or more replicas."}

            # Create subscriptions on all replicas
            sub_futures = [executor.submit(create_subscription, schema_name, replica_host, index, isCopy) 
                           for index, replica_host in enumerate(config.replica_hosts)]
            sub_results = concurrent.futures.wait(sub_futures)

        return {
            "status": "success",
            "message": f"Publication created and subscriptions started on all replica servers."
        }
    except Exception as e:
        logging.error(f"Error in create_pub_sub: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if primary_conn:
            primary_conn.close()

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
        with primary_conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = %s
                );
            """, (schema_name,))
            schema_exists = cur.fetchone()[0]

        if not schema_exists:
            return {"status": "error", "message": f"Schema '{schema_name}' does not exist on the primary server."}

        primary_conn = get_connection(config.primary_host_local)
        
        comparison_results = []
        
        with primary_conn.cursor() as primary_cur:
            # Get tables from primary
            primary_cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE';
            """, (schema_name,))
            primary_tables = [row[0] for row in primary_cur.fetchall()]
            
            for table in primary_tables:
                # Estimated row count from primary using pg_class
                primary_cur.execute("""
                    SELECT reltuples::BIGINT
                    FROM pg_class
                    WHERE oid = %s::regclass;
                """, (f"{schema_name}.{table}",))
                primary_count = primary_cur.fetchone()[0] if primary_cur.rowcount > 0 else 0
                
                replica_counts = {}
                
                for replica_host in config.replica_hosts:
                    replica_conn = get_connection(replica_host)
                    with replica_conn.cursor() as replica_cur:
                        # Check if table exists in replica
                        replica_cur.execute("""
                            SELECT EXISTS (
                                SELECT 1
                                FROM information_schema.tables
                                WHERE table_schema = %s AND table_name = %s
                            );
                        """, (schema_name, table))
                        table_exists = replica_cur.fetchone()[0]
                        
                        if table_exists:
                            # Estimated row count from replica using pg_class
                            replica_cur.execute("""
                                SELECT reltuples::BIGINT
                                FROM pg_class
                                WHERE oid = %s::regclass;
                            """, (f"{schema_name}.{table}",))
                            replica_count = replica_cur.fetchone()[0] if replica_cur.rowcount > 0 else 0
                        else:
                            replica_count = 0
                        
                        replica_counts[replica_host] = replica_count
                    
                    replica_conn.close()
                
                comparison_results.append({
                    "table": f"{schema_name}.{table}",
                    "primary_row_count": primary_count,
                    "replica_row_counts": replica_counts,
                    "differences": {host: primary_count - count for host, count in replica_counts.items()}
                })
        
        return {"status": "success", "data": comparison_results}
    except Exception as e:
        logging.error(f"Error in compare_row_counts: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        primary_conn.close()

def get_publication_stats():
    try:
        conn = get_connection(config.primary_host_local)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot
                FROM pg_publication;
            """)
            stats = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            publication_stats = []
            for row in stats:
                row_dict = dict(zip(columns, row))
                publication_stats.append(row_dict)
        return {"status": "success", "data": publication_stats}
    except Exception as e:
        logging.error(f"Error retrieving publication stats: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()

def get_subscription_stats():
    try:
        stats = []
        for replica_host in config.replica_hosts:
            conn = get_connection(replica_host)
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
                columns = [desc[0] for desc in cur.description]
                subscription_stats = [dict(zip(columns, row)) for row in cur.fetchall()]
                stats.extend([{**stat, "replica_host": replica_host} for stat in subscription_stats])
            conn.close()
        return {"status": "success", "data": stats}
    except Exception as e:
        logging.error(f"Error retrieving subscription stats: {e}")
        return {"status": "error", "message": str(e)}

def remove_schema_from_replica(schema_name):
    try:
        for replica_host in config.replica_hosts:
            conn = get_connection(replica_host)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE;").format(sql.Identifier(schema_name)))
                logging.info(f"Schema {schema_name} removed from replica {replica_host}.")
        return {"status": "success", "message": f"Schema {schema_name} successfully removed from all replicas."}
    except Exception as e:
        logging.error(f"Error removing schema from replica: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if conn:
            conn.close()

def check_subscription_status(schema_name):
    try:
        results = []
        for index, replica_host in enumerate(config.replica_hosts):
            conn = get_connection(replica_host)
            with conn.cursor() as cur:
                subscription_name = f"{schema_name}_sub" if index == 0 else f"{schema_name}_sub_{index}"
                cur.execute("""
                    SELECT subname, subenabled
                    FROM pg_subscription
                    WHERE subname = %s;
                """, (subscription_name,))
                result = cur.fetchone()
                
                if result:
                    results.append({
                        "replica_host": replica_host,
                        "subscription_name": result[0],
                        "enabled": result[1],
                        "state": "created" if result[1] else "creating"
                    })
                else:
                    results.append({
                        "replica_host": replica_host,
                        "status": "pending",
                        "message": f"Subscription {subscription_name} is still being created."
                    })
            conn.close()
        
        return {"status": "success", "data": results}
    except Exception as e:
        logging.error(f"Error checking subscription status: {e}")
        return {"status": "error", "message": str(e)}