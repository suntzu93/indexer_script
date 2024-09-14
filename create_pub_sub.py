import psycopg2
from psycopg2 import sql
import logging
import config

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
        conn = get_connection(config.primary_host)
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
        conn = get_connection(config.primary_host)
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

def drop_and_create_schema(schema_name):
    try:
        conn = get_connection(config.replica_host)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE;").format(sql.Identifier(schema_name)))
            cur.execute(sql.SQL("CREATE SCHEMA {};").format(sql.Identifier(schema_name)))
            logging.info(f"Schema {schema_name} dropped and created on replica.")
    except Exception as e:
        logging.error(f"Error dropping/creating schema: {e}")
        raise
    finally:
        conn.close()

def dump_and_restore_schema(schema_name):
    try:
        primary_conn = get_connection(config.primary_host)
        replica_conn = get_connection(config.replica_host)
        with primary_conn.cursor() as primary_cur, replica_conn.cursor() as replica_cur:
            # Fetch and create tables
            primary_cur.execute(sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;"), [schema_name])
            tables = primary_cur.fetchall()
            for table in tables:
                table_name = table[0]
                primary_cur.execute(sql.SQL("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = %s;"),
                                    [schema_name, table_name])
                columns = primary_cur.fetchall()
                columns_def = ", ".join([f"{col[0]} {col[1]}" for col in columns])
                replica_cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.SQL(columns_def)
                ))
                
                # Fetch and recreate indexes
                primary_cur.execute(sql.SQL("SELECT indexdef FROM pg_indexes WHERE schemaname = %s AND tablename = %s;"), [schema_name, table_name])
                indexes = primary_cur.fetchall()
                for index in indexes:
                    index_def = index[0]
                    try:
                        replica_cur.execute(index_def)
                    except psycopg2.errors.DuplicateObject:
                        logging.warning(f"Index already exists on {schema_name}.{table_name}, skipping.")
            logging.info(f"Schema {schema_name} dumped from primary and restored on replica with indexes.")
    except Exception as e:
        logging.error(f"Error dumping/restoring schema: {e}")
        raise
    finally:
        primary_conn.close()
        replica_conn.close()

def create_subscription(schema_name):
    try:
        conn = get_connection(config.replica_host)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                DROP SUBSCRIPTION IF EXISTS {};
                CREATE SUBSCRIPTION {}
                CONNECTION 'host={host} port={port} user={user} password={password} dbname={dbname}'
                PUBLICATION {publication} WITH (copy_data = true);
            """).format(
                sql.Identifier(f"{schema_name}_sub"),
                sql.Identifier(f"{schema_name}_sub"),
                host=sql.Literal(config.primary_host),
                port=sql.Literal(config.db_port),
                user=sql.Literal(config.username),
                password=sql.Literal(config.password),
                dbname=sql.Literal(config.primary_database),
                publication=sql.Identifier(f"{schema_name}_pub")
            ))
            logging.info(f"Subscription {schema_name}_sub created on replica.")
    except Exception as e:
        logging.error(f"Error creating subscription: {e}")
        raise
    finally:
        conn.close()

def handle_create_pub_sub(schema_name):
    try:
        create_publication(schema_name)
        # Check if schema exists on replica
        conn = get_connection(config.replica_host)
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);"), [schema_name])
            schema_exists = cur.fetchone()[0]

        if schema_exists:
            drop_and_create_schema(schema_name)
        else:
            drop_and_create_schema(schema_name)

        dump_and_restore_schema(schema_name)
        create_subscription(schema_name)
        return {"status": "success", "message": "Publication and Subscription created successfully."}
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
        primary_conn = get_connection(config.primary_host)
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
        conn = get_connection(config.primary_host)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT application_name, state, sent_lsn, write_lsn, flush_lsn, replay_lsn, 
                       write_lag, flush_lag, replay_lag, sync_state, reply_time 
                FROM pg_stat_replication;
            """)
            stats = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            publication_stats = [dict(zip(columns, row)) for row in stats]
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