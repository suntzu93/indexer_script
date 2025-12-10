import psycopg2
import config
import logging

def get_subgraphs_by_status(is_offchain):
    """
    Get list of subgraphs based on offchain status
    is_offchain = False: return subgraphs where paused_at is NOT NULL
    is_offchain = True: return subgraphs where paused_at is NULL
    """
    subgraphs = []
    try:
        conn = psycopg2.connect(
            host=config.primary_host,
            port=config.db_port,
            database=config.graph_node_database[0],
            user=config.username,
            password=config.password
        )
        
        cursor = conn.cursor()
        
        if is_offchain:
            # Get subgraphs where paused_at is NULL (active/offchain)
            query = """
                SELECT a.subgraph
                FROM deployment_schemas as a
                JOIN subgraphs.subgraph_deployment_assignment as b
                ON a.id = b.id
                WHERE b.paused_at IS NULL
            """
        else:
            # Get subgraphs where paused_at is NOT NULL (paused/onchain)
            query = """
                SELECT a.subgraph
                FROM deployment_schemas as a
                JOIN subgraphs.subgraph_deployment_assignment as b
                ON a.id = b.id
                WHERE b.paused_at IS NOT NULL
            """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        for row in results:
            subgraphs.append(row[0])
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error getting subgraphs by status: {str(e)}")
        print(f"Error getting subgraphs by status: {str(e)}")
    
    return subgraphs

def get_subgraph_sizes():
    all_subgraph_sizes = []
    
    # First, get deployment schemas mapping from primary host
    deployment_schemas = {}
    try:
        conn = psycopg2.connect(
            host=config.primary_host,
            port=config.db_port,
            database=config.graph_node_database[0],  # Use first database
            user=config.username,
            password=config.password
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT subgraph, name FROM public.deployment_schemas")
        results = cursor.fetchall()
        
        # Create mapping: schema_name -> subgraph_hash
        for row in results:
            deployment_schemas[row[1]] = row[0]  # name -> subgraph
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error getting deployment schemas from primary host: {str(e)}")
        print(f"Error getting deployment schemas from primary host: {str(e)}")
    
    # Now iterate through all shards to get schema sizes
    for shard_host in config.shards:
        for db in config.graph_node_database:
            try:
                conn = psycopg2.connect(
                    host=shard_host,
                    port=config.db_port,
                    database=db,
                    user=config.username,
                    password=config.password
                )
                
                cursor = conn.cursor()
                
                query = """
                SELECT 
                    n.nspname as schema_name,
                    sum(COALESCE(pg_total_relation_size(c.oid), 0)) AS total_bytes
                FROM pg_class c
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE relkind = 'r' 
                AND n.nspname LIKE 'sgd%'
                GROUP BY n.nspname
                """
                
                cursor.execute(query)
                results = cursor.fetchall()
                
                for row in results:
                    schema_name = row[0]
                    total_bytes = row[1]
                    
                    # Create response with only deployment_name and size_total_bytes
                    size_data = {
                        'deployment_name': deployment_schemas.get(schema_name, schema_name),
                        'size_total_bytes': total_bytes
                    }
                    
                    all_subgraph_sizes.append(size_data)
                
                cursor.close()
                conn.close()
                
            except Exception as e:
                logging.error(f"Error in get_subgraph_sizes for shard {shard_host} database {db}: {str(e)}")
                print(f"Error in get_subgraph_sizes for shard {shard_host} database {db}: {str(e)}")
    
    return all_subgraph_sizes if all_subgraph_sizes else None