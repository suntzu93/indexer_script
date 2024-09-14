import psycopg2
import config
import logging

def get_subgraph_sizes():
    all_subgraph_sizes = []
    
    for db in config.graph_node_database:
        try:
            conn = psycopg2.connect(
                host=config.db_host,
                port=config.db_port,
                database=db,
                user=config.username,
                password=config.password
            )
            
            cursor = conn.cursor()
            
            query = """
            SELECT 
                sg.name as deployment_name,
                size.total_bytes as size_total_bytes
            FROM subgraphs."subgraph_deployment" as sd
            left join subgraphs."subgraph_version" as sv on (sv.deployment = sd.deployment)
            left join subgraphs."subgraph" as sg on (sv.id in (sg.current_version, sg.pending_version))
            left join public."deployment_schemas" as ds on (ds.subgraph = sd.deployment)
            left join (
                SELECT *, total_bytes-index_bytes-coalesce(toast_bytes,0) AS table_bytes FROM (
                    SELECT nspname AS table_schema
                        , ds.subgraph
                        , sum(pg_total_relation_size(c.oid)) AS total_bytes
                        , sum(pg_indexes_size(c.oid)) AS index_bytes
                        , sum(pg_total_relation_size(reltoastrelid)) AS toast_bytes
                    FROM pg_class c
                    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                        INNER JOIN deployment_schemas ds ON ds.name = n.nspname
                    WHERE relkind = 'r'
                        GROUP BY ds.subgraph,table_schema
                ) a
            ) as size on (sd.deployment = size.subgraph) order by sd.id
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            columns = [desc[0] for desc in cursor.description]
            subgraph_sizes = [dict(zip(columns, row)) for row in results]
            
            # Add database name to each result
            for size in subgraph_sizes:
                size['database'] = db
            
            all_subgraph_sizes.extend(subgraph_sizes)
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logging.error(f"Error in get_subgraph_sizes for database {db}: {str(e)}")
            print(f"Error in get_subgraph_sizes for database {db}: {str(e)}")
    
    return all_subgraph_sizes if all_subgraph_sizes else None