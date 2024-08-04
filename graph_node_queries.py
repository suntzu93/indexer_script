import psycopg2
import config
import logging

def get_subgraph_sizes():
    try:
        conn = psycopg2.connect(
            host=config.db_host,
            port=config.db_port,
            database=config.graph_node_database,
            user=config.username,
            password=config.password
        )
        
        cursor = conn.cursor()
        
        query = """
        SELECT 
            ds.name as deployment_schema,
            sg.name as deployment_name,
            sd.id as deployment_id,
            substring(sm.repository,20,31) as git_repo,
            sda.node_id as node_id,
            sd.health as sub_health,
            sd.failed as sub_failed,
            sd.synced as sub_sync,
            sd.latest_ethereum_block_number as last_block,
            sd.entity_count as sub_entity,
            size.total_bytes as size_total, 
            size.total_bytes as size_total_bytes,
            sd.reorg_count as reorg_count, 
            sd.current_reorg_depth as curr_reorg_count, 
            sd.max_reorg_depth as max_reorg
        FROM subgraphs."subgraph_deployment" as sd
        left join subgraphs."subgraph_manifest" as sm on (sm.id = sd.id)
        left join subgraphs."subgraph_deployment_assignment" as sda on (sda.id = sd.id)
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
        
        cursor.close()
        conn.close()
        
        return subgraph_sizes
    except Exception as e:
        logging.error(f"Error in get_subgraph_sizes: {str(e)}")
        print(f"Error in get_subgraph_sizes: {str(e)}")
        return None