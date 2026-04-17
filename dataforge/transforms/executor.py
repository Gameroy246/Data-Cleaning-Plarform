import uuid, duckdb
from dataforge.core.engine import Engine

class TransformExecutor:
    def __init__(self, engine: Engine): self.engine = engine
    def execute_transform(self, node_type: str, config: dict, input_tables: list[str]) -> str:
        out = f"temp_{uuid.uuid4().hex[:8]}"
        
        if not input_tables or not input_tables[0]:
            raise Exception("Missing input data! Ensure Step 1 is a Source node.")
            
        inp = input_tables[0]
        sql = f"SELECT * FROM {inp}" 
        
        try:
            if node_type == "join": 
                if len(input_tables) < 2: raise Exception("Join requires two input sources.")
                col = config.get('on_column', '').strip()
                if col: sql = f"SELECT * FROM {input_tables[0]} {config.get('how', 'INNER')} JOIN {input_tables[1]} USING ({col})"
            elif node_type == "quarantine": 
                cond = config.get('condition', '').strip()
                if cond: sql = f"SELECT * FROM {inp} WHERE {cond}"
            elif node_type == "regex_extract": 
                c = config.get('column', '').strip(); p = config.get('pattern', '').strip(); nc = config.get('new_column', '').strip()
                if c and p and nc: sql = f"SELECT *, regexp_extract({c}, '{p}') AS {nc} FROM {inp}"
            elif node_type == "aggregate": 
                gb = [x for x in config.get('group_by', []) if x.strip()]
                ac = config.get('aggregate_column', '').strip()
                if gb and ac: sql = f"SELECT {', '.join(gb)}, {config.get('aggregate_function', 'SUM')}({ac}) AS {ac}_agg FROM {inp} GROUP BY {', '.join(gb)}"
            elif node_type == "custom_sql": 
                c_sql = config.get("sql", "")
                if "{input}" in c_sql and "SELECT *,  AS" not in c_sql: sql = c_sql.replace("{input}", inp)
            elif node_type == "deduplicate": sql = f"SELECT DISTINCT * FROM {inp}"
            elif node_type == "drop_columns":
                cols = [x for x in config.get("columns", []) if x.strip()]
                if cols: sql = f"SELECT * EXCLUDE ({', '.join(cols)}) FROM {inp}"
            elif node_type == "rename_column": 
                old = config.get("old_col", "").strip(); new = config.get("new_col", "").strip()
                if old and new: sql = f"SELECT * RENAME ({old} AS {new}) FROM {inp}"
            elif node_type == "sort": 
                col = config.get("column", "").strip()
                if col: sql = f"SELECT * FROM {inp} ORDER BY {col} {config.get('order', 'ASC')}"
            elif node_type == "null_handling":
                cols = [x for x in config.get("columns", []) if x.strip()]
                if cols:
                    if config.get("strategy") == "drop": sql = f"SELECT * FROM {inp} WHERE " + " AND ".join([f"{c} IS NOT NULL" for c in cols])
                    else: sql = f"SELECT * REPLACE (" + ", ".join([f"COALESCE(CAST({c} AS VARCHAR), '{config.get('fill_value', 'Unknown')}') AS {c}" for c in cols]) + f") FROM {inp}"
            
            self.engine.con.execute(f"CREATE TABLE {out} AS {sql}")
            return out
        except Exception as e: 
            raise Exception(f"Failed at '{node_type}'. Details: {e}")
