import duckdb, polars as pl, pandas as pd, os

class Engine:
    def __init__(self, db_path=":memory:"):
        self.con = duckdb.connect(db_path)
            
    def execute_sql(self, sql: str) -> pl.DataFrame:
        return self.con.execute(sql).pl()

    def register_view(self, name: str, path: str, format_type: str) -> bool:
        abs_path = os.path.abspath(path)
        sql_path = abs_path.replace('\\', '/')
        if not os.path.exists(abs_path): raise Exception(f"File not found: {abs_path}")
        
        fmt = format_type.lower()
        if fmt in ["csv", "tsv"]:
            df = pd.read_csv(abs_path, on_bad_lines='skip', sep=None, engine='python')
            self.con.register(name, df)
        elif fmt in ["excel", "xlsx", "xls"]:
            df = pd.read_excel(abs_path)
            self.con.register(name, df)
        elif fmt in ["json", "jsonl"]:
            self.con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_json_auto('{sql_path}')")
        elif fmt in ["parquet"]:
            self.con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{sql_path}')")
        return True
