import os
from pathlib import Path

PROJECT_FILES = {
    "requirements.txt": """fastapi>=0.103.0
uvicorn>=0.23.2
duckdb>=0.10.0
polars>=0.19.0
pandas>=2.0.0
pyarrow>=15.0.0
openpyxl>=3.1.0
plotly>=5.18.0
streamlit>=1.31.0
psutil>=5.9.0
python-multipart>=0.0.6""",

    "dataforge/__init__.py": "",
    "dataforge/core/__init__.py": "",
    "dataforge/transforms/__init__.py": "",
    "dataforge/api/__init__.py": "",
    "ui/__init__.py": "",

    "dataforge/exceptions.py": "class DataForgeException(Exception): pass",

    "dataforge/core/memory_manager.py": """import os, psutil, duckdb
class MemoryManager:
    def __init__(self, con, temp_dir="./dataforge_temp", cpu_cores=None):
        self.con = con; self.temp_dir = os.path.abspath(temp_dir)
        os.makedirs(self.temp_dir, exist_ok=True)
        try:
            allocated_gb = max(1, int((psutil.virtual_memory().available / (1024**3)) * 0.6)) 
            self.con.execute(f"PRAGMA memory_limit='{allocated_gb}GB'")
            self.con.execute(f"PRAGMA temp_directory='{self.temp_dir}'")
        except: pass""",

    "dataforge/core/engine.py": """import duckdb, polars as pl, pandas as pd, os
from dataforge.core.memory_manager import MemoryManager
from dataforge.exceptions import DataForgeException

class Engine:
    def __init__(self, db_path=":memory:", cpu_cores=None):
        self.con = duckdb.connect(db_path)
        self.memory_manager = MemoryManager(self.con, cpu_cores=cpu_cores or 4)
            
    def execute_sql(self, sql: str) -> pl.DataFrame:
        try: return self.con.execute(sql).pl()
        except duckdb.Error as e: raise DataForgeException(f"SQL failed: {e}")

    def register_view(self, name: str, path: str, format_type: str) -> bool:
        abs_path = os.path.abspath(path)
        sql_path = abs_path.replace('\\\\', '/')
        
        if not os.path.exists(abs_path): 
            raise DataForgeException(f"File not found at: {abs_path}")
        
        try:
            fmt = format_type.lower()
            if fmt in ["csv", "tsv", "txt"]:
                df = pd.read_csv(abs_path, on_bad_lines='skip', sep=None, engine='python')
                self.con.register(name, df)
            elif fmt in ["excel", "xlsx", "xls"]:
                df = pd.read_excel(abs_path)
                self.con.register(name, df)
            elif fmt in ["json", "jsonl"]:
                self.con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_json_auto('{sql_path}')")
            elif fmt in ["parquet"]:
                self.con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{sql_path}')")
            else:
                raise DataForgeException(f"Unsupported format: {fmt}")
            return True
        except Exception as e:
            raise DataForgeException(f"Engine failed to load {fmt} file: {str(e)}")""",

    "dataforge/transforms/executor.py": """import uuid, duckdb
from dataforge.core.engine import Engine
from dataforge.exceptions import DataForgeException

class TransformExecutor:
    def __init__(self, engine: Engine): self.engine = engine
    def execute_transform(self, node_type: str, config: dict, input_tables: list[str]) -> str:
        out = f"temp_{uuid.uuid4().hex[:8]}"; sql = ""
        try:
            inp = input_tables[0] if input_tables else ""
            if node_type == "join": sql = f"SELECT * FROM {input_tables[0]} {config.get('how', 'INNER')} JOIN {input_tables[1]} USING ({config.get('on_column')})"
            elif node_type == "union": sql = " UNION ALL ".join([f"SELECT * FROM {t}" for t in input_tables])
            elif node_type == "quarantine": sql = f"SELECT * FROM {inp} WHERE {config.get('condition')}"
            elif node_type == "regex_extract": sql = f"SELECT *, regexp_extract({config.get('column')}, '{config.get('pattern')}') AS {config.get('new_column')} FROM {inp}"
            elif node_type == "aggregate": sql = f"SELECT {', '.join(config.get('group_by', []))}, {config.get('aggregate_function', 'SUM')}({config.get('aggregate_column')}) AS {config.get('aggregate_column')}_agg FROM {inp} GROUP BY {', '.join(config.get('group_by', []))}"
            elif node_type == "custom_sql": sql = config.get("sql", "").replace("{input}", inp)
            elif node_type == "deduplicate": sql = f"SELECT DISTINCT * FROM {inp}"
            elif node_type == "drop_columns":
                cols = ", ".join(config.get("columns", []))
                sql = f"SELECT * EXCLUDE ({cols}) FROM {inp}" if cols else f"SELECT * FROM {inp}"
            elif node_type == "rename_column":
                old = config.get("old_col"); new = config.get("new_col")
                sql = f"SELECT * RENAME ({old} AS {new}) FROM {inp}" if old and new else f"SELECT * FROM {inp}"
            elif node_type == "sort":
                col = config.get("column"); order = config.get("order", "ASC")
                sql = f"SELECT * FROM {inp} ORDER BY {col} {order}" if col else f"SELECT * FROM {inp}"
            elif node_type == "null_handling":
                cols = config.get("columns", [])
                if not cols or cols == [""]: sql = f"SELECT * FROM {inp}"
                elif config.get("strategy") == "drop":
                    sql = f"SELECT * FROM {inp} WHERE " + " AND ".join([f"{c} IS NOT NULL" for c in cols])
                else:
                    val = config.get("fill_value", "Unknown")
                    sql = f"SELECT * REPLACE (" + ", ".join([f"COALESCE(CAST({c} AS VARCHAR), '{val}') AS {c}" for c in cols]) + f") FROM {inp}"
            elif node_type == "sink":
                fmt = config.get("format", "parquet").upper()
                self.engine.con.execute(f"COPY {inp} TO '{config.get('path', 'output')}' (FORMAT {fmt}{', HEADER, DELIMITER '',' if fmt=='CSV' else ''})")
                sql = f"SELECT * FROM {inp}" 
            else: sql = f"SELECT * FROM {inp}"
            
            if sql: self.engine.con.execute(f"CREATE TABLE {out} AS {sql}")
            return out
        except Exception as e: raise DataForgeException(f"Transform '{node_type}' failed: {e}")""",

    "dataforge/api/models.py": """from pydantic import BaseModel
from typing import List, Dict, Any
class NodeConfig(BaseModel): id: str; type: str; config: Dict[str, Any]; inputs: List[str] = []
class PipelineConfig(BaseModel): id: str = "default"; nodes: List[NodeConfig]
class RunResponse(BaseModel): status: str; message: str; rows_processed: int; duration_sec: float; output_table: str
class SQLQuery(BaseModel): query: str""",

    "dataforge/api/main.py": """import time, asyncio, shutil, os
from fastapi import FastAPI, HTTPException, UploadFile, File
from dataforge.api.models import PipelineConfig, RunResponse, SQLQuery
from dataforge.core.engine import Engine
from dataforge.transforms.executor import TransformExecutor

app = FastAPI()
preview_engine = Engine()

@app.post("/upload")
async def upload_files(files: list[UploadFile] = File(...)):
    names = []
    for f in files:
        with open(f"./{f.filename}", "wb+") as buf: shutil.copyfileobj(f.file, buf)
        try: preview_engine.register_view(f.filename.split('.')[0], f"./{f.filename}", f.filename.split('.')[-1])
        except Exception as e: raise HTTPException(400, f"Engine failed to read {f.filename}: {e}")
        names.append(f.filename)
    return {"filenames": names}

@app.get("/data/{filename}")
async def get_raw_data(filename: str):
    try:
        e = Engine()
        e.register_view("raw", f"./{filename}", filename.split('.')[-1])
        return e.execute_sql("SELECT * FROM raw LIMIT 100").to_dicts()
    except Exception as e: raise HTTPException(400, str(e))

@app.post("/sql/execute")
async def execute_raw_sql(q: SQLQuery):
    try: return preview_engine.execute_sql(q.query).to_dicts()
    except Exception as e: raise HTTPException(400, str(e))

@app.post("/pipeline/execute", response_model=RunResponse)
async def execute_pipeline(pipe: PipelineConfig):
    start = time.time()
    def _run():
        eng = Engine(); exec = TransformExecutor(eng); tbls = {}; last = ""
        for n in pipe.nodes:
            if n.type == "source":
                eng.register_view(n.id, n.config.get("path"), n.config.get("format"))
                tbls[n.id] = last = n.id
            else: tbls[n.id] = last = exec.execute_transform(n.type, n.config, [tbls[i] for i in n.inputs])
        res = eng.execute_sql(f"SELECT COUNT(*) as c FROM {last}")
        preview_engine.con.register(last, eng.execute_sql(f"SELECT * FROM {last}").to_arrow())
        return last, res['c'][0]
    try:
        last, rows = await asyncio.to_thread(_run)
        return RunResponse(status="success", message="Success", rows_processed=rows, duration_sec=time.time()-start, output_table=last)
    except Exception as e: raise HTTPException(400, str(e))""",

    "ui/app.py": """import streamlit as st, requests, pandas as pd, plotly.express as px, os, json
API_URL = "http://localhost:8000"
TEMPLATES_DIR = "./templates"
os.makedirs(TEMPLATES_DIR, exist_ok=True)
st.set_page_config(page_title="Data Architect: Streamlit Edition", layout="wide")
st.markdown("<style>.timeline-node { background: #1f2428; border: 2px solid #58a6ff; border-radius: 6px; padding: 10px; margin: 5px; text-align: center; color: white; box-shadow: 0 4px 6px rgba(0,0,0,0.3);} .node-arrow { display: flex; align-items: center; justify-content: center; font-size: 24px; color: #8b949e; }</style>", unsafe_allow_html=True)

for key in ['pipeline', 'history', 'files']:
    if key not in st.session_state: st.session_state[key] = []
if 'last_table' not in st.session_state: st.session_state['last_table'] = None
if 'raw_table' not in st.session_state: st.session_state['raw_table'] = None

def save_state(): st.session_state['history'].append(list(st.session_state['pipeline']))
def undo():
    if st.session_state['history']: st.session_state['pipeline'] = st.session_state['history'].pop()

st.title("🗄️ Local Data Architect")
st.markdown("### ⏱️ Visual Transformation Timeline")
if not st.session_state['pipeline']: st.info("Pipeline is empty. Add steps below.")
else:
    cols = st.columns(len(st.session_state['pipeline']) * 2 - 1)
    for i, node in enumerate(st.session_state['pipeline']):
        with cols[i * 2]: st.markdown(f"<div class='timeline-node'><b>Step {i+1}</b><br>{node['type'].upper()}</div>", unsafe_allow_html=True)
        if i < len(st.session_state['pipeline']) - 1:
            with cols[i * 2 + 1]: st.markdown("<div class='node-arrow'>➔</div>", unsafe_allow_html=True)

tab_build, tab_view, tab_pivot, tab_code = st.tabs(["1. Pipeline Builder", "2. Split View & Diff", "3. Pivot & Charts", "4. SQL & Python"])

with tab_build:
    c1, c2 = st.columns([1, 2], gap="large")
    with c1:
        st.subheader("📥 Data Sources")
        uploaded = st.file_uploader("Upload CSV/Excel", accept_multiple_files=True)
        if uploaded and st.button("Register Files to Memory"):
            files_data = [("files", (f.name, f.getvalue(), f.type)) for f in uploaded]
            res = requests.post(f"{API_URL}/upload", files=files_data)
            st.session_state['files'] = list(set(st.session_state['files'] + res.json().get("filenames", [])))
            st.rerun()
        if st.session_state['files']:
            for f in st.session_state['files']: st.code(f)

    with c2:
        st.subheader("🧩 Add Pipeline Step")
        step_type = st.selectbox("Select Operation", [
            "Source (Load File)", "Filter Rows", "Join Data", "Regex Extractor", 
            "Formula (Math/SQL)", "Aggregate (Group By)", "Drop/Fill Nulls", 
            "Deduplicate Rows", "Drop Columns", "Rename Column", "Sort Data"
        ])
        
        with st.form("add_node_form"):
            config = {}
            if step_type == "Source (Load File)":
                sel_file = st.selectbox("Select File", st.session_state['files'] if st.session_state['files'] else ["None"])
                config = {"path": f"./{sel_file}", "format": sel_file.split('.')[-1] if '.' in sel_file else 'csv'}
            elif step_type == "Filter Rows": config['condition'] = st.text_input("SQL Condition (e.g., age > 18)")
            elif step_type == "Join Data":
                config['how'] = st.selectbox("Join Type", ["INNER", "LEFT"])
                config['on_column'] = st.text_input("Common Column")
            elif step_type == "Regex Extractor":
                c_1, c_2, c_3 = st.columns(3)
                with c_1: config['column'] = st.text_input("Target Column")
                with c_2: config['pattern'] = st.text_input("Regex Pattern")
                with c_3: config['new_column'] = st.text_input("New Column Name")
            elif step_type == "Formula (Math/SQL)":
                c_name = st.text_input("New Column")
                formula = st.text_input("Formula (e.g., price * 1.2)")
                config['sql'] = f"SELECT *, {formula} AS {c_name} FROM {{input}}"
            elif step_type == "Aggregate (Group By)":
                config['group_by'] = [x.strip() for x in st.text_input("Group By (comma sep)").split(',')]
                config['aggregate_function'] = st.selectbox("Function", ["SUM", "AVG", "MAX", "MIN", "COUNT"])
                config['aggregate_column'] = st.text_input("Target Column")
            elif step_type == "Drop/Fill Nulls":
                strat = st.selectbox("Strategy", ["drop", "fill"])
                config['strategy'] = strat
                config['columns'] = [x.strip() for x in st.text_input("Columns to Check (comma sep)").split(',')]
                if strat == "fill": config['fill_value'] = st.text_input("Fill Value", "Unknown")
            elif step_type == "Drop Columns": config['columns'] = [x.strip() for x in st.text_input("Columns to Drop (comma sep)").split(',')]
            elif step_type == "Rename Column":
                c_1, c_2 = st.columns(2)
                with c_1: config['old_col'] = st.text_input("Current Name")
                with c_2: config['new_col'] = st.text_input("New Name")
            elif step_type == "Sort Data":
                c_1, c_2 = st.columns(2)
                with c_1: config['column'] = st.text_input("Column to Sort")
                with c_2: config['order'] = st.selectbox("Order", ["ASC", "DESC"])

            col_add, col_undo = st.columns([3, 1])
            with col_add:
                if st.form_submit_button("➕ Add to Pipeline", type="primary"):
                    save_state()
                    node_id = f"{step_type.split()[0].lower()}_{len(st.session_state['pipeline'])}"
                    node_type = "source" if "Source" in step_type else "quarantine" if "Filter" in step_type else "join" if "Join" in step_type else "regex_extract" if "Regex" in step_type else "custom_sql" if "Formula" in step_type else "aggregate" if "Aggregate" in step_type else "null_handling" if "Nulls" in step_type else "deduplicate" if "Deduplicate" in step_type else "drop_columns" if "Drop Columns" in step_type else "rename_column" if "Rename" in step_type else "sort"
                    inputs = []
                    if node_type != "source" and st.session_state['pipeline']:
                        inputs = [st.session_state['pipeline'][-2]['id'], st.session_state['pipeline'][-1]['id']] if node_type == "join" and len(st.session_state['pipeline']) >= 2 else [st.session_state['pipeline'][-1]['id']]
                    st.session_state['pipeline'].append({"id": node_id, "type": node_type, "config": config, "inputs": inputs})
                    st.rerun()

        c_exec, c_rev = st.columns([3, 1])
        with c_exec:
            if st.button("🚀 Execute Full Pipeline", use_container_width=True):
                with st.spinner("Processing..."):
                    try:
                        if st.session_state['pipeline'] and st.session_state['pipeline'][0]['type'] == 'source':
                            src_file = st.session_state['pipeline'][0]['config']['path'].split('/')[-1]
                            raw_res = requests.get(f"{API_URL}/data/{src_file}")
                            if raw_res.status_code == 200: st.session_state['raw_table'] = pd.DataFrame(raw_res.json())
                        res = requests.post(f"{API_URL}/pipeline/execute", json={"id": "st_pipe", "nodes": st.session_state['pipeline']})
                        if res.status_code == 200:
                            st.session_state['last_table'] = res.json().get('output_table')
                            st.success(f"Executed in {res.json().get('duration_sec'):.2f}s")
                        else: st.error(res.json().get('detail'))
                    except Exception as e: st.error(e)
        with c_rev:
            if st.button("↩ Undo Last", use_container_width=True): undo(); st.rerun()

with tab_view:
    if st.session_state['last_table'] and st.session_state['raw_table'] is not None:
        c_left, c_right = st.columns(2)
        try:
            trans_df = pd.DataFrame(requests.get(f"{API_URL}/sql/execute", json={"query": f"SELECT * FROM {st.session_state['last_table']} LIMIT 1000"}).json())
            with c_left: st.markdown("**Original Source**"); st.dataframe(st.session_state['raw_table'], use_container_width=True)
            with c_right: st.markdown("**Transformed Output**"); st.dataframe(trans_df, use_container_width=True)
            st.download_button("📥 Export Results (CSV)", trans_df.to_csv(index=False), "pipeline_output.csv")
        except Exception as e: st.error("Failed to load output.")

with tab_pivot:
    if st.session_state['last_table']:
        try:
            df = pd.DataFrame(requests.get(f"{API_URL}/sql/execute", json={"query": f"SELECT * FROM {st.session_state['last_table']} LIMIT 2000"}).json())
            with st.expander("Build Pivot Table", expanded=True):
                pc1, pc2, pc3 = st.columns(3)
                with pc1: p_row = st.selectbox("Row (Group By)", df.columns)
                with pc2: p_col = st.selectbox("Column (Pivot On)", df.columns)
                with pc3: p_val = st.selectbox("Value (Metric)", df.columns)
                if st.button("Generate Pivot Data"):
                    st.dataframe(pd.pivot_table(df, values=p_val, index=p_row, columns=p_col, aggfunc='sum').reset_index(), use_container_width=True)
            num_cols = df.select_dtypes(include=['float64', 'int64']).columns
            if len(num_cols) > 0: st.plotly_chart(px.histogram(df, x=num_cols[0], title=f"Distribution of {num_cols[0]}"), use_container_width=True)
        except Exception as e: st.error(e)

with tab_code:
    c_sql, c_py = st.columns(2)
    with c_sql:
        st.subheader("💻 SQL Studio")
        query = st.text_area("DuckDB SQL", value=f"SELECT * FROM {st.session_state.get('last_table', 'temp_table')} LIMIT 10")
        if st.button("Run SQL"):
            res = requests.post(f"{API_URL}/sql/execute", json={"query": query})
            if res.status_code == 200: st.dataframe(pd.DataFrame(res.json()), use_container_width=True)
            else: st.error(res.json().get('detail'))
    with c_py:
        st.subheader("🐍 Python Scratchpad")
        py_code = st.text_area("Python Code", value="result = df.describe()")
        if st.button("Run Python"):
            if st.session_state['last_table']:
                try:
                    df = pd.DataFrame(requests.get(f"{API_URL}/sql/execute", json={"query": f"SELECT * FROM {st.session_state['last_table']} LIMIT 1000"}).json())
                    local_vars = {"df": df, "pd": pd, "result": None}
                    exec(py_code, {"__builtins__": {}}, local_vars)
                    st.dataframe(local_vars.get("result", df), use_container_width=True)
                except Exception as e: st.error(f"Python Error: {e}")

st.sidebar.markdown("### 💾 Template Library")
tpl_name = st.sidebar.text_input("Save Template As:")
if st.sidebar.button("Save Pipeline"):
    with open(f"{TEMPLATES_DIR}/{tpl_name}.json", "w") as f: json.dump(st.session_state['pipeline'], f)
    st.sidebar.success("Saved!")
saved_tpls = [f for f in os.listdir(TEMPLATES_DIR) if f.endswith('.json')]
if saved_tpls:
    load_name = st.sidebar.selectbox("Load Template", saved_tpls)
    if st.sidebar.button("Load"):
        with open(f"{TEMPLATES_DIR}/{load_name}", "r") as f: st.session_state['pipeline'] = json.load(f)
        st.rerun()"""
}

if __name__ == "__main__":
    for path_str, content in PROJECT_FILES.items():
        p = Path(path_str)
        p.parent.mkdir(parents=True, exist_ok=True)
        # BUG FIXED: Using standard newline character
        with open(p, "w", encoding="utf-8") as f: f.write(content.strip() + "\n") 
    print("✅ Master Architect Built successfully.")