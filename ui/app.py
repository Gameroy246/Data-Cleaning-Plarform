import streamlit as st, requests, pandas as pd, plotly.express as px, os, json

API_URL = os.getenv("API_URL", "http://localhost:8000")
TEMPLATES_DIR = "./templates"
os.makedirs(TEMPLATES_DIR, exist_ok=True)
st.set_page_config(page_title="Data Architect: Max", layout="wide")
st.markdown("<style>.timeline-node { background: #1f2428; border: 2px solid #58a6ff; border-radius: 6px; padding: 10px; margin: 5px; text-align: center; color: white; box-shadow: 0 4px 6px rgba(0,0,0,0.3);} .node-arrow { display: flex; align-items: center; justify-content: center; font-size: 24px; color: #8b949e; }</style>", unsafe_allow_html=True)

for key in ['pipeline', 'history', 'files']:
    if key not in st.session_state: st.session_state[key] = []
if 'last_table' not in st.session_state: st.session_state['last_table'] = None
if 'raw_table' not in st.session_state: st.session_state['raw_table'] = None

def save_state(): st.session_state['history'].append(list(st.session_state['pipeline']))
def undo():
    if st.session_state['history']: st.session_state['pipeline'] = st.session_state['history'].pop()

st.title("Local Data Architect")
st.markdown("### ⏱️ Visual Transformation Timeline")
if not st.session_state['pipeline']: st.info("Pipeline is empty. Add steps below.")
else:
    cols = st.columns(len(st.session_state['pipeline']) * 2 - 1)
    for i, node in enumerate(st.session_state['pipeline']):
        with cols[i * 2]: st.markdown(f"<div class='timeline-node'><b>Step {i+1}</b><br>{node['type'].upper()}</div>", unsafe_allow_html=True)
        if i < len(st.session_state['pipeline']) - 1:
            with cols[i * 2 + 1]: st.markdown("<div class='node-arrow'>➔</div>", unsafe_allow_html=True)

tab_build, tab_view, tab_pivot, tab_code = st.tabs(["1. Pipeline Builder", "2. Split View", "3. Pivot", "4. Code"])

with tab_build:
    c1, c2 = st.columns([1, 2], gap="large")
    with c1:
        st.subheader("Data Sources")
        uploaded = st.file_uploader("Upload CSV/Excel", accept_multiple_files=True)
        if uploaded and st.button("Register Files"):
            files_data = [("files", (f.name, f.getvalue(), f.type)) for f in uploaded]
            res = requests.post(f"{API_URL}/upload", files=files_data)
            st.session_state['files'] = list(set(st.session_state['files'] + res.json().get("filenames", [])))
            st.rerun()
        if st.session_state['files']:
            for f in st.session_state['files']: st.code(f)

    with c2:
        st.subheader("Add Pipeline Step")
        step_type = st.selectbox("Operation", ["Source", "Filter Rows", "Join Data", "Regex Extractor", "Formula", "Aggregate", "Drop/Fill Nulls", "Deduplicate Rows", "Drop Columns", "Rename Column", "Sort Data"])
        
        with st.form("node_form"):
            config = {}
            if step_type == "Source":
                sel_file = st.selectbox("File", st.session_state['files'] if st.session_state['files'] else ["None"])
                config = {"path": f"./{sel_file}", "format": sel_file.split('.')[-1] if '.' in sel_file else 'csv'}
            elif step_type == "Filter Rows": config['condition'] = st.text_input("SQL Condition (e.g. age > 18)")
            elif step_type == "Join Data":
                config['how'] = st.selectbox("Join", ["INNER", "LEFT"])
                config['on_column'] = st.text_input("Common Column")
            elif step_type == "Regex Extractor":
                c_1, c_2, c_3 = st.columns(3)
                with c_1: config['column'] = st.text_input("Target Col")
                with c_2: config['pattern'] = st.text_input("Regex")
                with c_3: config['new_column'] = st.text_input("New Col")
            elif step_type == "Formula":
                c_name = st.text_input("New Col")
                config['sql'] = f"SELECT *, {st.text_input('Formula')} AS {c_name} FROM {{input}}"
            elif step_type == "Aggregate":
                config['group_by'] = [x.strip() for x in st.text_input("Group By (comma sep)").split(',')]
                config['aggregate_function'] = st.selectbox("Func", ["SUM", "AVG", "MAX", "MIN", "COUNT"])
                config['aggregate_column'] = st.text_input("Target Col")
            elif step_type == "Drop/Fill Nulls":
                strat = st.selectbox("Strategy", ["drop", "fill"])
                config['strategy'] = strat
                config['columns'] = [x.strip() for x in st.text_input("Cols (comma sep)").split(',')]
                if strat == "fill": config['fill_value'] = st.text_input("Fill Value", "Unknown")
            elif step_type == "Drop Columns": config['columns'] = [x.strip() for x in st.text_input("Cols to Drop").split(',')]
            elif step_type == "Rename Column":
                c_1, c_2 = st.columns(2)
                with c_1: config['old_col'] = st.text_input("Old")
                with c_2: config['new_col'] = st.text_input("New")
            elif step_type == "Sort Data":
                c_1, c_2 = st.columns(2)
                with c_1: config['column'] = st.text_input("Column")
                with c_2: config['order'] = st.selectbox("Order", ["ASC", "DESC"])

            col_add, col_undo = st.columns([3, 1])
            with col_add:
                if st.form_submit_button("➕ Add", type="primary"):
                    save_state()
                    node_id = f"{step_type.split()[0].lower()}_{len(st.session_state['pipeline'])}"
                    ntype = "source" if step_type=="Source" else "quarantine" if step_type=="Filter Rows" else "join" if step_type=="Join Data" else "regex_extract" if step_type=="Regex Extractor" else "custom_sql" if step_type=="Formula" else "aggregate" if step_type=="Aggregate" else "null_handling" if step_type=="Drop/Fill Nulls" else "deduplicate" if step_type=="Deduplicate Rows" else "drop_columns" if step_type=="Drop Columns" else "rename_column" if step_type=="Rename Column" else "sort"
                    inputs = []
                    if ntype != "source" and st.session_state['pipeline']:
                        inputs = [st.session_state['pipeline'][-2]['id'], st.session_state['pipeline'][-1]['id']] if ntype == "join" and len(st.session_state['pipeline']) >= 2 else [st.session_state['pipeline'][-1]['id']]
                    st.session_state['pipeline'].append({"id": node_id, "type": ntype, "config": config, "inputs": inputs})
                    st.rerun()

        c_exec, c_rev = st.columns([3, 1])
        with c_exec:
            if st.button("Execute Pipeline", use_container_width=True):
                if st.session_state['pipeline'] and st.session_state['pipeline'][0]['type'] == 'source':
                    src_file = st.session_state['pipeline'][0]['config']['path'].split('/')[-1]
                    raw_res = requests.get(f"{API_URL}/data/{src_file}")
                    if raw_res.status_code == 200: st.session_state['raw_table'] = pd.DataFrame(raw_res.json())
                res = requests.post(f"{API_URL}/pipeline/execute", json={"id": "st_pipe", "nodes": st.session_state['pipeline']})
                if res.status_code == 200:
                    st.session_state['last_table'] = res.json().get('output_table')
                    st.success("Executed!")
                else: st.error(f"Backend Error: {res.json().get('detail', res.text)}")
        with c_rev:
            if st.button("↩ Undo", use_container_width=True): undo(); st.rerun()

with tab_view:
    if st.session_state['last_table'] and st.session_state['raw_table'] is not None:
        c_left, c_right = st.columns(2)
        try:
            trans_df = pd.DataFrame(requests.post(f"{API_URL}/sql/execute", json={"query": f"SELECT * FROM {st.session_state['last_table']} LIMIT 1000"}).json())
            with c_left: 
                st.markdown("**Original Data**")
                st.dataframe(st.session_state['raw_table'])
            with c_right: 
                st.markdown("**Transformed Output**")
                st.dataframe(trans_df)
                st.download_button("📥 Export Results (CSV)", trans_df.to_csv(index=False).encode('utf-8'), "pipeline_output.csv")
        except Exception as e: st.error("Failed to load output table. Pipeline may be empty.")

with tab_pivot:
    if st.session_state['last_table']:
        try:
            df = pd.DataFrame(requests.post(f"{API_URL}/sql/execute", json={"query": f"SELECT * FROM {st.session_state['last_table']} LIMIT 2000"}).json())
            pc1, pc2, pc3 = st.columns(3)
            with pc1: p_row = st.selectbox("Row", df.columns)
            with pc2: p_col = st.selectbox("Col", df.columns)
            with pc3: p_val = st.selectbox("Val", df.columns)
            if st.button("Pivot"): 
                pivot_df = pd.pivot_table(df, values=p_val, index=p_row, columns=p_col, aggfunc='sum').reset_index()
                st.dataframe(pivot_df)
                st.download_button(
                    label="Download Pivot CSV",
                    data=pivot_df.to_csv(index=False).encode('utf-8'),
                    file_name="pivot_summary.csv",
                    mime="text/csv"
                )
        except Exception as e: st.error(e)

with tab_code:
    st.subheader("SQL Studio")
    query = st.text_area("SQL", value=f"SELECT * FROM {st.session_state.get('last_table', 'temp')} LIMIT 10")
    if st.button("Run"): st.dataframe(pd.DataFrame(requests.post(f"{API_URL}/sql/execute", json={"query": query}).json()))

st.sidebar.markdown("Template Library")
tpl_name = st.sidebar.text_input("Save As:")
if st.sidebar.button("Save"):
    with open(f"{TEMPLATES_DIR}/{tpl_name}.json", "w") as f: json.dump(st.session_state['pipeline'], f)
saved = [f for f in os.listdir(TEMPLATES_DIR) if f.endswith('.json')]
if saved:
    lname = st.sidebar.selectbox("Load", saved)
    if st.sidebar.button("Load Tpl"):
        with open(f"{TEMPLATES_DIR}/{lname}", "r") as f: st.session_state['pipeline'] = json.load(f)
        st.rerun()
