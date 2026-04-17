import time, asyncio, shutil
from fastapi import FastAPI, UploadFile, File, HTTPException
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
        preview_engine.register_view(f.filename.split('.')[0], f"./{f.filename}", f.filename.split('.')[-1])
        names.append(f.filename)
    return {"filenames": names}

@app.get("/data/{filename}")
async def get_raw_data(filename: str):
    e = Engine()
    e.register_view("raw", f"./{filename}", filename.split('.')[-1])
    return e.execute_sql("SELECT * FROM raw LIMIT 100").to_dicts()

@app.post("/sql/execute")
async def execute_raw_sql(q: SQLQuery):
    try: return preview_engine.execute_sql(q.query).to_dicts()
    except Exception as e: raise HTTPException(400, str(e))

@app.post("/pipeline/execute", response_model=RunResponse)
async def execute_pipeline(pipe: PipelineConfig):
    start = time.time()
    def _run():
        try:
            eng = Engine(); exec_obj = TransformExecutor(eng); tbls = {}; last = ""
            for n in pipe.nodes:
                if n.type == "source":
                    eng.register_view(n.id, n.config.get("path"), n.config.get("format"))
                    tbls[n.id] = last = n.id
                else: 
                    inputs = [tbls[i] for i in n.inputs if i in tbls]
                    tbls[n.id] = last = exec_obj.execute_transform(n.type, n.config, inputs)
            res = eng.execute_sql(f"SELECT COUNT(*) as c FROM {last}")
            preview_engine.con.register(last, eng.execute_sql(f"SELECT * FROM {last}").to_arrow())
            return last, res['c'][0]
        except Exception as e:
            raise Exception(str(e))
            
    try:
        last, rows = await asyncio.to_thread(_run)
        return RunResponse(status="success", message="Success", rows_processed=rows, duration_sec=time.time()-start, output_table=last)
    except Exception as e: 
        raise HTTPException(400, str(e))
