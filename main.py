import subprocess
import sys
import time

def main():
    print("STARTING DATA ARCHITECT")

    print("[1/2] Booting Python Data Engine")
    api_proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "dataforge.api.main:app", "--host", "127.0.0.1", "--port", "8000"])
    
    time.sleep(3)

    print("[2/2] Booting Streamlit UI")
    ui_proc = subprocess.Popen([sys.executable, "-m", "streamlit", "run", "ui/app.py"])

    try:
        ui_proc.wait()
    except KeyboardInterrupt:
        print("\nShutting down servers...")
    finally:
        api_proc.terminate()
        ui_proc.terminate()
        print("Servers shut down.")

if __name__ == "__main__":
    main()