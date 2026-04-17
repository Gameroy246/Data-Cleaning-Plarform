# 🗄️ Local Data Architect

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.103.0+-009688.svg?logo=fastapi)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.31.0+-FF4B4B.svg?logo=streamlit)](https://streamlit.io/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10.0+-FFF000.svg)](https://duckdb.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-the-project">About The Project</a></li>
    <li><a href="#architecture">Architecture</a></li>
    <li><a href="#key-features">Key Features</a></li>
    <li><a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage-guide">Usage Guide</a></li>
    <li><a href="#project-structure">Project Structure</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
  </ol>
</details>

## About The Project

**Local Data Architect** is a high-performance, locally hosted data transformation and ETL (Extract, Transform, Load) pipeline builder. Designed as a lightweight, offline alternative to heavy enterprise tools (like Alteryx or cloud-based pipelines), it empowers analysts and engineers to visually construct complex data workflows, execute them instantly using an in-memory SQL engine, and export the refined datasets—all without leaving their local machine.

By leveraging **DuckDB** for lightning-fast vectorized queries and **FastAPI / Streamlit** for a decoupled frontend-backend architecture, Local Data Architect handles complex demographic records, financial logs, and multi-table relationships with ease.

## Architecture

The application operates on a modern, decoupled dual-server architecture:

1. **Data Engine (Backend):** A headless **FastAPI** server running on port `8000`. It wraps a high-speed **DuckDB** in-memory database. It exposes RESTful endpoints to handle file ingestion (via Pandas/PyArrow fallbacks), parse JSON-based pipeline configurations, execute dynamic SQL transformations, and serve the resulting datasets.
2. **Interactive UI (Frontend):** A reactive **Streamlit** application running on port `8501`. It acts as the client, offering a drag-and-drop-style form interface, a visual step-by-step transformation timeline, and interactive Plotly visualizations.

## Key Features

* ⚡ **Ultra-Fast Processing:** Powered by DuckDB's columnar, vectorized in-memory analytical SQL engine.
* 🧩 **Visual Pipeline Builder:** Sequentially chain data operations (Filter, Join, Aggregate, Deduplicate, Drop Nulls, Regex Extracts) without writing code.
* 🛡️ **Bulletproof Execution:** Advanced error handling ensures the engine gracefully catches empty fields or missing data without crashing.
* 📊 **Split-View Diffing:** Instantly compare your original raw data alongside the transformed output in a side-by-side data grid.
* 📈 **Interactive Analytics:** Auto-generated pivot tables and Plotly distribution histograms based on your live pipeline output.
* 💾 **Template Library:** Save your complex pipeline configurations as JSON templates and reload them instantly for future data drops.
* 💻 **Developer Scratchpad:** Built-in SQL and Python terminals to run ad-hoc queries and scripts directly against the pipeline memory.

## Getting Started

To get a local copy up and running, follow these simple steps.

### Prerequisites

You will need Python 3.11 or higher installed on your system.

### Installation

1. **Clone the repository**

        git clone [https://github.com/YOUR_USERNAME/LocalDataArchitect.git](https://github.com/YOUR_USERNAME/LocalDataArchitect.git)
        cd LocalDataArchitect

2. **Create a Virtual Environment**

        # Windows
        python -m venv venv
        venv\Scripts\activate

        # macOS / Linux
        python3 -m venv venv
        source venv/bin/activate

3. **Install Dependencies**

        pip install -r requirements.txt

## Usage Guide

1. **Start the Application:**
   Run the master build script to boot both the FastAPI backend and Streamlit frontend concurrently.

        python main.py

2. **Access the Dashboard:**
   Open your browser and navigate to `http://localhost:8501`.

3. **Build a Pipeline:**
   * **Step 1:** In the UI, upload a CSV or Excel file and click *Register Files*.
   * **Step 2:** Under *Add Pipeline Step*, select **Source**, choose your file, and click **+ Add**.
   * **Step 3:** Add subsequent transformations (e.g., *Filter Rows*, *Drop Columns*).
   * **Step 4:** Click **Execute Pipeline**.

4. **Export Data:**
   Navigate to the **Split View** tab and click the **Export Results (CSV)** button.

## Project Structure

    LocalDataArchitect/
    ├── main.py                     # Master launcher
    ├── requirements.txt            # Python dependencies
    ├── dataforge/                  # BACKEND ENGINE
    │   ├── api/                    # FastAPI routing and models
    │   ├── core/                   # DuckDB engine config
    │   └── transforms/             # Translates UI nodes to SQL
    ├── ui/                         # FRONTEND CLIENT
    │   └── app.py                  # Streamlit interface
    └── templates/                  # Saved JSON configurations

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are greatly appreciated.

1. Fork the Project
2. Create your Feature Branch
3. Commit your Changes 
4. Push to the Branch 
5. Open a Pull Request

## License

Distributed under the MIT License. See `LICENSE` for more information.
