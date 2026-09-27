# 🏦 Bank Statement Analyzer

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Production--Ready-success)
![Tests](https://img.shields.io/badge/Tests-Pytest-informational)

------------------------------------------------------------------------

## 📌 Overview

**Bank Statement Analyzer** is an enterprise-grade financial analytics
engine designed to process **PDF and JSON bank statements**, extract
transactional data, and generate structured financial insights.

Built for **fintech platforms, digital lenders, credit risk teams, and
financial institutions, specifically in Nigeria**, this system automates income verification,
spending analysis, and behavioral risk assessment.

------------------------------------------------------------------------

## 🚀 Core Capabilities

### 📂 Multi-Format Input Support

-   PDF bank statements
-   Structured JSON transaction data
-   Extensible parser architecture

Key computed metrics include:

-   Total inflow & outflow
-   Average monthly income
-   Net cash flow
-   Expense-to-income ratio
-   Salary detection logic
-   Loan repayment detection
-   Gambling behavior detection
-   Transaction frequency
-   Income consistency score
-   Account balance trend analysis
-   Behavioral financial scoring

------------------------------------------------------------------------

## 🏗️ System Architecture

    Input Layer (PDF / JSON)
            │
            ▼
    Document Parsing Engine
            │
            ▼
    Data Cleaning & Normalization
            │
            ▼
    Feature Engineering
            │
            ▼
    Financial Analysis Engine
            │
            ▼
    Structured Output (JSON / API Response/Excel)

Designed with modular components to support scaling and microservice
deployment.

------------------------------------------------------------------------

## Model Deployment

### Prerequisites

- Python 3.10-3.12
- Pip (Python package installer)
-   Pandas
-   NumPy
-   PDF Parsing Libraries (pdfplumber)
-   Flask (API layer)

### Setup

1. **Clone the repository:**

    ```sh
    git clone https://github.com/rojuadeyemi/statementanalyzer.git
    cd statementanalyzer
    ```

2. **Create a virtual environment:**

Create a virtual environment using.

For *Linux/Mac*:

```sh
python -m venv .venv
source .venv/bin/activate 
```

For *Windows*:
    
```sh
python -m venv .venv
.venv\Scripts\activate
```

3. **Install the required dependencies:**

```bash
pip install -e .            # library + CLI
pip install -e ".[app]"     # + the Streamlit interface
pip install -e ".[llm]"     # + the optional AI fallback
```

### Deploy the Application Locally

```python
from statement_analyzer import StatementAnalyzer, write_excel, build_json

a = StatementAnalyzer.from_source("statement.pdf")   # or a dict / JSON string / .json path
print(a.risk_indicators)
print(a.affordability.as_series())                   # underwriting view
write_excel(a, "reports/statement.xlsx")             # or write_excel(a) -> BytesIO for APIs
report = build_json(a)                               # plain dict, ready for FastAPI/Django
```

CLI:

```bash
python -m statement_analyzer analyze statement.pdf --excel out.xlsx --json out.json
python -m statement_analyzer check ./sample_statements/   # extraction quality per file
```

Streamlit app:

```bash
streamlit run app.py
```

Then open your web browser and navigate to http://127.0.0.1:5000 to access the aplication.

Upload one or more statements, review extraction quality, the risk summary and the
affordability view, then download the Excel or JSON report. Every threshold in the
sidebar maps to a field on `Settings`, so what you tune in the UI is what you pass
in code.



## AI fallback and the API key

```python
from statement_analyzer import Settings, load_statement

s = load_statement("statement.pdf", settings=Settings(enable_llm=True, llm_api_key="sk-ant-..."))
print(s.quality.warnings)   # e.g. "LLM fallback skipped: no API key: set ANTHROPIC_API_KEY, ..."
```

Environment variables (`ANTHROPIC_API_KEY`, `STATEMENT_ENABLE_LLM`,
`STATEMENT_LLM_MODEL`) are read when `Settings()` is constructed. In the
Streamlit app, paste the key in the sidebar; it tells you up front whether the
fallback is usable.


## Adding a bank

```python
# parsers/banks.py
KUDA = LineSpec(
    name="Kuda",
    detector=contains(r"kuda\.com", "last"),
    patterns=[rf"^(?P<date>\d{{2}}/\d{{2}}/\d{{2}})\s+(?P<narration>.+?)\s+(?P<amount>{AMT})\s+(?P<balance>{AMT})$"],
    date_formats=["%d-%m-%y"],
)
BANK_PARSERS.insert(0, KUDA)
```

Many banks need no spec at all: if the statement has a readable header row, the generic column parser handles it. Run `check` on a folder of samples and only write a spec for files that come back with a low reconciliation rate.

## Tests

```bash
pip install -e ".[dev]" && pytest
```

Try it out [analyzerplus](https://statement-analyzer-pro.streamlit.app/)



