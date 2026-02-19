# 🏦 Bank Statement Analyzer

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Docker](https://img.shields.io/badge/Docker-Supported-blue)
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

    ```sh
    pip install -U pip
    pip install -r requirements.txt
    ```

### Deploy the Application Locally

A Flask-based web application was developed to deploy the analyzer, providing an interface for users to upload a statement and receive an output in Excel.

To start the *Flask development server* locally, you can also use the following commands:

For *Linux/Mac*:
```sh
export FLASK_APP=app.py
export FLASK_ENV=development
export FLASK_DEBUG=1
flask run   
```
For *Windows*:
```sh
set FLASK_APP=app.py
set FLASK_ENV=development
set FLASK_DEBUG=1
flask run
```

Then open your web browser and navigate to http://127.0.0.1:5000 to access the aplication.

See [reports](/reports/) for sample outputs



