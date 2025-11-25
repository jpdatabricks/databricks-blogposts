# Sales Support Multi-Agent Framework

This repository provides a comprehensive solution for building a sophisticated sales support system on Databricks, utilizing a multi-agent architecture powered by large language models (LLMs), specialized tools, **LangChain**, and **GEPA Prompt Optimization**.

This system combines insights from **structured sales metrics** (via the **GenieAgent**) and **unstructured customer data** (via the **RAGAgent**), routing queries to the appropriate tools and synthesizing a final answer.

---

## 🚀 Project Overview

The core goal is to create a multi-agent system capable of:
* **Answering quantitative questions** (e.g., "What was our win rate in the West region last quarter?") by querying structured data tables.
* **Answering qualitative questions** (e.g., "What were the main objections raised by a specific customer?") by searching unstructured documents.
* **Synthesizing answers** that require both data sources (e.g., "Is our largest deal at risk, given their latest feedback?").

This framework uses Databricks Unity Catalog, Delta Lake, Vector Search, and the integrated LLM platform to manage data and deploy the solution efficiently.

---

## 🧠 Multi-Agent Architecture

The solution uses a **LangGraph** workflow managed by a **Supervisor Agent** that orchestrates specialized agents.

| Agent | Role | Data Source | Primary Use Cases |
| :--- | :--- | :--- | :--- |
| **Supervisor Agent** | Routes queries to the best agent(s) and synthesizes the final answer. | N/A | Orchestration, complex query breakdown, final response generation. |
| **GenieAgent (Structured Data)** | Queries structured sales data for metrics and analytics. | Delta Tables (`sales_opportunities`, `customer_accounts`, `sales_reps`, `sales_activities`). | Pipeline value, win rate, quota attainment, deal counts, revenue, rep performance. |
| **RAGAgent (Unstructured Data)** | Retrieves context and qualitative information from documents. | Vector Search Indexes (Emails, Meeting Notes, Customer Feedback). | Customer sentiment, pricing discussions, objections, agreed action items, product feedback. |

---

## 🛠️ Data & Technology Stack

The framework is based entirely on the Databricks Lakehouse Platform.

### Data Overview
The project uses synthetic **sales and CRM data** for the Americas (US) region.

* **Structured Data (Delta Tables):**
    * `sales_reps`
    * `customer_accounts`
    * `sales_opportunities`
    * `sales_activities`
* **Unstructured Data (Unity Catalog Volume):**
    * Customer Feedback PDFs
    * Meeting Notes PDFs
    * Email Communications PDFs

### Databricks Technologies Used
* **Unity Catalog (UC)**: Governs Delta tables, MLflow models (for the Supervisor prompt), and unstructured data volumes.
* **Delta Lake**: Stores all structured sales and CRM data tables.
* **Vector Search**: Used to build searchable indexes over the unstructured PDF data after parsing.
* **Genie Agent**: Provides a powerful Natural Language to SQL interface for the structured data component.
* **LLMs**: Utilizes hosted foundation models (e.g., Databricks-GPT-5-1, Databricks-Claude-Sonnet-4-5) for reasoning, routing, and synthesis.

---

## 🧑‍💻 Setup and Walkthrough

Follow the notebooks in sequential order to set up the entire environment and deploy the agent.

1.  **`00-init-requirements.ipynb`**: **Initialization**
    * Installs necessary Python libraries (`reportlab`, `mlflow`, `langgraph`, `databricks-langchain`, etc.).
    * Sets up a unique **Unity Catalog (UC) Catalog**, **Schema**, and **Volume** for the project data.
2.  **`01-create-synthetic-sales-data.ipynb`**: **Data Generation**
    * Generates and writes structured tables (`sales_reps`, `customer_accounts`, `sales_opportunities`, `sales_activities`) to Delta tables.
    * Generates and uploads sample **PDF documents** (feedback, notes, emails) to the UC Volume for unstructured data analysis.
3.  **`02-create-vector-index.ipynb`**: **Vector Search Setup**
    * Uses Databricks' `ai_parse_document` function to extract text content from the PDF documents.
    * Creates a **Vector Search Endpoint** and three dedicated **Vector Search Indexes** (`email_communications_index`, `meeting_notes_index`, `customer_feedback_index`) in Unity Catalog for the unstructured data.
4.  **`03-create-multi-agent-with-genie-rag-optimized.ipynb`**: **Agent Deployment**
    * Creates the **GenieAgent** and **RAGAgent** tools.
    * Defines the **Supervisor Agent's prompt** and **LangGraph workflow** to orchestrate the two tools.
    * Optimizes the Supervisor's prompt using **GEPA** (Generative AI Prompt Optimizer) for better performance.
    * The compiled multi-agent system is now ready to be tested and used.
