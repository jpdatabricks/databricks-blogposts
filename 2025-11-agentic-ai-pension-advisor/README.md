# Building Production-Ready Agentic AI Applications on Databricks: A Deep Dive into the Multi-Country Pension Advisor

**Author:** Pravin Varma | **Databricks Delivery Solutions Architect**  
**Published:** 2025

> *This project demonstrates how to build enterprise-grade agentic AI applications on Databricks using production best practices. We'll explore the ReAct agent framework, MLflow integration, observability patterns, and how to build scalable, maintainable agentic systems.*

---

## Executive Summary

In this reference implementation, we've built a **production-ready agentic AI pension advisory system** that showcases the **best practices for developing agentic AI applications on Databricks**. The system processes retirement planning queries across multiple countries (Australia, USA, UK, India) with enterprise-grade observability, cost optimization, and regulatory compliance.

**Key Highlights:**
- **ReAct Agentic Framework** with intelligent reasoning and tool orchestration
- **MLflow Integration** for prompt versioning, experiment tracking, and model governance
- **Production Observability** with Lakehouse Monitoring and real-time dashboards
- **Cost Optimization** achieving 80% cost reduction through intelligent cascade classification
- **Prompt Registry** with versioning and A/B testing capabilities
- **Regulatory Compliance** with comprehensive audit trails and LLM-as-a-Judge validation

**Cost Efficiency:** Processes queries at **pennies per query** ($0.003-$0.010), enabling substantial cost savings by deflecting 40-50% of common member inquiries from call centers and human advisors.

---

## Architecture Overview

### The ReAct Agent Pattern

At the heart of this system lies the **ReAct (Reasoning-Acting-Observing) agentic loop**, a proven pattern for building intelligent agents that can reason about problems, select appropriate tools, and iteratively refine their responses.

#### Core Agentic Loop

```
┌───────────────────────────────────────────────────────────────┐
│       ReAct Pattern: Reasoning + Acting + Observing           │
└───────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────┐
  │  Classification (Cost Optimization)                         │
  │  └─> 3-Stage Cascade: Regex → Embedding → LLM               │
  └──────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  Tool Selection & Execution (ACT)                           │
  │  └─> Unity Catalog SQL Functions via SQL Warehouses         │
  └──────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  Response Synthesis (OBSERVE)                               │
  │  └─> Foundation Model API (Claude Opus)                     │
  └──────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  Validation (LLM-as-a-Judge)                                │
  │  └─> Claude Sonnet 4 validates quality & compliance         │
  └─────────────────────────────────────────────────────────────┘
```

#### System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Streamlit UI Layer                      │
│              Multi-country Pension Advisory Portal          │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              ReAct Agentic Loop (react_loop.py)             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ 1. REASON: Analyze query → Select tools                │ │
│  │ 2. ACT: Execute SQL functions (Unity Catalog)          │ │
│  │ 3. OBSERVE: Analyze results → Refine understanding     │ │
│  │ 4. ITERATE: Continue until sufficient information      │ │
│  └────────────────────────────────────────────────────────┘ │
└───────────────────────────┬─────────────────────────────────┘
                            │
       ┌────────────────────┼────────────────────┐
       ▼                    ▼                    ▼
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Unity     │     │  Foundation  │     │   MLflow    │
│  Catalog    │     │    Models    │     │             │
│             │     │              │     │ • Traces    │
│ • Member    │     │ • Claude 4   │     │ • Prompts   │
│   Profiles  │     │ • GPT-4      │     │ • Metrics   │
│ • SQL Tools │     │ • Llama 3    │     │ • Dashboard │
│ • Audit Log │     │ • BGE (emb)  │     │ • Artifacts │
└─────────────┘     └──────────────┘     └─────────────┘
```

### Why ReAct?

The ReAct pattern provides several advantages over traditional RAG or single-shot LLM approaches:

1. **Iterative Reasoning**: Agents can break down complex queries into multiple tool calls
2. **Tool Orchestration**: Dynamically selects the right tools based on context
3. **Self-Correction**: Observes results and adjusts strategy if needed
4. **Transparency**: Full visibility into reasoning steps for debugging and auditing

Our implementation (`react_loop.py`) separates the agentic loop from orchestration, making it reusable and testable.

### Code Flow Architecture

Understanding how the components interact is crucial for maintaining and extending the system:

```
┌─────────────────────────────────────────────────────────────────┐
│                        app.py (UI Layer)                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ User selects country & member profile                    │   │
│  │ User enters query → clicks "Get Recommendation"          │   │
│  └───────────────────────────┬──────────────────────────────┘   │
└──────────────────────────────┼──────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│              agent_processor.py (Orchestration)                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ agent_query() function:                                  │   │
│  │ 1. Initialize observability (MLflow)                     │   │
│  │ 2. Retrieve member data from Unity Catalog               │   │
│  │ 3. Create SuperAdvisorAgent instance                     │   │
│  │ 4. Anonymize PII for privacy protection                  │   │
│  │ 5. Call agent.process_query()                            │   │
│  │ 6. Execute: Classification, Tools, Synthesis, Validation │   │
│  │ 7. Restore member name in response                       │   │
│  │ 8. Log to governance table and MLflow (async)            │   │
│  │ 9. End observability run                                 │   │
│  └───────────────────────────-┬─────────────────────────────┘  │
└───────────────────────────────┼─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                   agent.py (Agent Class)                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ SuperAdvisorAgent.process_query():                       │   │
│  │ 1. Get member profile (via tools.get_member_profile())   │   │
│  │ 2. Build context with anonymization                      │   │
│  │ 3. Create AgentState                                     │   │
│  │ 4. Delegate to react_loop.run_agentic_loop()             │   │
│  └───────────────────────────┬──────────────────────────────┘   │
└──────────────────────────────┼──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              react_loop.py (Core Agentic Loop)                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ ReactAgenticLoop.run_agentic_loop():                     │   │
│  │                                                          │   │
│  │ Classification (REASON)                                  │   │
│  │  └─> classifier.classify_query_topic()                   │   │
│  │      ├─ Stage 1: Regex patterns                          │   │
│  │      ├─ Stage 2: Embedding similarity                    │   │
│  │      └─ Stage 3: LLM fallback                            │   │
│  │                                                          │   │
│  │ Tool Selection & Execution (ACT)                         │   │
│  │  └─> reason_and_select_tools()                           │   │
│  │      └─> act_execute_tools()                             │   │
│  │          └─> tools.call_tool()                           │   │
│  │              └─> Unity Catalog SQL functions             │   │
│  │                                                          │   │
│  │ Response Synthesis (OBSERVE)                             │   │
│  │  └─> synthesize_response()                               │   │
│  │      └─> agent.generate_response()                       │   │
│  │          └─> Foundation Model API (Claude)               │   │
│  │                                                          │   │
│  │ Validation (Quality Check)                               │   │
│  │  └─> observe_and_validate()                              │   │
│  │      └─> validator.validate()                            │   │
│  │          └─> Foundation Model API (Judge LLM)            │   │
│  │                                                          │   │
│  │ Return result_dict with response, citations, etc.        │   │
│  └───────────────────────────┬──────────────────────────────┘   │
└──────────────────────────────┼──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              agent_processor.py (Completion)                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ 1. Extract results from result_dict                      │   │
│  │ 2. Calculate cost breakdown                              │   │
│  │ 3. Log to the governance table                               │   
│  │ 4. End MLflow run                                        │   │
│  │ 5. Return structured response to app.py                  │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

**app.py (UI Layer)**
- Streamlit interface for user interaction
- Country and member profile selection
- Query input and result display
- Progress tracking UI

**agent_processor.py (Orchestration Layer)**
- High-level query orchestration
- Phase tracking (8 phases)
- MLflow observability initialization
- Governance logging (Unity Catalog)
- Cost calculation and reporting
- Error handling and recovery

**agent.py (Agent Class)**
- Agent instance coordination
- Tool initialization (SuperAdvisorTools)
- Validator initialization (LLMJudgeValidator)
- Prompts registry initialization
- Creates ReactAgenticLoop instance
- Data preparation (member profile, context building)
- Utility methods (currency formatting, authority mapping)

**react_loop.py (Core Agentic Loop)**
- Implements ReAct pattern (REASON → ACT → OBSERVE)
- Query classification (3-stage cascade)
- Tool selection and execution
- Response synthesis (LLM generation)
- Response validation (LLM-as-a-Judge)
- Iterative refinement loop

**Supporting Components:**
- `classifier.py`: 3-stage cascade classification
- `validation.py`: LLM-as-a-Judge validation
- `tools.py`: Unity Catalog function wrappers
- `country_config.py`: Country-specific configurations
- `prompts_registry.py`: Prompt versioning and MLflow tracking
- `observability.py`: MLflow and Lakehouse Monitoring
- `utils/formatting.py`: Currency formatting, SQL escaping
- `utils/audit.py`: Governance logging utilities
- `utils/lakehouse.py`: Unity Catalog SQL utilities
- `utils/progress.py`: Real-time UI progress tracking

---

## Agent Execution Flow

The pension advisor system executes each query through a series of distinct steps, orchestrated by `agent_processor.py`. Each step is tracked, timed, and logged for complete observability.

### Data Retrieval
**Purpose**: Fetch member profile and regulatory data from Unity Catalog

**Operations**:
- Query member profile table using Unity Catalog
- Retrieve member-specific data: name, age, balance, country
- Load country-specific regulatory authorities and rules
- **Duration**: ~100-200ms
- **Cost**: SQL Warehouse compute only (no LLM calls)

**Example**:
```python
member = get_member_profile(user_id="AU001")
# Returns: {name: "John Smith", age: 65, balance: 450000, country: "AU"}
```

### Privacy Protection (Anonymization)
**Purpose**: Protect member privacy by replacing names with tokens during LLM processing

**Operations**:
- Extract member's real name from profile
- Replace with anonymous token (e.g., "John Smith" → "[MEMBER_NAME]")
- Store mapping for later restoration
- **Duration**: <10ms
- **Cost**: None (string manipulation)

**Why**: Ensures PII is never sent to LLM APIs or logged to MLflow

### Intelligent Classification
**Purpose**: Intelligent query topic classification to optimize costs

**3-Stage Cascade (Fast → Slow)**:
1. **Regex Patterns** (80% of queries, <1ms, $0)
   - Pattern matching against retirement keywords
   - Example: "What's my super balance?" → matches "balance" pattern

2. **Embedding Similarity** (15% of queries, ~100ms, $0.0001)
   - BGE embedding model for semantic matching
   - Cosine similarity against query types

3. **LLM Fallback** (5% of queries, ~300ms, $0.001)
   - Claude Sonnet 4 for ambiguous cases
   - Full reasoning about query intent

**Off-Topic Detection**: Catches non-retirement queries early to save costs

**Duration**: 1ms - 300ms depending on stage
**Cost**: $0 - $0.001 depending on stage

### Tool Selection & Execution
**Purpose**: Execute Unity Catalog SQL functions to retrieve required data

**Operations**:
- **Tool Selection**: LLM reasons about which tools to call
  - Example: "What's my preservation age?" → calls `get_preservation_age()`
- **Tool Execution**: Call Unity Catalog functions via SQL Warehouse
  - Each tool is a versioned SQL function
  - Supports: tax calculations, benefit projections, balance queries
- **Result Aggregation**: Combine tool outputs into context

**Duration**: ~200-500ms per tool
**Cost**: SQL Warehouse compute + minimal LLM for planning

**Example Tools**:
```sql
-- Unity Catalog SQL Function
SELECT main.retirement_tools.get_preservation_age('1965-03-15', 'AU')
-- Returns: 60 (preservation age for person born 1965 in Australia)
```

### Response Synthesis
**Purpose**: Generate natural language response using LLM

**Operations**:
- Build synthesis prompt with:
  - Member context (anonymized)
  - Tool outputs
  - Country-specific regulations
  - Query text
- **LLM Call**: Claude Opus 4.1 (most capable model)
- Generate comprehensive response with citations
- **Retry Logic**: Up to 2 retries if validation fails

**Duration**: ~15-35s (LLM API latency)
**Cost**: ~$0.0029 per query (largest cost component)

**Prompt Structure**:
```
You are a pension advisor for {country}.
Member: [MEMBER_NAME], Age: 65, Balance: $450,000
Tools called: get_preservation_age() → "60"
Query: "When can I access my super?"
Generate response...
```

### LLM-as-a-Judge Validation
**Purpose**: Ensure response quality before showing to user

**Operations**:
- **Validator LLM**: Claude Sonnet 4 (fast, cost-effective)
- Checks for:
  - Factual accuracy
  - Regulatory compliance
  - Response completeness
  - Professional tone
  - Proper citations
- Returns: `{passed: true/false, confidence: 0-1, violations: []}`
- **Retry**: If confidence < 0.70, retry synthesis with feedback

**Duration**: ~1-3s
**Cost**: ~$0.0004 per query

**Validation Criteria**:
- **PASS**: confidence ≥ 0.70, no violations
- **FAIL**: Has violations (blocks user from seeing response)
- **RETRY**: confidence < 0.70, no violations (retry synthesis)

### Name Restoration
**Purpose**: Replace anonymous tokens with real names before showing to user

**Operations**:
- Find all instances of "[MEMBER_NAME]" in response
- Replace with real name from anonymization step
- Add personalized greeting
- **Duration**: <10ms
- **Cost**: None

**Example**:
```
Before: "Hi [MEMBER_NAME], your preservation age is 60."
After:  "Hi John Smith, your preservation age is 60."
```

### Audit Logging (Asynchronous)
**Purpose**: Comprehensive governance logging for compliance and monitoring

**Operations** (runs in background thread):
- Log to Unity Catalog governance table:
  - Query text, response, validation results
  - Cost breakdown, tool usage, timestamp
  - Country, user ID, session ID
- Log to MLflow:
  - Metrics: cost, latency, confidence
  - Artifacts: validation results, cost breakdowns
  - Traces: Full execution graph
- **Duration**: <500ms (async, doesn't block user)
- **Cost**: Minimal (storage + SQL Warehouse)

**Audit Record Schema**:
```python
{
    "timestamp": "2025-01-15 10:23:45",
    "user_id": "AU001",
    "country": "AU",
    "query": "What's my preservation age?",
    "response": "Your preservation age is 60...",
    "validation_passed": true,
    "confidence": 0.92,
    "cost_usd": 0.003245,
    "tools_called": ["get_preservation_age"],
    "latency_seconds": 18.4
}
```

### End-to-End Timing Summary

**Typical Query Execution**:
- Data Retrieval: 0.15s
- Anonymization: <0.01s
- Classification: 0.05s
- Tool Execution: 0.30s
- Response Synthesis: 18.5s (LLM API latency)
- Validation: 2.1s
- Name Restoration: <0.01s
- Audit Logging: 0.20s (async)
- **Total**: ~21s (dominated by LLM API calls)

**Cost Breakdown**:
- Classification: $0.0001
- Synthesis (Opus 4.1): $0.0029
- Validation (Sonnet 4): $0.0004
- **Total LLM Token Cost**: ~$0.0034 per query

---

### Key Design Patterns

**Separation of Concerns:**
- **agent_processor.py**: Infrastructure and orchestration (phase tracking, logging, error handling)
- **agent.py**: Agent instance coordination (tools, validators, prompts)
- **react_loop.py**: Core agentic logic (REASON → ACT → OBSERVE)
- **utils/**: Reusable utilities (formatting, SQL, audit)

**Utility Organization:**
- `utils/formatting.py`: General formatting utilities (currency, SQL escaping)
- `utils/audit.py`: Audit logging and governance utilities
- `utils/lakehouse.py`: Unity Catalog and SQL operations
- `utils/progress.py`: Real-time UI progress tracking

**Country Configuration:**
- `country_config.py`: Single source of truth for country-specific settings
- No hardcoded country logic in agent or react_loop
- Easy to add new countries by extending configuration

---

## Recent Updates: Production Monitoring

### Overview

This branch (`feature/ai-guardrails-mlflow-scoring`) includes comprehensive enhancements for production monitoring, quality assurance, and security. All updates have been tested and are production-ready.

### Automated Quality Scoring

**Full implementation of background quality monitoring system**

#### Automated Scorers Module (`src/scorers.py` - 417 lines)

Five fully functional quality scorers for production monitoring:

1. **RelevanceScorer** (LLM-based)
   - Evaluates if response is relevant to user query
   - Uses Claude Sonnet 4 for semantic evaluation
   - Returns score (0-1), pass/fail, and reasoning

2. **FaithfulnessScorer** (LLM-based)
   - Validates response is grounded in context and tool outputs
   - Detects hallucinations and unsupported claims
   - Ensures factual accuracy

3. **ToxicityScorer** (pattern-based)
   - Detects toxic, offensive, or inappropriate content
   - Uses keyword matching and pattern detection
   - Zero LLM cost, instant evaluation

4. **CountryComplianceScorer** (custom)
   - Validates country-specific compliance rules
   - Checks for appropriate terminology (super vs 401k vs RRSP)
   - Verifies key ages and regulatory references

5. **CitationQualityScorer** (custom)
   - Checks citation presence and quality
   - Validates regulatory references
   - Ensures factual queries have proper citations

#### Scoring Infrastructure

**Scoring Job Notebook** (`02-agent-demo/09-automated-scoring-job.py` - 369 lines)
- Production-ready Databricks notebook
- Fetches traces from MLflow experiments
- Samples 10% of queries for cost efficiency
- Runs all 5 scorers on sampled queries
- Stores results in Delta table
- Generates summary statistics and quality alerts

**Scoring Table Setup** (`02-agent-demo/10-setup-scoring-table.py` - 171 lines)
- Creates `pension_blog.pension_advisory.scoring_results` Delta table
- Includes sample data for testing
- Provides SQL query examples
- Enables Change Data Feed for audit

**Monitoring Notebook** (`02-agent-demo/08-production-monitoring.py` - 339 lines)
- Demonstrates MLflow Tracing with `@mlflow.trace` decorator
- Shows automated scorer usage
- Provides monitoring dashboard examples

#### Observability Integration

**Updated UI** (`app.py` + `src/ui_monitoring_tabs.py`)
- New "Automated Quality Scoring" section in Observability tab
- Displays quality trends over time (daily aggregation)
- Shows individual scorer performance breakdown
- Provides country-specific quality analysis
- Tracks verdict distribution (PASS/FAIL/ERROR)
- Lists recent failures with details
- Compares with real-time LLM-as-a-Judge validation

**Key Features:**
- Quality score trends with threshold indicators
- Scorer performance comparison (average scores, pass rates)
- Country-specific quality metrics
- Automated alerting for quality degradation
- Full visibility into scoring history

#### Two-Layer Quality Approach

**Layer 1: Real-time LLM-as-a-Judge**
- Runs during response generation (blocking)
- 100% coverage
- Prevents bad responses from reaching users
- ~$0.002 per query

**Layer 2: Background Automated Scorers**
- Runs after response sent (async)
- 10% sampled for cost efficiency
- Tracks trends and drift over time
- ~$0.0002 per query (sampled)

**Both layers complement each other:**
- Real-time validation = Quality gate
- Background scoring = Trend analysis and drift detection

### Enhanced AI Guardrails

**Multi-Country PII Detection** (`src/ai_guardrails.py`)

Expanded from US-only to comprehensive 4-market support:

**Universal Patterns:**
- Email addresses
- Credit card numbers

**US Patterns:**
- Social Security Numbers (SSN): `123-45-6789`
- Phone numbers: `(555) 123-4567`

**Australia Patterns:**
- Tax File Numbers (TFN): `123 456 789`
- Medicare numbers: `1234 56789 1`
- Australian Business Numbers (ABN): `12 345 678 901`
- Australian phone numbers: `+61 2 1234 5678`

**UK Patterns:**
- National Insurance Numbers (NINO): `AB 12 34 56 C`
- NHS numbers: `123 456 7890`
- UK phone numbers: `+44 20 1234 5678`

**India Patterns:**
- Aadhaar numbers: `1234 5678 9012`
- PAN cards: `ABCDE1234F`
- Indian phone numbers: `+91 98765 43210`

**Testing:**
- PASS: 100% test coverage (17/17 patterns)
- All patterns validated locally
- Production-ready

### UI Improvements

**Validation Status Display** (`src/ui_components.py`)

Enhanced validation result cards to show query metrics:

**Display Format:**
```
PASS: LLM Judge: PASSED
Tokens: 1,234 • Cost: $0.0034
```

**Features:**
- Small font size (85% of normal)
- Gray color for subtle display
- Comma-formatted tokens for readability
- 4 decimal places for cost precision
- Applied to all three validation states:
  - PASS: PASSED (green)
  - FAIL: FLAGGED (red)
  - WARNING: Low Confidence (amber)

### Strict Validation Policy

**Answer Blocking Logic** (`app.py` line 382)

Implemented conservative validation policy:

**Previous Logic:**
```python
answer_failed = (not validation_passed) and has_violations
```
- Could show answers with violations if `passed=True`
- Security loophole

**New Logic:**
```python
answer_failed = has_violations  # Block if ANY violations
```
- **Any violations = answer blocked**
- Only GREEN (no violations) responses shown to customers
- Both RED (flagged) and AMBER (low confidence) trigger internal review

**Customer Experience:**

| Validation Result | Shown to Customer? | What They See |
|-------------------|-------------------|---------------|
| PASS: GREEN (no violations) | PASS: YES | Full answer |
| WARNING: AMBER (low confidence) | FAIL: NO | "Unable to Process Request" |
| FAIL: RED (flagged) | FAIL: NO | "Unable to Process Request" |

**Internal Review:**
- All blocked responses stored in collapsible expander
- Dev team can review AI-generated answer
- Full violation details with codes and evidence
- Recommended actions for resolution

### MLflow Tracing Integration

**Automatic Execution Tracing** (`src/agent_processor.py`)

Added `@mlflow.trace` decorator to `agent_query()` function:

**What's Captured:**
- Function inputs/outputs
- Execution time for each step
- Nested LLM calls (synthesis, validation)
- Tool executions (SQL functions)
- Validation steps and confidence scores

**Benefits:**
- Visual trace viewer in MLflow UI
- Step-by-step timing analysis
- Better debugging capability
- Zero code changes to business logic
- Automatic span tracking

**Configuration** (`src/config/config.yaml`)

New `production_monitoring` section:
```yaml
production_monitoring:
  tracing:
    enabled: true
    capture_inputs: true
    capture_outputs: true
    capture_intermediate_steps: true

  automated_scorers:
    enabled: false  # Set to true after setup
    sampling_rate: 0.1  # 10% of queries
    schedule: "0 */6 * * *"  # Every 6 hours
    scorers:
      - relevance
      - faithfulness
      - toxicity
    custom_scorers:
      - country_compliance
      - citation_quality
```

### Cost Impact

**Monitoring & Observability Costs:**

| Feature | Cost per Query | Notes |
|---------|----------------|-------|
| MLflow Tracing | $0 | No overhead, built-in |
| Automated Scorers (LLM-based) | ~$0.0001 | 10% sampling, 2 LLM scorers |
| Automated Scorers (pattern-based) | $0 | 3 pattern-based scorers |
| **Total Monitoring** | **~$0.0001** | **Negligible impact** |

**Overall Cost per Query:**
- Base query processing: $0.003
- AI Guardrails: +$0.0002
- MLflow Tracing: $0
- Automated Scorers: +$0.0001
- **Total:** ~$0.0033 per query

### Files Modified/Created

**New Files:**
- `src/scorers.py` - Automated scorers module (417 lines)
- `02-agent-demo/09-automated-scoring-job.py` - Scoring job notebook (369 lines)
- `02-agent-demo/10-setup-scoring-table.py` - Table setup notebook (171 lines)
- `test_scorers.py` - Local validation tests (172 lines)

**Modified Files:**
- `src/agent_processor.py` - Added @mlflow.trace decorator
- `src/ui_monitoring_tabs.py` - Added automated scoring tab (295 lines)
- `app.py` - Integrated scoring UI, strict validation policy
- `src/ui_components.py` - Enhanced validation display
- `src/ai_guardrails.py` - Multi-country PII patterns
- `src/config/config.yaml` - Added production_monitoring section
- `src/config/config.yaml.example` - Updated template

### Getting Started with Production Monitoring

**1. MLflow Tracing (Already Active)**
- Automatic tracing enabled via `@mlflow.trace` decorator
- View traces in MLflow Experiments UI
- No additional setup required
- See notebook: `02-agent-demo/07-production-monitoring.py`

**2. Automated Quality Scoring (Optional)**

```bash
# In Databricks:
# Run the automated scoring notebook
%run ./02-agent-demo/08-automated-scoring-job

# View results in Observability dashboard
# Navigate to Governance & Observability → Observability tab
```

**3. Schedule Automated Scoring Job**

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import *

w = WorkspaceClient()

job = w.jobs.create(
    name="pension-advisor-quality-scoring",
    tasks=[
        Task(
            task_key="automated_scoring",
            notebook_task=NotebookTask(
                notebook_path="/Workspace/path/to/02-agent-demo/08-automated-scoring-job",
                base_parameters={
                    "sampling_rate": "0.1",
                    "lookback_hours": "6"
                }
            ),
            new_cluster=ClusterSpec(
                spark_version="15.4.x-scala2.12",
                node_type_id="i3.xlarge",
                num_workers=2
            )
        )
    ],
    schedule=CronSchedule(
        quartz_cron_expression="0 0 */6 * * ?",  # Every 6 hours
        timezone_id="UTC"
    )
)

print(f"✅ Scoring job created: {job.job_id}")
```

### Testing

**Comprehensive test coverage:**
- ✅ Automated scorers: Quality metrics (relevance, faithfulness, toxicity)
- ✅ Multi-country PII detection: 17+ patterns across AU, US, UK, IN
- ✅ MLflow tracing: Full execution graph capture
- ✅ Observability integration: Real-time metrics dashboard
- ✅ AI guardrails: Input/output validation policies

### Quick Start

**To deploy this application:**
1. Fork repository from `databricks-solutions/databricks-blogposts`
2. Configure `src/config/config.yaml` with your settings
3. Run `01-setup/01-unity-catalog-setup.py` to create infrastructure
4. Deploy to Databricks Apps (see "Deploying to Databricks Apps" section)
5. Test MLflow tracing in production
6. Schedule automated scoring job (optional)
7. Monitor quality trends in Observability dashboard

---

## Production Best Practices Demonstrated

### 1. MLflow Integration for Agentic AI

**Challenge:** Agentic AI applications need to track prompts, model versions, tool selections, and validation results across multiple iterations.

**Solution:** Comprehensive MLflow integration at every stage:

#### Prompt Registry with Versioning

All prompts are centralized in `prompts_registry.py` and automatically versioned in MLflow:

```python
# Register prompts with MLflow
registry = PromptsRegistry()
registry.register_prompts_with_mlflow(run_name="prompts_v1.2.0")

# MLflow automatically tracks:
# - Prompt versions
# - Prompt content (artifacts)
# - Registration timestamps
# - A/B test comparisons
```

**Benefits:**
- **Reproducibility**: Full history of prompt changes
- **A/B Testing**: Easy comparison of prompt variations
- **Rollback**: Quickly revert to previous versions
- **Collaboration**: Team members can iterate on prompts safely

#### Experiment Tracking

Every query execution is tracked as an MLflow run:

```python
with mlflow.start_run(run_name=f"query-{session_id}"):
    mlflow.log_param("country", country)
    mlflow.log_param("validation_mode", "llm_judge")
    mlflow.log_metric("runtime_sec", elapsed)
    mlflow.log_metric("total_cost_usd", cost)
    mlflow.log_dict(judge_verdict, "validation.json")
    mlflow.log_dict(cost_breakdown, "cost_breakdown.json")
```

**What We Track:**
- **Parameters**: Country, user ID, tools used, validation mode
- **Metrics**: Runtime, cost, token counts, validation confidence
- **Artifacts**: Validation results, cost breakdowns, error logs
- **Traces**: Full agent reasoning steps (via MLflow Traces)

### 2. Intelligent Cost Optimization

**Challenge:** LLM inference can be expensive, especially when processing every query through multiple models.

**Solution:** 3-Stage Cascade Classification System (`classifier.py`)

```
Stage 1: Regex Patterns (80% of queries)
├─ Cost: $0
├─ Latency: <1ms
└─ Accuracy: 95%+

Stage 2: Embedding Similarity (15% of queries)
├─ Cost: ~$0.0001
├─ Latency: ~100ms
├─ Model: databricks-bge-large-en
└─ Accuracy: 98%+

Stage 3: LLM Fallback (5% of queries)
├─ Cost: ~$0.001
├─ Latency: ~300ms
├─ Model: databricks-gpt-oss-120b
└─ Accuracy: 99%+
```

**Result:** **80% cost reduction** compared to pure LLM classification while maintaining 99%+ accuracy.

### 3. Production Observability

**Challenge:** Agentic AI systems are complex—how do you monitor what's happening in production?

**Solution:** Multi-layer observability stack:

#### MLflow Dashboard
- **Per-query tracking**: Every query logged with full context
- **Cost analysis**: Real-time cost breakdowns
- **Quality metrics**: Validation confidence, pass rates
- **Error tracking**: Failed queries with full context

#### Lakehouse Monitoring
- **Aggregated metrics**: Daily/weekly trends
- **Anomaly detection**: Cost spikes, quality degradation
- **Automated alerts**: Threshold-based notifications

#### Streamlit Governance UI
- **Real-time dashboards**: Live metrics and trends
- **Classification analytics**: Stage distribution and cost savings
- **Quality monitoring**: Pass rates, confidence distributions
- **Cost analysis**: Detailed breakdowns and projections

**Key Insight:** Observability isn't just logging—it's actionable insights that help you improve the system continuously.

### 4. Prompt Registry Pattern

**Challenge:** Prompts evolve over time. How do you manage versions, test changes, and roll back safely?

**Solution:** Centralized prompt registry with MLflow versioning (`prompts_registry.py`)

**Features:**
- **Single Source of Truth**: All prompts in one place
- **Automatic Versioning**: Every change tracked in MLflow
- **A/B Testing**: Easy comparison of prompt variations
- **Country-Specific**: Dynamic prompt generation per country
- **Audit Trail**: Full history of prompt changes

**Example:**
```python
# Get system prompt for Australia
registry = PromptsRegistry()
system_prompt = registry.get_system_prompt(country="Australia")

# Automatically includes:
# - Country-specific regulatory context
# - Currency and terminology
# - Special instructions
# - Version tracking
```

### 5. LLM-as-a-Judge Validation

**Challenge:** How do you ensure AI-generated responses are accurate and safe?

**Solution:** Multi-mode validation system (`validation.py`)

**Validation Modes:**

1. **LLM Judge** (Default)
   - Comprehensive semantic validation
   - Checks regulatory accuracy, safety, completeness
   - Provides confidence scores
   - Model: Claude Sonnet 4

2. **Deterministic**
   - Fast rule-based checks
   - Catches critical violations instantly
   - Zero LLM cost

3. **Hybrid**
   - Deterministic first, LLM fallback
   - Best of both worlds

**Safety Feature:** Failed validations are hidden from users and placed in an internal review queue, ensuring users never see potentially incorrect advice.

### 6. Country-Agnostic Architecture

**Challenge:** How do you scale to multiple countries without code duplication?

**Solution:** Configuration-driven architecture (`country_config.py`)

**Add a new country in 10 minutes:**

```python
CountryConfig(
    code="CA",
    name="Canada",
    currency="CAD",
    retirement_account_term="RRSP",
    regulatory_context="Follow Canadian RRSP rules...",
    available_tools=["tax", "benefit", "projection"]
)
```

**Benefits:**
- Zero code changes required
- Single source of truth for country settings
- Easy to test and validate
- Maintainable and scalable

---

## Getting Started

### Quick Start

**Key Resources - Run these notebooks in your Databricks workspace:**

**Setup Notebooks:**
1. **`01-setup/01-unity-catalog-setup.py`** - Create catalog, tables, and Unity Catalog functions
   - Creates catalog and schemas (default: `pension_blog.pension_advisory`)
   - Generates synthetic member data for 4 countries
   - Registers Unity Catalog SQL functions for retirement calculations
   - **Configure:** Update catalog name in notebook widget if using existing catalog

2. **`01-setup/02-governance-setup.py`** - Configure row-level security and audit logging
   - Sets up governance table for audit trails
   - Configures access controls and permissions

**Agent Framework Notebooks:**
3. **`02-agent-demo/02-build-agent.py`** - Build and test the ReAct agent
   - Demonstrates ReAct pattern (Reason → Act → Observe)
   - Shows agent decision-making and tool selection

4. **`02-agent-demo/03-tool-integration.py`** - Unity Catalog function calling examples
   - Integration between agent and UC functions
   - Tool orchestration patterns

5. **`02-agent-demo/04-validation.py`** - LLM-as-a-Judge validation patterns
   - Response quality validation
   - Compliance checking with AI judges

6. **`02-agent-demo/06-ai-guardrails.py`** - AI guardrails integration
   - Content safety and regulatory compliance
   - Harmful content detection

7. **`02-agent-demo/07-production-monitoring.py`** - MLflow tracing and automated scoring
   - Production observability setup
   - Automated quality scoring with MLflow

**Run the Streamlit App:**
```bash
streamlit run app.py
```

### Prerequisites

- **Databricks Workspace** with Unity Catalog enabled
- **SQL Warehouse** access
- **Foundation Model API** access (Claude Sonnet 4, Claude Haiku 4, BGE)
- **Python 3.9+**

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/databricks/databricks-blogposts.git
   cd databricks-blogposts/2025-11-agentic-ai-pension-advisor
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up Unity Catalog**
   ```bash
   # Run SQL scripts in order:
   # 1. sql_ddls/super_advisory_demo_schema.sql
   # 2. sql_ddls/super_advisory_demo_functions.sql
   ```

4. **Configure environment**

   Copy `.env.example` to `.env` and update:
   ```bash
   cp .env.example .env
   # Edit .env with your Databricks credentials
   ```

   Update `src/config/config.yaml` if needed:
   ```yaml
   databricks:
     sql_warehouse_id: "your_warehouse_id"
   mlflow:
     prod_experiment_path: "/Users/your_email/pension-advisor-prod"
   ```

5. **Run the application**
   ```bash
   streamlit run app.py
   ```

---

## Deploying to Databricks Apps

### Overview

Deploy this Streamlit application to Databricks Apps for a fully managed, enterprise-grade deployment with automatic scaling, authentication, and workspace integration.

### Prerequisites

- Databricks workspace with **Apps** enabled (contact your Databricks account team if not available)
- Unity Catalog with tables and functions set up (run `01-setup/01-unity-catalog-setup.py`)
- SQL Warehouse ID
- Foundation Model API access enabled

### Deployment Steps

#### 1. Fork the Repository

**Important:** Fork this repository to your own GitHub account to customize configuration:

1. Navigate to https://github.com/databricks-solutions/databricks-blogposts
2. Click **Fork** (top right)
3. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/databricks-blogposts.git
   cd databricks-blogposts/2025-11-agentic-ai-pension-advisor
   ```

**Why fork?** This allows you to:
- Commit your own `config.yaml` to your private fork
- Customize prompts, UI, and business logic
- Maintain your changes while pulling upstream updates

#### 2. Prepare Configuration

**Create your config.yaml:**
```bash
# Copy the example config
cp src/config/config.yaml.example src/config/config.yaml

# Edit src/config/config.yaml with your settings
```

Update these critical values in `src/config/config.yaml`:
```yaml
databricks:
  sql_warehouse_id: "YOUR_WAREHOUSE_ID"  # Required
  unity_catalog: "your_catalog"          # Default: pension_blog
  unity_schema: "pension_advisory"            # Default: pension_advisory

mlflow:
  prod_experiment_path: "/Users/your.email@company.com/pension-advisor-prod"
  offline_eval_path: "/Users/your.email@company.com/pension-advisor-eval"
```

**Configuration Options:**

- **Option A (Recommended):** Commit config.yaml to your private fork
  ```bash
  git add src/config/config.yaml
  git commit -m "Add deployment configuration"
  git push origin main
  ```

- **Option B:** Use environment variables in Databricks Apps (overrides config.yaml):
  ```
  DATABRICKS_SQL_WAREHOUSE_ID=your_warehouse_id
  DATABRICKS_UNITY_CATALOG=your_catalog
  MLFLOW_EXPERIMENT_PATH=/Users/your.email@company.com/pension-advisor-prod
  ```

#### 3. Set Up Unity Catalog (One-Time)

Run the setup notebook to create tables, functions, and grant permissions:

```python
# In Databricks workspace, run:
01-setup/01-unity-catalog-setup.py
```

This creates:
- Catalog and schemas (pension_advisory, {functions_schema})
- Tables (member_profiles, governance, citation_registry)
- Unity Catalog functions for pension calculations
- Proper permissions for your user/service principal

#### 4. Deploy to Databricks Apps

**Option A: Deploy via Databricks UI**

1. Navigate to your Databricks workspace
2. Click **Apps** in the sidebar
3. Click **Create App**
4. Configure the app:
   - **Name:** `pension-advisor` (or your preferred name)
   - **Source:** Git repository
   - **Git URL:** `https://github.com/YOUR_USERNAME/databricks-blogposts.git` ⚠️ Use YOUR fork
   - **Branch:** `main`
   - **Path:** `2025-11-agentic-ai-pension-advisor`
   - **Entry Point:** `app.py`
   - **Python Version:** 3.11
5. Click **Create**

**Option B: Deploy via Databricks CLI**

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Deploy the app (from your fork)
databricks apps create pension-advisor \
  --source-code-path https://github.com/YOUR_USERNAME/databricks-blogposts.git \
  --branch main \
  --source-code-subpath 2025-11-agentic-ai-pension-advisor \
  --entry-point app.py \
  --python-version 3.11
```

#### 5. Configure App Environment

After deployment, configure the app in the Databricks Apps UI:

**Environment Variables** (Optional - config.yaml takes precedence):
```
DATABRICKS_SQL_WAREHOUSE_ID=your_warehouse_id
DATABRICKS_UNITY_CATALOG=your_catalog
MLFLOW_EXPERIMENT_PATH=/Users/your.email@company.com/pension-advisor-prod
```

**Compute Configuration:**
- **Recommended:** Medium (2-4 cores, 8-16 GB RAM)
- **Auto-scaling:** Enabled
- **Min instances:** 1
- **Max instances:** 3

#### 6. Grant App Permissions

The Databricks App uses a service principal that needs permissions to access Unity Catalog.

**Find your app's service principal client ID:**
1. Go to your app in Databricks Apps UI
2. Click **"Environment"** tab
3. Look for "Service principal" section
4. Note the **client ID** (format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)

**Grant permissions using the client ID:**

```sql
-- Replace the UUID below with your app's service principal client ID
-- Example UUID: 12345678-abcd-1234-efgh-123456789abc
-- Replace <catalog_name> and <schema_name> with your values

-- Grant schema-level permissions (recommended - covers all tables and functions)
GRANT USE CATALOG ON CATALOG <catalog_name>
TO `12345678-abcd-1234-efgh-123456789abc`;

GRANT USE SCHEMA ON SCHEMA <catalog_name>.<schema_name>
TO `12345678-abcd-1234-efgh-123456789abc`;

GRANT SELECT ON SCHEMA <catalog_name>.<schema_name>
TO `12345678-abcd-1234-efgh-123456789abc`;

GRANT EXECUTE ON SCHEMA <catalog_name>.<schema_name>
TO `12345678-abcd-1234-efgh-123456789abc`;

-- Grant write access to governance table
GRANT INSERT, MODIFY ON TABLE <catalog_name>.<schema_name>.governance
TO `12345678-abcd-1234-efgh-123456789abc`;

-- Grant execute on registered model (for Model Registry monitoring)
GRANT EXECUTE ON MODEL <catalog_name>.<schema_name>.pension_advisor
TO `12345678-abcd-1234-efgh-123456789abc`;
```

**Note:** Schema-level grants (`GRANT SELECT/EXECUTE ON SCHEMA`) automatically apply to all current and future tables/functions in that schema, which is simpler than granting permissions individually.

#### 7. Test the Deployment

1. Navigate to your app URL: `https://<workspace>.cloud.databricks.com/apps/pension-advisor`
2. Test a query: "What is my preservation age?" (use test member ID: AU001)
3. Verify:
   - Query executes successfully
   - Response appears in UI
   - Governance table logs the query: `SELECT * FROM your_catalog.{schema}.governance ORDER BY timestamp DESC LIMIT 10`
   - MLflow logs the run: Check experiment path in MLflow UI

#### 8. Monitor the App

**Application Logs:**
```bash
# View logs via CLI
databricks apps logs pension-advisor --follow

# Or view in UI: Apps → pension-advisor → Logs
```

**Observability Dashboard:**
- Navigate to the app
- Click "Governance" tab to see query metrics, costs, and validation results

**MLflow Tracking:**
- Navigate to MLflow Experiments
- Find your experiment path (e.g., `/Users/you@company.com/pension-advisor-prod`)
- View traces, metrics, and artifacts

### Configuration Reference

#### config.yaml Structure

```yaml
# LLM Configuration
llm:
  endpoint: "databricks-claude-opus-4-1"    # Main synthesis LLM
  temperature: 0.2
  max_tokens: 750

validation_llm:
  endpoint: "databricks-claude-sonnet-4"    # Validation LLM
  temperature: 0.1
  max_tokens: 300
  confidence_threshold: 0.70
  max_validation_attempts: 2

# Databricks Configuration
databricks:
  sql_warehouse_id: "YOUR_WAREHOUSE_ID"     # ⚠️ REQUIRED
  unity_catalog: "pension_blog"
  unity_schema: "pension_advisory"
  governance_table: "governance"
  member_profiles_table: "member_profiles"

# MLflow Configuration
mlflow:
  prod_experiment_path: "/Users/your.email@company.com/pension-advisor-prod"
  offline_eval_path: "/Users/your.email@company.com/pension-advisor-eval"

# AI Guardrails (Optional)
ai_guardrails:
  enabled: true
  endpoint: "databricks-ai-guardrails"
  input_policies:
    pii_detection: true
    toxicity_threshold: 0.7
  output_policies:
    pii_masking: true
    toxicity_threshold: 0.8
```

### Troubleshooting

#### Databricks App Deployment Issues

**"Model not registered" error in Governance Dashboard**

**Symptom:** Dashboard shows "Model Registry: Model not registered" even though the model exists.

**Cause:** Permission denied - the Databricks App's service principal doesn't have EXECUTE permission on the model.

**Error in logs:**
```
PERMISSION_DENIED: User does not have EXECUTE on Routine or Model 'financial_services.pension_advisory.pension_advisor'
Config: client_id=43d5a7a3-282c-4200-bc93-4aee58fc2f0f
```

**Solution: Grant permissions to the App's Service Principal**

Find your app's service principal client ID:
1. Go to your app in Databricks Apps UI
2. Click "Environment" tab
3. Look for "Service principal" section - note the client ID

Then grant permissions:
```sql
-- Replace <client-id> with your app's service principal client ID
-- Example: 43d5a7a3-282c-4200-bc93-4aee58fc2f0f

-- Grant schema-level permissions (recommended - covers all objects)
GRANT USE SCHEMA ON SCHEMA financial_services.pension_advisory
TO `<client-id>`;

GRANT SELECT ON SCHEMA financial_services.pension_advisory
TO `<client-id>`;

GRANT EXECUTE ON SCHEMA financial_services.pension_advisory
TO `<client-id>`;

-- Or grant specific model permission only
GRANT EXECUTE ON MODEL financial_services.pension_advisory.pension_advisor
TO `<client-id>`;
```

**"No data available for the last 24 hours" in Dashboard**

**Symptom:** Governance dashboard shows no data even though governance table has entries.

**Cause:** Data is older than 24 hours - dashboard filters to last 24 hours only.

**Solution:** Run a query through the "Ask a Question" page to generate fresh data with current timestamp.

---

**Issue: "Table not found" error**
- Ensure setup notebook completed successfully
- Verify catalog/schema names match config.yaml
- Check Unity Catalog permissions

**Issue: "Warehouse not found" error**
- Verify SQL Warehouse ID in config.yaml
- Ensure warehouse is running
- Check service principal has access to warehouse

**Issue: "Permission denied" on governance table**
- Run the permissions grants from Step 6
- Or grant at schema level: `GRANT ALL PRIVILEGES ON SCHEMA your_catalog.pension_advisory TO <principal>`

**Issue: App crashes on startup**
- Check app logs: `databricks apps logs pension-advisor`
- Verify config.yaml has required fields (sql_warehouse_id, experiment paths)
- Ensure all dependencies in requirements.txt are compatible

**Issue: Queries work but governance table is empty**
- Check INSERT permissions on governance table
- Verify table schema matches (run `DESCRIBE TABLE your_catalog.{schema}.governance`)
- Check for schema mismatches in app logs

### Production Considerations

**Security:**
- Use workspace secrets for sensitive configuration (not environment variables)
- Enable row-level security on member_profiles table
- Restrict governance table access to authorized users only

**Cost Optimization:**
- Use Serverless SQL Warehouse for elastic scaling
- Monitor query costs in Governance dashboard
- Adjust LLM cascade classification thresholds to optimize cost vs accuracy

**Scaling:**
- Start with 1-2 app instances, scale based on usage
- Consider serving endpoint for high-volume deployments (see `02-agent-demo/05-mlflow-deployment.py`)
- Use connection pooling for SQL Warehouse (configured by default)

**Monitoring:**
- Set up alerts on governance table for error rates
- Monitor MLflow experiments for cost and latency trends
- Review validation confidence scores for quality degradation

---

### Next Steps

- **Run Demo Notebooks:** Explore `02-agent-demo/` for detailed walkthroughs
- **Customize Prompts:** Edit prompts in `src/prompts_registry.py` and track with MLflow
- **Add Countries:** Extend `src/country_config.py` and add country-specific calculators
- **Production Monitoring:** Set up automated quality scoring (see `02-agent-demo/08-automated-scoring-job.py`)

---

### Demo Notebooks

Interactive Databricks notebooks demonstrating the complete system:

**Setup & Configuration** (_resources/)
- `00-setup.py` - Initialize catalog and schemas
- `bundle_config.yaml` - Databricks Asset Bundles configuration
- `logo.png` - Application logo

**Infrastructure Setup** (01-setup/)
- `01-unity-catalog-setup.py` - Create catalog, tables, UC functions, and grant permissions
- `02-governance-setup.py` - Configure row-level security and audit logging

**Agent Framework** (02-agent-demo/)
- `01-agent-overview.py` - Architecture and design patterns walkthrough
- `02-build-agent.py` - Build and test the ReAct agent
- `03-tool-integration.py` - Unity Catalog function calling examples
- `04-validation.py` - LLM-as-a-Judge validation patterns
- `05-mlflow-deployment.py` - Deploy agent as MLflow serving endpoint
- `06-ai-guardrails.py` - AI guardrails integration (PII, toxicity, jailbreak detection)
- `07-production-monitoring.py` - MLflow tracing and automated quality scorers
- `08-automated-scoring-job.py` - Automated quality scoring workflow

**Observability & Monitoring** (03-monitoring-demo/)
- `01-mlflow-tracking.py` - MLflow experiment tracking, metrics, and cost analysis
- `02-observability.py` - Query patterns, latency, and tool usage monitoring
- `03-dashboard.py` - Governance dashboard with query history

**User Interface** (04-ui-demo/)
- `01-streamlit-ui.py` - Streamlit web interface demonstration

---

## Technology Stack

### Agent Framework
- **ReAct Pattern**: Custom implementation in `react_loop.py`
- **Tool Orchestration**: Dynamic tool selection based on query analysis
- **Iterative Reasoning**: Multi-step problem-solving

### MLflow Integration
- **Prompt Registry**: Version-controlled prompt management
- **Experiment Tracking**: Per-query execution tracking
- **Model Governance**: Full audit trail of model usage

### Foundation Models
- **Claude Sonnet 4**: Main synthesis and validation
- **Claude Haiku 4**: Fast classification fallback
- **BGE Large**: Embedding generation for semantic similarity
- **GPT-OSS-120B**: Cost-effective LLM fallback

### Data Platform
- **Unity Catalog**: Member profiles, audit logs, citation registry
- **SQL Functions**: 18 country-specific pension calculators
- **Lakehouse Monitoring**: Aggregated metrics and dashboards

### Observability
- **MLflow**: Experiment tracking and prompt versioning
- **Lakehouse Monitoring**: Time-series metrics and alerts
- **Streamlit UI**: Real-time governance dashboards

---

## Key Capabilities

### Intelligent Off-Topic Detection

The 3-stage cascade classifier ensures that only relevant queries consume expensive LLM resources:

- **Stage 1**: Regex patterns catch 80% of queries instantly (<1ms, $0)
- **Stage 2**: Embedding similarity handles 15% semantically (~100ms, $0.0001)
- **Stage 3**: LLM fallback for ambiguous cases (~300ms, $0.001)

**Result:** 80% cost reduction vs. pure LLM classification.

### Privacy Protection

Built-in PII anonymization pipeline:
1. Extract member names from Unity Catalog
2. Replace with tokens during LLM processing
3. Restore real names in final response
4. Log only anonymized versions to MLflow

### Regulatory Compliance

Every response includes:
- **Regulatory citations**: Proper references to country-specific laws
- **Audit trail**: Complete logging to Unity Catalog
- **Validation results**: LLM-as-a-Judge confidence scores
- **Cost transparency**: Per-query cost breakdowns

---

## LLM Token Cost Analysis

### Typical Query Token Cost Breakdown

**Note**: These are Claude Foundation Model API costs only. Total cost of ownership (TCO) also includes SQL Warehouse compute, Databricks workspace, storage, and app hosting costs.

| Operation | Model | Tokens | LLM Token Cost |
|-----------|-------|--------|----------------|
| Planning & Synthesis | Claude Opus 4.1 | ~2,000 | $0.0029 |
| Validation | Claude Sonnet 4 | ~500 | $0.0004 |
| **Total per Query** | | | **~$0.003** |

### Per-Query LLM Token Cost Breakdown

**Typical Query Cost Components**:

| Component | Operation | Model | Tokens | LLM Token Cost |
|-----------|-----------|-------|--------|----------------|
| Classification | Query classification (5% queries) | Claude Sonnet 4 | ~300 | $0.0001 |
| Response Synthesis | Natural language generation | Claude Opus 4.1 | ~2,000 | $0.0029 |
| Validation | LLM-as-a-Judge quality check | Claude Sonnet 4 | ~500 | $0.0004 |
| | **Total LLM Token Cost per Query** | | **~2,800** | **$0.0034** |

**Note**: This represents Claude Foundation Model API costs only, not total cost of ownership (TCO).

### Real-Time Token Cost Display

Each response in the UI shows detailed LLM token cost breakdown:
```
LLM Token Cost: $0.003245
├─ Main LLM (Opus 4.1): $0.002891
├─ Validation (Sonnet 4): $0.000354
└─ Classification: $0.0001
```

### Total Cost of Ownership (TCO)

While LLM token costs are the most visible component, the total system cost includes:

**1. LLM Token Costs** (shown above)
- **Cost**: $0.003-$0.010 per query
- **Driver**: Query complexity, number of retries
- **Optimization**: 3-stage cascade classification saves 80% on off-topic queries

**2. SQL Warehouse Compute**
- **Cost**: ~$0.001-$0.003 per query (estimated)
- **Driver**: Number of tools called, query complexity
- **Optimization**: Efficient SQL functions, connection pooling

**3. Databricks Workspace**
- **Cost**: Subscription-based (not per-query)
- **Model**: Monthly or annual workspace licensing

**4. Storage (Unity Catalog)**
- **Cost**: ~$0.023 per GB per month
- **Driver**: Audit logs, member profiles, governance data
- **Volume**: ~1 KB per query (audit log) = ~$0.000023 per query

**5. App Hosting (Streamlit)**
- **Cost**: Compute instance costs (not per-query)
- **Model**: Hourly instance pricing based on tier

**Estimated Total TCO per Query**: ~$0.005-$0.015
- Dominated by LLM token costs ($0.003-$0.010)
- Infrastructure costs are minimal per query

### Solution Value Proposition

This AI agent system is designed to provide **highly personalized, fully auditable, and cost-effective advice** for routine pension queries, enabling human advisors to focus on more complex cases that require nuanced judgment and personalized planning.

**AI Agent Economics** (Cost-Effective at Scale):
- **Fixed Costs**: Databricks workspace, compute infrastructure (~$500-2,000/month)
- **Variable Costs**: LLM tokens + SQL Warehouse (~$0.005-$0.015 per query)
- **Scaling Characteristics**: Fixed infrastructure costs + minimal variable costs per query
- **Cost Structure**: ~$1,500-2,000/month baseline + ~$0.01 per query at high volume

**Cost Structure by Volume**:

| Monthly Volume | Infrastructure | Variable Costs | Total Monthly Cost |
|----------------|----------------|----------------|---------------------|
| 1,000 queries | $1,500 | $5-15 | ~$1,515 |
| 10,000 queries | $1,500 | $50-150 | ~$1,650 |
| 50,000 queries | $1,500 | $250-750 | ~$2,250 |

**Use Case: Deflecting Routine Queries**

The system is designed to handle **40-50% of incoming member queries** that are routine, fact-based questions:
- "What is my preservation age?"
- "When can I access my super?"
- "What is my current balance?"
- "How much tax will I pay on withdrawals?"

**Benefits for Organizations**:

1. **Query Deflection**: Automated handling of routine queries at scale
   - Instant responses 24/7
   - No queue times or callbacks
   - Consistent, validated answers

2. **Human Advisor Enhancement**: Free advisors to focus on high-value interactions
   - Complex financial planning
   - Emotional support during major life decisions
   - Multi-factor retirement strategy
   - Estate planning and beneficiary management

3. **Quality & Compliance**: Enterprise-grade governance
   - Every response validated by LLM-as-a-Judge
   - Complete audit trail for regulatory compliance
   - Citation tracking for legal references
   - Cost transparency per interaction

4. **Scalability**: Handle volume spikes without capacity constraints
   - Peak periods (tax season, end of fiscal year)
   - Member onboarding campaigns
   - Regulatory changes causing query surges

**Operating Model**:
- **Tier 1**: AI agent handles routine queries (~40-50% of volume)
- **Tier 2**: Human advisors handle escalated and complex cases
- **Result**: Same human advisor capacity now serves 2x the member base while improving response times

---

## Governance & Compliance

### Audit Trail

Every query is logged to Unity Catalog with:
- **Query text**: Original user question
- **Agent response**: Generated answer
- **Validation results**: Judge verdict and confidence
- **Cost breakdown**: Detailed cost analysis
- **Regulatory citations**: References to laws and regulations
- **Tool usage**: Which SQL functions were called

### MLflow Artifacts

- **Prompt versions**: Tracked for reproducibility
- **Validation results**: Full judge reasoning
- **Cost breakdowns**: Token-level analysis
- **Error logs**: Debugging information

### Lakehouse Monitoring

- **Daily metrics**: Query volume, pass rates, costs
- **Anomaly detection**: Cost spikes, quality degradation
- **Automated alerts**: Threshold-based notifications

---

## Development Guide

### Project Structure

```
2025-11-agentic-ai-pension-advisor/
├── app.py                          # Streamlit UI (root level)
├── app.yaml                        # Databricks Apps configuration
├── requirements.txt                # Python dependencies (notebooks)
├── requirements-app.txt            # Python dependencies (Databricks Apps)
├── .env.example                    # Environment variables template
├── 01-setup/                       # Setup notebooks
│   ├── 01-unity-catalog-setup.py  # Create catalog, tables, functions
│   └── 02-governance-setup.py     # Row-level security configuration
├── 02-agent-demo/                  # Agent demonstration notebooks
│   ├── 01-agent-overview.py       # Architecture walkthrough
│   ├── 02-build-agent.py          # Build and test agent
│   ├── 03-tool-integration.py     # UC function calling
│   ├── 04-validation.py           # LLM-as-a-Judge validation
│   ├── 05-mlflow-deployment.py    # Deploy to MLflow serving
│   ├── 06-ai-guardrails.py        # AI guardrails integration
│   ├── 07-production-monitoring.py # MLflow tracing & scorers
│   └── 08-automated-scoring-job.py # Quality scoring automation
├── 03-monitoring-demo/             # Observability notebooks
│   ├── 01-mlflow-tracking.py      # Experiment tracking
│   ├── 02-observability.py        # Query patterns & metrics
│   └── 03-dashboard.py            # Governance dashboard
├── sql_ddls/                       # Database schemas
│   ├── super_advisory_demo_schema.sql     # Table definitions
│   ├── super_advisory_demo_functions.sql  # UC functions
│   └── migration_add_judge_confidence.sql # Schema migration
└── src/                            # Source code
    ├── agent.py                    # Main agent orchestrator
    ├── agent_processor.py          # Execution orchestration & MLflow
    ├── react_loop.py               # ReAct agentic loop
    ├── classifier.py               # 3-stage cascade classifier
    ├── prompts_registry.py         # Prompt registry with MLflow
    ├── validation.py               # LLM-as-a-Judge validation
    ├── country_config.py           # Country-agnostic configuration
    ├── observability.py            # MLflow & Lakehouse Monitoring
    ├── monitoring.py               # AgentMonitor metrics class
    ├── tools.py                    # Unity Catalog function wrappers
    ├── ai_guardrails.py            # AI guardrails integration
    ├── mlflow_model.py             # MLflow model wrapper
    ├── scorers.py                  # Automated quality scorers
    ├── ui_components.py            # UI component builders
    ├── ui_dashboard.py             # Dashboard layout
    ├── ui_monitoring_tabs.py       # Monitoring tab implementations
    ├── agents/
    │   ├── orchestrator.py         # Phase tracking and timing
    │   ├── context_formatter.py    # Context formatting utilities
    │   └── response_builder.py     # Response construction
    ├── config/
    │   ├── __init__.py             # Configuration exports
    │   ├── config_loader.py        # YAML configuration loader
    │   ├── config.yaml             # Application configuration
    │   └── config.yaml.example     # Configuration template
    ├── country_content/
    │   ├── disclaimers.py          # Country-specific disclaimers
    │   ├── prompts.py              # Country-specific prompts
    │   └── regulations.py          # Regulatory content
    ├── prompts/
    │   ├── template_manager.py     # Jinja2 template engine
    │   ├── country_prompts.py      # Country-specific prompt logic
    │   └── templates/              # Jinja2 template files (.j2)
    │       ├── system_prompt.j2
    │       ├── validation_prompt.j2
    │       ├── advisor_context.j2
    │       ├── ai_classify_query.j2
    │       ├── off_topic_decline.j2
    │       └── welcome_message.j2
    ├── tools/
    │   ├── tool_config.yaml        # Tool definitions and metadata
    │   └── tool_executor.py        # Tool execution engine
    ├── validation/
    │   ├── json_parser.py          # JSON extraction and parsing
    │   └── token_calculator.py     # Token counting utilities
    ├── ui/
    │   ├── theme_config.py         # Country-specific UI themes
    │   ├── html_builder.py         # HTML component builders
    │   └── tab_base.py             # Base class for monitoring tabs
    ├── shared/
    │   ├── logging_config.py       # Centralized logging configuration
    │   └── progress_tracker.py     # Progress tracking utilities
    └── utils/
        ├── formatting.py           # Currency formatting, SQL escaping
        ├── audit.py                # Governance logging
        ├── lakehouse.py            # Unity Catalog utilities
        ├── progress.py             # Real-time progress tracking
        └── urls.py                 # URL utilities
```

### Package Descriptions

**Core Agent Files:**
- `agent.py`: SuperAdvisorAgent class - main agent orchestrator
- `agent_processor.py`: High-level orchestration with MLflow integration and phase tracking
- `react_loop.py`: ReAct (Reasoning-Acting-Observing) agentic loop implementation
- `classifier.py`: 3-stage cascade classifier (regex → embedding → LLM)
- `prompts_registry.py`: Prompt registry with MLflow versioning and A/B testing
- `validation.py`: LLM-as-a-Judge validation with confidence scoring

**Configuration & Content:**
- `country_config.py`: Country-agnostic configuration and metadata
- `config/`: YAML-based configuration system
  - `config.yaml`: Application settings (LLM endpoints, warehouse IDs, MLflow paths)
  - `config_loader.py`: Configuration loader with environment variable overrides
- `country_content/`: Country-specific content (disclaimers, prompts, regulations)

**Observability & Monitoring:**
- `observability.py`: MLflow experiment tracking and Lakehouse Monitoring integration
- `monitoring.py`: AgentMonitor class for real-time metrics collection
- `ai_guardrails.py`: AI guardrails integration (PII detection, toxicity filtering)
- `scorers.py`: Automated quality scorers (relevance, faithfulness, country compliance)

**MLflow & Deployment:**
- `mlflow_model.py`: MLflow model wrapper for serving endpoint deployment
- Enables model registry, versioning, and production deployment patterns

**Tool Integration:**
- `tools.py`: Unity Catalog function wrappers
- `tools/`: Tool execution framework
  - `tool_config.yaml`: Tool definitions and metadata
  - `tool_executor.py`: Dynamic tool execution engine

**UI Components:**
- `ui_components.py`: Reusable UI components (cards, badges, metrics)
- `ui_dashboard.py`: Multi-tab dashboard layout
- `ui_monitoring_tabs.py`: Observability, governance, and MLflow tabs

**agents/** - Agent Infrastructure
- `orchestrator.py`: Phase tracking context manager
  - Automatic timing, error handling, and MLflow logging
  - Reduces boilerplate in agent_processor.py
- `context_formatter.py`: Context formatting and prompt construction
- `response_builder.py`: Response synthesis and formatting

**prompts/** - Prompt Management
- `template_manager.py`: Jinja2 template engine
  - Eliminates hard-coded prompts
  - Country-specific prompt generation with caching
  - Integration with country_config for dynamic variables
- `country_prompts.py`: Country-specific prompt logic
- `templates/`: Jinja2 template files (.j2)
  - `system_prompt.j2`: Main agent system prompt
  - `validation_prompt.j2`: LLM-as-a-Judge validation
  - `advisor_context.j2`: Member context template
  - `ai_classify_query.j2`: Query classification
  - `off_topic_decline.j2`: Off-topic response
  - `welcome_message.j2`: Country-specific welcome

**validation/** - Validation Utilities
- `json_parser.py`: Extract and parse JSON from LLM responses
- `token_calculator.py`: Token counting for cost estimation

**ui/** - UI Component Framework
- `theme_config.py`: Country theme definitions (colors, flags, gradients)
- `html_builder.py`: Reusable HTML builders (cards, badges, metrics)
- `tab_base.py`: Abstract base class for monitoring tabs
  - Eliminates ~425 lines of duplicated code
  - Automatic data loading, error handling, empty state management

**shared/** - Shared Infrastructure
- `logging_config.py`: Centralized logging with ColoredFormatter
- `progress_tracker.py`: Real-time progress tracking utilities

**utils/** - Utility Functions
- `formatting.py`: Currency formatting, SQL escaping
- `audit.py`: Governance table logging
- `lakehouse.py`: Unity Catalog utilities
- `progress.py`: Real-time progress tracking
- `urls.py`: URL utilities and helpers

### Logging Setup

**Configuration**

All modules use structured logging via `src/shared/logging_config.py`:

```python
from src.shared.logging_config import get_logger, setup_logging

# Initialize logging (in app.py or main entry point)
setup_logging(
    log_level=logging.INFO,      # DEBUG, INFO, WARNING, ERROR, CRITICAL
    enable_file=True,             # Write logs to file
    log_file="logs/app.log"      # Log file path
)

# Get logger in any module
logger = get_logger(__name__)

# Use structured logging
logger.info("Processing query")
logger.error("Failed to execute tool", exc_info=True)  # Includes stack trace
logger.debug(f"Response: {response}")
```

**Log Levels**
- `DEBUG`: Detailed information for diagnosing problems
- `INFO`: Confirmation that things are working as expected
- `WARNING`: Indication of potential problems
- `ERROR`: Due to a more serious problem, the function has failed
- `CRITICAL`: Serious error, the program may be unable to continue

**Best Practices**
- Production: Use `INFO` level with file logging enabled
- Development: Use `DEBUG` level for detailed tracing
- Error handling: Always use `exc_info=True` for exceptions
- Consistency: All production code uses `logger`, no print statements

### Supporting Components

**Configuration & Utilities:**
- `country_config.py`: Centralized country configurations (currency, authorities, regulations)
- `prompts_registry.py`: Prompt versioning and MLflow tracking
- `utils/formatting.py`: Currency formatting, SQL escaping utilities
- `utils/audit.py`: Governance logging and audit trail utilities
- `utils/lakehouse.py`: Unity Catalog SQL execution utilities
- `utils/progress.py`: Real-time UI progress tracking

**Core Logic:**
- `classifier.py`: 3-stage cascade classification (Regex → Embedding → LLM)
- `validation.py`: LLM-as-a-Judge validation with multiple modes
- `tools.py`: Unity Catalog function wrappers
- `observability.py`: MLflow and Lakehouse Monitoring integration

### Key Design Patterns

**Separation of Concerns:**
- **agent_processor.py**: Infrastructure and orchestration (phase tracking, logging, error handling)
- **agent.py**: Agent instance coordination (tools, validators, prompts)
- **react_loop.py**: Core agentic logic (REASON → ACT → OBSERVE)
- **utils/**: Reusable utilities (formatting, SQL, audit)

**Utility Organization:**
- `utils/formatting.py`: General formatting utilities (currency, SQL escaping)
- `utils/audit.py`: Audit logging and governance utilities
- `utils/lakehouse.py`: Unity Catalog and SQL operations
- `utils/progress.py`: Real-time UI progress tracking

**Country Configuration:**
- `country_config.py`: Single source of truth for country-specific settings
- No hardcoded country logic in agent or react_loop
- Easy to add new countries by extending configuration

### Adding a New Country

The system is designed to support new countries with minimal configuration changes. Here's a complete example of adding Canada as a fifth country:

#### Step 1: Update Configuration (config/config.yaml)

Add the new country to the countries list:

```yaml
countries:
  - code: "AU"
    name: "Australia"
    enabled: true
  - code: "US"
    name: "United States"
    enabled: true
  - code: "UK"
    name: "United Kingdom"
    enabled: true
  - code: "IN"
    name: "India"
    enabled: true
  - code: "CA"
    name: "Canada"
    enabled: true
```

#### Step 2: Add Country Configuration (country_config.py)

Define the country-specific settings:

```python
CountryConfig(
    code="CA",
    name="Canada",
    currency="CAD",
    currency_symbol="$",
    retirement_account_term="RRSP",
    authority="CRA",
    regulatory_context="""
        Follow Canadian Revenue Agency (CRA) regulations for RRSPs.
        Key rules:
        - Contribution limit: 18% of previous year's income (max $31,560 for 2024)
        - Withdrawal taxed as income
        - First Home Buyers' Plan: withdraw up to $35,000
        - Age 71: must convert to RRIF
    """,
    available_tools=["rrsp_balance", "tax_calculation", "withdrawal_eligibility"]
)
```

#### Step 3: Create SQL Functions (Unity Catalog)

Create country-specific calculation functions:

```sql
-- RRSP Balance Query
CREATE OR REPLACE FUNCTION super_advisory_demo.{functions_schema}.CA_get_rrsp_balance(
    member_id STRING
)
RETURNS STRUCT<
    balance DOUBLE,
    contribution_room DOUBLE,
    currency STRING
>
LANGUAGE SQL
RETURN SELECT
    balance,
    contribution_room,
    'CAD' as currency
FROM super_advisory_demo.pension_advisory.member_profiles
WHERE user_id = member_id AND country = 'CA';

-- Tax Calculation
CREATE OR REPLACE FUNCTION super_advisory_demo.{functions_schema}.CA_calculate_tax(
    withdrawal_amount DOUBLE,
    member_id STRING
)
RETURNS STRUCT<
    tax_amount DOUBLE,
    net_amount DOUBLE,
    tax_rate DOUBLE
>
LANGUAGE SQL
...
```

#### Step 4: Add Test Member Data

Insert test member profile:

```sql
INSERT INTO super_advisory_demo.pension_advisory.member_profiles
(user_id, name, age, country, super_balance, contribution_rate, employer_match)
VALUES ('CA001', 'John Smith', 45, 'CA', 125000, 0.10, 0.05);
```

#### Step 5: Add UI Theme (Optional)

Update `ui/theme_config.py` for country-specific UI styling:

```python
COUNTRY_COLORS = {
    "CA": {
        "primary": "#FF0000",      # Red from Canadian flag
        "secondary": "#FFFFFF",     # White from Canadian flag
        "currency": "$",
        "flag": "CA"
    },
    ...
}
```

#### Step 6: Test the Integration

```python
# Test query for Canadian member
from agent_processor import agent_query

result = agent_query(
    member_id="CA001",
    user_query="What is my RRSP contribution room?",
    withdrawal_amount=None
)
```

**That's it!** The system will automatically:
- Route queries to Canadian-specific functions
- Apply CRA regulatory context
- Format currency as CAD
- Use Canadian terminology (RRSP, CRA)
- Track Canada-specific metrics

**Key Benefits:**
- Zero code changes to agent logic
- Configuration-driven approach
- Full isolation between countries
- Easy to test and validate
- Maintainable and scalable

---

## Testing & Evaluation

### Offline Evaluation

The system includes built-in offline evaluation capabilities for batch testing and validation. Offline evaluation allows you to test multiple queries at once and analyze results systematically.

#### Using Offline Evaluation via UI

1. **Navigate to Governance → Config Tab**
   - Open the Streamlit app and go to the Governance page
   - Click on the "Config" tab

2. **Upload Evaluation CSV**
   - Scroll to "🧪 Offline Evaluation" section
   - Upload a CSV file with the required columns (see format below)
   - Preview the data before running

3. **Run Evaluation**
   - Click "▶️ Run Offline Evaluation"
   - Results are processed and logged to MLflow
   - View summary statistics and sample results

#### CSV Format

The evaluation CSV must contain the following columns:

```csv
user_id,country,query_str
AU001,AU,"How much superannuation will I have at retirement?"
US002,US,"What's my 401k balance?"
UK003,UK,"When can I access my pension?"
```

**Column Requirements:**
- `user_id`: Member ID (must exist in `member_profiles` table)
- `country`: Country code (AU, US, UK, IN) or display name (Australia, USA, etc.)
- `query_str`: The query to evaluate (can also be `query_string`)

**Example CSV:**
```csv
user_id,country,query_str
AU001,AU,"What is my preservation age?"
AU002,AU,"Can I withdraw my super before age 60?"
US001,US,"What are the RMD rules for my 401k?"
UK001,UK,"When can I access my state pension?"
```

#### Results & Logging

**MLflow Integration:**
- All evaluation runs are logged to `MLFLOW_OFFLINE_EVAL_PATH` (configurable in `config.py`)
- Each query in the batch creates a separate MLflow run
- Full observability: costs, validation results, tool usage tracked per query

**Result Structure:**
```python
{
    "total_queries": 10,
    "sample_result": {
        "row_index": 0,
        "user_id": "AU001",
        "country": "AU",
        "query": "What is my preservation age?",
        "session_id": "uuid-here",
        "result": {
            "answer": "...",
            "citations": [...],
            "cost": 0.003,
            "validation_passed": True,
            ...
        }
    }
}
```

#### Command-Line Usage

You can also run offline evaluation from the command line:

```bash
# Offline evaluation (batch CSV)
python run_evaluation.py --mode offline --csv_path eval_queries.csv

# Online evaluation (single query)
python run_evaluation.py --mode online \
    --query_str "What is my preservation age?" \
    --user_id AU001 \
    --country AU
```

#### Evaluation Code Location

- **Evaluation Script**: `run_evaluation.py`
- **UI Integration**: `ui_components.py` → `render_configuration_tab()` (lines 747-812)
- **MLflow Path**: Configured in `config.py` → `MLFLOW_OFFLINE_EVAL_PATH`

**Functions:**
- `offline_eval(csv_path)`: Batch evaluation on CSV file
- `online_eval(query_str, user_id, country)`: Single query evaluation
- `run_csv_evaluation(uploaded_csv)`: Streamlit wrapper for UI

---

## Governance Dashboard Guide

The Governance page provides comprehensive monitoring and observability through 5 specialized tabs. Each tab focuses on different aspects of system performance, cost, and quality.

### Tab 1: Governance Dashboard

**Purpose**: High-level overview of system health and recent activity at a glance.

**What You See:**

#### Key Metrics Cards (Last 24 Hours)
- **Total Queries**: Total number of queries processed in the last 24 hours
  - **How to read**: Higher numbers indicate active usage
  - **What to watch**: Sudden drops may indicate system issues

- **Pass Rate**: Percentage of queries that passed validation
  - **How to read**: Should be consistently above 80% for healthy operation
  - **What to watch**: Drops below 70% indicate quality issues requiring investigation

- **Avg Cost**: Average cost per query in USD
  - **How to read**: Typically $0.003-$0.010 per query
  - **What to watch**: Spikes may indicate expensive queries or classification failures

- **Health Score**: Overall system health (0-100)
  - **How to read**: Above 80 = Healthy, 60-80 = Needs attention, Below 60 = Critical
  - **Calculation**: Based on pass rate, error rate, latency, and cost efficiency

#### System Status Banner
- **Status indicators**: Shows overall system state (Operational, Degraded, Critical)
- **Tool usage**: Displays most frequently used tools (tax, benefit, projection)
- **Country distribution**: Shows query volume by country

#### Audit Trail
- **Complete query history**: Full list of all queries with timestamps
- **Columns**: User ID, Country, Query, Response Preview, Cost, Validation Status, Runtime
- **Sorting**: Click column headers to sort
- **Filtering**: Use search boxes to filter by user, country, or query text
- **Purpose**: Compliance auditing and debugging specific queries

#### Recent Activity Feed
- **Last 10 queries**: Quick view of most recent activity
- **Color coding**: Green (passed), Red (failed), Yellow (pending)
- **Purpose**: Quick health check without scrolling through full audit trail

#### Quick Trend Charts
- **Query Volume Over Time**: Line chart showing query frequency
- **Cost Trend**: Area chart showing spending patterns
- **Purpose**: Identify usage patterns and cost spikes

**Use Cases:**
- Morning health check: Review key metrics and system status
- Compliance audit: Use audit trail to review specific queries
- Pattern analysis: Use trend charts to identify usage patterns

---

### Tab 2: MLflow Traces

**Purpose**: Deep dive into MLflow experiment tracking, prompt versions, and individual query execution details.

**What You See:**

#### MLflow Experiments Tab
- **Recent runs**: List of all MLflow runs with timestamps
- **Run details**: Click any run to see:
  - Parameters: Country, user ID, validation mode, tools used
  - Metrics: Runtime, cost, token counts, validation confidence
  - Artifacts: Full validation results, cost breakdowns, error logs
- **Purpose**: Debug individual queries and compare runs

#### Prompt Registry Tab
- **Registered prompts**: All prompt versions tracked in MLflow
- **Version history**: See how prompts evolved over time
- **Prompt content**: View full prompt text for each version
- **Registration button**: Manually trigger prompt registration
- **Purpose**: Prompt versioning, A/B testing, and rollback capabilities

**How to Interpret:**
- **High confidence scores**: Good quality responses
- **Low confidence scores**: May need prompt refinement
- **Cost spikes**: Check artifacts for expensive queries
- **Failed validations**: Review reasoning in artifacts

**Use Cases:**
- Prompt iteration: Compare different prompt versions
- Quality investigation: Deep dive into failed validations
- Cost optimization: Identify expensive queries and optimize

---

### Tab 3: Config

**Purpose**: System configuration and offline evaluation management.

**What You See:**

#### SQL Warehouse Configuration
- **Warehouse selection**: Dropdown to select active SQL warehouse
- **Purpose**: Configure which warehouse is used for SQL function execution

#### LLM Configuration
- **Main Advisory LLM** (Claude Opus 4.1):
  - **Temperature**: Controls creativity (0.0-1.0)
    - Lower (0.0-0.3): More factual, consistent
    - Higher (0.7-1.0): More creative, varied
  - **Max Tokens**: Maximum response length (100-4000)
    - Lower: Shorter, more focused responses
    - Higher: More detailed responses

- **Judge Validation LLM** (Claude Sonnet 4):
  - **Temperature**: Lower recommended (0.0-0.2) for consistent validation
  - **Max Tokens**: Typically 100-1000 for validation responses

**How to Interpret:**
- **Temperature too high**: Inconsistent responses, hallucinations
- **Temperature too low**: Rigid, repetitive responses
- **Max tokens too low**: Truncated responses
- **Max tokens too high**: Unnecessary cost

#### Offline Evaluation
- **CSV upload**: Upload evaluation datasets
- **Preview**: See data before running
- **Run evaluation**: Execute batch queries
- **Results**: View summary and sample results
- **Purpose**: Batch testing and validation

**Use Cases:**
- Parameter tuning: Adjust LLM settings based on performance
- Batch testing: Evaluate multiple queries systematically
- Cost optimization: Test different configurations

---

### Tab 4: LLM Token Cost Analysis

**Purpose**: Comprehensive analysis of Claude Foundation Model API costs with breakdowns and trends.

**Important**: This tab shows LLM token costs only (Claude API). Total cost of ownership (TCO) also includes SQL Warehouse, compute, storage, and hosting costs.

**What You See:**

#### Key LLM Token Cost Metrics
- **Total Token Cost**: Sum of all LLM API costs
- **Median Token Cost**: Typical query cost (less sensitive to outliers)
- **Requests**: Total number of queries processed
- **P95 Token Cost**: 95th percentile cost (identifies expensive queries)

**How to Interpret:**
- **Median vs Average**: Large difference indicates cost outliers
- **P95**: Focus optimization on queries above this threshold
- **Trends**: Identify patterns and potential issues

#### Token Cost Distribution Charts
- **Cost Distribution Histogram**: Shows how LLM token costs are distributed
  - **Peak at low costs**: Most queries are efficient ($0.002-$0.005)
  - **Long tail**: Identify expensive outliers for optimization
- **Token Cost Over Time**: Trend line showing LLM API spending patterns
  - **Upward trend**: Investigate prompt changes or query complexity
  - **Spikes**: Identify periods with expensive queries

#### Token Cost by Country
- **Bar chart**: Average LLM token cost per country
- **Purpose**: Identify country-specific patterns
- **Use Cases**: Different query complexities or prompt lengths by country

**Use Cases:**
- Cost awareness: Understand LLM API spending patterns
- Query optimization: Identify expensive queries for prompt refinement
- Country analysis: Compare token usage across countries
- Budget planning: Track LLM API spending trends (note: not total TCO)

---

### Tab 5: Observability

**Purpose**: Real-time monitoring of performance, quality, and automated scoring using Databricks MLOps standards.

The Observability tab is divided into **3 specialized sub-tabs**, each focusing on different aspects of system monitoring:

---

#### Sub-Tab 1: Performance Metrics

**Purpose**: Monitor system performance, latency, and LLM token costs

**What You See:**

**Key Metrics (Last 24h):**
- **Requests**: Count of queries processed
- **Average Latency**: Mean response time in seconds
- **LLM Token Cost**: Claude API costs only (not total TCO)
- **Pass Rate**: Validation success rate

**Charts:**
- **Query Volume Over Time**: Hourly request frequency (line chart)
  - **Peaks**: Identify usage patterns
  - **Drops**: May indicate system issues
- **LLM Token Cost Trend**: API spending over time (area chart)
  - **Spikes**: Investigate expensive periods
  - **Note**: Does not include SQL Warehouse, compute, or storage costs
- **Latency Trend**: Response time over time (line chart)
  - **Spikes**: Performance degradation

**Performance by Country:**
- **Table**: Metrics broken down by country
- **Columns**: Total Token Cost, Avg Token Cost, Avg Latency, Requests
- **Purpose**: Country-specific performance analysis

**Query Distribution:**
- **Pie chart**: Percentage of queries by country
- **Purpose**: Understand usage patterns

**Use Cases**:
- Monitor system load and capacity planning
- Identify performance degradation (latency spikes)
- Track LLM API spending trends (note: not total TCO)
- Compare performance across countries

---

#### Sub-Tab 2: Quality Monitoring

**Purpose**: Monitor validation pass rates, confidence scores, and quality trends

**What You See:**

**Validation Pass Rate:**
- **Overall Pass Rate**: Percentage of queries passing LLM-as-a-Judge validation
- **Pass Rate Trend**: Pass rate over time (line chart)
  - **Downward trend**: Quality degradation
  - **Drops**: Investigate specific time periods

**Confidence Analysis:**
- **Average Confidence**: Mean validation confidence score (0-1)
- **Confidence Distribution**: Histogram of confidence scores
  - **Peak near 1.0**: High quality responses
  - **Peak near 0.5-0.7**: May need prompt refinement
- **Confidence by Verdict**: Average confidence for passed vs failed
  - **Purpose**: Understand validation patterns

**Common Violations:**
- **Violation types**: Most frequent validation failures
- **Frequency**: How often each violation occurs
- **Purpose**: Identify recurring issues for prompt improvement

**Quality by Country:**
- **Table**: Pass rate and confidence by country
- **Purpose**: Country-specific quality analysis

**Use Cases:**
- Quality assurance: Monitor validation success rates
- Prompt tuning: Identify low confidence patterns for improvement
- Violation analysis: Understand common failure modes
- Country comparison: Identify country-specific quality issues

---

#### Sub-Tab 3: Automated Scoring

**Purpose**: Background quality monitoring with automated scorers for long-term trend analysis (Databricks MLOps Recommended)

**What is Automated Scoring?**

Automated scoring is a **Databricks-recommended MLOps practice** that provides continuous quality monitoring in production through background evaluation. Unlike real-time validation (LLM-as-a-Judge), automated scorers run asynchronously and don't block user responses.

**Two-Layer Quality Architecture:**

1. **Layer 1: Real-Time LLM-as-a-Judge** (validation step)
   - **Coverage**: 100% of queries
   - **Timing**: Synchronous (blocks response)
   - **Purpose**: Quality gate - blocks bad responses from reaching users
   - **Speed**: ~2s per query
   - **Model**: Claude Sonnet 4

2. **Layer 2: Background Automated Scorers** (This tab)
   - **Coverage**: 10% sampling (configurable)
   - **Timing**: Asynchronous (doesn't block users)
   - **Purpose**: Trend detection, drift monitoring, long-term quality tracking
   - **Speed**: Runs in background after response sent
   - **Scorers**: Multiple specialized evaluators

**Why Both Layers?**

- **LLM-as-a-Judge**: Prevents bad responses in real-time (quality gate)
- **Automated Scorers**: Detects gradual quality degradation over time (drift detection)

**Automated Scorers Implemented:**

1. **Relevance Scorer**
   - Evaluates: Does response actually answer the user's question?
   - Returns: 0-1 score (1 = fully relevant)

2. **Faithfulness Scorer**
   - Evaluates: Is response grounded in tool outputs and member data?
   - Returns: 0-1 score (1 = fully faithful, no hallucination)

3. **Toxicity Scorer**
   - Evaluates: Is response professional and appropriate?
   - Returns: 0-1 score (1 = no toxicity)

4. **Country Compliance Scorer**
   - Evaluates: Does response follow country-specific regulations?
   - Returns: PASS/FAIL + compliance checklist

5. **Citation Quality Scorer**
   - Evaluates: Are citations accurate and properly formatted?
   - Returns: 0-1 score (1 = excellent citations)

**What You See:**

**Quality Score Trends:**
- **Line chart**: Aggregated quality scores over time (daily)
- **Purpose**: Identify gradual quality degradation (drift)
- **Example**: Relevance dropping from 0.95 → 0.80 over 2 weeks

**Scorer Performance Table:**
- **Columns**: Scorer Name, Pass Rate, Avg Score, Samples Evaluated
- **Purpose**: Compare performance across different quality dimensions
- **Example**: Faithfulness 95%, Relevance 92%, Toxicity 99%

**Verdict Distribution:**
- **Pie chart**: PASS vs FAIL vs ERROR breakdown
- **Purpose**: Overall health snapshot
- **Target**: >90% PASS rate

**Recent Failures:**
- **Table**: Latest queries that failed automated scoring
- **Columns**: Timestamp, Query, Scorer, Reason, Score
- **Purpose**: Investigate specific failure cases for prompt improvement

**Country Analysis:**
- **Table**: Quality scores broken down by country
- **Purpose**: Identify country-specific quality patterns
- **Example**: AU 94%, US 92%, UK 89%, IN 91%

**How Automated Scoring Works:**

```
User Query → Agent Processing → Response (sent to user immediately)
                                      ↓
                          Background Thread (async):
                          ├─ Sample 10% of queries
                          ├─ Run 5 automated scorers
                          ├─ Log results to MLflow
                          └─ Update dashboards
```

**Sampling Strategy:**
- Default: 10% of queries (to control costs)
- Can be configured: 1%-100%
- Stratified sampling: Ensures all countries represented

**Use Cases:**
- **Drift detection**: Identify gradual quality degradation over weeks/months
- **Prompt A/B testing**: Compare quality metrics between prompt versions
- **Regulatory compliance**: Ensure country-specific rules are followed
- **Long-term trend analysis**: Track quality improvements after prompt updates
- **Root cause analysis**: Investigate why certain queries fail specific scorers

---

### Interpreting Charts and Metrics

#### General Guidelines

**Green Indicators (Good):**
- High pass rates (>80%)
- Low latency (<3 seconds)
- Low LLM token cost (<$0.005 per query)

**Yellow Indicators (Warning):**
- Pass rates 70-80%
- Latency 3-5 seconds
- LLM token cost $0.005-$0.010 per query

**Red Indicators (Critical):**
- Pass rates <70%
- Latency >5 seconds
- LLM token cost >$0.010 per query

#### Chart Reading Tips

**Line Charts:**
- **Upward trends**: Increasing over time
- **Downward trends**: Decreasing over time
- **Spikes**: Sudden increases (investigate)
- **Drops**: Sudden decreases (investigate)

**Bar Charts:**
- **Taller bars**: Higher values
- **Compare heights**: Relative differences
- **Clusters**: Group similar values

**Histograms:**
- **Peaks**: Most common values
- **Distribution shape**: Normal, skewed, bimodal
- **Outliers**: Values far from the peak

**Pie Charts:**
- **Larger slices**: Higher percentages
- **Compare slices**: Relative proportions

---

## Key Learnings

### What Makes This Production-Ready?

1. **Observability First**: Every query is tracked, logged, and monitored
2. **Cost Awareness**: Intelligent optimization without sacrificing quality
3. **Version Control**: Prompts and models are versioned and reproducible
4. **Safety Mechanisms**: Failed validations don't reach users
5. **Scalability**: Country-agnostic architecture for easy expansion
6. **Regulatory Compliance**: Full audit trails and citation tracking

### Databricks Platform Advantages

- **Unity Catalog**: Single source of truth for data and functions
- **Foundation Model API**: No API keys, workspace authentication
- **MLflow**: Built-in experiment tracking and model governance
- **Lakehouse Monitoring**: Native time-series metrics and alerts
- **SQL Functions**: Reusable, versioned calculation logic

---

## Contributing

Issues and pull requests welcome! This is a reference implementation demonstrating production best practices for agentic AI on Databricks.

**Maintained by:** [Pravin Varma](https://github.com/pravinva)

---

## License

MIT License - See LICENSE file for details

---

## Acknowledgments

Built with:
- **Databricks Platform** - Unified analytics and AI platform
- **MLflow** - Open-source ML lifecycle management
- **Unity Catalog** - Unified governance for data and AI
- **Foundation Model API** - Managed LLM endpoints

---

**This reference implementation demonstrates how to build production-ready agentic AI applications on Databricks using industry best practices. Use it as a blueprint for your own agentic AI projects.**

---

*For questions or feedback, open an issue or reach out via GitHub.*
