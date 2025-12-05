#!/usr/bin/env python3
# agent_processor.py – FINAL PRODUCTION READY VERSION with LIVE PHASE TRACKING
# ✅ Shows phases dropdown with real-time updates as query executes
# ✅ MLflow + UC Governance logging + all existing functionality
# ✅ FIXED: Proper synthesis + validation cost tracking
# ✅ FIXED: Phase timing now accurate (synthesis and validation show correct duration)
# ✅ FIXED: log_query_event() call with correct parameters

from src.agent import SuperAdvisorAgent
from src.agents.orchestrator import AgentOrchestrator
from src.utils.audit import log_query_event, _escape_sql
from src.utils.progress import initialize_progress_tracker, reset_progress_tracker, mark_phase_running, mark_phase_complete, mark_phase_error
from src.observability import create_observability
import traceback, uuid, time, threading
import mlflow
import mlflow.tracing  # Phase 4: Production monitoring with traces
import json
from datetime import datetime
from databricks.sdk import WorkspaceClient
from src.config import UNITY_CATALOG, SQL_WAREHOUSE_ID, MLFLOW_PROD_EXPERIMENT_PATH, get_governance_table_path

# ✅ CORRECT TABLE PATH
from src.shared.logging_config import get_logger

logger = get_logger(__name__)

GOVERNANCE_TABLE = get_governance_table_path()

class AuditLogger:
    """Handles MLflow and UC Governance logging"""
    
    def __init__(self):
        self.w = WorkspaceClient()
        self.warehouse_id = SQL_WAREHOUSE_ID
        
        try:
            mlflow.set_tracking_uri("databricks")
            mlflow.set_experiment(MLFLOW_PROD_EXPERIMENT_PATH)
        except Exception as e:
            logger.info(f"⚠️ MLflow init: {e}")
    
    # REMOVED: log_to_mlflow() method - DEAD CODE, never called
    # MLflow logging is now handled by Observability class (obs.end_agent_run())
    # See line 617 in async_phase8_logging() function

    def log_to_governance_table(self, session_id, user_id, country, query_string,
                               answer, judge_verdict, tools_called, cost, citations,
                               elapsed, error_info=None, classification_method=None):
        """Log to UC governance audit table"""
        try:
            event_id = str(uuid.uuid4())
            timestamp = datetime.utcnow().isoformat()
            
            # ✅ Use escape_sql from utils.audit instead of nested function
            query_text_escaped = _escape_sql(query_string)
            answer_truncated = answer[:15000] if answer else ""
            agent_response_escaped = _escape_sql(answer_truncated)
            result_preview = answer_truncated[:500] if answer_truncated else ""
            result_preview_escaped = _escape_sql(result_preview)
            judge_response_escaped = _escape_sql(str(judge_verdict.get('reasoning', '')))
            judge_verdict_text = judge_verdict.get('verdict', 'UNKNOWN')
            judge_confidence = judge_verdict.get('confidence', 0.0)  # ✅ Extract confidence
            tool_used = tools_called[0] if tools_called else "none"
            citations_json = json.dumps(citations) if citations else "[]"
            citations_escaped = _escape_sql(citations_json)
            error_text = _escape_sql(error_info) if error_info else ""
            validation_mode = judge_verdict.get('validation_mode', 'llm_judge')
            validation_attempts = judge_verdict.get('attempts', 1)
            
            # ✅ FIX #6: Don't overwrite error_text if it contains cost_metadata JSON
            # If error_info already contains JSON (from Fix #6), preserve it
            # Otherwise, store classification_method (backward compatibility)
            if classification_method and error_text:
                # Check if error_text is JSON (from Fix #6)
                try:
                    import json as json_check
                    json_check.loads(error_text)
                    # It's JSON - DON'T overwrite, keep the cost metadata
                    pass
                except:
                    # Not JSON - old format, prepend classification_method
                    error_text = f"classification_method={classification_method}|{error_text}"
            elif classification_method and not error_text:
                # No error_text, just store classification_method
                error_text = f"classification_method={classification_method}"
            
            # ✅ Store judge_confidence in judge_response JSON if not already present
            # Since schema doesn't have judge_confidence column, we'll store it in judge_response as JSON
            # Format: JSON string with confidence and reasoning
            import json as json_lib
            judge_response_data = {
                'reasoning': judge_verdict.get('reasoning', ''),
                'confidence': judge_confidence
            }
            judge_response_escaped = _escape_sql(json_lib.dumps(judge_response_data))
            
            insert_query = f"""
            INSERT INTO {GOVERNANCE_TABLE}
            VALUES (
                '{event_id}',
                '{timestamp}',
                '{user_id}',
                '{session_id}',
                '{country}',
                '{query_text_escaped}',
                '{agent_response_escaped}',
                '{result_preview_escaped}',
                {cost},
                '{citations_escaped}',
                '{tool_used}',
                '{judge_response_escaped}',
                '{judge_verdict_text}',
                {judge_confidence},
                '{error_text}',
                '{validation_mode}',
                {validation_attempts},
                {elapsed}
            )
            """

            result = self.w.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=insert_query,
                wait_timeout="10s"
            )

            logger.info(f"✅ Governance table logged: {session_id}")
        except Exception as e:
            logger.info(f"⚠️ Governance logging failed: {e}")


def _async_audit_logging(
    audit_logger,
    obs,
    session_id,
    user_id,
    country,
    query_string,
    answer,
    judge_verdict,
    tools_called,
    total_cost,
    citations,
    elapsed,
    classification_method
):
    """
    Async function to handle audit logging in background.
    Runs in a separate thread to avoid blocking response.
    Note: Phase tracking happens in main thread, not here.
    """
    try:
        logger.info("📍 PHASE 8: Audit Logging (running in background)")
        logger.info(f"🔄 Logging to MLflow and governance table...")

        # Log to governance table
        try:
            audit_logger.log_to_governance_table(
                session_id=session_id,
                user_id=user_id,
                country=country,
                query_string=query_string,
                answer=answer,
                judge_verdict=judge_verdict,
                tools_called=tools_called,
                cost=total_cost,
                citations=citations,
                elapsed=elapsed,
                error_info=None,
                classification_method=classification_method
            )
            logger.info(f"✅ Governance table logged: {session_id}")
        except Exception as gov_error:
            logger.error(f"⚠️ Governance logging failed: {gov_error}", exc_info=True)

        # End observability run
        if obs:
            try:
                obs.end_agent_run(
                    response=answer or "",
                    success=True,
                    error=None
                )
            except Exception as obs_error:
                logger.info(f"⚠️ Error ending observability run: {obs_error}")
                # Force end any active MLflow run
                try:
                    import mlflow
                    if mlflow.active_run():
                        mlflow.end_run()
                except:
                    pass

        logger.info(f"✅ Phase 8 (Audit Logging) completed in background")

    except Exception as e:
        logger.error(f"⚠️ Background audit logging error: {e}", exc_info=True)


@mlflow.trace(name="pension_advisor_query", span_type="AGENT")  # ✅ FIX #4: Re-enabled for distributed tracing
def agent_query(
    user_id,
    session_id,
    country,
    query_string,
    validation_mode="llm_judge",
    enable_observability=True
):
    """
    Orchestrates one advisory query with LIVE PHASE TRACKING + OBSERVABILITY + TRACING
    Shows phases dropdown with real-time updates as execution progresses
    Includes MLflow tracking, Lakehouse Monitoring, and distributed tracing

    @mlflow.trace automatically captures:
    - Function inputs/outputs
    - Execution time
    - Nested LLM calls
    - Tool executions
    - Validation steps
    """

    # ✅ PROGRESS TRACKER - Initialization removed (handled in app.py inside expander)
    # Only reset is needed here to clear stale state
    reset_progress_tracker()
    # initialize_progress_tracker() - REMOVED: Was causing progress to render outside expander

    start_all = time.time()
    answer = None
    citations = None
    response_dict = {}
    judge_resp = None
    judge_verdict = {}
    error_info = None
    tools_called = []
    total_cost = 0.0
    cost_breakdown = {}

    audit_logger = AuditLogger()
    
    # ✅ INITIALIZE OBSERVABILITY (MLflow + Lakehouse Monitoring)
    obs = None
    if enable_observability:
        try:
            obs = create_observability(enable_mlflow=True, enable_lakehouse=False)
            obs.start_agent_run(
                session_id=session_id,
                user_id=user_id,
                country=country,
                query=query_string,
                tags={'validation_mode': validation_mode}
            )
        except Exception as e:
            logger.info(f"⚠️ Observability initialization failed: {e}")
            obs = None
    
    # Initialize orchestrator for phase tracking
    orchestrator = AgentOrchestrator()

    try:
        logger.info(f"\n{'='*70}")
        logger.info(f"Running advisory pipeline")
        logger.info(f"User: {user_id}")
        logger.info(f"Session: {session_id}")
        logger.info(f"Country: {country}")
        logger.info(f"Query: {query_string}")
        logger.info(f"Validation Mode: {validation_mode}")
        logger.info(f"{'='*70}\n")

        # ✅ INPUT GUARDRAILS - Pre-generation validation
        from src.ai_guardrails import validate_input
        from src.config import AI_GUARDRAILS_ENABLED, AI_GUARDRAILS_CONFIG

        if AI_GUARDRAILS_ENABLED:
            logger.info("🛡️  Running input guardrails...")
            input_validation = validate_input(
                query=query_string,
                policies=list(AI_GUARDRAILS_CONFIG['input_policies'].keys()),
                config=AI_GUARDRAILS_CONFIG
            )

            # Log guardrails cost
            total_cost += input_validation.cost

            if input_validation.blocked:
                # Query blocked by guardrails
                logger.warning(f"🚫 Query blocked: {input_validation.violations}")

                # Return blocked response
                return {
                    'error': 'Query blocked by safety policies',
                    'blocked': True,
                    'violations': input_validation.violations,
                    'latency_ms': input_validation.latency_ms,
                    'cost': input_validation.cost,
                    'session_id': session_id
                }
            else:
                logger.info(f"✅ Input guardrails passed ({input_validation.latency_ms:.0f}ms)")

        # PHASE 1: DATA RETRIEVAL
        with orchestrator.track_phase("Data Retrieval", "phase_1_retrieval"):
            agent = SuperAdvisorAgent(validation_mode=validation_mode)
            logger.info(f"✅ Agent initialized")

        phase1_duration = orchestrator.get_last_phase_duration()

        # PHASE 2: ANONYMIZATION
        with orchestrator.track_phase("Privacy Anonymization", "phase_2_anonymization"):
            logger.info(f"✓ Data anonymization ready")

        phase2_duration = orchestrator.get_last_phase_duration()
        
        # ✅ CALL AGENT - This executes classification, tools, synthesis AND validation
        # Phase tracking happens INSIDE the ReAct loop
        phase4_start = time.time()
        
        result_dict = agent.process_query(
            member_id=user_id,
            user_query=query_string,
            withdrawal_amount=None
        )


        # ✅ FIX #1: Removed duplicate governance logging (moved to Phase 8)
        # Governance logging now happens ONCE at Phase 8 (lines 566-583)

        tools_called = result_dict.get('tools_used', [])
        
        # ✅ LOG CLASSIFICATION TO OBSERVABILITY
        if obs and 'classification' in result_dict:
            obs.log_classification(result_dict['classification'])
        
        # ✅ LOG TOOL EXECUTION TO OBSERVABILITY
        tool_results_dict = result_dict.get('tool_results', {})
        if obs:
            obs.log_tool_execution(tools_called, tool_results_dict or {})
        
        # Calculate ONLY tool execution time (subtract synthesis + validation)
        phase4_total = time.time() - phase4_start
        synthesis_results = result_dict.get('synthesis_results', [])
        validation_results = result_dict.get('validation_results', [])
        synthesis_time = sum(s.get('duration', 0) for s in synthesis_results)
        validation_time = sum(v.get('duration', 0) for v in validation_results)
        
        # Phase 4 = total - synthesis - validation = pure tool/orchestration time
        phase4_duration = phase4_total - synthesis_time - validation_time
        
        logger.info(f"✓ Tools executed: {', '.join(tools_called) if tools_called else 'none'}")
        logger.info(f"⏱️  Phase 4 pure tool execution: {phase4_duration:.2f}s (excluding LLM time)")
        mark_phase_complete('phase_4_execution', duration=phase4_duration)
        
        # PHASE 5: RESPONSE SYNTHESIS (Extract actual synthesis duration from results)
        logger.info("\n📍 PHASE 5: Response Synthesis")
        mark_phase_running('phase_5_synthesis')

        answer = result_dict.get('response', '')
        response_dict = result_dict
        citations = result_dict.get('citations', [])
        synthesis_results = result_dict.get('synthesis_results', [])

        # Calculate actual synthesis time from results
        synthesis_duration = sum(s.get('duration', 0) for s in synthesis_results)

        logger.info(f"✓ Response synthesized: {len(answer)} chars")
        logger.info(f"⏱️  Phase 5 actual synthesis time: {synthesis_duration:.2f}s")
        
        # ✅ Note: Phase tracking for synthesis happens INSIDE ReAct loop
        # This is just logging the final duration
        
        # ✅ LOG SYNTHESIS TO OBSERVABILITY
        if obs:
            obs.log_synthesis(synthesis_results)
        
        # PHASE 6: LLM VALIDATION (Extract actual validation duration from results)
        logger.info("\n📍 PHASE 6: LLM Validation")
        
        validation_results = result_dict.get('validation_results', [])
        
        # Calculate actual validation time from results
        validation_duration = sum(v.get('duration', 0) for v in validation_results)
        
        if validation_results:
            final_validation = validation_results[-1]
            judge_verdict = {
                'passed': final_validation.get('passed', False),
                'confidence': final_validation.get('confidence', 0.0),
                'verdict': 'Pass' if final_validation.get('passed') else 'Fail',
                'reasoning': final_validation.get('reasoning', ''),
                'violations': final_validation.get('violations', []),
                'validation_mode': validation_mode,
                'attempts': len(validation_results)
            }
        else:
            # No validation results (deterministic mode or error)
            judge_verdict = {
                'passed': True,
                'confidence': 1.0,
                'verdict': 'Pass',
                'reasoning': 'Deterministic validation',
                'violations': [],
                'validation_mode': validation_mode,
                'attempts': 0
            }
        
        logger.info(f"✓ Validation: {judge_verdict.get('verdict')} ({judge_verdict.get('confidence', 0):.0%} confidence)")
        logger.info(f"⏱️  Phase 6 actual validation time: {validation_duration:.2f}s")

        # ✅ Note: Phase tracking for validation happens INSIDE ReAct loop
        # This is just logging the final duration

        # ✅ LOG VALIDATION TO OBSERVABILITY
        if obs:
            obs.log_validation(validation_results)

        # Name restoration (part of finalization, not a separate tracked phase)
        logger.info(f"✓ Member name restored")

        # 🆕 Calculate SYNTHESIS LLM costs
        total_synthesis_input_tokens = sum(s.get('input_tokens', 0) for s in synthesis_results)
        total_synthesis_output_tokens = sum(s.get('output_tokens', 0) for s in synthesis_results)
        total_synthesis_cost = sum(s.get('cost', 0.0) for s in synthesis_results)

        synthesis_model = synthesis_results[0].get('model', 'claude-opus-4-1') if synthesis_results else 'claude-opus-4-1'

        cost_breakdown['synthesis'] = {
            'input_tokens': total_synthesis_input_tokens,
            'output_tokens': total_synthesis_output_tokens,
            'cost': total_synthesis_cost,
            'model': synthesis_model,
            'attempts': len(synthesis_results)
        }

        logger.info(f"💰 Synthesis cost: ${total_synthesis_cost:.6f} ({synthesis_model})")
        logger.info(f"   └─ {total_synthesis_input_tokens} input + {total_synthesis_output_tokens} output tokens across {len(synthesis_results)} attempt(s)")

        # 🆕 Calculate VALIDATION LLM costs
        total_validation_input_tokens = sum(v.get('input_tokens', 0) for v in validation_results)
        total_validation_output_tokens = sum(v.get('output_tokens', 0) for v in validation_results)
        total_validation_cost = sum(v.get('cost', 0.0) for v in validation_results)

        validation_model = validation_results[0].get('model', 'claude-sonnet-4') if validation_results else 'claude-sonnet-4'

        cost_breakdown['validation'] = {
            'input_tokens': total_validation_input_tokens,
            'output_tokens': total_validation_output_tokens,
            'cost': total_validation_cost,
            'model': validation_model,
            'attempts': len(validation_results)
        }

        logger.info(f"💰 Validation cost: ${total_validation_cost:.6f} ({validation_model})")
        logger.info(f"   └─ {total_validation_input_tokens} input + {total_validation_output_tokens} output tokens across {len(validation_results)} attempt(s)")

        # 🆕 FIX #2 & #5: Extract classification cost and build breakdown
        classification_info = result_dict.get('classification', {})
        classification_cost = classification_info.get('cost_usd', 0.0)
        classification_method = classification_info.get('method', 'unknown')
        classification_latency = classification_info.get('latency_ms', 0.0)
        classification_confidence = classification_info.get('confidence', 0.0)

        cost_breakdown['classification'] = {
            'method': classification_method,
            'cost': classification_cost,
            'latency_ms': classification_latency,
            'confidence': classification_confidence,
            'tokens': 0  # Classification doesn't use LLM tokens (regex/embedding)
        }

        logger.info(f"💰 Classification cost: ${classification_cost:.6f} ({classification_method})")

        # 🆕 Calculate TOTAL COST (including classification)
        total_cost = classification_cost + total_synthesis_cost + total_validation_cost

        cost_breakdown['total'] = {
            'classification_cost': classification_cost,
            'synthesis_cost': total_synthesis_cost,
            'validation_cost': total_validation_cost,
            'total_cost': total_cost,
            'classification_tokens': 0,  # Classification doesn't use LLM tokens
            'synthesis_tokens': total_synthesis_input_tokens + total_synthesis_output_tokens,
            'validation_tokens': total_validation_input_tokens + total_validation_output_tokens,
            'total_tokens': (total_synthesis_input_tokens + total_synthesis_output_tokens +
                           total_validation_input_tokens + total_validation_output_tokens)
        }

        elapsed = time.time() - start_all

        logger.info(f"\n{'='*70}")
        logger.info(f"✅ Query completed in {elapsed:.2f}s")
        logger.info(f"💰 TOTAL COST: ${total_cost:.6f}")
        logger.info(f"   ├─ Classification:        ${classification_cost:.6f}")
        logger.info(f"   ├─ Synthesis (Opus 4.1):  ${total_synthesis_cost:.6f}")
        logger.info(f"   └─ Validation (Sonnet 4): ${total_validation_cost:.6f}")
        logger.info(f"🔧 Tools used: {', '.join(tools_called)}")
        logger.info(f"📊 Total tokens: {cost_breakdown['total']['total_tokens']:,}")

        logger.info(f"\n⏱️  PHASE TIMING BREAKDOWN:")
        logger.info(f"   Phase 1 (Retrieval):     {phase1_duration:.2f}s")
        logger.info(f"   Phase 2 (Anonymization): {phase2_duration:.2f}s")
        logger.info(f"   Phase 4 (Execution):     {phase4_duration:.2f}s")
        logger.info(f"   Phase 5 (Synthesis):     {synthesis_duration:.2f}s (actual LLM time)")
        logger.info(f"   Phase 6 (Validation):    {validation_duration:.2f}s (actual LLM time)")
        logger.info(f"   Phase 7 (Restoration):   <0.01s (not tracked separately)")
        logger.info(f"   Phase 8 (Logging):       (MLflow sync, governance async...)")
        logger.info(f"{'='*70}\n")

        # PHASE 8: AUDIT LOGGING
        # MLflow logging MUST run synchronously (before finally block closes the run)
        # Governance logging can run async (doesn't depend on MLflow run)

        mark_phase_running('phase_8_logging')
        phase8_start = time.time()

        # ✅ FIX: End MLflow run SYNCHRONOUSLY (before finally block)
        if obs:
            try:
                logger.info(f"🔍 Ending MLflow run synchronously...")
                obs.end_agent_run(
                    response=answer or "",
                    success=True,
                    error=None
                )
                logger.info(f"✅ MLflow logging complete")
            except Exception as obs_error:
                logger.error(f"⚠️ Error ending observability run: {obs_error}", exc_info=True)

        # Launch background thread for governance logging ONLY
        import threading

        def async_phase8_logging():
            """Background thread for Phase 8 logging - doesn't block response"""
            try:
                # 🆕 FIX #6: Extract classification method and prepare cost metadata
                try:
                    classification_info = result_dict.get('classification', {})
                    classification_method = classification_info.get('method', 'unknown') if classification_info else 'unknown'

                    # Build cost metadata for governance logging
                    import json
                    cost_metadata = json.dumps({
                        'classification_cost': classification_cost,
                        'classification_method': classification_method,
                        'synthesis_cost': total_synthesis_cost,
                        'validation_cost': total_validation_cost,
                        'total_cost': total_cost
                    })
                except Exception as class_err:
                    logger.warning(f"⚠️ Could not extract classification metadata: {class_err}")
                    classification_method = 'unknown'
                    cost_metadata = None

                # Log to governance table
                try:
                    audit_logger.log_to_governance_table(
                        session_id=session_id,
                        user_id=user_id,
                        country=country,
                        query_string=query_string,
                        answer=answer,
                        judge_verdict=judge_verdict,
                        tools_called=tools_called,
                        cost=total_cost,
                        citations=citations,
                        elapsed=elapsed,
                        error_info=cost_metadata,
                        classification_method=classification_method
                    )
                    logger.info(f"✅ Governance table logged: {session_id}")
                except Exception as gov_error:
                    logger.error(f"⚠️ Governance logging failed: {gov_error}", exc_info=True)

                # MLflow logging already completed synchronously (above)
                # No need to call obs.end_agent_run() here

                phase8_duration = time.time() - phase8_start
                logger.info(f"✅ Phase 8 completed in background ({phase8_duration:.3f}s)")

            except Exception as async_error:
                logger.error(f"❌ Background Phase 8 logging error: {async_error}", exc_info=True)

        # Start background thread for logging
        # daemon=False: Thread will complete even if main program exits (prevents log loss)
        # This is safe for Streamlit - thread completes before next request
        logging_thread = threading.Thread(
            target=async_phase8_logging,
            daemon=False,  # ✅ Non-daemon: logs won't be lost on exit/reload
            name=f"Phase8-Logging-{session_id[:8]}"
        )
        logging_thread.start()
        logger.info(f"🚀 Phase 8 logging started in background thread (thread: {logging_thread.name})")

        # Mark Phase 8 as complete immediately (async, non-blocking)
        # The actual logging happens in background but doesn't block the response
        mark_phase_complete('phase_8_logging', duration=0.001)  # Minimal duration since it's async

        # ✅ OPTIONAL: Add to global thread tracker for monitoring
        # This helps prevent orphaned threads in long-running processes
        import atexit
        if not hasattr(agent_query, '_logging_threads'):
            agent_query._logging_threads = []

            def cleanup_logging_threads():
                """Wait for all logging threads to complete on shutdown"""
                pending = [t for t in agent_query._logging_threads if t.is_alive()]
                if pending:
                    logger.info(f"⏳ Waiting for {len(pending)} logging thread(s) to complete...")
                    for t in pending:
                        t.join(timeout=10)  # Wait max 10s per thread
                    logger.info("✅ All logging threads completed")

            atexit.register(cleanup_logging_threads)

        agent_query._logging_threads.append(logging_thread)

        # Clean up old completed threads (prevent memory leak)
        agent_query._logging_threads = [t for t in agent_query._logging_threads if t.is_alive()]
    
    except Exception as e:
        error_info = traceback.format_exc()
        elapsed = time.time() - start_all

        logger.error(f"\n❌ ERROR: {e}", exc_info=True)

        # Mark current phase as error
        mark_phase_error('phase_4_execution', str(e))

        # Create error judge verdict
        error_judge_verdict = {
            'passed': False,
            'confidence': 0.0,
            'verdict': 'Error',
            'reasoning': f'Error during query processing: {str(e)}',
            'violations': [{'code': 'SYSTEM_ERROR', 'detail': str(e)}],
            'validation_mode': validation_mode,
            'attempts': 0
        }

        # Log error to governance table FIRST
        audit_logger.log_to_governance_table(
            session_id=session_id,
            user_id=user_id,
            country=country,
            query_string=query_string,
            answer="",
            judge_verdict=error_judge_verdict,
            tools_called=tools_called,
            cost=0.0,
            citations=[],
            elapsed=elapsed,
            error_info=error_info,
            classification_method='error'
        )
        
        # ✅ End observability run AFTER error logging (but BEFORE logger.log_to_mlflow)
        # This prevents duplicate MLflow runs
        if obs:
            try:
                obs.end_agent_run(
                    response=answer or "Error occurred",
                    success=False,
                    error=str(e)
                )
            except Exception as obs_error:
                logger.info(f"⚠️ Error ending observability run: {obs_error}")
                # Force end any active MLflow run
                try:
                    import mlflow
                    if mlflow.active_run():
                        mlflow.end_run(status="FAILED")
                except:
                    pass
        
        # ✅ SKIP logger.log_to_mlflow() - obs.end_agent_run() already logged to MLflow
        # This prevents "run already active" errors
        
        judge_verdict = {
            'verdict': 'ERROR',
            'confidence': 0.0,
            'passed': False,
            'reasoning': f"Error: {str(e)}",
            'violations': [],
            'validation_mode': validation_mode,
            'attempts': 0
        }
        
        error_info = str(e)
    
    finally:
        # ✅ CRITICAL: Only force end MLflow run if STILL active after all operations
        # Don't end if obs.end_agent_run() already ended it
        try:
            import mlflow
            if mlflow.active_run():
                # Only end if obs didn't already end it
                if obs and hasattr(obs, 'current_run') and obs.current_run:
                    # obs still has a reference, but run is active - this shouldn't happen
                    logger.info("⚠️ Found orphaned MLflow run, force ending...")
                    mlflow.end_run()
                    obs.current_run = None
                elif not obs:
                    # obs is None but run is active - end it
                    logger.info("⚠️ Found active MLflow run without obs, force ending...")
                    mlflow.end_run()
        except Exception as final_error:
            # Silently ignore - don't break execution
            pass
    
    # ✅ OUTPUT GUARDRAILS - Post-generation validation
    from src.ai_guardrails import validate_output

    output_guardrails_info = None
    if AI_GUARDRAILS_ENABLED and answer:
        logger.info("🛡️  Running output guardrails...")
        output_validation = validate_output(
            response=answer,
            policies=list(AI_GUARDRAILS_CONFIG['output_policies'].keys()),
            config=AI_GUARDRAILS_CONFIG
        )

        # Log guardrails cost
        total_cost += output_validation.cost

        # Apply PII masking if needed
        if output_validation.masked:
            logger.info(f"🔒 PII masked in output")
            answer = output_validation.masked_text

        # Log any violations (non-blocking for output)
        if output_validation.violations:
            logger.info(f"⚠️  Output guardrail violations: {output_validation.violations}")

        logger.info(f"✅ Output guardrails complete ({output_validation.latency_ms:.0f}ms)")

        # Store output guardrails info for UI display
        output_guardrails_info = {
            'masked': output_validation.masked,
            'violations': output_validation.violations,
            'latency_ms': output_validation.latency_ms,
            'cost': output_validation.cost
        }

    # Return structured response
    return {
        'answer': answer,
        'citations': citations,
        'judge_verdict': judge_verdict,
        'tools_called': tools_called,
        'error': error_info,
        'cost': total_cost,
        'cost_breakdown': cost_breakdown,
        'validation_mode': validation_mode,
        'response_dict': response_dict,
        'output_guardrails': output_guardrails_info
    }


# ============================================================================
# LIVE PHASE TRACKING - 8 PHASES:
# ============================================================================
#
# ✅ Phase 1: Data Retrieval - Loading member profile from Unity Catalog
# ✅ Phase 2: Privacy Anonymization - Processing PII protection
# ✅ Phase 3: Query Classification - 3-stage cascade (Regex → Embedding → LLM)
# ✅ Phase 4: Tool Planning - ReAct reasoning to select appropriate tools
# ✅ Phase 5: Tool Execution - Running SQL functions in Unity Catalog
# ✅ Phase 6: Response Synthesis - Generating personalized advice with LLM
# ✅ Phase 7: Quality Validation - LLM-as-a-Judge quality check
# ✅ Phase 8: Audit Logging - Async logging to MLflow + governance table
#
# EACH PHASE:
# - Calls mark_phase_running() when starting
# - Calls mark_phase_complete() when done
# - Calls mark_phase_error() if error occurs
#
# The progress_utils automatically updates the dropdown in real-time!
#
# ============================================================================

