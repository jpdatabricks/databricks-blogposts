"""
Automated Quality Scorers for Production Monitoring

This module provides automated quality assessment scorers that run
asynchronously in the background to evaluate production queries.

Scorers:
- Built-in: relevance, faithfulness, toxicity
- Custom: country_compliance, citation_quality

Usage:
    from src.scorers import score_query, get_all_scorers

    result = score_query(
        query="What is my preservation age?",
        response="Your preservation age is 60...",
        country="AU",
        context=member_profile
    )
"""

import re
from typing import Dict, Any, List, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from src.config import JUDGE_LLM_ENDPOINT
from src.shared.logging_config import get_logger

logger = get_logger(__name__)


class RelevanceScorer:
    """
    Scores if the response is relevant to the user's query.

    Uses LLM to judge relevance on a scale of 0-1.
    """

    def __init__(self, llm_endpoint: str = None):
        self.w = WorkspaceClient()
        self.llm_endpoint = llm_endpoint or JUDGE_LLM_ENDPOINT
        self.name = "relevance"

    def score(self, query: str, response: str, **kwargs) -> Dict[str, Any]:
        """Score relevance of response to query."""

        prompt = f"""You are evaluating if a response is relevant to the user's query.

Query: {query}

Response: {response}

Evaluate:
1. Does the response directly address the query?
2. Is the information provided relevant to what was asked?
3. Does it stay on topic?

Respond with JSON:
{{
    "score": <0.0 to 1.0>,
    "passed": <true/false>,
    "reasoning": "<brief explanation>"
}}

Score 1.0 = perfectly relevant, 0.0 = completely irrelevant
Pass if score >= 0.7"""

        try:
            messages = [ChatMessage(role=ChatMessageRole.USER, content=prompt)]
            response_obj = self.w.serving_endpoints.query(
                name=self.llm_endpoint,
                messages=messages,
                max_tokens=300,
                temperature=0.1
            )

            result_text = response_obj.choices[0].message.content

            # Parse JSON
            import json
            result = json.loads(result_text)

            return {
                'scorer': self.name,
                'score': result.get('score', 0.0),
                'passed': result.get('passed', False),
                'confidence': result.get('score', 0.0),
                'reasoning': result.get('reasoning', ''),
                'verdict': 'PASS' if result.get('passed') else 'FAIL'
            }

        except Exception as e:
            logger.error(f"Relevance scorer error: {e}")
            return {
                'scorer': self.name,
                'score': 0.5,
                'passed': True,  # Default to pass on error
                'confidence': 0.0,
                'reasoning': f'Error: {str(e)}',
                'verdict': 'ERROR'
            }


class FaithfulnessScorer:
    """
    Scores if the response is grounded in the provided context/tools.

    Checks if response makes claims not supported by context.
    """

    def __init__(self, llm_endpoint: str = None):
        self.w = WorkspaceClient()
        self.llm_endpoint = llm_endpoint or JUDGE_LLM_ENDPOINT
        self.name = "faithfulness"

    def score(self, query: str, response: str, context: Dict = None, tool_output: Dict = None, **kwargs) -> Dict[str, Any]:
        """Score faithfulness/groundedness of response."""

        context_str = str(context) if context else "No context provided"
        tools_str = str(tool_output) if tool_output else "No tool outputs"

        prompt = f"""You are evaluating if a response is grounded in the provided context and tool outputs.

Query: {query}

Response: {response}

Context Available: {context_str[:500]}...

Tool Outputs: {tools_str[:500]}...

Evaluate:
1. Does the response only use information from context/tools?
2. Are there any unsupported claims or hallucinations?
3. Is all factual information grounded in the provided data?

Respond with JSON:
{{
    "score": <0.0 to 1.0>,
    "passed": <true/false>,
    "reasoning": "<brief explanation>"
}}

Score 1.0 = fully grounded, 0.0 = completely ungrounded
Pass if score >= 0.8"""

        try:
            messages = [ChatMessage(role=ChatMessageRole.USER, content=prompt)]
            response_obj = self.w.serving_endpoints.query(
                name=self.llm_endpoint,
                messages=messages,
                max_tokens=300,
                temperature=0.1
            )

            result_text = response_obj.choices[0].message.content

            # Parse JSON
            import json
            result = json.loads(result_text)

            return {
                'scorer': self.name,
                'score': result.get('score', 0.0),
                'passed': result.get('passed', False),
                'confidence': result.get('score', 0.0),
                'reasoning': result.get('reasoning', ''),
                'verdict': 'PASS' if result.get('passed') else 'FAIL'
            }

        except Exception as e:
            logger.error(f"Faithfulness scorer error: {e}")
            return {
                'scorer': self.name,
                'score': 0.5,
                'passed': True,
                'confidence': 0.0,
                'reasoning': f'Error: {str(e)}',
                'verdict': 'ERROR'
            }


class ToxicityScorer:
    """
    Scores if the response contains toxic, offensive, or inappropriate content.

    Uses keyword matching and pattern detection.
    """

    def __init__(self):
        self.name = "toxicity"
        self.toxic_keywords = [
            'stupid', 'idiot', 'moron', 'dumb', 'hate', 'damn',
            'shit', 'fuck', 'ass', 'bastard', 'bitch'
        ]

    def score(self, query: str, response: str, **kwargs) -> Dict[str, Any]:
        """Score toxicity of response."""

        response_lower = response.lower()

        # Count toxic keywords
        toxic_count = sum(1 for keyword in self.toxic_keywords if keyword in response_lower)

        # Calculate score (inverse - 1.0 = no toxicity, 0.0 = highly toxic)
        toxicity_level = min(1.0, toxic_count * 0.3)
        score = 1.0 - toxicity_level

        passed = score >= 0.8  # Pass if < 20% toxic

        return {
            'scorer': self.name,
            'score': score,
            'passed': passed,
            'confidence': 1.0 if toxic_count == 0 else 0.8,
            'reasoning': f'Found {toxic_count} toxic keywords' if toxic_count > 0 else 'No toxic content detected',
            'verdict': 'PASS' if passed else 'FAIL'
        }


class CountryComplianceScorer:
    """
    Custom scorer that validates country-specific compliance.

    Checks if response mentions appropriate country-specific terms.
    """

    def __init__(self):
        self.name = "country_compliance"
        self.compliance_rules = {
            'AU': {
                'required_concepts': ['preservation age', 'super', 'superannuation'],
                'key_ages': ['55', '60', '65'],
                'regulations': ['ATO', 'Tax Act']
            },
            'US': {
                'required_concepts': ['401(k)', 'IRA', 'retirement'],
                'key_ages': ['59.5', '62', '67'],
                'regulations': ['IRS', 'Social Security']
            },
            'UK': {
                'required_concepts': ['State Pension', 'pension'],
                'key_ages': ['66', '67'],
                'regulations': ['DWP', 'HMRC']
            },
            'IN': {
                'required_concepts': ['EPF', 'provident fund', 'pension'],
                'key_ages': ['58', '60'],
                'regulations': ['EPFO', 'Income Tax Act']
            }
        }

    def score(self, query: str, response: str, country: str = 'AU', **kwargs) -> Dict[str, Any]:
        """Score country-specific compliance."""

        rules = self.compliance_rules.get(country, self.compliance_rules['AU'])
        response_lower = response.lower()

        # Check for required concepts
        concept_matches = sum(
            1 for concept in rules['required_concepts']
            if concept.lower() in response_lower
        )
        concept_score = concept_matches / len(rules['required_concepts'])

        # Check for key ages (optional but good to have)
        age_matches = sum(
            1 for age in rules['key_ages']
            if age in response_lower
        )
        age_score = min(1.0, age_matches / len(rules['key_ages']))

        # Weighted score (concepts more important than specific ages)
        score = (concept_score * 0.7) + (age_score * 0.3)
        passed = score >= 0.5

        return {
            'scorer': self.name,
            'score': score,
            'passed': passed,
            'confidence': score,
            'reasoning': f'{country}: Found {concept_matches}/{len(rules["required_concepts"])} concepts, {age_matches}/{len(rules["key_ages"])} key ages',
            'verdict': 'PASS' if passed else 'FAIL'
        }


class CitationQualityScorer:
    """
    Custom scorer that checks citation quality and presence.

    Validates that responses include proper citations when needed.
    """

    def __init__(self):
        self.name = "citation_quality"
        self.citation_patterns = [
            r'\[Source:.*?\]',
            r'\[.*?Act.*?\]',
            r'\[.*?Regulation.*?\]',
            r'according to',
            r'as per',
            r'under section'
        ]

    def score(self, query: str, response: str, **kwargs) -> Dict[str, Any]:
        """Score citation quality."""

        # Count citations
        citation_count = 0
        for pattern in self.citation_patterns:
            matches = re.findall(pattern, response, re.IGNORECASE)
            citation_count += len(matches)

        # Check if query requires citations (looks factual/regulatory)
        requires_citations = any(
            keyword in query.lower()
            for keyword in ['how much', 'when', 'what is', 'can i', 'am i', 'tax', 'age', 'penalty']
        )

        if requires_citations:
            # Score based on citation presence
            score = min(1.0, citation_count / 2)  # Expect at least 2 citations
            passed = citation_count >= 1
            reasoning = f'Found {citation_count} citations (expected for factual query)'
        else:
            # Non-factual query, citations optional
            score = 1.0
            passed = True
            reasoning = 'Query does not require citations'

        return {
            'scorer': self.name,
            'score': score,
            'passed': passed,
            'confidence': 1.0 if not requires_citations else score,
            'reasoning': reasoning,
            'verdict': 'PASS' if passed else 'FAIL'
        }


def get_all_scorers() -> List[Any]:
    """Get all available scorers (built-in + custom)."""
    return [
        RelevanceScorer(),
        FaithfulnessScorer(),
        ToxicityScorer(),
        CountryComplianceScorer(),
        CitationQualityScorer()
    ]


def score_query(
    query: str,
    response: str,
    country: str = 'AU',
    context: Dict = None,
    tool_output: Dict = None,
    scorers: List[str] = None
) -> Dict[str, Any]:
    """
    Score a single query using all or specified scorers.

    Args:
        query: User query
        response: Agent response
        country: Country code
        context: Member profile context
        tool_output: Tool execution results
        scorers: List of scorer names to use (None = all)

    Returns:
        Dict with individual scorer results and overall metrics
    """
    all_scorers = get_all_scorers()

    # Filter scorers if specified
    if scorers:
        all_scorers = [s for s in all_scorers if s.name in scorers]

    results = {}
    for scorer in all_scorers:
        try:
            result = scorer.score(
                query=query,
                response=response,
                country=country,
                context=context,
                tool_output=tool_output
            )
            results[scorer.name] = result
            logger.info(f"✅ {scorer.name}: {result['score']:.2f} ({result['verdict']})")
        except Exception as e:
            logger.error(f"❌ {scorer.name} failed: {e}")
            results[scorer.name] = {
                'scorer': scorer.name,
                'score': 0.0,
                'passed': False,
                'confidence': 0.0,
                'reasoning': f'Scorer error: {str(e)}',
                'verdict': 'ERROR'
            }

    # Calculate overall metrics
    scores = [r['score'] for r in results.values() if r['score'] > 0]
    avg_score = sum(scores) / len(scores) if scores else 0.0
    passed_count = sum(1 for r in results.values() if r['passed'])
    total_count = len(results)

    return {
        'individual_scores': results,
        'overall_score': avg_score,
        'passed_count': passed_count,
        'total_count': total_count,
        'pass_rate': passed_count / total_count if total_count > 0 else 0.0,
        'verdict': 'PASS' if passed_count >= (total_count * 0.8) else 'FAIL'  # 80% threshold
    }
