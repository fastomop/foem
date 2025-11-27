import json
import os
import time
from typing import Any, Dict, List
from openai import AzureOpenAI
from dotenv import load_dotenv


def load_json_file(file_path: str) -> Any:
    """Load and parse a JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def normalize_execution_result(result: Any) -> Any:
    """
    Normalize execution result to a comparable format.
    Handles various result formats like [[4]], [4], 4, etc.
    """
    if result is None:
        return None

    # If it's already a simple value, return it
    if isinstance(result, (int, float, str)):
        return result

    # If it's a nested list structure, try to flatten if it's a single value
    if isinstance(result, list):
        if len(result) == 0:
            return None
        if len(result) == 1:
            # Single row result
            if isinstance(result[0], list):
                if len(result[0]) == 1:
                    # [[value]] -> value
                    return result[0][0]
                else:
                    # [[val1, val2, ...]] -> keep as is for comparison
                    return result[0]
            else:
                # [value] -> value
                return result[0]
        # Multiple rows, keep as is
        return result

    return result


def format_execution_result_for_display(result: Any) -> str:
    """Format execution result for human-readable display."""
    normalized = normalize_execution_result(result)

    if normalized is None:
        return "No results"

    if isinstance(normalized, (int, float)):
        return str(normalized)

    if isinstance(normalized, list):
        # Format as a simple list or table
        if all(isinstance(item, (int, float, str)) for item in normalized):
            return str(normalized)
        else:
            return json.dumps(normalized, indent=2)

    return str(normalized)


class LLMEvaluator:
    """Evaluates if LLM responses match execution results using another LLM."""

    def __init__(self, azure_endpoint: str, api_key: str, api_version: str, model_name: str):
        """
        Initialize the LLM evaluator with Azure OpenAI.

        Args:
            azure_endpoint: Azure OpenAI endpoint URL
            api_key: Azure OpenAI API key
            api_version: API version
            model_name: Model deployment name
        """
        self.client = AzureOpenAI(
            azure_endpoint=azure_endpoint,
            api_key=api_key,
            api_version=api_version
        )
        self.model_name = model_name

    def evaluate_response(self, query: str, execution_result: Any, response: str) -> Dict[str, Any]:
        """
        Evaluate if the response correctly represents the execution result.

        Args:
            query: The original query
            execution_result: The actual execution result from the database
            response: The LLM's natural language response

        Returns:
            Dictionary with evaluation results:
            - correct: bool - whether the response is correct
            - confidence: str - high/medium/low
            - explanation: str - why it's correct or incorrect
            - extracted_value: Any - what value the LLM extracted from the response
        """
        formatted_result = format_execution_result_for_display(execution_result)

        evaluation_prompt = f"""You are evaluating whether an LLM's natural language response correctly represents the actual database query execution result.

Original Query: {query}

Actual Execution Result: {formatted_result}

LLM's Response: {response}

Your task:
1. Determine if the LLM's response correctly conveys the information from the execution result
2. Extract the key numeric value(s) or information from the response
3. Compare it with the actual execution result
4. Provide a judgment

Respond in JSON format:
{{
    "correct": true/false,
    "confidence": "high"/"medium"/"low",
    "extracted_value": <the value you extracted from the response>,
    "explanation": "<brief explanation of why it's correct or incorrect>"
}}

Important:
- If the execution result is a single number (e.g., 4), check if the response correctly states that number
- If the response lists separate counts when a combined count was expected, mark as incorrect
- If the response says "zero" or "no patients" when the result shows patients exist, mark as incorrect
- If the response correctly interprets the result in context, mark as correct
- Be strict: the response must accurately represent the execution result
- SEMANTIC EQUIVALENCE: Consider these terms as equivalent when comparing values:
  * "Unknown", "No matching concept", "Not specified", "No information", "N/A"
  * "Male", "MALE", "M", "male"
  * "Female", "FEMALE", "F", "female"
  * Other semantic equivalents in medical/database contexts
- Focus on whether the MEANING and NUMERIC VALUES are correct, not just exact string matching

Respond ONLY with valid JSON, no additional text."""

        try:
            completion = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": "You are a precise evaluator that checks if natural language responses correctly represent database query results. Always respond with valid JSON."},
                    {"role": "user", "content": evaluation_prompt}
                ],
                temperature=0.0,
                response_format={"type": "json_object"}
            )

            result_text = completion.choices[0].message.content
            evaluation = json.loads(result_text)

            return {
                'correct': evaluation.get('correct', False),
                'confidence': evaluation.get('confidence', 'low'),
                'extracted_value': evaluation.get('extracted_value'),
                'explanation': evaluation.get('explanation', 'No explanation provided')
            }

        except Exception as e:
            return {
                'correct': False,
                'confidence': 'low',
                'extracted_value': None,
                'explanation': f'Error during evaluation: {str(e)}',
                'error': str(e)
            }


def compare_results_with_llm(results_path: str, evaluator: LLMEvaluator,
                             delay_seconds: float = 0.5, batch_size: int = 50) -> Dict[str, Any]:
    """
    Compare execution results against responses using LLM evaluation.

    Args:
        results_path: Path to results.json (contains both execution_result and response)
        evaluator: LLMEvaluator instance
        delay_seconds: Delay between API calls to avoid rate limits
        batch_size: Number of evaluations before longer delay

    Returns:
        Dictionary containing comparison statistics and detailed results
    """
    print("Loading results.json...")
    results_data = load_json_file(results_path)
    results = results_data.get('results', [])

    print(f"Found {len(results)} entries in results.json")
    print(f"Starting LLM-based evaluation (this may take a while)...\n")

    # Comparison results
    comparisons = []
    correct_responses = 0
    incorrect_responses = 0
    evaluation_errors = 0

    # Progress tracking
    start_time = time.time()

    # Process each result
    for idx, result in enumerate(results, 1):
        query_id = result.get('query_id')
        query = result.get('query', '')
        response = result.get('response', '')

        # Get execution result from metadata (ground truth)
        metadata = result.get('metadata', {})
        execution_result = metadata.get('execution_result')

        if execution_result is None:
            evaluation_errors += 1
            comparisons.append({
                'id': query_id,
                'query': query,
                'execution_result': None,
                'response': response,
                'correct': False,
                'confidence': 'low',
                'explanation': 'No execution result found in metadata',
                'error': 'Missing execution_result'
            })
            continue

        # Evaluate using LLM
        evaluation = evaluator.evaluate_response(query, execution_result, response)

        if evaluation.get('correct'):
            correct_responses += 1
        else:
            incorrect_responses += 1

        # Store comparison
        comparisons.append({
            'id': query_id,
            'query': query,
            'execution_result': normalize_execution_result(execution_result),
            'response': response,
            'correct': evaluation['correct'],
            'confidence': evaluation['confidence'],
            'extracted_value': evaluation.get('extracted_value'),
            'explanation': evaluation['explanation']
        })

        # Progress update
        if idx % 10 == 0:
            elapsed = time.time() - start_time
            rate = idx / elapsed
            remaining = (len(results) - idx) / rate if rate > 0 else 0
            print(f"Progress: {idx}/{len(results)} ({idx/len(results)*100:.1f}%) - "
                  f"Correct: {correct_responses}, Incorrect: {incorrect_responses}, "
                  f"ETA: {remaining:.0f}s")

        # Rate limiting
        if idx % batch_size == 0:
            print(f"  Batch complete, waiting 10s to respect rate limits...")
            time.sleep(10)
        else:
            time.sleep(delay_seconds)

    # Calculate statistics
    total_evaluated = correct_responses + incorrect_responses
    accuracy = (correct_responses / total_evaluated * 100) if total_evaluated > 0 else 0

    # Calculate confidence breakdown
    """
    - High: The evaluator is very certain about its judgment
        - Clear match or clear mismatch
        - Unambiguous comparison
    - Medium: Some uncertainty in the evaluation
        - Ambiguous response formatting
        - Complex result structures
        - Unclear wording
    - Low: Difficult to determine correctness
        - Very unclear responses
        - Hard to extract meaningful comparison
    """
    high_confidence = len([c for c in comparisons if c.get('confidence') == 'high'])
    medium_confidence = len([c for c in comparisons if c.get('confidence') == 'medium'])
    low_confidence = len([c for c in comparisons if c.get('confidence') == 'low'])

    return {
        'summary': {
            'total_entries': len(results),
            'total_evaluated': total_evaluated,
            'correct_responses': correct_responses,
            'incorrect_responses': incorrect_responses,
            'evaluation_errors': evaluation_errors,
            'accuracy': accuracy,
            'confidence_breakdown': {
                'high': high_confidence,
                'medium': medium_confidence,
                'low': low_confidence
            }
        },
        'comparisons': comparisons
    }


def print_summary(comparison_data: Dict[str, Any]):
    """Print a summary of the comparison results."""
    summary = comparison_data['summary']

    print("\n" + "="*60)
    print("LLM EVALUATION SUMMARY")
    print("="*60)
    print(f"Total entries: {summary['total_entries']}")
    print(f"Total evaluated: {summary['total_evaluated']}")
    print(f"\nCorrect responses: {summary['correct_responses']}")
    print(f"Incorrect responses: {summary['incorrect_responses']}")
    print(f"Evaluation errors: {summary['evaluation_errors']}")
    print(f"\nAccuracy: {summary['accuracy']:.2f}%")
    print(f"\nConfidence breakdown:")
    print(f"  High: {summary['confidence_breakdown']['high']}")
    print(f"  Medium: {summary['confidence_breakdown']['medium']}")
    print(f"  Low: {summary['confidence_breakdown']['low']}")
    print("="*60)


def print_incorrect_responses(comparison_data: Dict[str, Any], limit: int = 10):
    """Print details of incorrect responses."""
    incorrect = [c for c in comparison_data['comparisons'] if not c.get('correct', False)]

    if not incorrect:
        print("\nNo incorrect responses found!")
        return

    print(f"\n{'='*60}")
    print(f"INCORRECT RESPONSES (showing first {min(limit, len(incorrect))} of {len(incorrect)})")
    print("="*60)

    for i, item in enumerate(incorrect[:limit], 1):
        print(f"\n{i}. ID: {item['id']}")
        print(f"   Query: {item['query']}")
        print(f"   Execution Result: {item['execution_result']}")
        print(f"   Extracted Value: {item.get('extracted_value')}")
        print(f"   Confidence: {item['confidence']}")
        print(f"   Explanation: {item['explanation']}")
        print(f"   Response: {item['response']}")


def save_detailed_report(comparison_data: Dict[str, Any], output_path: str):
    """Save detailed comparison report to a JSON file."""
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(comparison_data, f, indent=2, ensure_ascii=False)
    print(f"\nDetailed report saved to: {output_path}")


def save_incorrect_queries(comparison_data: Dict[str, Any], output_path: str):
    """Save only incorrect queries to a separate JSON file for easy analysis."""
    incorrect = [c for c in comparison_data['comparisons'] if not c.get('correct', False)]

    if not incorrect:
        print("\nNo incorrect queries to save!")
        return

    incorrect_report = {
        'summary': {
            'total_incorrect': len(incorrect),
            'accuracy': comparison_data['summary']['accuracy'],
            'total_evaluated': comparison_data['summary']['total_evaluated']
        },
        'incorrect_queries': incorrect
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(incorrect_report, f, indent=2, ensure_ascii=False)
    print(f"Incorrect queries saved to: {output_path} ({len(incorrect)} entries)")


def main():
    """Main function to run the LLM-based comparison."""
    # Load environment variables from .env file
    load_dotenv()

    # Hardcoded configuration
    results_path = 'output/results.json'
    output_path = 'output/llm_comparison_report.json'
    incorrect_output_path = 'output/incorrect_queries.json'
    show_incorrect = 10
    azure_endpoint = 'https://openai-omop-dev-01.openai.azure.com/'
    api_key = os.getenv('AZURE_OPENAI_API_KEY')
    api_version = '2025-01-01-preview'
    model_name = 'gpt-4.1'
    delay_seconds = 0.5

    # Validate API key is provided
    if not api_key:
        raise ValueError("Azure OpenAI API key is required. Please set AZURE_OPENAI_API_KEY in .env file.")

    # Initialize evaluator
    print("Initializing LLM evaluator...")
    evaluator = LLMEvaluator(
        azure_endpoint=azure_endpoint,
        api_key=api_key,
        api_version=api_version,
        model_name=model_name
    )

    # Run comparison
    comparison_data = compare_results_with_llm(
        results_path=results_path,
        evaluator=evaluator,
        delay_seconds=delay_seconds
    )

    # Print summary
    print_summary(comparison_data)

    # Print incorrect responses
    print_incorrect_responses(comparison_data, limit=show_incorrect)

    # Save detailed report
    save_detailed_report(comparison_data, output_path)

    # Save incorrect queries to separate file
    save_incorrect_queries(comparison_data, incorrect_output_path)


if __name__ == '__main__':
    main()
