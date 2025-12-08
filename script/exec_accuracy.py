#!/usr/bin/env python3
"""
Script to generate execution accuracy summary from dataset_results.json

This script compares the execution_result against the response to evaluate
whether the LLM correctly extracted and reported the value from the query execution.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


def extract_numbers_from_text(text: str) -> List[float]:
    """Extract all numeric values from text, handling commas and decimals."""
    text = text.replace(',', '')
    pattern = r'-?\d+\.?\d*'
    matches = re.findall(pattern, text)
    return [float(m) for m in matches if m]


def extract_value_from_execution_result(execution_result: Any) -> Optional[Union[int, float, str]]:
    """
    Extract the primary value from execution_result.

    For single count queries: [[count]] -> count
    For grouped queries: [[val1, val2, count], ...] -> sum or list
    """
    if not execution_result:
        return None

    if isinstance(execution_result, (int, float)):
        return execution_result

    if isinstance(execution_result, list):
        if len(execution_result) == 0:
            return 0

        if len(execution_result) == 1 and len(execution_result[0]) == 1:
            return execution_result[0][0]

        try:
            if all(isinstance(row, list) and len(row) > 1 for row in execution_result):
                total = sum(row[-1] for row in execution_result if isinstance(row[-1], (int, float)))
                return total
            return len(execution_result)
        except (TypeError, IndexError):
            return None

    return None


def evaluate_response(execution_result: Any, response: str) -> Dict[str, Any]:
    """
    Evaluate if the response correctly mentions the execution result value.

    Returns:
        dict with keys: correct (bool), confidence (str), extracted_value (Any), explanation (str)
    """
    expected_value = extract_value_from_execution_result(execution_result)

    error_indicators = [
        "failed", "error", "incorrect", "may not be correct",
        "i will", "i'll", "let me", "please provide",
        "i'm ready", "i can help"
    ]

    response_lower = response.lower()
    has_error_indicator = any(indicator in response_lower for indicator in error_indicators)

    response_numbers = extract_numbers_from_text(response)

    if has_error_indicator and not response_numbers:
        return {
            "correct": False,
            "confidence": "high",
            "extracted_value": None,
            "explanation": "The LLM's response indicates an error or does not provide a numeric answer."
        }

    if expected_value is None:
        return {
            "correct": False,
            "confidence": "low",
            "extracted_value": response_numbers[0] if response_numbers else None,
            "explanation": "Could not extract expected value from execution result."
        }

    if not response_numbers:
        return {
            "correct": False,
            "confidence": "high",
            "extracted_value": None,
            "explanation": f"The LLM's response does not provide any numeric value. Expected: {expected_value}."
        }

    tolerance = 0.01
    for num in response_numbers:
        if abs(num - expected_value) <= tolerance:
            return {
                "correct": True,
                "confidence": "high",
                "extracted_value": num,
                "explanation": f"The LLM correctly reported the value {num} which matches the execution result."
            }

    return {
        "correct": False,
        "confidence": "high",
        "extracted_value": response_numbers[0],
        "explanation": f"The LLM reported {response_numbers[0]} but the execution result was {expected_value}."
    }


def generate_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate accuracy summary from evaluation results."""
    total_entries = len(results)
    correct_responses = sum(1 for r in results if r.get("correct", False))
    incorrect_responses = sum(1 for r in results if not r.get("correct", False))

    confidence_breakdown = {"high": 0, "medium": 0, "low": 0}
    for r in results:
        conf = r.get("confidence", "high")
        confidence_breakdown[conf] = confidence_breakdown.get(conf, 0) + 1

    accuracy = (correct_responses / total_entries * 100) if total_entries > 0 else 0

    return {
        "total_entries": total_entries,
        "total_evaluated": total_entries,
        "correct_responses": correct_responses,
        "incorrect_responses": incorrect_responses,
        "evaluation_errors": 0,
        "accuracy": accuracy,
        "confidence_breakdown": confidence_breakdown
    }


def main():
    """Main function to process dataset_results.json and generate accuracy summary."""
    input_file = Path("output/dataset_results.json")
    output_file = Path("output/accuracy_summary.json")
    incorrect_file = Path("output/incorrect_queries.json")

    if not input_file.exists():
        print(f"Error: {input_file} not found.")
        return

    print(f"Reading {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    results = data.get("results", [])
    print(f"Found {len(results)} query results to evaluate.")

    comparisons = []
    incorrect_comparisons = []

    for result in results:
        query_id = result.get("query_id")
        query = result.get("query", "")
        response = result.get("response", "")
        execution_result = result.get("metadata", {}).get("execution_result")

        evaluation = evaluate_response(execution_result, response)

        comparison = {
            "id": query_id,
            "query": query,
            "execution_result": execution_result,
            "response": response,
            "correct": evaluation["correct"],
            "confidence": evaluation["confidence"],
            "extracted_value": evaluation["extracted_value"],
            "explanation": evaluation["explanation"]
        }
        comparisons.append(comparison)

        if not evaluation["correct"]:
            incorrect_comparisons.append(comparison)

    summary = generate_summary(comparisons)

    output_data = {
        "summary": summary,
        "comparisons": comparisons
    }

    print(f"Writing accuracy summary to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    incorrect_data = {
        "summary": {
            "total_incorrect": len(incorrect_comparisons),
            "percentage_of_total": (len(incorrect_comparisons) / len(comparisons) * 100) if comparisons else 0
        },
        "incorrect_queries": incorrect_comparisons
    }

    print(f"Writing incorrect queries report to {incorrect_file}...")
    with open(incorrect_file, 'w', encoding='utf-8') as f:
        json.dump(incorrect_data, f, indent=2, ensure_ascii=False)

    print("\n" + "="*60)
    print("EXECUTION ACCURACY SUMMARY")
    print("="*60)
    print(f"Total Entries:        {summary['total_entries']}")
    print(f"Total Evaluated:      {summary['total_evaluated']}")
    print(f"Correct Responses:    {summary['correct_responses']}")
    print(f"Incorrect Responses:  {summary['incorrect_responses']}")
    print(f"Accuracy:             {summary['accuracy']:.2f}%")
    print(f"\nConfidence Breakdown:")
    print(f"  High:   {summary['confidence_breakdown']['high']}")
    print(f"  Medium: {summary['confidence_breakdown']['medium']}")
    print(f"  Low:    {summary['confidence_breakdown']['low']}")
    print("="*60)

    print(f"\nFull report saved to: {output_file}")
    print(f"Incorrect queries report saved to: {incorrect_file}")


if __name__ == "__main__":
    main()
