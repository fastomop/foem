from foem import SqlTest
import os
import json
from decimal import Decimal
import sys
import time
import re


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles Decimal objects"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)


def write_output(data):
    output_dir = os.path.join(os.getcwd(), "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.join(output_dir, "dataset.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, cls=DecimalEncoder)


if __name__ == "__main__":
    test_generator = SqlTest()

    # Wrap the _run_query method to log queries
    original_run_query = test_generator._run_query

    def logged_run_query(query, params=None):
        # Clean up query for display
        clean_query = re.sub(r'\s+', ' ', query).strip()
        query_preview = clean_query[:150] + "..." if len(clean_query) > 150 else clean_query

        print(f"\n    [SQL] Executing: {query_preview}")
        start_time = time.time()

        try:
            result = original_run_query(query, params)
            duration = time.time() - start_time
            print(f"    [SQL] ✓ Completed in {duration:.2f}s - {len(result)} rows")
            return result
        except Exception as e:
            duration = time.time() - start_time
            print(f"    [SQL] ✗ Failed after {duration:.2f}s - {str(e)[:100]}")
            raise

    test_generator._run_query = logged_run_query

    funcs = [
        ("patients_group_by_gender_and_ethn", test_generator.patients_group_by_gender_and_ethn),
        ("patients_group_by_race", test_generator.patients_group_by_race),
        ("patients_2drugs_and_time", test_generator.patients_2drugs_and_time),
        ("patients_2drugs_and", test_generator.patients_2drugs_and),
        ("patients_2drugs_or", test_generator.patients_2drugs_or),
        ("patients_4drugs_and_time", test_generator.patients_4drugs_and_time),
        ("patients_4drugs_and", test_generator.patients_4drugs_and),
        ("patients_4drugs_or", test_generator.patients_4drugs_or),
        ("patients_3drugs_and_time", test_generator.patients_3drugs_and_time),
        ("patients_3drugs_and", test_generator.patients_3drugs_and),
        ("patients_3drugs_or", test_generator.patients_3drugs_or),
        ("patients_2conditions_and_time", test_generator.patients_2conditions_and_time),
        ("patients_2conditions_and", test_generator.patients_2conditions_and),
        ("patients_2conditions_or", test_generator.patients_2conditions_or),
        ("patients_4conditions_and_time", test_generator.patients_4conditions_and_time),
        ("patients_4conditions_and", test_generator.patients_4conditions_and),
        ("patients_4conditions_or", test_generator.patients_4conditions_or),
        ("patients_3conditions_and_time", test_generator.patients_3conditions_and_time),
        ("patients_3conditions_and", test_generator.patients_3conditions_and),
        ("patients_3conditions_or", test_generator.patients_3conditions_or),
        ("patients_distribution_by_birth", test_generator.patients_distribution_by_birth),
        ("patients_condition_followed_condition", test_generator.patients_condition_followed_condition),
        ("patients_condition_time_condition", test_generator.patients_condition_time_condition),
        ("patients_condition_age", test_generator.patients_condition_age),
        ("patients_condition_race", test_generator.patients_condition_race),
        ("patients_condition_state", test_generator.patients_condition_state),
        ("patients_condition_year", test_generator.patients_condition_year),
        ("patients_drug_time_drug", test_generator.patients_drug_time_drug),
        ("patients_drug_followed_drug", test_generator.patients_drug_followed_drug),
        ("patients_condition_ethnicity", test_generator.patients_condition_ethnicity),
        ("patients_drug_year", test_generator.patients_drug_year),
        ("patients_drug_after_condition", test_generator.patients_drug_after_condition),
        ("patients_drug_time_after_condition", test_generator.patients_drug_time_after_condition),
        ("patients_gender_condition", test_generator.patients_gender_condition),
        ("patients_year", test_generator.patients_year),
        ("patients_gender_state", test_generator.patients_gender_state),
        ("patients_group_by_ethnicity_location", test_generator.patients_group_by_ethnicity_location),
        ("patients_group_by_ethnicity_birth", test_generator.patients_group_by_ethnicity_birth),
        ("patients_group_by_ethnicity", test_generator.patients_group_by_ethnicity),
        ("patients_group_by_gender", test_generator.patients_group_by_gender),
        ("patients_group_by_race_ethnicity", test_generator.patients_group_by_race_ethnicity),
        ("patients_grouped_by_race_gender", test_generator.patients_grouped_by_race_gender),
        ("patients_group_by_race_location", test_generator.patients_group_by_race_location),
        ("patients_group_by_race_birth", test_generator.patients_group_by_race_birth),
        ("patients_group_by_location", test_generator.patients_group_by_location),
        ("patients_group_by_birth_gender", test_generator.patients_group_by_birth_gender),
        ("patients_group_by_birth_location", test_generator.patients_group_by_birth_location),
        ("patients_count", test_generator.patients_count),
        ("patients_count_by_ethnicity", test_generator.patients_count_by_ethnicity),
        ("patients_count_by_race", test_generator.patients_count_by_race),
        ("patients_count_by_gender", test_generator.patients_count_by_gender),
        ("patients_drug", test_generator.patients_drug),
        ("patients_condition", test_generator.patients_condition),
        ("patients_count_by_location", test_generator.patients_count_by_location),
        ("patients_condition_group_by_year", test_generator.patients_condition_group_by_year),
        ("patients_drug_group_by_year", test_generator.patients_drug_group_by_year),
    ]

    total_funcs = len(funcs)
    results = []

    # Limit to ~1000 total test cases distributed across functions
    MAX_TOTAL_CASES = 1000
    MAX_PER_FUNCTION = max(5, MAX_TOTAL_CASES // total_funcs)  # ~18 per function, minimum 5

    print(f"Starting dataset generation with {total_funcs} test functions...")
    print(f"Target: ~{MAX_TOTAL_CASES} total test cases (max {MAX_PER_FUNCTION} per function)")
    print("=" * 70)

    for idx, (name, func) in enumerate(funcs, 1):
        try:
            print(f"[{idx}/{total_funcs}] Running: {name}...", end=" ", flush=True)
            result = func()
            if result:
                # Limit results per function to avoid generating too many
                limited_result = result[:MAX_PER_FUNCTION]
                count = len(limited_result)
                original_count = len(result)
                results.extend(limited_result)

                if original_count > MAX_PER_FUNCTION:
                    print(f"✓ Generated {count} test case(s) (limited from {original_count}). Total: {len(results)}")
                else:
                    print(f"✓ Generated {count} test case(s). Total: {len(results)}")
            else:
                print(f"⚠ No results")
        except Exception as e:
            print(f"✗ Error: {str(e)[:50]}")
            # Rollback the transaction to recover from errors
            test_generator.conn.rollback()
            continue

    print("=" * 70)
    print(f"Dataset generation complete!")
    print(f"Total test cases: {len(results)}")
    print(f"Writing to output/dataset.json...")

    write_output(results)

    print(f"✓ Done! Dataset saved to output/dataset.json")
    test_generator.close()
