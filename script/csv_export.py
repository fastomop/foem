import json
import csv
import argparse

def export_to_csv(json_file_path, csv_file_path):
    """
    Export dataset.json to CSV format.
    Uses execution_result instead of expected_output.
    """
    # Read the JSON file
    with open(json_file_path, 'r') as f:
        data = json.load(f)

    # Open CSV file for writing
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        # Define CSV headers
        fieldnames = ['id', 'input', 'expected_output']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header
        writer.writeheader()

        # Write each row
        for entry in data:
            # Convert execution_result to a simple format
            execution_result = entry.get('execution_result', [])

            # Handle different types of execution results
            if not execution_result:
                result_value = None
            elif len(execution_result) == 1 and len(execution_result[0]) == 1:
                # Single value result: [[value]] -> value
                result_value = execution_result[0][0]
            else:
                # Multiple rows or multiple columns: keep as JSON string
                result_value = json.dumps(execution_result)

            writer.writerow({
                'id': entry.get('id'),
                'input': entry.get('input'),
                'expected_output': result_value
            })

    print(f"Successfully exported {len(data)} entries to {csv_file_path}")

def export_expected_output_to_csv(json_file_path, csv_file_path):
    """
    Export dataset.json to CSV format using the expected_output field.
    """
    # Read the JSON file
    with open(json_file_path, 'r') as f:
        data = json.load(f)

    # Open CSV file for writing
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        # Define CSV headers
        fieldnames = ['id', 'input', 'expected_output']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header
        writer.writeheader()

        # Write each row
        for entry in data:
            # Use the expected_output field directly
            expected_output = entry.get('expected_output', [])

            # Handle different types of expected output
            if not expected_output:
                result_value = None
            elif len(expected_output) == 1 and len(expected_output[0]) == 1:
                # Single value result: [[value]] -> value
                result_value = expected_output[0][0]
            else:
                # Multiple rows or multiple columns: keep as JSON string
                result_value = json.dumps(expected_output)

            writer.writerow({
                'id': entry.get('id'),
                'input': entry.get('input'),
                'expected_output': result_value
            })

    print(f"Successfully exported {len(data)} entries to {csv_file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export dataset.json to CSV format')
    parser.add_argument('--type', choices=['execution', 'expected'], default='execution',
                        help='Type of export: execution (execution_result) or expected (expected_output)')
    parser.add_argument('--input', default='output/dataset.json',
                        help='Path to input JSON file (default: output/dataset.json)')
    parser.add_argument('--output', default='output/dataset.csv',
                        help='Path to output CSV file (default: output/dataset.csv)')

    args = parser.parse_args()

    if args.type == 'execution':
        export_to_csv(args.input, args.output)
    else:
        export_expected_output_to_csv(args.input, args.output)
