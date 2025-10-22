import json
import time
import os
from langfuse import get_client
from dotenv import load_dotenv

load_dotenv()

langfuse = get_client()

if langfuse.auth_check():
    print("Langfuse client is authenticated and ready!")
else:
    print("Authentication failed. Please check your credentials and host.")
    exit(1)

print("\nLoading dataset from output/dataset.json...")
with open('output/dataset.json', 'r') as f:
    dataset = json.load(f)

print(f"Found {len(dataset)} items in dataset")

PROGRESS_FILE = "output/upload_progress.json"

# Load progress if it exists
start_idx = 0
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, 'r') as f:
        progress = json.load(f)
        start_idx = progress.get('last_uploaded_idx', 0)
        print(f"Resuming from item {start_idx + 1}")

# Upload each item to Langfuse with rate limiting and retry logic
print("\nUploading items to Langfuse...")
successful_uploads = 0
failed_uploads = 0
BATCH_SIZE = 90  # Upload 90 items per batch to stay under 100 limit
BATCH_DELAY = 65  # Wait 65 seconds between batches (to account for 60s rate limit window)
ITEM_DELAY = 0.5  # Delay between individual items

for idx, item in enumerate(dataset[start_idx:], start_idx + 1):
    max_retries = 5
    retry_delay = 10  # Start with longer delay for rate limits

    for attempt in range(max_retries):
        try:
            langfuse.create_dataset_item(
                dataset_name="foem",
                input={
                    "text": item["input"]
                },
                expected_output={
                    "sql": item["expected_output"],
                    # "execution_result": item["execution_result"]
                },
                metadata={
                    "item_id": item["id"]
                }
            )
            successful_uploads += 1

            # Save progress
            with open(PROGRESS_FILE, 'w') as f:
                json.dump({'last_uploaded_idx': idx}, f)

            if idx % 10 == 0:
                print(f"  Uploaded {idx}/{len(dataset)} items (Success: {successful_uploads}, Failed: {failed_uploads})")

            # Batch delay: wait longer every BATCH_SIZE items
            if idx % BATCH_SIZE == 0:
                print(f"  Completed batch of {BATCH_SIZE}. Waiting {BATCH_DELAY}s to respect rate limits...")
                time.sleep(BATCH_DELAY)
            else:
                time.sleep(ITEM_DELAY)

            break  # Success, exit retry loop

        except Exception as e:
            error_msg = str(e)

            # Check if it's a rate limit error
            if "429" in error_msg or "rate limit" in error_msg.lower():
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    print(f"  Rate limit hit at item {idx} (ID: {item['id']}), waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                    time.sleep(wait_time)
                else:
                    print(f"  Failed to upload item {idx} (ID: {item['id']}) after {max_retries} attempts due to rate limiting")
                    failed_uploads += 1
                    break
            else:
                print(f"  Error uploading item {idx} (ID: {item['id']}): {e}")
                failed_uploads += 1
                break  # Non-rate-limit error, don't retry

print(f"\nUpload complete!")
print(f"  Total items: {len(dataset)}")
print(f"  Successful: {successful_uploads}")
print(f"  Failed: {failed_uploads}")

# Clean up progress file if all items uploaded
if idx == len(dataset):
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        print("\n All items uploaded successfully. Progress file removed.")