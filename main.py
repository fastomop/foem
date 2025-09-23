from validator import SqlTest
import os
import json


def write_output(data):
    output_dir = os.path.join(os.getcwd(), "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    out_path = os.path.join(output_dir, "dataset.json")
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    test_generator = SqlTest() 
    funcs = [test_generator.patients_2drugs_and_time, 
             test_generator.patients_2drugs_and,
             test_generator.patients_2drugs_or,
             test_generator.patients_gender_state,
             test_generator.patients_group_by_ethnicity_location,
             test_generator.patients_group_by_ethnicity_birth,
             test_generator.patients_group_by_ethnicity,
             test_generator.patients_group_by_gender,
             test_generator.patients_group_by_race_ethnicity,
             test_generator.patients_grouped_by_race_gender,
             test_generator.patients_group_by_race_location,
             test_generator.patients_group_by_race_birth,
             test_generator.patients_group_by_location,
             test_generator.patients_group_by_birth_gender,
             test_generator.patients_group_by_birth_location,
             test_generator.patients_count
             ] 
    
    results = []
    for func in funcs:
        result = func()
        if result:
            results.extend(result)
    write_output(results)
