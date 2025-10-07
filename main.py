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
             test_generator.patients_4drugs_and_time,
             test_generator.patients_4drugs_and,
             test_generator.patients_4drugs_or,
             test_generator.patients_3drugs_and_time,
             test_generator.patients_3drugs_and,
             test_generator.patients_3drugs_or,
             test_generator.patients_2conditions_and_time,
             test_generator.patients_2conditions_and,
             
             ] 
    
    results = []
    for func in funcs:
        result = func()
        if result:
            results.extend(result)
    write_output(results)
