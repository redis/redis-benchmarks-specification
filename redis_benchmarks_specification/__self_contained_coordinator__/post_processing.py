import os
import json


def post_process_vector_db(temporary_dir):
    results_dir = os.path.join(temporary_dir, "results")
    results = {}
    for file in os.listdir(results_dir):
        if "upload" in file:
            with open(os.path.join(results_dir, file), "r") as f:
                upload_results = json.load(f)
                results["upload_time"] = upload_results["results"]["upload_time"]
        else:
            with open(os.path.join(results_dir, file), "r") as f:
                query_results = json.load(f)
                results["rps"] = query_results["results"]["rps"]
                results["precision"] = query_results["results"]["mean_precisions"]
                results["total_time"] = query_results["results"]["total_time"]
    return results
