import urllib.request
import yaml
import json
import os

CRD_URL = "https://raw.githubusercontent.com/kubeflow/spark-operator/master/config/crd/bases/sparkoperator.k8s.io_sparkapplications.yaml"
OUTPUT_DIR = "api_generator"
OUTPUT_FILE = f"{OUTPUT_DIR}/spark_app_openapi.json"

def fetch_and_extract_schema():
    print(f"Fetching CRD from {CRD_URL}...")
    with urllib.request.urlopen(CRD_URL) as response:
        raw_yaml = response.read().decode('utf-8')
        
    print("Parsing YAML...")
    crd_dict = yaml.safe_load(raw_yaml) 
    
    try:
        open_api_schema = crd_dict['spec']['versions'][0]['schema']['openAPIV3Schema']
    except KeyError as e:
        print(f"Failed to find OpenAPI schema block. Error: {e}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(open_api_schema, f, indent=2)
        
    print(f"✅ Successfully extracted OpenAPI schema to {OUTPUT_FILE}")

if __name__ == "__main__":
    fetch_and_extract_schema()
