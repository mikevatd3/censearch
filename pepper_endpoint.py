import json
from pathlib import Path
import requests
from time import sleep


def main():
    sleep(1) # Because on local, saving resets the server
    queries_path = Path.cwd() / "fixtures" / "acs_term_queries.json"

    with open(queries_path) as f:
        queries = json.load(f)

    results = []
    for query in queries:
        q = query["query"]
        response = requests.get(
            f"http://localhost:5000/censearch/text-search?q={q}&how=json"
        )

        try:
            returns = response.json()
        except:
            returns = None

        results.append({
            **query,
            "returns": returns
        })
        sleep(0.02)

    with open(Path.cwd() / "fixtures" / "output.json", "w") as f:
        json.dump(results, f, indent=4)


if __name__ == "__main__":
    main()
