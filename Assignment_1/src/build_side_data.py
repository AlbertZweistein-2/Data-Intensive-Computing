import json
import sys

def main():
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    n_total = None
    cat_counts = {}

    with open(input_path, 'r') as f:
        for line in f:
            line = line.rstrip('\n')
            if not line:
                continue

            key_json, value_json = line.split('\t', 1)
            key = json.loads(key_json)
            value = json.loads(value_json)

            tag = key[0]

            if tag == '_n_':
                n_total = value
            elif tag == '_cat_':
                category = key[1]
                cat_counts[category] = value

    if n_total is None:
        raise ValueError("Did not find _n_ in job1 output")

    side_data = {
        'n': n_total,
        'cat_counts': cat_counts
    }

    with open(output_path, 'w') as f:
        json.dump(side_data, f)

if __name__ == '__main__':
    main()