import json
import sys


def main():
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    category_to_terms = {}
    merged_terms = set()

    with open(input_path, 'r') as f:
        for line in f:
            line = line.rstrip('\n')
            if not line:
                continue

            key_json, value_json = line.split('\t', 1)
            category = json.loads(key_json)
            top75 = json.loads(value_json)

            category_to_terms[category] = top75

            for chi2, word in top75:
                merged_terms.add(word)

    with open(output_path, 'w') as out:
        for category in sorted(category_to_terms.keys()):
            terms = category_to_terms[category]

            parts = [category]
            chi2_prev = 0
            word_prev = ""
            delta = 1e-9
            for chi2, word in terms:
                if abs(chi2_prev - chi2) < delta and word_prev > word:
                    prev_entry = parts.pop()
                    parts.append(f"{word}:{chi2}")
                    parts.append(prev_entry)
                else:
                    parts.append(f"{word}:{chi2}")
                chi2_prev = chi2
                word_prev = word
        
            out.write(" ".join(parts) + "\n")

        out.write(" ".join(sorted(merged_terms)) + "\n")


if __name__ == '__main__':
    main()