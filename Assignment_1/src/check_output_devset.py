import json
import re
def load_stopwords():
    with open("../Assets/stopwords.txt", "r") as f:
        stopwords = set(line.strip() for line in f if line.strip())
    return stopwords

STOPWORDS = load_stopwords()

def main():
    data = {}
    with open("output.txt", "r") as f:
        
        """
        {
            "category": [
                {
                    "word": "word",
                    "chi2": chi2
                }
            ]
        }
        """
        category = ""
        lines = f.readlines()
        for line in lines[:-1]:
            entires = line.split()
            category = entires[0]
            data[category] = []
            for i in range(1, len(entires)):
                word, chi2 = entires[i].split(":")
                data[category].append({"word": word, "chi2": float(chi2)})
        # Run through the reviews locally and check if the top 75 words are the same
    with open("../Assets/reviews_devset.json", "r") as f:
        lines = f.readlines()
        devset = [json.loads(review) for review in lines]
    
    rev_count = 0
    cat_count = {}
    word_count = {}
    A = {}
    for review in devset:
        category = review["category"]
        review_text = review["reviewText"]
        regex = re.compile(r'[^a-zA-Z<>^|]+')
        words = map(str.lower, regex.split(review_text))
        unique_words = {
            w for w in words
            if len(w) > 1 and w not in STOPWORDS
        }
        rev_count += 1
        cat_count[category] = cat_count.get(category, 0) + 1
        for word in unique_words:
            word_count[word] = word_count.get(word, 0) + 1
            try:
                A[category][word] = A[category].get(word, 0) + 1
            except KeyError:
                A[category] = {word: 1}

    #Compute Chi-squared
    Chi_squared = {}
    B = {}
    C = {}
    D = {}
    for category in A:
        Chi_squared[category] = []
        B[category] = {}
        C[category] = {}
        D[category] = {}
        for word in A[category]:
            B[category][word] = word_count[word] - A[category][word]
            C[category][word] = cat_count[category] - A[category][word]
            D[category][word] = rev_count - word_count[word] - cat_count[category] + A[category][word]

            enum = rev_count * ((A[category][word] * D[category][word] - B[category][word] * C[category][word]) ** 2)
            denom = (A[category][word] + B[category][word]) * (A[category][word] + C[category][word]) * (B[category][word] + D[category][word]) * (C[category][word] + D[category][word])
            if denom == 0:
                continue
            Chi_squared[category].append({"word": word, "chi2": enum / denom})
    
    # Sort by chi-squared
    for category in Chi_squared:
        Chi_squared[category] = sorted(Chi_squared[category], key=lambda x: x["chi2"], reverse=True)
        Chi_squared[category] = Chi_squared[category][:75]
    
    # Compare with data
    for category in data:
        for i in range(75):
            delta = 1e-9    
            if abs(data[category][i]["chi2"] - Chi_squared[category][i]["chi2"]) > delta:
                print("Mismatch in category", category, "at index", i)
                print("Data:", data[category][i])
                print("Chi_squared:", Chi_squared[category][i])
                return
    print("All match")
        
                
        

if __name__ == '__main__':
    main()