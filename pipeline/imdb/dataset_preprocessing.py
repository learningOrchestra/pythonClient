import re;


def preprocessor(text):
    global re
    text = re.sub("<[^>]*>", "", text)
    emojis = re.findall("(?::|;|=)(?:-)?(?:\)|\(|D|P)", text)
    text = re.sub("[\W]+", " ", text.lower()) + \
           " ".join(emojis).replace("-", "")
    return text


data["text"] = data["text"].apply(preprocessor)

from nltk.stem.porter import PorterStemmer

porter = PorterStemmer()


def tokenizer_porter(text):
    global porter
    return [porter.stem(word) for word in text.split()]


from sklearn.feature_extraction.text import TfidfVectorizer

tfidf = TfidfVectorizer(strip_accents=None,
                        lowercase=False,
                        preprocessor=None,
                        tokenizer=tokenizer_porter,
                        use_idf=True,
                        norm="l2",
                        smooth_idf=True)

y = data.label.values
x = tfidf.fit_transform(data.text)

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(x, y,
                                                    random_state=1,
                                                    test_size=0.5,
                                                    shuffle=False)

response = {
    "X_train": X_train,
    "X_test": X_test,
    "y_train": y_train,
    "y_test": y_test
}