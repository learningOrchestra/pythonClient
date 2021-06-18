from learning_orchestra_client.dataset.csv import DatasetCsv
from learning_orchestra_client.function.python import FunctionPython
from learning_orchestra_client.model.scikitlearn import ModelScikitLearn
from learning_orchestra_client.train.scikitlearn import TrainScikitLearn
from learning_orchestra_client.predict.scikitlearn import PredictScikitLearn

CLUSTER_IP = "http://35.247.203.13"

dataset_csv = DatasetCsv(CLUSTER_IP)

dataset_csv.insert_dataset_sync(
    dataset_name="sentiment_analysis",
    url="https://drive.google.com/u/0/uc?"
        "id=1PSLWHbKR_cuKvGKeOSl913kCfs-DJE2n&export=download",
)

function_python = FunctionPython(CLUSTER_IP)

explore_dataset = '''
pos=data[data["label"]=="1"]
neg=data[data["label"]=="0"]

total_rows = len(pos) + len(neg)

print("Positive = " + str(len(pos) / total_rows))
print("Negative = " + str(len(neg) / total_rows))

response = None
'''

function_python.run_function_sync(
    name="sentiment_analysis_exploring",
    parameters={"data": "$sentiment_analysis"},
    code=explore_dataset)

print(function_python.search_execution_content(
    name="sentiment_analysis_exploring",
    limit=1,
    skip=1,
    pretty_response=True))

dataset_preprocessing = '''
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
'''

function_python.run_function_sync(
    name="sentiment_analysis_preprocessed",
    parameters={
        "data": "$sentiment_analysis"
    },
    code=dataset_preprocessing
)

model_scikitlearn = ModelScikitLearn(CLUSTER_IP)

model_scikitlearn.create_model_sync(
    name="sentiment_analysis_logistic_regression_cv",
    module_path="sklearn.linear_model",
    class_name="LogisticRegressionCV",
    class_parameters={
        "cv": 5,
        "scoring": "accuracy",
        "random_state": 0,
        "n_jobs": -1,
        "verbose": 3,
        "max_iter": 100
    }

)

train_scikitlearn = TrainScikitLearn(CLUSTER_IP)
train_scikitlearn.create_training_sync(
    parent_name="sentiment_analysis_logistic_regression_cv",
    name="sentiment_analysis_logistic_regression_cv_trained",
    model_name="sentiment_analysis_logistic_regression_cv",
    method_name="fit",
    parameters={
        "X": "$sentiment_analysis_preprocessed.X_train",
        "y": "$sentiment_analysis_preprocessed.y_train",
    }
)

predict_scikitlearn = PredictScikitLearn(CLUSTER_IP)
predict_scikitlearn.create_prediction_sync(
    parent_name="sentiment_analysis_logistic_regression_cv_trained",
    name="sentiment_analysis_logistic_regression_cv_predicted",
    model_name="sentiment_analysis_logistic_regression_cv",
    method_name="predict",
    parameters={
        "X": "$sentiment_analysis_preprocessed.X_test",
    }

)

logistic_regression_cv_accuracy = '''
from sklearn import metrics

print("Accuracy: ",metrics.accuracy_score(y_test, y_pred))

response = None
'''
function_python.run_function_sync(
    name="sentiment_analysis_logistic_regression_cv_accuracy",
    parameters={
        "y_test": "$sentiment_analysis_preprocessed.y_test",
        "y_pred": "$sentiment_analysis_logistic_regression_cv_predicted"
    },
    code=logistic_regression_cv_accuracy
)

print(function_python.search_execution_content(
    name="sentiment_analysis_logistic_regression_cv_accuracy",
    pretty_response=True))
