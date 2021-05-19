from learning_orchestra_client.dataset.csv import DatasetCsv
from learning_orchestra_client.function.python import FunctionPython
from learning_orchestra_client.model.scikitlearn import ModelScikitLearn
from learning_orchestra_client.train.scikitlearn import TrainScikitLearn
from learning_orchestra_client.predict.scikitlearn import PredictScikitLearn
from learning_orchestra_client.utils.file_caster import python_file_caster

CLUSTER_IP = "http://35.238.145.237"

dataset_csv = DatasetCsv(CLUSTER_IP)

dataset_csv.insert_dataset_sync(
    dataset_name="sentiment_analysis",
    url="https://drive.google.com/u/0/uc?"
        "id=1PSLWHbKR_cuKvGKeOSl913kCfs-DJE2n&export=download",
)

function_python = FunctionPython(CLUSTER_IP)

explore_dataset = python_file_caster('imdb/explore_dataset.py')

function_python.run_function_sync(
    name="sentiment_analysis_exploring",
    parameters={"data": "$sentiment_analysis"},
    code=explore_dataset)

print(function_python.search_execution_content(
    name="sentiment_analysis_exploring",
    limit=1,
    skip=1,
    pretty_response=True))

dataset_preprocessing = python_file_caster('imdb/dataset_preprocessing.py')

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

logistic_regression_cv_accuracy = \
    python_file_caster('imdb/logistic_regression_cv_accuracy.py')

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
