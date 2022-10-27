from learning_orchestra_client.model.pycaret import ModelPycaret
from learning_orchestra_client.tune.pycaret import TunePycaret
from learning_orchestra_client.dataset.csv import DatasetCsv
from learning_orchestra_client.transform.data_type import TransformDataType
from learning_orchestra_client.predict.pycaret import PredictPycaret
from learning_orchestra_client.evaluate.pycaret import EvaluatePycaret
import time

start = time.time()

CLUSTER_IP = "http://34.151.210.120"

dataset_csv = DatasetCsv(CLUSTER_IP)

dataset_csv.insert_dataset_async(
    url="https://raw.githubusercontent.com/JonatasMiguel/"
        "PycaretTitanic/main/train.csv",
    dataset_name="train",
)
dataset_csv.wait(dataset_name="train",
                 timeout=1)

dataset_csv.insert_dataset_async(
    url="https://raw.githubusercontent.com/JonatasMiguel/"
        "PycaretTitanic/main/test.csv",
    dataset_name="test",
)
dataset_csv.wait(dataset_name="test",
                 timeout=1)                 

transform_data_type = TransformDataType(CLUSTER_IP)
type_fields = {
    "Age": "number",
    "Fare": "number",
    "Parch": "number",
    "Pclass": "number",
    "SibSp": "number"
}
transform_data_type.update_dataset_type_async(
    dataset_name=f"train",
    types=type_fields)

transform_data_type.wait(dataset_name=f"train",
                         timeout=1)

model_pycaret = ModelPycaret(CLUSTER_IP)
model_pycaret.create_model_async(
    name="titanic_model_pycaret",
    module_path="pycaret.classification.functional",
    class_name="setup",
    class_parameters={
        "data": "$train",
        "target":"Survived",
        "ignore_features": ['Ticket'],
        "numeric_features": ['Age','Fare','Parch','Pclass','SibSp'],
        "session_id": 1,
)
model_pycaret.wait("titanic_model_pycaret")


tune_pycaret = TunePycaret(CLUSTER_IP)
tune_pycaret.create_tune_async(
    name="titanic_tune_pycaret",
    parent_name="titanic_model_pycaret",
    model="titanic_model_pycaret"
    module_path="pycaret.classification.functional",
    class_name="tune_model",
    class_parameters={
        "n_iter" = 100,
        "choose_better"=True
)
tune_pycaret.wait("titanic_tune_pycaret")


predict_pycaret = PredictPycaret(CLUSTER_IP)
predict_pycaret.create_prediction_async(
    name="titanic_predicted_pycaret",
    model_name="titanic_tune_pycaret",
    parent_name="titanic_model_pycaret",
    method_name="predict_model",
    parameters={
        "data": "$test"
    }
)

predict_pycaret.wait("titanic_predicted_pycaret")

evaluate_pycaret = EvaluatePycaret(CLUSTER_IP)
evaluate_pycaret.create_evaluate_async(
    name="titanic_evaluate_pycaret",
    model_name="titanic_tune_pycaret",
    parent_name="titanic_model_pycaret",
    method_name="pull",
)

evaluate_pycaret.wait("titanic_evaluate_pycaret")

show_mnist_evaluate = '''
print(titanic_evaluate_pycaret)
response = None
'''
function_python.run_function_async(
    name="titanic_evaluate_pycaret_print",
    parameters={
        "titanic_evaluate_pycaret": "$titanic_evaluate_pycaret"
    },
    code=show_mnist_evaluate
)

function_python.wait("titanic_evaluate_pycaret_print")