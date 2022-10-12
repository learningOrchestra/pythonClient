from learning_orchestra_client.function.python import FunctionPython
from learning_orchestra_client.dataset.generic import DatasetGeneric
from learning_orchestra_client.model.autokeras import ModelAutoKeras
from learning_orchestra_client.train.autokeras import TrainAutokeras
from learning_orchestra_client.predict.autokeras import PredictAutokeras
from learning_orchestra_client.evaluate.autokeras import EvaluateAutokeras

CLUSTER_IP = "http://34.125.22.143"

dataset_generic = DatasetGeneric(CLUSTER_IP)
dataset_generic.insert_dataset_async(
    dataset_name=f"mnist_dataset_autokeras",
    url="https://storage.googleapis.com/tensorflow/tf-keras-datasets/mnist.npz",
)
dataset_generic.wait(f"mnist_dataset_autokeras")

function_python = FunctionPython(CLUSTER_IP)
mnist_load_data = '''

def load_data(path):
    import numpy as np
    with np.load(path) as f:
        x_train, y_train = f['x_train'], f['y_train']
        x_test, y_test = f['x_test'], f['y_test']
        return (x_train, y_train), (x_test, y_test)

(x_train, y_train), (x_test, y_test) = load_data(mnist_dataset_autokeras)

response = {
    "test_images": x_test,
    "test_labels": y_test,
    "train_images": x_train,
    "train_labels": y_train
}
'''

function_python.run_function_async(
    name=f"mnist_load_data",
    parameters={
        "mnist_dataset_autokeras": f"$mnist_dataset_autokeras"
    },
    code=mnist_load_data)
function_python.wait(f"mnist_load_data")

model_autokeras = ModelAutoKeras(CLUSTER_IP)
model_autokeras.create_model_async(
    name=f"mnist_model_autokeras",
    module_path="autokeras.tasks.image",
    class_name="ImageClassifier",
    class_parameters={
        "overwrite": True,
        "max_trials": 1
    }
)
model_autokeras.wait(f"mnist_model_autokeras")

train_autokeras = TrainAutokeras(CLUSTER_IP)
train_autokeras.create_training_async(
    name=f"mnist_model_trained_autokeras",
    model_name=f"mnist_model_autokeras",
    parent_name=f"mnist_model_autokeras",
    method_name="fit",
    parameters={
        "x": f"$mnist_load_data.train_images",
        "y": f"$mnist_load_data.train_labels",
        "epochs": 1,
    }
)
train_autokeras.wait(f"mnist_model_trained_autokeras")

predict_autokeras = PredictAutokeras(CLUSTER_IP)
predict_autokeras.create_prediction_async(
    name=f"mnist_model_predicted_autokeras",
    model_name=f"mnist_model_autokeras",
    parent_name=f"mnist_model_trained_autokeras",
    method_name="predict",
    parameters={
        "x": f"$mnist_load_data.test_images"
    }
)

predict_autokeras.wait(f"mnist_model_predicted_autokeras")

evaluate_autokeras = EvaluateAutokeras(CLUSTER_IP)
evaluate_autokeras.create_evaluate_async(
    name=f"mnist_model_evaluated_autokeras",
    model_name=f"mnist_model_autokeras",
    parent_name=f"mnist_model_trained_autokeras",
    method_name="evaluate",
    parameters={
        "x": f"$mnist_load_data.test_images",
        "y": f"$mnist_load_data.test_labels"
    }
)

evaluate_autokeras.wait(f"mnist_model_evaluated_autokeras")

show_mnist_predict = '''
print(mnist_predicted)
response = None
'''
function_python.run_function_async(
    name=f"mnist_model_predicted_print",
    parameters={
        "mnist_predicted": f"$mnist_model_predicted_autokeras"
    },
    code=show_mnist_predict
)

show_mnist_evaluate = '''
print(mnist_evaluated)
response = None
'''
function_python.run_function_async(
    name=f"mnist_model_evaluated_print",
    parameters={
        "mnist_evaluated": f"$mnist_model_evaluated_autokeras"
    },
    code=show_mnist_evaluate
)

function_python.wait(f"mnist_model_evaluated_print")
function_python.wait(f"mnist_model_predicted_print")

print(function_python.search_execution_content(
    name=f"mnist_model_predicted_print",
    limit=1,
    skip=1,
    pretty_response=True))

print(function_python.search_execution_content(
    name=f"mnist_model_evaluated_print",
    limit=1,
    skip=1,
    pretty_response=True))