from learning_orchestra_client.dataset.generic import DatasetGeneric
from learning_orchestra_client.function.python import FunctionPython
from learning_orchestra_client.model.tensorflow import ModelTensorflow
from learning_orchestra_client.train.tensorflow import TrainTensorflow
from learning_orchestra_client.predict.tensorflow import PredictTensorflow
from learning_orchestra_client.evaluate.tensorflow import EvaluateTensorflow
from learning_orchestra_client.utils.file_caster import python_file_caster
CLUSTER_IP = "http://35.238.145.237"

dataset_generic = DatasetGeneric(CLUSTER_IP)
dataset_generic.insert_dataset_async(
    dataset_name="mnist_train_images",
    url="https://drive.google.com/u/0/uc?"
        "id=1ec6hVwvq4UPyQ7DmJVxzofE2TcKUTHNY&export=download",
)

dataset_generic.insert_dataset_async(
    dataset_name="mnist_train_labels",
    url="https://drive.google.com/u/0/uc?"
        "id=187ID_LfQCTJOYieC-yo94jxnWiFJZ7uX&export=download",
)

dataset_generic.insert_dataset_async(
    dataset_name="mnist_test_images",
    url="https://drive.google.com/u/0/uc?"
        "id=1ZNuiRJLKSFzmegRgIHXUl4t-UdjEPYVf&export=download",
)

dataset_generic.insert_dataset_async(
    dataset_name="mnist_test_labels",
    url="https://drive.google.com/u/0/uc?"
        "id=1v0PRmUL8nOg3mHakjfTxOjNA9bi6XkkA&export=download",
)

dataset_generic.wait("mnist_train_images")
dataset_generic.wait("mnist_train_labels")
dataset_generic.wait("mnist_test_images")
dataset_generic.wait("mnist_test_labels")

function_python = FunctionPython(CLUSTER_IP)
mnist_datasets_treatment = \
    python_file_caster('mnist/mnist_datasets_treatment.py')

function_python.run_function_async(
    name="mnist_datasets_treated",
    parameters={
        "mnist_train_images": "$mnist_train_images",
        "mnist_train_labels": "$mnist_train_labels",
        "mnist_test_images": "$mnist_test_images",
        "mnist_test_labels": "$mnist_test_labels"
    },
    code=mnist_datasets_treatment)
function_python.wait("mnist_datasets_treated")

mnist_datasets_normalization = \
    python_file_caster('mnist/mnist_datasets_normalization.py')

function_python.run_function_async(
    name="mnist_datasets_normalized",
    parameters={
        "train_images": "$mnist_datasets_treated.train_images",
        "train_labels": "$mnist_datasets_treated.train_labels",
        "test_images": "$mnist_datasets_treated.test_images",
        "test_labels": "$mnist_datasets_treated.test_labels"
    },
    code=mnist_datasets_normalization)
function_python.wait("mnist_datasets_normalized")

model_tensorflow = ModelTensorflow(CLUSTER_IP)
model_tensorflow.create_model_async(
    name="mnist_model",
    module_path="tensorflow.keras.models",
    class_name="Sequential",
    class_parameters={
        "layers":
            [
                "#tensorflow.keras.layers.Flatten(input_shape=(28, 28))",
                "#tensorflow.keras.layers.Dense(128, activation='relu')",
                "#tensorflow.keras.layers.Dense(10, activation='softmax')",
            ]}
)
model_tensorflow.wait("mnist_model")

model_compilation = python_file_caster('mnist/model_compilation.py')

function_python.run_function_async(
    name="mnist_model_compiled",
    parameters={
        "model": "$mnist_model"
    },
    code=model_compilation)
function_python.wait("mnist_model_compiled")

train_tensorflow = TrainTensorflow(CLUSTER_IP)
train_tensorflow.create_training_async(
    name="mnist_model_trained",
    model_name="mnist_model",
    parent_name="mnist_model_compiled",
    method_name="fit",
    parameters={
        "x": "$mnist_datasets_normalized.train_images",
        "y": "$mnist_datasets_normalized.train_labels",
        "validation_split": 0.1,
        "epochs": 6,
    }
)
train_tensorflow.wait("mnist_model_trained")

predict_tensorflow = PredictTensorflow(CLUSTER_IP)
predict_tensorflow.create_prediction_async(
    name="mnist_model_predicted",
    model_name="mnist_model",
    parent_name="mnist_model_trained",
    method_name="predict",
    parameters={
        "x": "$mnist_datasets_normalized.test_images"
    }
)

evaluate_tensorflow = EvaluateTensorflow(CLUSTER_IP)
evaluate_tensorflow.create_evaluate_async(
    name="mnist_model_evaluated",
    model_name="mnist_model",
    parent_name="mnist_model_trained",
    method_name="evaluate",
    parameters={
        "x": "$mnist_datasets_normalized.test_images",
        "y": "$mnist_datasets_normalized.test_labels"
    }
)

predict_tensorflow.wait("mnist_model_predicted")
evaluate_tensorflow.wait("mnist_model_evaluated")

show_mnist_predict = python_file_caster('mnist/show_mnist_predict.py')

function_python.run_function_async(
    name="mnist_model_predicted_print",
    parameters={
        "mnist_predicted": "$mnist_model_predicted"
    },
    code=show_mnist_predict
)

show_mnist_evaluate = python_file_caster('mnist/show_mnist_evaluate.py')

function_python.run_function_async(
    name="mnist_model_evaluated_print",
    parameters={
        "mnist_evaluated": "$mnist_model_evaluated"
    },
    code=show_mnist_evaluate
)

function_python.wait("mnist_model_evaluated_print")
function_python.wait("mnist_model_predicted_print")

print(function_python.search_execution_content(
    name="mnist_model_predicted_print",
    limit=1,
    skip=1,
    pretty_response=True))

print(function_python.search_execution_content(
    name="mnist_model_evaluated_print",
    limit=1,
    skip=1,
    pretty_response=True))
