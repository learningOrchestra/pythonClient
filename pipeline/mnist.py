from learning_orchestra_client.function.python import FunctionPython
from learning_orchestra_client.model.tensorflow import ModelTensorflow
from learning_orchestra_client.train.tensorflow import TrainTensorflow
from learning_orchestra_client.predict.tensorflow import PredictTensorflow
from learning_orchestra_client.evaluate.tensorflow import EvaluateTensorflow

from timer import Timer

CLUSTER_IP = "http://34.210.211.109"

run = 2
function_python = FunctionPython(CLUSTER_IP)
mnist_datasets_treatment = '''
import tensorflow as tf

(train_images, train_labels), (test_images, test_labels) = tf.keras.datasets.mnist.load_data(
    path="mnist-%d.npz"
)

train_images = train_images / 255
test_images = test_images / 255
    
response = {
    "train_images": train_images,
    "train_labels": train_labels,
    "test_images": test_images,
    "test_labels": test_labels,
}
'''

function_python.run_function_async(
    name=f'mnist_datasets_treated{run}',
    parameters={},
    code=mnist_datasets_treatment
)

function_python.wait(f'mnist_datasets_treated{run}')

model_tensorflow = ModelTensorflow(CLUSTER_IP)
model_tensorflow.create_model_async(
    name=f'mnist_model{run}',
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

model_tensorflow.wait(f'mnist_model{run}')

model_compilation = '''
import tensorflow as tf

model.compile(
    optimizer=tf.keras.optimizers.Adam(0.001),
    loss=tf.keras.losses.SparseCategoricalCrossentropy(),
    metrics=[tf.keras.metrics.SparseCategoricalAccuracy()],
)

response = model
'''

function_python.run_function_async(
    name=f'mnist_model_compiled{run}',
    parameters={
        "model": f'$mnist_model{run}'
    },
    code=model_compilation)
function_python.wait(f'mnist_model_compiled{run}')
with Timer() as t:
    train_tensorflow = TrainTensorflow(CLUSTER_IP)
    result = train_tensorflow.create_training_async(
        name=f'mnist_model_trained{run}',
        model_name=f'mnist_model{run}',
        parent_name=f'mnist_model_compiled{run}',
        method_name="fit",
        parameters={
            "x": f'$mnist_datasets_treated{run}.train_images',
            "y": f'$mnist_datasets_treated{run}.train_labels',
            "validation_split": 0.1,
            "epochs": 500,
        },
    )

    print(result)
    train_tensorflow.wait(f'mnist_model_trained{run}')
    t.partial('model trained')
    predict_tensorflow = PredictTensorflow(CLUSTER_IP)
    predict_tensorflow.create_prediction_async(
        name=f'mnist_model_predicted{run}',
        model_name=f'mnist_model{run}',
        parent_name=f'mnist_model_trained{run}',
        method_name="predict",
        parameters={
            "x": f'$mnist_datasets_treated{run}.test_images',
        }
    )
    evaluate_tensorflow = EvaluateTensorflow(CLUSTER_IP)
    evaluate_tensorflow.create_evaluate_async(
        name=f'mnist_model_evaluated{run}',
        model_name=f'mnist_model{run}',
        parent_name=f'mnist_model_trained{run}',
        method_name="evaluate",
        parameters={
            "x": f'$mnist_datasets_treated{run}.test_images',
            "y": f'$mnist_datasets_treated{run}.test_labels'
        }
    )
    predict_tensorflow.wait(f'mnist_model_predicted{run}')
    t.partial('model predicted')
    evaluate_tensorflow.wait(f'mnist_model_evaluated{run}')
    t.partial('model evaluated')

show_mnist_predict = '''
print(mnist_predicted)
response = None
'''
function_python.run_function_async(
    name=f'mnist_model_predicted_print{run}',
    parameters={
        "mnist_predicted": f'$mnist_model_predicted{run}'
    },
    code=show_mnist_predict
)

show_mnist_evaluate = '''
print(mnist_evaluated)
response = None
'''
function_python.run_function_async(
    name=f'mnist_model_evaluated_print{run}',
    parameters={
        "mnist_evaluated": f'$mnist_model_evaluated{run}'
    },
    code=show_mnist_evaluate
)
function_python.wait(f'mnist_model_evaluated_print{run}')
function_python.wait(f'mnist_model_predicted_print{run}')

print(function_python.search_execution_content(
    name=f'mnist_model_predicted_print{run}',
    limit=1,
    skip=1,
    pretty_response=True))

print(function_python.search_execution_content(
    name=f'mnist_model_evaluated_print{run}',
    limit=1,
    skip=1,
    pretty_response=True))
