from learning_orchestra_client.dataset.generic import DatasetGeneric
from learning_orchestra_client.function.python import FunctionPython
from learning_orchestra_client.model.tensorflow import ModelTensorflow
from learning_orchestra_client.train.horovod import TrainHorovod
from learning_orchestra_client.predict.tensorflow import PredictTensorflow
from learning_orchestra_client.evaluate.tensorflow import EvaluateTensorflow

CLUSTER_IP = "http://34.125.36.237"

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
mnist_datasets_treatment = '''
import numpy as np
import struct as st
import math
training_filenames = {'images': mnist_train_images,
                      'labels': mnist_train_labels}
test_filenames = {'images': mnist_test_images, 
                  'labels': mnist_test_labels}
data_types = {
    0x08: ('ubyte', 'B', 1),
    0x09: ('byte', 'b', 1),
    0x0B: ('>i2', 'h', 2),
    0x0C: ('>i4', 'i', 4),
    0x0D: ('>f4', 'f', 4),
    0x0E: ('>f8', 'd', 8)}
def treat_dataset(dataset: dict) -> tuple:
    global np, st, math, data_types
    for name in dataset.keys():
        if name == 'images':
            images_file = dataset[name]
        if name == 'labels':
            labels_file = dataset[name]
    images_file.seek(0)
    magic = st.unpack('>4B', images_file.read(4))
    data_format = data_types[magic[2]][1]
    data_size = data_types[magic[2]][2]
    images_file.seek(4)
    content_amount = st.unpack('>I', images_file.read(4))[0]
    rows_amount = st.unpack('>I', images_file.read(4))[0]
    columns_amount = st.unpack('>I', images_file.read(4))[0]
    labels_file.seek(8)
    labels_array = np.asarray(
        st.unpack(
            '>' + data_format * content_amount,
            labels_file.read(content_amount * data_size))).reshape(
        (content_amount, 1))
    n_batch = 10000
    n_iter = int(math.ceil(content_amount / n_batch))
    n_bytes = n_batch * rows_amount * columns_amount * data_size
    images_array = np.array([])
    for i in range(0, n_iter):
        temp_images_array = np.asarray(
            st.unpack('>' + data_format * n_bytes,
                      images_file.read(n_bytes))).reshape(
            (n_batch, rows_amount, columns_amount))
        if images_array.size == 0:
            images_array = temp_images_array
        else:
            images_array = np.vstack((images_array, temp_images_array))
        temp_images_array = np.array([])
    return images_array, labels_array
train_images, train_labels = treat_dataset(training_filenames)
test_images, test_labels = treat_dataset(test_filenames)
response = {
    "train_images": train_images,
    "train_labels": train_labels,
    "test_images": test_images,
    "test_labels": test_labels,
}
'''

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

mnist_datasets_normalization = '''
test_images = test_images / 255
train_images = train_images / 255
print(train_images.shape)
print(train_labels.shape)
print(test_images.shape)
print(test_labels.shape)
response = {
    "test_images": test_images,
    "test_labels": test_labels,
    "train_images": train_images,
    "train_labels": train_labels
}
'''

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
    name="mnist_model_compiled",
    parameters={
        "model": "$mnist_model"
    },
    code=model_compilation)
function_python.wait("mnist_model_compiled")

distributed_model_compilation = '''
import tensorflow as tf
import horovod.tensorflow.keras as hvd
hvd.init()
mnist_model.compile(
    optimizer=hvd.DistributedOptimizer(tf.optimizers.Adam(0.001* hvd.size()),
    loss=tf.keras.losses.SparseCategoricalCrossentropy(),
    metrics=[tf.keras.metrics.SparseCategoricalAccuracy()],
)
response = mnist_model
'''

train_horovod = TrainHorovod(CLUSTER_IP)
train_horovod.create_training_async(
    name="mnist_model_trained",
    model_name="mnist_model",
    parent_name="mnist_model_compiled",
    compiling_code=distributed_model_compilation,
    parameters={
        "x": "$mnist_datasets_normalized.train_images",
        "y": "$mnist_datasets_normalized.train_labels",
        "validation_split": 0.1,
        "epochs": 5,
        "callbacks": [
            '#hvd.callbacks.BroadcastGlobalVariablesCallback(0)',
            '#hvd.callbacks.MetricAverageCallback()',
            '#hvd.callbacks.LearningRateWarmupCallback(initial_lr=0.0125 * hvd.size(), warmup_epochs=3, verbose=1)',
        ],
        "rank0callbacks": [
            '#tensorflow.keras.callbacks.TensorBoard(histogram_freq=1)'
        ]
    },
    monitoring_path='logs/'
)
train_horovod.wait("mnist_model_trained")

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

show_mnist_predict = '''
print(mnist_predicted)
response = None
'''
function_python.run_function_async(
    name="mnist_model_predicted_print",
    parameters={
        "mnist_predicted": "$mnist_model_predicted"
    },
    code=show_mnist_predict
)

show_mnist_evaluate = '''
print(mnist_evaluated)
response = None
'''
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
