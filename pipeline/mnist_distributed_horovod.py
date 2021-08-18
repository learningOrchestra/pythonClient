from learning_orchestra_client.function.python import FunctionPython

CLUSTER_IP = "http://34.210.211.109"

function_python = FunctionPython(CLUSTER_IP)
horovod_code = """
import tensorflow as tf
import horovod.tensorflow.keras as hvd

hvd.init()

(mnist_images, mnist_labels), (x_test, y_test) = tf.keras.datasets.mnist.load_data(
    path="mnist-%d.npz" % hvd.rank()
)

mnist_model = tf.keras.Sequential(
    [
        tf.keras.layers.Flatten(input_shape=(28, 28)),
        tf.keras.layers.Dense(128, activation="relu"),
        tf.keras.layers.Dense(10, activation="softmax"),
    ]
)

scaled_lr = 0.001 * hvd.size()
opt = tf.optimizers.Adam(scaled_lr)

opt = hvd.DistributedOptimizer(
    opt
)

mnist_model.compile(
    loss=tf.losses.SparseCategoricalCrossentropy(),
    optimizer=opt,
    metrics=[tf.keras.metrics.SparseCategoricalAccuracy()],
    experimental_run_tf_function=False,
)

callbacks = [
    hvd.callbacks.BroadcastGlobalVariablesCallback(0),
    hvd.callbacks.MetricAverageCallback(),
    hvd.callbacks.LearningRateWarmupCallback(
        initial_lr=scaled_lr, warmup_epochs=3, verbose=1
    ),
]

if hvd.rank() == 0:
    callbacks.append(tf.keras.callbacks.ModelCheckpoint("./checkpoint-{epoch}.h5"))

verbose = 1 if hvd.rank() == 0 else 0

mnist_model.fit(
    x=mnist_images,
    y=mnist_labels,
    steps_per_epoch=500 // hvd.size(),
    callbacks=callbacks,
    epochs=num_epochs,
    verbose=verbose,
)

loss, acc = mnist_model.evaluate(x=x_test, y=y_test, verbose=0)

response = { 
    loss: loss,
    Accuracy: acc,
}
"""

function_python.run_function_async(
    name="mnist_horovod_distributed",
    parameters={
        "distributed": True,
    },
    code=horovod_code,
)
function_python.wait("mnist_horovod_distributed")
