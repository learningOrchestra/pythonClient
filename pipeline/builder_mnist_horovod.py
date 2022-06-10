from learning_orchestra_client.builder.builder_horovod import BuilderHorovod

code = """
def train(num_epochs):
    import tensorflow as tf
    import horovod.tensorflow.keras as hvd
    hvd.init()

    (x_train, y_train), (x_test, y_test) = tf.keras.datasets.cifar10.load_data()
    y_train = tf.keras.utils.to_categorical(y_train, 10)
    y_test = tf.keras.utils.to_categorical(y_test, 10)

    train_dat = tf.keras.preprocessing.image.ImageDataGenerator(
        width_shift_range=0.33, height_shift_range=0.33, zoom_range=0.5, horizontal_flip=True,
        preprocessing_function=tf.keras.applications.resnet50.preprocess_input
    )

    test_dat = tf.keras.preprocessing.image.ImageDataGenerator(
        zoom_range=(0.875, 0.875),
        preprocessing_function=tf.keras.applications.resnet50.preprocess_input
    )

    model = tf.keras.applications.ResNet50(
        include_top=False,
        weights=None,
        input_shape=[32, 32, 3],
        classes=10,
    )

    scaled_lr = 0.0125 * hvd.size()
    opt = tf.optimizers.SGD(lr=scaled_lr, momentum=0.9)

    opt = hvd.DistributedOptimizer(
        opt
    )

    model_config = model.get_config()
    for layer, layer_config in zip(model.layers, model_config['layers']):
        if hasattr(layer, 'kernel_regularizer'):
            regularizer = tf.keras.regularizers.l2(0.00005)
            layer_config['config']['kernel_regularizer'] = \
                {'class_name': regularizer.__class__.__name__,
                 'config': regularizer.get_config()}
        if type(layer) == tf.keras.layers.BatchNormalization:
            layer_config['config']['momentum'] = 0.9
            layer_config['config']['epsilon'] = 1e-5

    model = tf.keras.models.Model.from_config(model_config)

    model.compile(
        loss=tf.losses.categorical_crossentropy,
        optimizer=opt,
        metrics=['accuracy', 'top_k_categorical_accuracy'],
    )

    callbacks = [
        hvd.callbacks.BroadcastGlobalVariablesCallback(0),
        hvd.callbacks.MetricAverageCallback(),
        hvd.callbacks.LearningRateWarmupCallback(
            initial_lr=scaled_lr, warmup_epochs=5, verbose=1
        )
    ]

    if hvd.rank() == 0:
        callbacks.append(tf.keras.callbacks.ModelCheckpoint("./checkpoint-{epoch}.h5"))
        callbacks.append(tf.keras.callbacks.TensorBoard('./logs'))

    verbose = 1 if hvd.rank() == 0 else 0

    model.fit_generator(train_dat.flow(x_train, y_train, batch_size=32),
                        steps_per_epoch=len(x_train) // hvd.size(),
                        callbacks=callbacks,
                        epochs=num_epochs,
                        verbose=verbose,
                        validation_data=test_dat.flow(x_test, y_test, batch_size=32),
                        validation_steps=3 * len(x_test) // hvd.size(),
                        )

    score = hvd.allreduce(model.evaluate(test_dat.flow(x_test, y_test)))

    if verbose:
        print('Test loss:', score[0])
        print('Test accuracy:', score[1])
"""

if __name__ == '__main__':
    CLUSTER_IP = "http://34.125.36.237"
    builder = BuilderHorovod(CLUSTER_IP)
    builder.run_horovod_sync(
        'my builder',
        code,
    )
