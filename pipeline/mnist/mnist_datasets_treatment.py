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