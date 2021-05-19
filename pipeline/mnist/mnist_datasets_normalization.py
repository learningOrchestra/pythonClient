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