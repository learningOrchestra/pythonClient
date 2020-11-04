<p align="center">
    <img src="./learningOrchestra-python-client.png">
    <img src="https://img.shields.io/badge/build-passing-brightgreen?style=flat-square" href="https://shields.io/" alt="build-passing">
    <img src="https://img.shields.io/github/v/tag/learningOrchestra/learningOrchestra-python-client?style=flat-square" href="https://github.com/riibeirogabriel/learningOrchestra/tags" alt="tag">
    <img src="https://img.shields.io/github/last-commit/learningOrchestra/learningOrchestra-python-client?style=flat-square" href="https://github.com/learningOrchestra/learningOrchestra-python-client/tags" alt="last-commit">
</p>

# pythonClient

Python client for [learningOrchestra](https://github.com/learningOrchestra/learningOrchestra).

# Installation

Requires Python 3.x

```
pip install learning-orchestra-client
```

# Usage

Import learning_orchestra_client:

```python
from learning_orchestra_client import *
```

Create a `Context` object passing an IP from your cluster  :

```python
cluster_ip = "34.95.222.197"
Context(cluster_ip)
```

After creating the `Context` object, you will be able to use learningOrchestra.

Each functionality in learningOrchestra is contained in its own class. Check below for all the available [function APIs]().

## Example

Shown below is an example usage of learning-orchestra-client using the [Titanic Dataset](https://www.kaggle.com/c/titanic/overview):

```python
from learning_orchestra_client import *

cluster_ip = "34.95.187.26"

Context(cluster_ip)

database_api = DatabaseApi()

print(database_api.create_file(
    "titanic_training",
    "https://filebin.net/rpfdy8clm5984a4c/titanic_training.csv?t=gcnjz1yo"))
print(database_api.create_file(
    "titanic_testing",
    "https://filebin.net/mguee52ke97k0x9h/titanic_testing.csv?t=ub4nc1rc"))

print(database_api.read_resume_files())


projection = Projection()
non_required_columns = ["Name", "Ticket", "Cabin",
                        "Embarked", "Sex", "Initial"]
print(projection.create("titanic_training",
                        "titanic_training_projection",
                        non_required_columns))
print(projection.create("titanic_testing",
                        "titanic_testing_projection",
                        non_required_columns))


data_type_handler = DataTypeHandler()
type_fields = {
    "Age": "number",
    "Fare": "number",
    "Parch": "number",
    "PassengerId": "number",
    "Pclass": "number",
    "SibSp": "number"
}

print(data_type_handler.change_file_type(
    "titanic_testing_projection",
    type_fields))

type_fields["Survived"] = "number"

print(data_type_handler.change_file_type(
    "titanic_training_projection",
    type_fields))


preprocessing_code = '''
from pyspark.ml import Pipeline
from pyspark.sql.functions import (
    mean, col, split,
    regexp_extract, when, lit)

from pyspark.ml.feature import (
    VectorAssembler,
    StringIndexer
)

TRAINING_DF_INDEX = 0
TESTING_DF_INDEX = 1

training_df = training_df.withColumnRenamed('Survived', 'label')
testing_df = testing_df.withColumn('label', lit(0))
datasets_list = [training_df, testing_df]

for index, dataset in enumerate(datasets_list):
    dataset = dataset.withColumn(
        "Initial",
        regexp_extract(col("Name"), "([A-Za-z]+)\.", 1))
    datasets_list[index] = dataset

misspelled_initials = [
    'Mlle', 'Mme', 'Ms', 'Dr',
    'Major', 'Lady', 'Countess',
    'Jonkheer', 'Col', 'Rev',
    'Capt', 'Sir', 'Don'
]
correct_initials = [
    'Miss', 'Miss', 'Miss', 'Mr',
    'Mr', 'Mrs', 'Mrs',
    'Other', 'Other', 'Other',
    'Mr', 'Mr', 'Mr'
]
for index, dataset in enumerate(datasets_list):
    dataset = dataset.replace(misspelled_initials, correct_initials)
    datasets_list[index] = dataset


initials_age = {"Miss": 22,
                "Other": 46,
                "Master": 5,
                "Mr": 33,
                "Mrs": 36}
for index, dataset in enumerate(datasets_list):
    for initial, initial_age in initials_age.items():
        dataset = dataset.withColumn(
            "Age",
            when((dataset["Initial"] == initial) &
                 (dataset["Age"].isNull()), initial_age).otherwise(
                    dataset["Age"]))
        datasets_list[index] = dataset


for index, dataset in enumerate(datasets_list):
    dataset = dataset.na.fill({"Embarked": 'S'})
    datasets_list[index] = dataset


for index, dataset in enumerate(datasets_list):
    dataset = dataset.withColumn("Family_Size", col('SibSp')+col('Parch'))
    dataset = dataset.withColumn('Alone', lit(0))
    dataset = dataset.withColumn(
        "Alone",
        when(dataset["Family_Size"] == 0, 1).otherwise(dataset["Alone"]))
    datasets_list[index] = dataset


text_fields = ["Sex", "Embarked", "Initial"]
for column in text_fields:
    for index, dataset in enumerate(datasets_list):
        dataset = StringIndexer(
            inputCol=column, outputCol=column+"_index").\
                fit(dataset).\
                transform(dataset)
        datasets_list[index] = dataset


non_required_columns = ["Name", "Embarked", "Sex", "Initial"]
for index, dataset in enumerate(datasets_list):
    dataset = dataset.drop(*non_required_columns)
    datasets_list[index] = dataset


training_df = datasets_list[TRAINING_DF_INDEX]
testing_df = datasets_list[TESTING_DF_INDEX]

assembler = VectorAssembler(
    inputCols=training_df.columns[:],
    outputCol="features")
assembler.setHandleInvalid('skip')

features_training = assembler.transform(training_df)
(features_training, features_evaluation) =\
    features_training.randomSplit([0.8, 0.2], seed=33)
features_testing = assembler.transform(testing_df)
'''

model_builder = Model()

print(model_builder.create_model(
    "titanic_training_projection",
    "titanic_testing_projection",
    preprocessing_code,
    ["lr", "dt", "gb", "rf", "nb"]))
```
# Function APIs

## Database API

### read_resume_files

```python
read_resume_files(pretty_response=True)
```
* `pretty_response`: returns indented `string` for visualization(default: `True`, returns `dict` if `False`)
(default `True`, if `False`, return dict)

### read_file

```python
read_file(filename, skip=0, limit=10, query={}, pretty_response=True)
```

* `filename` : name of file
* `skip`: number of rows  to skip in pagination(default: `0`)
* `limit`: number of rows to return in pagination(default: `10`)
(maximum is set at `20` rows per request)
* `query`: query to make in MongoDB(default: `empty query`)
* `pretty_response`: returns indented `string` for visualization(default: `True`, returns `dict` if `False`)

### create_file

```python
create_file(filename, url, pretty_response=True)
```

* `filename`: name of file to be created
* `url`: url to CSV file
* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

### delete_file

```python
delete_file(filename, pretty_response=True)
```

* `filename`: name of the file to be deleted
* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

## Projection API

### create_projection

```python
create_projection(filename, projection_filename, fields, pretty_response=True)
```

* `filename`: name of the file to make projection
* `projection_filename`: name of file used to create projection
* `fields`: list with fields to make projection 
* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

## Data type handler API 

### change_file_type

```python
change_file_type(filename, fields_dict, pretty_response=True)
```

* `filename`: name of file
* `fields_dict`: dictionary with `field`:`number` or `field`:`string` keys  
* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

## Histogram API

### create_histogram

```python
create_histogram(filename, histogram_filename, fields, 
                 pretty_response=True)
```

* `filename`: name of file to make histogram
* `histogram_filename`: name of file used to create histogram
* `fields`: list with fields to make histogram 
* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

## t-SNE API

### create_image_plot

```python
create_image_plot(tsne_filename, parent_filename,
                  label_name=None, pretty_response=True)
```

* `parent_filename`: name of file to make histogram
* `tsne_filename`: name of file used to create image plot
* `label_name`: label name to dataset with labeled tuples (default: `None`, to 
datasets without labeled tuples) 
* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

### read_image_plot_filenames

```python
read_image_plot_filenames(pretty_response=True)
```

* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

### read_image_plot

```python
read_image_plot(tsne_filename, pretty_response=True)
```

* tsne_filename: filename of a created image plot
* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

### delete_image_plot

```python
delete_image_plot(tsne_filename, pretty_response=True)
```

* `tsne_filename`: filename of a created image plot
* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

## PCA API

### create_image_plot

```python
create_image_plot(tsne_filename, parent_filename,
                  label_name=None, pretty_response=True)
```

* `parent_filename`: name of file to make histogram
* `pca_filename`: filename used to create image plot
* `label_name`: label name to dataset with labeled tuples (default: `None`, to 
datasets without labeled tuples) 
* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

### read_image_plot_filenames

```python
read_image_plot_filenames(pretty_response=True)
```

* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

### read_image_plot

```python
read_image_plot(pca_filename, pretty_response=True)
```

* `pca_filename`: filename of a created image plot
* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

### delete_image_plot

```python
delete_image_plot(pca_filename, pretty_response=True)
```

* `pca_filename`: filename of a created image plot
* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

## Model builder API

### create_model

```python
create_model(training_filename, test_filename, preprocessor_code, 
             model_classificator, pretty_response=True)
```

* `training_filename`: name of file to be used in training
* `test_filename`: name of file to be used in test
* `preprocessor_code`: Python3 code for pyspark preprocessing model
* `model_classificator`: list of initial classificators to be used in model
* `pretty_response`: returns indented `string` for visualization 
(default: `True`, returns `dict` if `False`)

#### model_classificator

* `lr`: LogisticRegression
* `dt`: DecisionTreeClassifier
* `rf`: RandomForestClassifier
* `gb`: Gradient-boosted tree classifier
* `nb`: NaiveBayes

to send a request with LogisticRegression and NaiveBayes Classifiers:

```python
create_model(training_filename, test_filename, preprocessor_code, ["lr", "nb"])
```

#### preprocessor_code environment

The Python 3 preprocessing code must use the environment instances as below:

* `training_df` (Instantiated): Spark Dataframe instance training filename
* `testing_df`  (Instantiated): Spark Dataframe instance testing filename

The preprocessing code must instantiate the variables as below, all instances must be transformed by pyspark VectorAssembler:

* `features_training` (Not Instantiated): Spark Dataframe instance for training the model
* `features_evaluation` (Not Instantiated): Spark Dataframe instance for evaluating trained model
* `features_testing` (Not Instantiated): Spark Dataframe instance for testing the model

In case you don't want to evaluate the model, set `features_evaluation` as `None`.

##### Handy methods

```python
self.fields_from_dataframe(dataframe, is_string)
```

This method returns `string` or `number` fields as a `string` list from a DataFrame.

* `dataframe`: DataFrame instance
* `is_string`: Boolean parameter(if `True`, the method returns the string DataFrame fields, otherwise, returns the numbers DataFrame fields)
