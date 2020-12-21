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

Each functionality in learningOrchestra is contained in its own class. Check the [python client docs](https://learningorchestra.github.io/pythonClient/) for all the available.

# Example

Shown below is an example usage of learning-orchestra-client using the [Titanic Dataset](https://www.kaggle.com/c/titanic/overview):

```python
from learning_orchestra_client import (
    dataset,
    builder,
    transform,
)

cluster_ip = "34.95.187.26"


dataset = Dataset(cluster_ip)

print(dataset.insert_dataset_sync(
    "titanic_training",
    "https://filebin.net/rpfdy8clm5984a4c/titanic_training.csv?t=gcnjz1yo"))
print(dataset.insert_dataset_sync(
    "titanic_testing",
    "https://filebin.net/mguee52ke97k0x9h/titanic_testing.csv?t=ub4nc1rc"))

print(dataset.search_all_datasets())


projection = Projection(cluster_ip)
required_columns = [
    "PassengerId",
    "Pclass",
    "Age",
    "SibSp",
    "Parch",
    "Fare",
    "Name",
    "Sex",
    "Embarked",
    "Survived"
    ]
print(projection.insert_dataset_attributes_sync(
        "titanic_training",
        "titanic_training_projection",
        required_columns))

required_columns.remove("Survived")

print(projection.insert_dataset_attributes_sync(
    "titanic_testing",
    "titanic_testing_projection",
    required_columns))


data_type_handler = DataType(cluster_ip)
type_fields = {
    "Age": "number",
    "Fare": "number",
    "Parch": "number",
    "PassengerId": "number",
    "Pclass": "number",
    "SibSp": "number"
}

print(data_type_handler.update_dataset_types(
    "titanic_testing_projection",
    type_fields))

type_fields["Survived"] = "number"

print(data_type_handler.update_dataset_types(
    "titanic_training_projection",
    type_fields))


modeling_code = '''
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

builder = Builder(cluster_ip)

print(builder.run_builder_sync(
    "titanic_training_projection",
    "titanic_testing_projection",
    modeling_code,
    ["lr", "dt", "gb", "rf", "nb"]))
```
