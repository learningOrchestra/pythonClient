from learning_orchestra_client.dataset.csv import DatasetCsv
from learning_orchestra_client.transform.projection import TransformProjection
from learning_orchestra_client.transform.data_type import TransformDataType
from learning_orchestra_client.builder.builder import BuilderSparkMl

CLUSTER_IP = "http://35.247.197.191"

dataset_csv = DatasetCsv(CLUSTER_IP)

dataset_csv.insert_dataset_async(
    url="https://filebin.net/48b0fwidk4amp7fa/train.csv",
    dataset_name="titanic_training",
)
dataset_csv.insert_dataset_async(
    url="https://filebin.net/1ewibio2rziv6lrm/test.csv",
    dataset_name="titanic_testing"
)

dataset_csv.wait(dataset_name="titanic_training")
dataset_csv.wait(dataset_name="titanic_testing")

print(dataset_csv.search_dataset_content("titanic_training", limit=1,
                                         pretty_response=True))
print(
    dataset_csv.search_dataset_content("titanic_testing", limit=1,
                                       pretty_response=True))

transform_projection = TransformProjection(CLUSTER_IP)
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

transform_projection.remove_dataset_attributes_async(
    dataset_name="titanic_training",
    projection_name="titanic_training_projection",
    fields=required_columns)

required_columns.remove("Survived")

transform_projection.remove_dataset_attributes_async(
    dataset_name="titanic_testing",
    projection_name="titanic_testing_projection",
    fields=required_columns)

transform_projection.wait(projection_name="titanic_training_projection")
transform_projection.wait(projection_name="titanic_testing_projection")

print(transform_projection.search_projection_content(
    projection_name="titanic_training_projection", limit=1,
    pretty_response=True))

print(transform_projection.search_projection_content(
    projection_name="titanic_testing_projection", limit=1,
    pretty_response=True))

transform_data_type = TransformDataType(CLUSTER_IP)
type_fields = {
    "Age": "number",
    "Fare": "number",
    "Parch": "number",
    "PassengerId": "number",
    "Pclass": "number",
    "SibSp": "number"
}

transform_data_type.update_dataset_type_async(
    dataset_name="titanic_testing_projection",
    types=type_fields)

type_fields.update({"Survived": "number"})

transform_data_type.update_dataset_type_async(
    dataset_name="titanic_training_projection",
    types=type_fields)

transform_data_type.wait(dataset_name="titanic_testing_projection")
transform_data_type.wait(dataset_name="titanic_training_projection")

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

columns_without_label = training_df.columns.copy()
columns_without_label.remove("label")

assembler = VectorAssembler(
    inputCols=columns_without_label,
    outputCol="features")
assembler.setHandleInvalid('skip')

features_training = assembler.transform(training_df)
(features_training, features_evaluation) =\
    features_training.randomSplit([0.8, 0.2], seed=33)
features_testing = assembler.transform(testing_df)
'''

builder = BuilderSparkMl(CLUSTER_IP)
result = builder.run_spark_ml_async(
    train_dataset_name="titanic_training_projection",
    test_dataset_name="titanic_testing_projection",
    modeling_code=modeling_code,
    model_classifiers=["LR", "DT", "GB", "RF", "NB"])

PREDICTION_NAME_INDEX_IN_URL = 6
INDEX_TO_REMOVE_URI_PARAMETERS = 0
for prediction_url in result["result"]:
    prediction_name = prediction_url. \
        split("/")[PREDICTION_NAME_INDEX_IN_URL]. \
        split("?")[INDEX_TO_REMOVE_URI_PARAMETERS]
    builder.wait(dataset_name=prediction_name)
    print(builder.search_builder_register_predictions(
        builder_name=prediction_name, limit=1, pretty_response=True))
