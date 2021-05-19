from learning_orchestra_client.dataset.csv import DatasetCsv
from learning_orchestra_client.transform.projection import TransformProjection
from learning_orchestra_client.transform.data_type import TransformDataType
from learning_orchestra_client.builder.builder import BuilderSparkMl
from learning_orchestra_client.utils.file_caster import python_file_caster

CLUSTER_IP = "http://35.193.116.104"

dataset_csv = DatasetCsv(CLUSTER_IP)

dataset_csv.insert_dataset_async(
    url="https://filebin.net/boniydu54k710l54/train.csv?t=s350xryf",
    dataset_name="titanic_training",
)
dataset_csv.insert_dataset_async(
    url="https://filebin.net/udtf7eogfgasqnx5/test.csv?t=h79pcy0l",
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

modeling_code = python_file_caster('titanic/modeling_code.py')

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
