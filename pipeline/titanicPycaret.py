from learning_orchestra_client.builder.builder import BuilderPycaret
from learning_orchestra_client.dataset.csv import DatasetCsv
from learning_orchestra_client.transform.data_type import TransformDataType
import time

start = time.time()

CLUSTER_IP = "http://34.151.210.120"

dataset_csv = DatasetCsv(CLUSTER_IP)


dataset_csv.insert_dataset_async(
    url="https://raw.githubusercontent.com/JonatasMiguel/"
        "PycaretTitanic/main/train.csv",
    dataset_name="train",
)
dataset_csv.wait(dataset_name="train",
                 timeout=1)

transform_data_type = TransformDataType(CLUSTER_IP)
type_fields = {
    "Age": "number",
    "Fare": "number",
    "Parch": "number",
    "Pclass": "number",
    "SibSp": "number"
}
transform_data_type.update_dataset_type_async(
    dataset_name=f"train",
    types=type_fields)

transform_data_type.wait(dataset_name=f"train",
                         timeout=1)

name = f'titanicPycaret'
code = """
from pycaret import classification

clas = classification.setup(
    data=train,
    target='Survived',
    ignore_features=['Ticket'],
    numeric_features=['Age','Fare','Parch','Pclass','SibSp'],
    session_id = 1,
    silent=True) 

best = classification.compare_models(turbo = False)

best_tuned = classification.tune_model(
    best,
    n_iter = 100,
    choose_better=True)

best_tuned = classification.create_model(best_tuned)

score = classification.pull()
score.to_csv('score', sep='\t', encoding='utf-8')

final_gbr = classification.finalize_model(best_tuned)

classification.save_model(final_gbr, 'titanic_pycaret')

response = None
"""
builder = BuilderPycaret(CLUSTER_IP)

builder.run_pycaret_async(
    name=name,
    parameters={
        "train": "$train"
    },
    code=code)
builder.wait(name, 1)

print(builder.search_execution_content(
    name=name,
    pretty_response=True))

print(builder.search_builder_register_predictions(file_name='score'))

end = time.time()
print(f'Run time: {end - start}')
