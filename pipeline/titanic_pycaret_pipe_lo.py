from learning_orchestra_client.builder.builder import BuilderPycaret
from learning_orchestra_client.dataset.csv import DatasetCsv

CLUSTER_IP = "http://34.151.200.12"

dataset_csv = DatasetCsv(CLUSTER_IP)
dataset_csv.delete_dataset("train")
dataset_csv.insert_dataset_async(
    url="https://raw.githubusercontent.com/JonatasMiguel/PycaretTitanic/main/train.csv",
    dataset_name="train",
)
dataset_csv.wait(dataset_name="train",
                 timeout=1)

version = 5
name = f'titanic{version}'
codigo = """
from pycaret import classification
from pycaret.classification import pull,tune_model

clas = classification.setup(data=train, target='Survived', train_size=0.7, silent=True)
best = classification.compare_models()

best_tuned = tune_model(best)

report = pull()
report.to_csv('report_tuned', sep='\t', encoding='utf-8')

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
    code=codigo)
builder.wait(name, 1)
print(builder.search_execution_content(
    name=name,
    pretty_response=True))

print(builder.search_report(file_name='report_tuned'))

print('fim')
