import json

from dataset.dataset import Dataset
from transform.projection import Projection


ip_from_cluster = "34.95.153.222"
dataset_name = "test-0"
url = "https://filebin.net/vhtptyxrd0bko6g0/train.csv?t=4emhny4s"
dataset_name01 = "train-0"
projection_name = "projection_train"

p = Dataset(ip_from_cluster)
pp = Projection(ip_from_cluster)
# just_print = p.insert_dataset_sync("brabo-1", url, True)
# print((just_print))
# just_print = p.insert_dataset_sync("brabo-2", url, True)
# print((just_print))
# just_print = p.insert_dataset_sync("brabo-3", url, True)
# print((just_print))
# print((just_print))
#
# just_print = p.search_all_datasets(True)
# just_print = p.search_dataset_content(dataset_name)
# just_print = p.delete_dataset(dataset_name)
# just_print = p.delete_dataset("brabo-1")
# print((just_print))
# just_print = p.delete_dataset("brabo-2")
# print((just_print))
# just_print = p.delete_dataset("brabo-3")
# print((just_print))




# just_print = pp.search_projections(dataset_name, pretty_response=True)
# just_print = pp.search_all_projections(True)
# just_print = pp.search_projections_content(dataset_name="test", pretty_response=True)
# just_print = pp.delete_projections(projection_name, pretty_response=True)

# print((just_print))
