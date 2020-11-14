import json

from dataset.dataset import Dataset
from transform.projection import Projection


ip_from_cluster = "35.199.118.241"
dataset_name = "train-01"
url = "https://filebin.net/vhtptyxrd0bko6g0/train.csv?t=ab9zu9zh"
dataset_name01 = "train-0"
projection_name = "projection_train"

p = Dataset(ip_from_cluster)
pp = Projection(ip_from_cluster)
just_print = p.insert_dataset_sync(dataset_name, url, True)
print((just_print))
# just_print = p.insert_dataset_sync("brabo-2", url, True)
# print((just_print))
# just_print = p.insert_dataset_sync("brabo-3", url, True)
# print((just_print))
# print((just_print))

# just_print = p.search_dataset_content(dataset_name)
# just_print = p.delete_dataset(dataset_name)
# print((just_print))
# just_print = p.search_all_datasets(True)
# just_print = p.delete_dataset("brabo-1")
# just_print = p.delete_dataset("brabo-2")
# print((just_print))
# just_print = p.delete_dataset("brabo-3")
# print((just_print))

# just_print = pp.search_projections(dataset_name, pretty_response=True)
# just_print = pp.search_all_projections(True)
# just_print = pp.search_projections_content(dataset_name="test", pretty_response=True)
# just_print = pp.delete_projections(projection_name, pretty_response=True)

# print((just_print))
