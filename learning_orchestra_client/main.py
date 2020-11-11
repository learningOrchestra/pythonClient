from dataset.dataset import Dataset

ip_from_cluster = "35.247.250.122"
dataset_name = "train_02"
url = "https://filebin.net/cudw93up9y2jsizg/train.xls?t=igt76bj6"

p = Dataset(ip_from_cluster)
# just_print = p.insert_dataset_sync(dataset_name, url)
just_print = p.search_all_datasets()
# just_print = p.search_dataset_content(dataset_name, "{}", limit=0, skip=0)
# just_print = p.delete_dataset(dataset_name)
print(just_print)
