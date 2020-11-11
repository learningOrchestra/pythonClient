from client.dataset.dataset import Dataset

ip_from_cluster = "35.247.250.122"
dataset_name = "train_02"
url = "https://filebin.net/cudw93up9y2jsizg/train.xls?t=jgc5pksx"

p = Dataset(ip_from_cluster)
#p.post(dataset_name, url)
p.get_dataset_list()
#p.get_dataset(dataset_name)
#p.delete(dataset_name)
