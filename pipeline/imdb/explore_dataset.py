pos=data[data["label"]=="1"]
neg=data[data["label"]=="0"]

total_rows = len(pos) + len(neg)

print("Positive = " + str(len(pos) / total_rows))
print("Negative = " + str(len(neg) / total_rows))

response = None