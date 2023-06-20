import requests
import csv

URL = "http://128.2.204.215:8080/user/"

with open('users.csv', 'w', newline='') as file:
    fieldnames = ["user_id","age","occupation","gender"]
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    for i in range(1,1000001):
        if (i%1000 == 0):
            print("Finished ", str(i)," rounds")
        msg = eval(requests.get(URL + str(i)).content.decode())
        #print(msg)
        writer.writerow(msg)

