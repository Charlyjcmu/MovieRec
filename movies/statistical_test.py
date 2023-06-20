import pandas as pd
from scipy.stats import ttest_rel
import datetime

# Define a datetime object with the current date and time
#will collect rmse from latest svd and nmf
svd_rmse=[]
svd_size=[]
nmf_rmse=[]
nmf_size=[]

with open('/Users/joonghochoi/Desktop/group-project-s23-strangers/Telemetry/Metrics.txt', 'r') as f:
    for line in f:
        if line.startswith('SVD'):
            svd_rmse.append(float(line.split()[1]))
            svd_size.append(float(line.split()[4]))
        elif line.startswith('NMF'):
            nmf_rmse.append(float(line.split()[1]))
            nmf_size.append(float(line.split()[4]))
print(f'SVD RMSE: {svd_rmse}\nNMF RMSE: {nmf_rmse}')
svd_size=svd_size[-1]
nmf_size=nmf_size[-1]

svd_rmse=svd_rmse[-5:]
nmf_rmse=nmf_rmse[-5:]

print("length of svd rmse",len(svd_rmse))
print("length of nmf rmse",len(nmf_rmse))

# # Perform paired t-test
t_stat, p_value = ttest_rel(svd_rmse, nmf_rmse)

# # Report results
average_svd_rmse=sum(svd_rmse)/len(svd_rmse)
latest_svd_rmse=svd_rmse[-1]
average_nmf_rmse=rmse=sum(nmf_rmse)/len(nmf_rmse)
latest_nmf_rmse=nmf_rmse[-1]
# print(f"Model 1 RMSE: {sum(svd_rmse)/len(svd_rmse):.3f}")
print(f"Model 1 RMSE: {average_svd_rmse:.3f}")
print(f"Model 2 RMSE: {average_nmf_rmse:.3f}")
print(f"Difference in RMSE: {average_svd_rmse - average_nmf_rmse:.3f}")
print(f"Paired t-test: t={t_stat:.3f}, p={p_value:.3f}")

better_average_model=""
if average_svd_rmse<=average_nmf_rmse:
    better_average_model="SVD"
else:
    better_average_model="NMF"
better_latest_model=""
if latest_svd_rmse<=latest_nmf_rmse:
    better_latest_model="SVD"
else:
    better_latest_model="NMF"

stat_sig="not statistically significant"
if p_value<0.05:
    stat_sig="statistically significant"

now = datetime.datetime.now()
res_msg= f"Time for comparison is {str(now)}; \n the better average model over past 5 versions is {better_average_model}, and \n the superiority over other algorithm's average 5 versions is {stat_sig}. \n The better latest model is {better_latest_model}."

if stat_sig=="not statistically significant":
    diff= abs(latest_svd_rmse-latest_nmf_rmse)
    if diff<=0.05:
        if svd_size<=nmf_size:
            smaller_model="SVD"
        else:
            smaller_model="NMF"
        res_msg=res_msg+" However, we choose to use "+smaller_model+" since difference in performance is \nnot statistically significant, and this model is much smaller."
#use latest 5 versions for statistical test
#say which latest version of the model has rmse. IF not by much, use model size.
#save it to a text file. 
print(res_msg)
with open('statistic_experimentation.txt', 'w') as f:
    f.write(res_msg)
