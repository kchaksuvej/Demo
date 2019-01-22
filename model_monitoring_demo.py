
# coding: utf-8

# ## Table of Contents
# 1. Data Simulation
# 2. AML Behavior 24 Execution
# 3. Review Result Simultion (True Positive, False Positive)
# 4. Performance Measure Calculation (Precision, False Discovery Rate)
# 5. Conclusion

# In[22]:

import random
import pandas as pd
import radar
import uuid
from datetime import timedelta
import warnings
warnings.filterwarnings("ignore")


# ### 1. Data Simulation

# In[4]:

ADMIN_CONTRACT_COUNT = 50

def generate_admin_contract_id(count, seed):
    random.seed(seed)
    return random.sample(range(10000000000000000, 99999999999999999), count)

def generate_trans_date(count, start, end):
    dates = []
    for i in range(count):
        date = radar.random_date(start=start, stop=end)
        dates.append(date)
    return dates

def generate_trans_amount(count):
    amounts = []
    for i in range(count):
        amount = random.randint(1000, 10000)
        amounts.append(amount)
    return amounts

def generate_trans_id(count):
    trans_id = []
    for i in range(count):
        trans_id.append(str(uuid.uuid4()))
    return trans_id

def generate_trans_individual(admin_contract_id, trans_type, start, end):
    random.seed(admin_contract_id)
    trans = pd.DataFrame()
    trans_count = random.randint(1, 15)
    trans["transaction_id"] = generate_trans_id(trans_count)
    trans["transaction_date"] = generate_trans_date(trans_count, start, end)
    trans["transaction_amount"] = generate_trans_amount(trans_count)
    trans["admin_contract_id"] = admin_contract_id
    trans["transaction_type"] = trans_type
    return trans

def generate_trans_final(admin_contract_ids, trans_type, start, end):
    trans_list = []
    for i in admin_contract_ids:
        trans = generate_trans_individual(i, trans_type, start, end)
        trans_list.append(trans)
    return pd.concat(trans_list)


# In[7]:

admin_contract_ids = generate_admin_contract_id(ADMIN_CONTRACT_COUNT, 1)
payment_transactions = generate_trans_final(admin_contract_ids, "payment", '2017-11-01', '2017-11-30')
payment_transactions.to_csv("payment_transaction_201711.csv", index=False)

admin_contract_ids = generate_admin_contract_id(ADMIN_CONTRACT_COUNT, 2)
payment_transactions = generate_trans_final(admin_contract_ids, "payment", '2017-12-01', '2017-12-31')
payment_transactions.to_csv("payment_transaction_201712.csv", index=False)

admin_contract_ids = generate_admin_contract_id(ADMIN_CONTRACT_COUNT, 3)
payment_transactions = generate_trans_final(admin_contract_ids, "payment", '2018-01-01', '2018-01-31')
payment_transactions.to_csv("payment_transaction_201801.csv", index=False)


# ### 2. AML Behavior 24 Execution

# In[8]:

THRESHOLD = 14

def aml_behavior24(csv_name):
    trans = pd.read_csv(csv_name)
    trans['transaction_date'] = pd.to_datetime(trans['transaction_date'])

    tmp_df_list = []

    admin_contract_ids = trans['admin_contract_id'].unique()

    for admin_contract_id in admin_contract_ids:
        individual_trans = trans[trans['admin_contract_id']==admin_contract_id][['transaction_amount', 'transaction_date']]
        individual_trans = individual_trans.set_index(['transaction_date'])
        individual_trans = individual_trans.resample('D').sum()
        for i in range(1, THRESHOLD):
            individual_trans['transaction_amount-'+str(i)] = individual_trans['transaction_amount'].shift(i)
        individual_trans['count'] = ((individual_trans[individual_trans.columns[2:]] >= 8000) & 
                                 (individual_trans[individual_trans.columns[2:]] <= 9999.99)).sum(axis=1)
        individual_trans = individual_trans[individual_trans['count']>=2]
        individual_trans = individual_trans.reset_index()
        individual_trans['admin_contract_id'] = admin_contract_id
        if individual_trans.shape[0] != 0:
            for date in individual_trans['transaction_date']:
                tmp = trans[(trans['transaction_date']<=date)&(trans['transaction_date']>=date-timedelta(days=THRESHOLD))&
                        (trans['admin_contract_id']==admin_contract_id)&(trans['transaction_amount']>=8000)& 
                        (trans['transaction_amount']<=9999.99)]
                tmp_df_list.append(tmp)
        if len(tmp_df_list) >= 1:
            results = pd.concat(tmp_df_list)
            results = results.drop_duplicates()
        else:
            results = pd.DataFrame()
    return results


# In[11]:

results_201711 = aml_behavior24("payment_transaction_201711.csv")
results_201712 = aml_behavior24("payment_transaction_201712.csv")
results_201801 = aml_behavior24("payment_transaction_201801.csv")


# ### 3. Review Result Simulation (True Positive, False Positive)

# In[29]:

results_201711["review_result"] = " "
results_201711["review_result"][:20] = "True Positive"
results_201711["review_result"][20:] = "False Positive"
results_201711


# In[30]:

results_201712["review_result"] = " "
results_201712["review_result"][:15] = "True Positive"
results_201712["review_result"][15:] = "False Positive"
results_201712


# In[31]:

results_201801["review_result"] = " "
results_201801["review_result"][:10] = "True Positive"
results_201801["review_result"][10:] = "False Positive"
results_201801


# ### 4. Performance Measure Calculation (Precision, False Discovery Rate)

# TP: True Positive
# 
# FP: False Positive
# 
# Precision: 
# $$precision = \frac{TP}{TP + NP}$$
# 
# False Discovery Rate (FDR):
# $$FDR = \frac{NP}{TP + NP} $$

# In[45]:

def cal_precision(result):
    return result.value_counts()["True Positive"] / result.value_counts().sum()

def cal_FDR(result):
    return result.value_counts()["False Positive"] / result.value_counts().sum()


# In[54]:

measures = pd.DataFrame()
measures["Time"] = ["2017/11", "2017/12", "2018/01"]
measures["Precision"] = [cal_precision(results_201711["review_result"]), 
                         cal_precision(results_201712["review_result"]), cal_precision(results_201801["review_result"])]
measures["FDR"] = [cal_FDR(results_201711["review_result"]), 
                   cal_FDR(results_201712["review_result"]), cal_FDR(results_201801["review_result"])]


# In[55]:

measures


# ### 5. Conclusion
# 
# The precision in the past three months are 47.62%, 39.47%, and 20%. THe false discovery rate in the past months are 52.38%, 60.52%, 80%. 
# 
# Because the precision has been decreasing and the false discovery rate has been increasing in the past three months, the performance of AML Behavior 24 is decaying. It should be examined.
