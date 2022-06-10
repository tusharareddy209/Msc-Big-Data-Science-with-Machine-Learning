import pyspark
import time

sc = pyspark.SparkContext()

def clean(line):
    try:
        fields = line.split(',')
        if len(fields) == 7:  
            str(fields[2])  
            if int(fields[3]) == 0:
                return False
        elif len(fields) == 5:  
            str(fields[0])  
        else:
            return False
        return True
    except:
        return False

def mapper(line):
    try:
        fields = line.split(',')
        raw_timestamp = int(fields[6])
        year_month = time.strftime('%Y-%m W%W', time.gmtime(raw_timestamp))
        key = (fields[2], year_month)

        gas_supplied = int(fields[4])
        return (key, (gas_supplied, 1))
    except:
        pass

def shift_key(line):
    try:
        return (line[0][0], (line[0][1], line[1][0], line[1][1]))
    except:
        pass

def read_contract(line):
    try:
        fields = line.split(',')
        return (fields[0], 'Contract')
    except:
        pass

def shift_key_contract(line):
    try:
        addr_type = 'Non-Contract' if line[1][1] is None else line[1][1]
        week_num = line[1][0][0]
        total_gas = line[1][0][1]
        total_trxn = line[1][0][2]
        return ((addr_type, week_num), (total_gas, total_trxn))
    except:
        pass

def shift_key_results(line):
    try:
        return (line[0][1], (line[0][0], line[1]))
    except:
        pass

cleaned_transactions = sc.textFile('/data/ethereum/transactions').filter(clean)
cleaned_contracts = sc.textFile('/data/ethereum/contracts').filter(clean)


mapped_transactions = cleaned_transactions.map(mapper)
keyed_transactions = mapped_transactions.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
shifted_keyed_transactions = keyed_transactions.map(shift_key)
contract_transactions = shifted_keyed_transactions.leftOuterJoin(cleaned_contracts.map(read_contract))
shifted_keyed_contract_transactions = contract_transactions.map(shift_key_contract)
keyed_contract_transactions = shifted_keyed_contract_transactions.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
final_values = keyed_contract_transactions.mapValues(lambda x: x[0]/x[1])
shifted_final_values = final_values.map(shift_key_results)
final_results = shifted_final_values.sortByKey()

print('YMW ,Year ,Week Number ,Type ,Average Amount of Gas')
for row in final_results.collect():
    print('{0},{1},{2},{3},{4}'.format(row[0], row[0][0:4], row[0][9:], row[1][0], row[1][1]))  