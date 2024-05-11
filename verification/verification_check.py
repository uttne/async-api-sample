import sqlite3
from verification import S3, DynamoDb, Ope, Current, Lambda

db_file = "./verification.db"

conn = sqlite3.connect(db_file)
cur = conn.cursor()


# id	i1	i2	i3	i4	i5	i6	j1	j2	j3	j4	j5	j6	k1	k2	k3	k4	k5	k6	lambda1_err	lambda2_err	lambda3_err	lambda4_err	lambda5_err	a3_s3_count	a3_dynamodb_cur	a3_dynamodb_prv_count	a5_s3_count	a5_dynamodb_cur	a5_dynamodb_prv_count
# 23814	0	1	2	3	6	7	8	9	10	11	12	13	4	5	14	15	16	17			step03			2	11	2	2	14	2

cur.execute("""
SELECT * FROM logs WHERE id = ?
""", (23814,))

item = [row for row in cur.fetchall()][0]


s3 = S3()
dynamoDb = DynamoDb()

ope1 = Ope("10")
ope2 = Ope("11")
ope3 = Ope("12")
ope4 = Ope("13")
ope5 = Ope("14")

current = Current("09", ["08", "07"])

lambda01 = Lambda(s3, dynamoDb, ope1)
lambda02 = Lambda(s3, dynamoDb, ope2)
lambda03 = Lambda(s3, dynamoDb, ope3)
lambda04 = Lambda(s3, dynamoDb, ope4)
lambda05 = Lambda(s3, dynamoDb, ope5)

ope1s = [
    lambda01.step01, lambda01.step02, lambda01.step03, lambda01.step04, lambda01.step05, lambda01.step06
]
ope2s = [
    lambda02.step01, lambda02.step02, lambda02.step03, lambda02.step04, lambda02.step05, lambda02.step06
]

ope3s = [
    lambda03.step01, lambda03.step02, lambda03.step03, lambda03.step04, lambda03.step05, lambda03.step06
]

ope4s = [
    lambda04.step01, lambda04.step02, lambda04.step03, lambda04.step04, lambda04.step05, lambda04.step06
]

ope5s = [
    lambda05.step01, lambda05.step02, lambda05.step03, lambda05.step04, lambda05.step05, lambda05.step06
]

operations = [None for _ in range(6*3)]

index_list = [r for r in item][1:]

for i in range(6):
    operations[index_list[i+0]] = ope1s[i]

for i in range(6):
    operations[index_list[i+6]] = ope2s[i]

for i in range(6):
    operations[index_list[i+12]] = ope3s[i]


def run():
    s3.clear()
    dynamoDb.clear()
    lambda01.clear()
    lambda02.clear()
    lambda03.clear()
    lambda04.clear()
    lambda05.clear()

    s3.data.add("09")
    s3.data.add("08")

    dynamoDb.cur = current

    for func in operations:
        func()

    # for func in ope4s:
    #     func()

    # for func in ope5s:
    #     func()


run()

print(s3.data)
print(dynamoDb.cur)
