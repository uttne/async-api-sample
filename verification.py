from dataclasses import dataclass, field
import time
import sqlite3
import os


@dataclass
class Ope:
    sky: str


@dataclass
class Current:
    cur: str
    prv: list[str]


class S3:

    def __init__(self) -> None:
        self.data: set = set()

    def save(self, sky: str) -> None:
        self.data.add(sky)

    def load(self, sky: str) -> str | None:
        if sky in self.data:
            return sky
        return None

    def delete(self, sky: str) -> None:
        self.data.discard(sky)

    def clear(self) -> None:
        self.data.clear()


class DynamoDb:

    def __init__(self) -> None:
        self.cur: Current | None = None
        self.opes: list[Ope | None] = [None for _ in range(20)]

    def put(self, ope: Ope) -> None:
        self.opes[int(ope.sky)] = ope

    def update(self, cur: Current) -> Current | None:
        prev = self.cur

        if prev and cur.cur <= prev.cur:
            # cur が古い場合は更新をしないようにする
            return None

        self.cur = cur
        return prev

    def query(self) -> tuple[Current | None, list[Ope]]:
        return (self.cur, [v for v in self.opes if v])

    def clear(self) -> None:
        self.cur = None
        self.opes = [None for _ in range(20)]


class Lambda:
    def __init__(self, s3: S3, dynamoDb: DynamoDb, ope: Ope) -> None:
        self.s3 = s3
        self.dynamoDb = dynamoDb
        self.ope = ope

        self.err = ""

        self.step02_opes: list[Current | Ope] = None
        self.step02_cur: Current | None = None

        self.step03_cur: str | None = None
        self.step03_prv: str | None = None

        self.step04_update_prev_cur: Current | None = None

    def clear(self) -> None:
        self.err = ""
        self.step02_opes = None
        self.step02_cur = None
        self.step03_cur = None
        self.step03_prv = None
        self.step04_update_prev_cur = None

    def step01(self) -> None:

        if self.err:
            return
        self.dynamoDb.put(self.ope)

    def step02(self) -> None:

        if self.err:
            return
        cur, opes = self.dynamoDb.query()

        self.step02_opes = opes
        self.step02_cur = cur

    def step03(self) -> None:

        if self.err:
            return
        s3_cur = None
        if self.step02_cur:
            s3_cur = self.s3.load(self.step02_cur.cur)
            s3_prv = self.step02_cur.prv[0]
            if not s3_cur:
                s3_cur = self.s3.load(self.step02_cur.prv[0])
                s3_prv = self.step02_cur.prv[1]

        if s3_cur:
            self.step03_cur = s3_cur
            self.step03_prv = s3_prv
            return

        self.err = "step03"

    def step04(self) -> None:

        if self.err:
            return
        self.s3.save(self.ope.sky)

    def step05(self) -> None:

        if self.err:
            return
        if self.step03_cur:
            prev = [self.step03_cur, self.step03_prv]
        else:
            prev = []
        cur = Current(self.ope.sky, prev)
        self.step04_update_prev_cur = self.dynamoDb.update(cur)

    def step06(self):

        if self.err:
            return
        if self.step04_update_prev_cur:
            # 更新成功
            if self.step04_update_prev_cur.prv[0] == self.step03_cur:
                # 同じ cur をもとに更新をしていた場合は削除する
                self.s3.delete(self.step04_update_prev_cur.cur)
            # 更新成功であれば
            if self.step03_prv:
                self.s3.delete(self.step03_prv)
        else:
            # update が失敗の時は自分のデータを削除
            self.s3.delete(self.ope.sky)


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


db_file = "./verification.db"

existed_db = os.path.exists(db_file)

conn = sqlite3.connect(db_file)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY,
                   
    i1 INTEGER NOT NULL,
    i2 INTEGER NOT NULL,
    i3 INTEGER NOT NULL,
    i4 INTEGER NOT NULL,
    i5 INTEGER NOT NULL,
    i6 INTEGER NOT NULL,
                   
    j1 INTEGER NOT NULL,
    j2 INTEGER NOT NULL,
    j3 INTEGER NOT NULL,
    j4 INTEGER NOT NULL,
    j5 INTEGER NOT NULL,
    j6 INTEGER NOT NULL,
                   
    k1 INTEGER NOT NULL,
    k2 INTEGER NOT NULL,
    k3 INTEGER NOT NULL,
    k4 INTEGER NOT NULL,
    k5 INTEGER NOT NULL,
    k6 INTEGER NOT NULL,
                   
    lambda1_err TEXT NOT NULL,
    lambda2_err TEXT NOT NULL,
    lambda3_err TEXT NOT NULL,
    lambda4_err TEXT NOT NULL,
    lambda5_err TEXT NOT NULL,
                   
    a3_s3_count INTEGER NOT NULL,
                   
    a3_dynamodb_cur TEXT NOT NULL,
    a3_dynamodb_prv_count INTEGER NOT NULL,
                   
    a5_s3_count INTEGER NOT NULL,
                   
    a5_dynamodb_cur TEXT NOT NULL,
    a5_dynamodb_prv_count INTEGER NOT NULL
    )
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS s3 (
    id INTEGER PRIMARY KEY,
    log_id INTEGER NOT NULL,
    
    name TEXT NOT NULL,
    data TEXT NOT NULL
    )
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS dynamodb (
    id INTEGER PRIMARY KEY,
    log_id INTEGER NOT NULL,
    
    name TEXT NOT NULL,
    prev_data TEXT NOT NULL
    );
""")
conn.commit()

a3_s3_data_count: int = None
a3_s3_data: set[str] = None
a3_dynamodb_cur: str = None
a3_dynamodb_prv_count: int = None
a3_dynamodb_prv: list[str] = None


def capcher_a3():
    global a3_s3_data_count
    global a3_s3_data
    global a3_dynamodb_cur
    global a3_dynamodb_prv_count
    global a3_dynamodb_prv

    a3_s3_data_count = len(s3.data)
    a3_s3_data = set(s3.data)
    a3_dynamodb_cur = dynamoDb.cur.cur
    a3_dynamodb_prv_count = len(dynamoDb.cur.prv)
    a3_dynamodb_prv = list(dynamoDb.cur.prv)


def insert_logs():

    a5_s3_data_count = len(s3.data)
    a5_s3_data = set(s3.data)
    a5_dynamodb_cur = dynamoDb.cur.cur
    a5_dynamodb_prv_count = len(dynamoDb.cur.prv)
    a5_dynamodb_prv = list(dynamoDb.cur.prv)

    cursor.execute('''
INSERT INTO logs (id,
    i1, i2, i3, i4, i5, i6,
    j1, j2, j3, j4, j5, j6,
    k1, k2, k3, k4, k5, k6,
    lambda1_err, lambda2_err, lambda3_err, lambda4_err, lambda5_err,
    a3_s3_count,
    a3_dynamodb_cur,
    a3_dynamodb_prv_count,
    a5_s3_count,
    a5_dynamodb_cur,
    a5_dynamodb_prv_count) VALUES (?,
    ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?)
''', (_id,
      i1, i2, i3, i4, i5, i6,
      j1, j2, j3, j4, j5, j6,
      k1, k2, k3, k4, k5, k6,
      lambda01.err, lambda02.err, lambda03.err, lambda04.err, lambda05.err,
      a3_s3_data_count,
      a3_dynamodb_cur,
      a3_dynamodb_prv_count,
      a5_s3_data_count,
      a5_dynamodb_cur,
      a5_dynamodb_prv_count))

    for d in a3_s3_data:
        cursor.execute('''
    INSERT INTO s3 (log_id,
        name,data) VALUES (?,
        ?, ?)
    ''', (_id,
          "a3", d))

    for d in a3_dynamodb_prv:
        cursor.execute('''
    INSERT INTO dynamodb (log_id,
        name,prev_data) VALUES (?,
        ?, ?)
    ''', (_id,
          "a3", d))

    for d in a5_s3_data:
        cursor.execute('''
    INSERT INTO s3 (log_id,
        name,data) VALUES (?,
        ?, ?)
    ''', (_id,
          "a5", d))

    for d in a5_dynamodb_prv:
        cursor.execute('''
    INSERT INTO dynamodb (log_id,
        name,prev_data) VALUES (?,
        ?, ?)
    ''', (_id,
          "a5", d))


operations = [None for _ in range(6*3)]


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

    capcher_a3()

    for func in ope4s:
        func()

    for func in ope5s:
        func()

    insert_logs()


_id = 0

t = time.perf_counter()
try:
    start = time.perf_counter()
    for i1 in range(0, len(operations)-5):
        for i2 in range(1 + i1, len(operations)-4):
            for i3 in range(1 + i2, len(operations)-3):
                for i4 in range(1 + i3, len(operations)-2):
                    for i5 in range(1 + i4, len(operations)-1):
                        for i6 in range(1 + i5, len(operations)-0):

                            j_index_list = set(
                                [i for i in range(len(operations))])
                            j_index_list.remove(i1)
                            j_index_list.remove(i2)
                            j_index_list.remove(i3)
                            j_index_list.remove(i4)
                            j_index_list.remove(i5)
                            j_index_list.remove(i6)

                            j_index_list = list(j_index_list)
                            for _j1 in range(0, len(j_index_list)-5):
                                for _j2 in range(1 + _j1, len(j_index_list)-4):
                                    for _j3 in range(1 + _j2, len(j_index_list)-3):
                                        for _j4 in range(1 + _j3, len(j_index_list)-2):
                                            for _j5 in range(1 + _j4, len(j_index_list)-1):
                                                for _j6 in range(1 + _j5, len(j_index_list)-0):

                                                    j1 = j_index_list[_j1]
                                                    j2 = j_index_list[_j2]
                                                    j3 = j_index_list[_j3]
                                                    j4 = j_index_list[_j4]
                                                    j5 = j_index_list[_j5]
                                                    j6 = j_index_list[_j6]

                                                    k_index_list = set(
                                                        j_index_list)
                                                    k_index_list.remove(j1)
                                                    k_index_list.remove(j2)
                                                    k_index_list.remove(j3)
                                                    k_index_list.remove(j4)
                                                    k_index_list.remove(j5)
                                                    k_index_list.remove(j6)

                                                    k_index_list = list(
                                                        k_index_list)
                                                    k1 = k_index_list[0]
                                                    k2 = k_index_list[1]
                                                    k3 = k_index_list[2]
                                                    k4 = k_index_list[3]
                                                    k5 = k_index_list[4]
                                                    k6 = k_index_list[5]

                                                    # print(
                                                    #     "------------------------")
                                                    # print(
                                                    #     f"{i1} , {i2} , {i3} , {i4} , {i5} , {i6}")
                                                    # print(
                                                    #     f"{j1} , {j2} , {j3} , {j4} , {j5} , {j6}")
                                                    # print(
                                                    #     f"{k1} , {k2} , {k3} , {k4} , {k5} , {k6}")

                                                    operations[i1] = ope1s[0]
                                                    operations[i2] = ope1s[1]
                                                    operations[i3] = ope1s[2]
                                                    operations[i4] = ope1s[3]
                                                    operations[i5] = ope1s[4]
                                                    operations[i6] = ope1s[5]

                                                    operations[j1] = ope2s[0]
                                                    operations[j2] = ope2s[1]
                                                    operations[j3] = ope2s[2]
                                                    operations[j4] = ope2s[3]
                                                    operations[j5] = ope2s[4]
                                                    operations[j6] = ope2s[5]

                                                    operations[k1] = ope3s[0]
                                                    operations[k2] = ope3s[1]
                                                    operations[k3] = ope3s[2]
                                                    operations[k4] = ope3s[3]
                                                    operations[k5] = ope3s[4]
                                                    operations[k6] = ope3s[5]

                                                    run()

                                                    if _id % 1000 == 0:
                                                        t2 = time.perf_counter()
                                                        one_t = (
                                                            t2 - t) * 0.001
                                                        remaining_time = (
                                                            17153136 - _id + 1) * one_t
                                                        print(str(_id).rjust(
                                                            8) + " / 17153136 : "+str(int(remaining_time)).rjust(5)+"[s]\r", end="")

                                                        t = t2

                                                    _id = _id + 1

finally:
    conn.commit()
    cursor.close()
    conn.close()
