## SQLite をシンプルに S3 に保存

API Gateway からのリクエストが必ず 1 つだけであれば以下のようなシーケンスで問題なく SQLite を使える

```mermaid
sequenceDiagram
    participant api as API Gateway
    participant lambda as Lambda
    participant s3 as S3
    api->>lambda: 書き込み
    lambda ->> s3: .db 取得
    s3 -->> lambda: .db ファイル
    lambda ->> lambda: .db に書き込み
    lambda ->> s3: .db 保存
    lambda ->> api: 結果
```

ただし、2 つ以上のリクエストが入ると DB への書き込みが上書きされてしまう場合が存在するので通常利用は不可

```mermaid
sequenceDiagram
    participant api as API Gateway
    participant lambda1 as Lambda1
    participant lambda2 as Lambda2
    participant s3 as S3
    api->>lambda1: 書き込み
    api->>lambda2: 書き込み
    lambda1 ->> s3: .db 取得
    s3 -->> lambda1: .db ファイル
    lambda2 ->> s3: .db 取得
    s3 -->> lambda2: .db ファイル
    lambda1 ->> lambda1: .db に書き込み
    lambda2 ->> lambda2: .db に書き込み
    lambda1 ->> s3: .db 保存
    lambda2 ->> s3: .db 保存
    Note right of s3: Lambda1の書き込み結果が消える
    lambda1 ->> api: 結果
    lambda2 ->> api: 結果
```

## S3 を Pub/Sub の Topic に利用して非同期

Sub. Lambda は同時実行数を 1 に設定しておく。  
S3 のイベントで Sub. Lambda を発火するときに複数のイベントが発火するときスロットリングが発生するが、S3 の場合はリトライをしてくれるのでイベントが落ちることはない様子。
ただし、10 topic で試してみたところすべての topic を処理するのに 30s ～ 2m 程かかったので実用性はあまりない。

```mermaid
sequenceDiagram
    participant api as API Gateway
    participant lambda1 as Pub. Lambda1
    participant lambda2 as Pub. Lambda2
    participant s3 as Topic S3
    participant lambda_sub as Sub. Lambda
    participant s3_db as DB S3
    participant s3_res as Response S3

    api->>lambda1: 書き込み
    lambda1 ->> s3: topic1 書き込み
    lambda1 ->> api: 結果

    api->>lambda2: 書き込み
    lambda2 ->> s3: topic2 書き込み
    lambda2 ->> api: 結果

    s3 ->> lambda_sub: topic1の イベント発火
    lambda_sub ->> s3_db: .db を取得
    s3_db ->> lambda_sub: .db ファイル
    lambda_sub ->> lambda_sub: .db に書き込み
    lambda_sub ->> s3_db: .db を保存
    lambda_sub ->> s3_res: topic1 の結果を保存

    s3 ->> lambda_sub: topic2の イベント発火
    lambda_sub ->> s3_db: .db を取得
    Note right of s3_db: キャッシュが残っていれば取得しない
    s3_db ->> lambda_sub: .db ファイル
    lambda_sub ->> lambda_sub: .db に書き込み
    lambda_sub ->> s3_db: .db を保存
    lambda_sub ->> s3_res: topic2 の結果を保存

```

## DynamoDB をオペレーションのキューとして扱う

```mermaid
sequenceDiagram
    participant api as API Gateway
    participant lambda1 as Worker Lambda1
    participant lambda2 as Worker Lambda2
    participant dynamo as Ope. DB DynanoDB
    participant s3_db as DB S3
    participant s3_res as Response S3

    api->>lambda1: 書き込み
    lambda1 ->> dynamo: ope1 書き込み
    lambda1 ->> api: 結果

    api->>lambda2: 書き込み
    lambda2 ->> dynamo: ope2 書き込み
    lambda2 ->> api: 結果

    lambda1 ->> s3_db: .db を取得
    s3_db -->> lambda1: .db ファイル
    lambda1 ->> dynamo: ope 読み取り
    dynamo -->> lambda1: .db に記録された最後の ope から<br>自分が登録した ope までを強い整合性で取得する

    loop 取得した ope をすべて
        lambda1 ->> lambda1: .db に ope の処理を実行
    end

    s3 ->> lambda_sub: topic1の イベント発火
    lambda_sub ->> s3_db: .db を取得
    s3_db ->> lambda_sub: .db ファイル
    lambda_sub ->> lambda_sub: .db に書き込み
    lambda_sub ->> s3_db: .db を保存
    lambda_sub ->> s3_res: topic1 の結果を保存

    s3 ->> lambda_sub: topic2の イベント発火
    lambda_sub ->> s3_db: .db を取得
    Note right of s3_db: キャッシュが残っていれば取得しない
    s3_db ->> lambda_sub: .db ファイル
    lambda_sub ->> lambda_sub: .db に書き込み
    lambda_sub ->> s3_db: .db を保存
    lambda_sub ->> s3_res: topic2 の結果を保存

```


## DynamoDB の役割

- 操作のキューイング
    - 操作の追加
    - どこまでの操作を実行したかの管理
- S3の削除管理

### 操作のキューイング

対象のオブジェクトに対してのパラレルな操作をキューイングしてシーケンシャルな操作に変換する。

#### キューイングされるオペレーション
| 列  | partition | sort | type    | 説明                                       | フォーマット                    |
| --- | --------- | ---- | ------- | ------------------------------------------ | ------------------------------- |
| cky | o         |      | string  | 操作対象のオブジェクトを判別するためのキー | `${system_name}:${object_path}` |
| sky |           | o    | string  | 作成日時順にソート可能なユニークなキー     | `OP:a1:${ulid}`                 |
| exp |           |      | numeric | TTL(現在時間 + 60s)                        | 整数                            |
| ver |           |      | numeric | フォーマットバージョン                     | 1                               |
| ope |           |      | string  | 操作内容                                   | JSON                            |

exp の計算
```python
import time
ttl = int(time.time()) + 60
```
```javascript
const now = new Date();
const ttl = Math.floor((new Date(now.getTime() + 60000)).getTime() / 1000);
```

`ope` の構造

```typescript
interface OpeSqliteV1{
    _type: "sqlite";
    ver: "1";
    sqls: string[];
}

type Ope = OpeSqliteV1;
```

#### 最新の実行
| 列  | partition | sort | type    | 説明                                                   | フォーマット                    |
| --- | --------- | ---- | ------- | ------------------------------------------------------ | ------------------------------- |
| cky | o         |      | string  | 操作対象のオブジェクトを判別するためのキー             | `${system_name}:${object_path}` |
| sky |           | o    | string  | 固定値                                                 | `OP:a0:`                        |
| ver |           |      | numeric | フォーマットバージョン                                 | 1                               |
| cur |           |      | string  | 最新のオペレーションのskey                             | `${ulid}`                       |
| prv |           |      | string  | 更新前のcur                                            | `${ulid}`                       |
| has |           |      | string  | 操作の連続性が保証されることを確かめるためのハッシュ値 | `${ulid}`                       |

`has` の計算

以下のような計算を行う
```python
import zlib
from functools import reduce

prev_has = "00000000"

ope_sky_list = [
    "01HWTMXQ3Q6BDR6TXA12GEYD5S"
    , "01HWTMY29K78V1S8M12GJ8C26C"
]

next_has = reduce(lambda prev, val: hex(zlib.crc32((prev + val).encode("ascii")))[2:],ope_sky_list, prev_has)

```

### S3 の削除管理

不要になったオブジェクトの削除に失敗したときに後でそのオブジェクトを削除できるように記録する。

| 列  | partition | sort | type    | 説明                                       | フォーマット                    |
| --- | --------- | ---- | ------- | ------------------------------------------ | ------------------------------- |
| cky | o         |      | string  | 操作対象のオブジェクトを判別するためのキー | `${system_name}:${object_path}` |
| sky |           | o    | string  | 作成日時順にソート可能なユニークなキー     | `DE:${ulid}`                    |
| ver |           |      | numeric | フォーマットバージョン                     | 1                               |
| sts |           |      | string  | ステータスコード                           | 文字列                          |
| err |           |      | string  | エラー内容                                 | 文字列                          |

## 操作の適用フローチャート

```mermaid
%%{init: {"flowchart": {"htmlLabels": false}} }%%
flowchart TD
    Start --> QueueOpe["`操作の書き込み
    note: トランザクションなどを使い書き込み時点で順番の保証を行うかは未定。
          エラーをなるべく返さないことを目標にした場合ここで書き込み保障をするのは難しい。`"]

    QueueOpe --> ReadOpes["`操作の読み込み
    note: 0.05s ディレイを前に入れて自分の操作より前に操作が挿入されるのを防ぐ`"]

    ReadOpes --> JudgePrev{最新のデータが存在する}
    JudgePrev -- Yes --> LoadObject[S3から最新のデータを取得する]
    LoadObject --> DoAll[取得した全ての操作を実行]
    JudgePrev -- No --> Init[初期化処理を実行]
    Init --> DoAll[取得した全ての操作を実行]
    DoAll --> UpdateCurrent[最新のデータに更新]
    UpdateCurrent --> SaveObject[S3に更新したオブジェクトを保存]

    SaveObject --> JudgeUpdateSuccess{"`最新のデータに更新できたか
    更新ができなかった場合のパターンは以下
    sky が古い場合
    sky が同一でも hash が異なる場合
    DynamoDB の書き込みエラー
    `"}
    JudgeUpdateSuccess -- Yes --> RemoveObject[更新元となったオブジェクトを削除]
    JudgeUpdateSuccess -- No --> End
    RemoveObject --> End
```

オブジェクトの削除をするため、別のリクエストでオブジェクトの読み込み時にエラーが発生する可能性がある  
それをどう保証するか？

sky と hash の両方が異なる場合、そのリクエストで実施した操作が全て正しいかわからない
