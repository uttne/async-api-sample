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
