## requirements.txt の作成

```powershell
pipreqs . --force
```

## layer の作成

```powershell
# python フォルダに書き出す必要あり
pip install -r .\requirements.txt -t ./layer/python
```
