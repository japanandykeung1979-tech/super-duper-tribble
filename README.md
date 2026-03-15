# super-duper-tribble

一個用 Python 寫嘅簡單「客戶記事簿」CLI 程式，方便你按日期（似日記）記錄客戶資料，包含：

- 登記日期
- 客戶名稱
- 客戶聯絡號碼
- 多個電話號碼
- 每個電話號碼各自生效日期

資料會儲存喺本地 SQLite（`customer_diary.db`），並可輸出簡單報表。

## 用法

### 1) 初始化資料庫

```bash
python3 customer_diary.py init
```

### 2) 新增客戶登記（互動式輸入）

```bash
python3 customer_diary.py add
```

### 3) 查看報表

```bash
python3 customer_diary.py report
```

### 4) 按日期範圍查看報表

```bash
python3 customer_diary.py report --start 2026-01-01 --end 2026-12-31
```
