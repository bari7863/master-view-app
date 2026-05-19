# CLAUDE.md

このファイルは、Claude Code がこのプロジェクトを修正するときに必ず守る作業ルールです。

GitHubで最初に読む概要は `README.md` を参照してください。  
詳細な開発経緯や仕様は `docs/handover.md` を参照してください。

---

## プロジェクト概要

このプロジェクトは、営業・マーケティングで使う企業マスタデータを管理し、企業HPから不足情報を補完するための社内向けWebアプリです。

主な目的は以下です。

- 企業リストの一元管理
- 企業情報の欠損確認
- 企業HPからのクローリング
- クローリング候補値の確認・保存
- フォーム営業・テレアポ・営業リスト精査への活用
- 管理者・従業員ごとのログイン制御
- 権限管理による操作範囲の制御

このプロジェクトでは、**取得精度、既存機能、データ、権限管理の維持を最優先**にします。

---

## 重要ファイル

主に確認・修正するファイルは以下です。

```text
app/page.tsx
app/api/master_data/route.ts
app/api/master_data/[id]/route.ts
app/api/master_data/crawl/route.ts
app/api/master_data/export/route.ts
app/api/master_data/item_inspection/route.ts
app/api/master_data/login/route.ts
app/api/master_data/permissions/route.ts
app/api/master_data/permissions/me/route.ts
app/api/master_data/mynavi/route.ts
lib/db.ts
lib/master-data-auth.ts
lib/master-data-permissions.ts
lib/master-data-crawler.ts
scripts/crawl-worker.ts
dist/worker/crawl-worker.cjs
release/MasterCrawlWorker/
scripts/mynavi_shinsotsu_unified.py
sql/add_column.sql
README.md
docs/handover.md
directory-tree.txt
```

---

## 作業前に必ず確認すること

修正前に、以下を確認してください。

- 依頼内容がUI修正か、API修正か、クローリング修正か、権限管理修正か
- 関係するファイルはどれか
- 既存機能への影響があるか
- DB変更が必要か
- 型定義の変更が必要か
- Vercel環境に影響するか
- ローカル環境に影響するか
- workerに影響するか
- クローリング精度が下がらないか
- ログイン制御に影響するか
- 権限管理に影響するか
- `dist/worker/crawl-worker.cjs` を直接修正すべき内容か、`scripts/crawl-worker.ts` を修正すべき内容か

---

## 絶対に守るルール

### 1. 既存機能を壊さない

修正対象以外の機能を勝手に変えないでください。

特に以下は慎重に扱ってください。

- 一覧表示
- フィルタ
- CSV取込
- CSV抽出
- 保存処理
- 個別データ更新
- 項目精査
- ログイン機能
- 権限管理機能
- クローリング開始
- クローリング結果画面
- worker連携
- マイナビ系データ処理

---

### 2. クローリング精度を落とさない

速度改善よりも取得精度を優先してください。

特に以下の精度を落とさないでください。

- 代表者名
- 従業員数
- 電話番号
- FAX番号
- 問い合わせフォームURL
- メールアドレス
- 設立年月
- 資本金
- 事業内容
- 許可番号

---

### 3. 権限管理を壊さない

権限管理は、管理者・従業員ごとの操作範囲を守る重要な機能です。

必ず以下を守ってください。

- 権限がないユーザーに操作を許可しない
- 画面で非表示にするだけでなく、API側でも制御する
- 管理者と従業員の扱いを混同しない
- 権限キー名を勝手に変えない
- 権限の初期値を勝手に変えない
- 既存ユーザーの権限が意図せず変わらないようにする
- `permissions/me` の返却内容を変える場合は、画面側の判定も確認する
- `lib/master-data-permissions.ts` を変更する場合は、使っているAPIと画面を確認する

権限管理に関係する主なファイル。

```text
app/page.tsx
app/api/master_data/login/route.ts
app/api/master_data/permissions/route.ts
app/api/master_data/permissions/me/route.ts
lib/master-data-auth.ts
lib/master-data-permissions.ts
```

---

### 4. 不要コードを勝手に削除しない

不要そうに見えるコードでも、必ず参照関係を確認してください。

確認対象。

- importされていないか
- 画面からリンクされていないか
- APIから呼ばれていないか
- npm scriptsから参照されていないか
- Vercelやworkerで使われていないか
- 状態保存や復元に使われていないか
- 権限管理で使われていないか
- release配下のexeやconfigに影響しないか

---

### 5. 大規模リファクタリングを勝手にしない

依頼されていない大規模な整理、共通化、ファイル分割、命名変更は避けてください。

このプロジェクトは既存機能が多く、少しの変更で別機能に影響する可能性があります。

---

### 6. DB・API・画面・CSV・権限管理の整合性を保つ

カラムを追加・変更する場合は、以下を必ず確認してください。

- DBカラム
- SQLファイル
- APIのSELECT
- APIのINSERT
- APIのUPDATE
- TypeScript型
- 画面表示
- フィルタ
- CSV取込
- CSV出力
- 保存処理
- クローリング結果保存
- 権限管理への影響

権限管理の保存形式を変更する場合は、以下も確認してください。

- 権限の取得API
- 権限の保存API
- ログイン中ユーザーの権限取得
- 画面側の表示制御
- API側の操作制御
- 管理者の初期権限
- 従業員の初期権限

---

## コード修正時の回答ルール

ユーザーは手動でコードを貼り替えることが多いため、修正内容は分かりやすく出してください。

必ず以下を守ってください。

- 修正するファイル名を明記する
- 検索ワードを出す
- 検索ワードは実在するコードから選ぶ
- 「このコードの上に追記」
- 「このコードの下に追記」
- 「この関数を丸ごと上書き」
- 「この範囲を削除」
- のように、修正位置を明確にする
- 実在しない「こんな感じのコード」を検索ワードにしない
- 修正する部分のコードだけを出す
- 影響範囲を簡単に説明する

---

## 回答の冒頭で説明すること

コード修正を提案するときは、冒頭に以下を説明してください。

```text
今回の修正は、〇〇という意味です。
そのため、〇〇を修正します。
既存の〇〇には影響しない想定です。
```

難しい言葉は避け、分かりやすく説明してください。

---

## クローリング修正の注意点

クローリングでは、以下を守ってください。

- 取得対象ページを狭めすぎない
- 会社概要ページだけに限定しすぎない
- 採用ページや求人ページの誤取得に注意する
- 代表者名だけ改善して他項目を悪化させない
- 従業員数で募集人数を拾わない
- 電話番号でFAXや他社番号を拾わない
- フォームURLで採用応募フォームを優先しない
- Pythonで取得できていたロジックは、可能な範囲でTypeScript側にも反映する
- Playwrightの高速化は、取得精度が落ちない範囲で行う

---

## UI修正の注意点

UI修正では、以下を守ってください。

- 既存デザインを大きく変えない
- 横並びだったUIを勝手に縦並びにしない
- 小さい画面でメニューや一覧が切れないようにする
- 長いラベルは必要に応じて文字サイズや幅を調整する
- ポップアップの位置や余白を崩さない
- ロゴやヘッダーの配置を勝手に変えない
- 権限管理画面のチェック項目やボタンを見切れさせない
- 全選択・選択解除・保存などのボタン配置を崩さない
- 権限がない機能は、画面上で分かりやすく非表示または操作不可にする

---

## 権限管理修正の注意点

権限管理では、以下を守ってください。

- 管理者だけが権限設定を変更できるようにする
- 従業員が他ユーザーの権限を変更できないようにする
- ログイン中ユーザー自身の権限は `permissions/me` で確認する
- 権限設定の保存・取得は `permissions` APIを確認する
- 共通の権限定義や判定は `lib/master-data-permissions.ts` を確認する
- 画面側だけの制御で終わらせない
- API側でも権限チェックを行う
- Vercel環境でもローカル環境でも同じ判定になるようにする

権限管理に関係する修正をした場合は、少なくとも以下を確認してください。

```text
app/page.tsx
app/api/master_data/login/route.ts
app/api/master_data/permissions/route.ts
app/api/master_data/permissions/me/route.ts
lib/master-data-auth.ts
lib/master-data-permissions.ts
```

---

## worker / Vercel / ローカル環境の注意点

このプロジェクトでは、Vercel環境とローカル環境でクローリングの扱いが異なります。

基本方針。

- Vercel公開URLではworker起動確認を行う
- 重いクローリングはworker側で処理する
- ローカル環境ではworkerなしでもクローリングできるようにする
- `http://localhost:3000/` では従来の動作を優先する
- workerソースは基本的に `scripts/crawl-worker.ts` を確認する
- `dist/worker/crawl-worker.cjs` はビルド後ファイルのため、直接編集は慎重に判断する
- `release/MasterCrawlWorker/` は配布・実行用のworker関連ファイルとして扱う

権限管理の修正をした場合も、Vercel環境でログイン・権限取得が正しく動くか確認してください。

---

## 秘密情報の扱い

以下は絶対に出力・コミットしないでください。

- `.env.local`
- DB接続文字列
- 認証トークン
- ログイン情報
- 個人情報を含むCSV
- 顧客情報を含むExcel

`release/MasterCrawlWorker/worker-config.json` や `worker-id.txt` に環境依存情報が含まれる場合は、取り扱いに注意してください。

権限管理に関係するユーザー情報やログイン情報も、外部に出さないでください。

---

## 参照すべき資料

作業前に必要に応じて以下を読んでください。

```text
README.md
docs/handover.md
directory-tree.txt
```

READMEは概要、handover.mdは詳細仕様、directory-tree.txtは現在のディレクトリ構成確認用です。