# マスタデータ

営業・マーケティングで使用する企業情報を管理し、企業HPから不足情報を補完するための社内向けWebアプリです。

企業リストの取り込み、一覧管理、検索・フィルタ、CSV出力、クローリング、候補値の確認・保存などを行い、フォーム営業・テレアポ・営業リスト精査に使いやすいマスタデータを作ることを目的としています。

---

## 公開URL

https://master-view-app-ruby.vercel.app/

---

## このアプリでできること

主な機能は以下です。

- 企業リストの一覧表示
- 企業名・住所・業種などによる検索、フィルタ、並び替え
- CSV / Excel系データの取り込み
- CSV抽出
- 企業HPからのクローリング
- クローリング候補値の確認
- 候補値の保存・反映
- クローリング進捗の表示
- クローリング結果画面の復元
- ログイン制御
- 権限管理
- Vercel環境での公開
- ローカル環境での開発・検証
- workerによるクローリング処理
- マイナビ系データ取得・取込処理

---

## 主に扱うデータ

このアプリでは、以下のような企業情報を扱います。

- 企業名
- 郵便番号
- 住所
- 業種
- 業界
- 企業概要
- 企業サイトURL
- お問い合わせフォームURL
- 電話番号
- FAX番号
- メールアドレス
- 設立年月
- 代表者名
- 代表者役職
- 資本金
- 従業員数
- 事業内容
- 許可番号

カラムを追加・変更する場合は、DB、API、画面、CSV取込、CSV出力、保存処理、クローリング結果保存に影響する可能性があります。

---

## DB構成

このアプリでは、Neon DBとSupabase DBを使用します。

DBはアプリ上で切り替えて使用します。

- Neonを選択した場合は、Neon DBのデータを表示・保存します
- Supabaseを選択した場合は、Supabase DBのデータを表示・保存します
- クローリング結果、項目精査結果、権限管理設定も、選択中のDB側に保存します
- NeonとSupabaseのデータは混在させない前提です

主な接続先は以下です。

```text
既存互換用: DATABASE_URL
Neon DB: DATABASE_URL_NEON
Supabase DB: DATABASE_URL_SUPABASE
```

`.env.local` とVercel環境変数の両方に、必要なDB接続情報を設定してください。

---

## 技術構成

主な技術構成は以下です。

- Next.js
- React
- TypeScript
- Node.js
- PostgreSQL / Neon / Supabase
- Playwright
- Vercel
- workerによるクローリング処理
- Pythonスクリプトによる一部データ取得・検証

---

## ディレクトリ構成

現在の主な構成は以下です。

```text
master-view-app/
├─ app
│  ├─ api
│  │  └─ master_data
│  │     ├─ [id]
│  │     │  └─ route.ts
│  │     ├─ crawl
│  │     │  └─ route.ts
│  │     ├─ export
│  │     │  └─ route.ts
│  │     ├─ item_inspection
│  │     │  └─ route.ts
│  │     ├─ login
│  │     │  └─ route.ts
│  │     ├─ mynavi
│  │     │  └─ route.ts
│  │     ├─ permissions
│  │     │  ├─ me
│  │     │  │  └─ route.ts
│  │     │  └─ route.ts
│  │     └─ route.ts
│  ├─ favicon.ico
│  ├─ globals.css
│  ├─ layout.tsx
│  └─ page.tsx
├─ dist
│  └─ worker
│     └─ crawl-worker.cjs
├─ docs
│  └─ handover.md
├─ lib
│  ├─ db.ts
│  ├─ master-data-auth.ts
│  ├─ master-data-crawler.ts
│  └─ master-data-permissions.ts
├─ public
│  ├─ file.svg
│  ├─ globe.svg
│  ├─ next.svg
│  ├─ vercel.svg
│  └─ window.svg
├─ release
│  └─ MasterCrawlWorker
│     ├─ master-crawl-worker.exe
│     ├─ worker-config.json
│     └─ worker-id.txt
├─ scripts
│  ├─ crawl-worker.ts
│  └─ mynavi_shinsotsu_unified.py
├─ sql
│  └─ add_column.sql
├─ .gitignore
├─ CLAUDE.md
├─ directory-tree.txt
├─ eslint.config.mjs
├─ next.config.ts
├─ package.json
├─ package-lock.json
├─ postcss.config.mjs
├─ README.md
└─ tsconfig.json
```

---

## 重要ファイル

### `app/page.tsx`

メイン画面です。

一覧表示、検索、フィルタ、CSV取込、CSV抽出、クローリング確認、クローリング結果画面、ログイン画面、権限管理画面、権限に応じた表示制御、UI全般を担当します。

---

### `app/api/master_data/route.ts`

マスタデータ本体のAPIです。

一覧取得、検索、フィルタ、保存、DB更新、CSV取込、候補値の反映などを担当します。

---

### `app/api/master_data/[id]/route.ts`

特定のマスタデータ1件に対するAPIです。

個別データの取得・更新・削除など、1件単位の処理に関係する可能性があります。

---

### `app/api/master_data/crawl/route.ts`

クローリングAPIです。

クローリング開始、job作成、進捗取得、停止、再開、worker連携、結果保存などを担当します。

---

### `app/api/master_data/export/route.ts`

CSV出力・エクスポート系のAPIです。

全件出力、処理完了分の出力、候補ありデータの出力などに関係します。

---

### `app/api/master_data/item_inspection/route.ts`

項目精査系のAPIです。

項目の候補値確認、不要値の判定、精査処理などに関係します。

---

### `app/api/master_data/login/route.ts`

ログイン認証用のAPIです。

管理者・従業員ログインや、認証トークン確認などに関係します。

---

### `app/api/master_data/permissions/route.ts`

権限管理用のAPIです。

権限一覧の取得、従業員ごとの権限取得、権限保存、権限設定画面との連携などに関係する可能性があります。

---

### `app/api/master_data/permissions/me/route.ts`

ログイン中ユーザー自身の権限を取得するAPIです。

画面側で、メニュー・ボタン・操作範囲などを権限に応じて出し分ける処理に関係する可能性があります。

---

### `app/api/master_data/mynavi/route.ts`

マイナビ系データの処理に関係するAPIです。

マイナビから取得した企業データの取り込み、取得、整形などに関係する可能性があります。

---

### `lib/db.ts`

DB接続設定です。

`DATABASE_URL` を使って PostgreSQL / Neon に接続します。

---

### `lib/master-data-auth.ts`

マスタデータアプリの認証処理に関係するファイルです。

ログイン情報、認証トークン、管理者・従業員の判定などに関係する可能性があります。

---

### `lib/master-data-permissions.ts`

権限管理の共通処理に関係するファイルです。

権限定義、権限キー、管理者・従業員ごとの権限判定、API側や画面側で使う権限チェックなどに関係する可能性があります。

---

### `lib/master-data-crawler.ts`

クローリングの中心ロジックです。

企業HPへのアクセス、HTML取得、PlaywrightによるJSレンダリング、候補ページ探索、代表者名・従業員数・電話番号・問い合わせフォームURLなどの抽出を担当します。

---

### `scripts/crawl-worker.ts`

workerの元となるTypeScriptファイルです。

クローリング処理を外部workerとして実行するためのソースです。

---

### `dist/worker/crawl-worker.cjs`

ビルド後のworkerファイルです。

実行用に変換されたworkerファイルのため、基本的には直接編集せず、必要な修正は `scripts/crawl-worker.ts` 側で行います。

---

### `release/MasterCrawlWorker/`

配布・実行用のworker関連ファイルです。

```text
master-crawl-worker.exe
worker-config.json
worker-id.txt
```

などが含まれます。

workerを別PCや別環境で起動する場合に関係します。

---

### `scripts/mynavi_shinsotsu_unified.py`

マイナビ新卒系のデータ取得・整形に関係するPythonスクリプトです。

アプリ本体とは別に、データ収集や検証用として使われる可能性があります。

---

### `sql/add_column.sql`

DBカラム追加用のSQLです。

カラム追加・変更時に使う可能性があります。

---

### `docs/handover.md`

開発経緯や仕様の詳細をまとめた引き継ぎ資料です。

Claude Codeや別AIに移行するときは、このファイルを読ませます。

---

### `CLAUDE.md`

Claude Code向けの作業ルールです。

修正時のルール、クローリング精度に関する注意、UI修正時の注意、不要コード削除時の注意などを記載しています。

---

### `directory-tree.txt`

現在のディレクトリ構成を確認するためのファイルです。

AIや別担当者にプロジェクト構成を共有するときに使います。

---

## 関連ドキュメント

このREADMEでは、開発に入るための概要を中心にまとめています。

詳しい内容は以下のファイルに分けています。

### `CLAUDE.md`

Claude Codeに守らせる作業ルールです。

主に以下を書いています。

- 修正時のルール
- 回答・作業スタイル
- クローリング精度に関する注意
- UI修正時の注意
- 不要コード削除時の注意
- 既存機能を壊さないためのルール

---

### `docs/handover.md`

詳細な引き継ぎ資料です。

主に以下を書いています。

- アプリの目的
- 開発経緯
- クローリング仕様
- 代表者名・従業員数・電話番号・フォームURLの抽出方針
- 過去に起きたエラー
- Vercel / worker / ローカル環境の考え方
- 権限管理の考え方
- 今後の改善候補

---

## セットアップ方法

### 1. リポジトリを取得

```bash
git clone <repository-url>
cd master-view-app
```

`<repository-url>` には、このプロジェクトのGitHub URLを入れます。

---

### 2. パッケージをインストール

```bash
npm install
```

これにより、`package.json` に記載された必要なパッケージがインストールされます。

`node_modules/` はGitHubにアップしません。

---

### 3. `.env.local` を作成

プロジェクト直下に `.env.local` を作成します。

配置場所：

```text
master-view-app/.env.local
```

中身の例：

```env
DATABASE_URL="postgresql://xxxxx"
DATABASE_URL_NEON="postgresql://xxxxx"
DATABASE_URL_SUPABASE="postgresql://xxxxx"

MASTER_DATA_AUTH_TOKEN="xxxxx"

MASTER_DATA_LOGIN_ADMINS="xxxxx"
MASTER_DATA_LOGIN_EMPLOYEES="xxxxx"
```

---

## 環境変数

### `DATABASE_URL`

既存互換用のDB接続URLです。

現在は、Neon DBの接続文字列を設定します。

```env
DATABASE_URL="postgresql://xxxxx"
```

---

### `DATABASE_URL_NEON`

Neon DBに接続するためのURLです。

アプリ上でNeonを選択した場合、この接続先を使用します。

```env
DATABASE_URL_NEON="postgresql://xxxxx"
```

---

### `DATABASE_URL_SUPABASE`

Supabase DBに接続するためのURLです。

アプリ上でSupabaseを選択した場合、この接続先を使用します。

```env
DATABASE_URL_SUPABASE="postgresql://xxxxx"
```

---

### `MASTER_DATA_AUTH_TOKEN`

認証やAPI保護で使うトークンです。

```env
MASTER_DATA_AUTH_TOKEN="xxxxx"
```

---

### `MASTER_DATA_LOGIN_ADMINS`

管理者ログイン用の値です。

```env
MASTER_DATA_LOGIN_ADMINS="xxxxx"
```

具体的な形式は、現在のログイン処理に合わせてください。

---

### `MASTER_DATA_LOGIN_EMPLOYEES`

従業員ログイン用の値です。

```env
MASTER_DATA_LOGIN_EMPLOYEES="xxxxx"
```

具体的な形式は、現在のログイン処理に合わせてください。

---

## `.env.local` の注意点

`.env.local` には秘密情報が入るため、GitHubにアップしないでください。

`.gitignore` に以下が含まれていることを確認します。

```gitignore
.env.local
```

各Keyには、それぞれ対応する値を設定してください。

---

## 開発サーバーの起動

```bash
npm run dev
```

起動後、ブラウザで以下を開きます。

```text
http://localhost:3000/
```

---

## よく使うコマンド

### 開発サーバー起動

```bash
npm run dev
```

ローカル開発サーバーを起動します。

---

### 本番ビルド確認

```bash
npm run build
```

本番環境向けにビルドできるか確認します。

Vercelにデプロイする前や、大きな修正後に実行します。

---

### 本番ビルド後の起動

```bash
npm run start
```

`npm run build` 後のアプリを起動します。

---

### Lintチェック

```bash
npm run lint
```

コードの書き方や構文の問題を確認します。

実際に使えるコマンドは `package.json` の `scripts` を確認してください。

---

### worker再ビルド

workerは、Vercel上ではなくPC側で動くクローリング専用の処理です。

そのため、下記のようなファイルを修正した場合は、workerを再ビルドする必要があります。

- `crawl-worker.ts`
- `lib/master-data-crawler.ts`
- worker側で使っているクローリング処理
- workerの処理件数、送信間隔、タイムアウトなどの設定

反対に、画面だけの修正やVercel側APIだけの修正であれば、基本的にworker再ビルドは不要です。

例：

```bash
npm run build:worker
```

---

### exeを作り直す

exeは下記ディレクトリにあります。

release/MasterCrawlWorker/master-crawl-worker.exe

例：

```bash
npm run package:worker:win
```

---

### worker起動

worker用のコマンドは `package.json` の `scripts` を確認してください。

例：

```bash
npm run worker
```

または、

```bash
npm run crawl-worker
```

実際のコマンド名は、現在の `package.json` に合わせてください。

---

## worker反映手順

npm run worker で起動している場合

```bash
Ctrl + C
npm run build:worker
npm run worker
```

exeで起動している場合

```bash
Ctrl + C
npm run build:worker
npm run package:worker:win
```

その後、新しく作成された下記exeを起動します。

release/MasterCrawlWorker/master-crawl-worker.exe

---

## Vercel環境変数

Vercelにデプロイする場合は、Vercel側にも環境変数を設定します。

ローカルの `.env.local` は、Vercelには自動反映されません。

---

### 設定場所

```text
Vercel
→ Project Settings
→ Environment Variables
```

---

### 登録する主なKey

- `DATABASE_URL`
- `DATABASE_URL_NEON`
- `DATABASE_URL_SUPABASE`
- `MASTER_DATA_AUTH_TOKEN`
- `MASTER_DATA_LOGIN_ADMINS`
- `MASTER_DATA_LOGIN_EMPLOYEES`

---

### 設定対象

必要に応じて以下に設定します。

- Production
- Preview
- Development

---

### 注意点

すでに同じKeyが登録されている場合、同じKeyを新規追加するとエラーになります。

その場合は、新規追加ではなく、既存のKeyを編集してください。

KeyとValueの考え方は以下です。

```text
Key: DATABASE_URL
Value: Neon DBの接続URL
```

```text
Key: DATABASE_URL_NEON
Value: Neon DBの接続URL
```

```text
Key: DATABASE_URL_SUPABASE
Value: Supabase DBの接続URL
```

```text
Key: MASTER_DATA_AUTH_TOKEN
Value: 認証用トークン
```

```text
Key: MASTER_DATA_LOGIN_ADMINS
Value: 管理者ログイン用の値
```

```text
Key: MASTER_DATA_LOGIN_EMPLOYEES
Value: 従業員ログイン用の値
```

---

## デプロイ方法

VercelとGitHubを連携している場合、基本的にはGitHubにpushすると自動デプロイされます。

```bash
git add .
git commit -m "READMEを更新"
git push
```

デプロイ前には、できれば以下を実行します。

```bash
npm run build
```

---

## GitHubに上げないもの

以下はGitHubに上げないでください。

- `.env.local`
- DB接続情報
- 認証トークン
- ログイン情報
- 個人情報を含むCSV
- 顧客情報を含むExcel
- 大量のクローリング結果ファイル
- `.next`
- `node_modules`
- ログファイル

---

## 開発時の基本方針

このプロジェクトでは、以下を最優先にします。

1. 取得精度を落とさない
2. 既存機能を壊さない
3. データを壊さない
4. 権限管理を壊さない
5. 不要コードを安易に削除しない
6. DB・API・画面・CSVの整合性を保つ
7. Vercel環境とローカル環境の違いを理解する
8. worker構成を理解する
9. 速度よりも安全性と正確性を優先する