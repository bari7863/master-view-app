# マスタデータ

営業・マーケティングで使用する企業情報を管理し、企業HPから不足情報を補完するための社内向けWebアプリ
企業リストの取り込み、一覧管理、検索・フィルタ、CSV出力、クローリング、候補値の確認・保存などを行い、フォーム営業・テレアポ・営業リスト精査に使いやすいマスタデータを作ることが目的

---

## 公開URL

https://master-view-app-ruby.vercel.app/

---

## 主な機能

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
- Vercel環境での公開
- ローカル環境での開発・検証

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

カラムを追加・変更する場合は、DB、API、画面、CSV取込、CSV出力、保存処理に影響する可能性があります。

---

## 技術構成

主な技術構成は以下です。

- Next.js
- React
- TypeScript
- Node.js
- PostgreSQL / Neon
- Playwright
- Vercel
- workerによるクローリング処理

---

## ディレクトリ構成

主なファイル・フォルダは以下です。

```text
master-view-app/
├─ app/
│  ├─ page.tsx
│  ├─ layout.tsx
│  ├─ globals.css
│  ├─ favicon.ico
│  ├─ companies/
│  │  └─ page.tsx
│  └─ api/
│     ├─ master_data/
│     │  ├─ route.ts
│     │  └─ crawl/
│     │     └─ route.ts
│     └─ export/
│        └─ route.ts
│
├─ lib/
│  ├─ db.ts
│  └─ master-data-crawler.ts
│
├─ docs/
│  └─ 引き継ぎ.md
│
├─ public/
├─ .crawl-job-state/
├─ CLAUDE.md
├─ README.md
├─ crawl-worker.ts
├─ package.json
├─ package-lock.json
├─ tsconfig.json
├─ next.config.ts
├─ eslint.config.mjs
├─ postcss.config.mjs
├─ .gitignore
└─ .env.local
```

実際の構成と完全に一致しない場合は、現在のプロジェクト内のファイルに合わせて読み替えてください。

---

## 重要ファイル

### `app/page.tsx`

メイン画面です。

一覧表示、検索、フィルタ、CSV取込、CSV抽出、クローリング確認、クローリング結果画面、ログイン画面、UI全般を担当します。

---

### `app/api/master_data/route.ts`

マスタデータ本体のAPIです。

一覧取得、検索、フィルタ、保存、DB更新、CSV取込、候補値の反映などを担当します。

---

### `app/api/master_data/crawl/route.ts`

クローリングAPIです。

クローリング開始、job作成、進捗取得、停止、再開、worker連携、結果保存などを担当します。

---

### `lib/master-data-crawler.ts`

クローリングの中心ロジックです。

企業HPへのアクセス、HTML取得、PlaywrightによるJSレンダリング、候補ページ探索、代表者名・従業員数・電話番号・問い合わせフォームURLなどの抽出を担当します。

---

### `crawl-worker.ts`

worker側のクローリング処理です。

Vercel環境で重いクローリングを直接実行すると不安定になりやすいため、workerで処理する構成があります。

---

### `lib/db.ts`

DB接続設定です。

`DATABASE_URL` を使って PostgreSQL / Neon に接続します。

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

### `docs/引き継ぎ.md`

詳細な引き継ぎ資料です。

主に以下を書いています。

- アプリの目的
- 開発経緯
- クローリング仕様
- 代表者名・従業員数・電話番号・フォームURLの抽出方針
- 過去に起きたエラー
- Vercel / worker / ローカル環境の考え方
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

MASTER_DATA_AUTH_TOKEN="xxxxx"

MASTER_DATA_LOGIN_ADMINS="xxxxx"
MASTER_DATA_LOGIN_EMPLOYEES="xxxxx"
```

---

## 環境変数

### `DATABASE_URL`

DBに接続するためのURLです。

Neon PostgreSQLなどの接続文字列を設定します。

```env
DATABASE_URL="postgresql://xxxxx"
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

また、以下のような設定は間違いです。

```env
MASTER_DATA_AUTH_TOKEN="DATABASE_URLの中身"
MASTER_DATA_LOGIN_ADMINS="DATABASE_URLの中身"
MASTER_DATA_LOGIN_EMPLOYEES="DATABASE_URLの中身"
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

すでに `DATABASE_URL` が登録されている場合、同じKeyを新規追加するとエラーになります。

その場合は、新規追加ではなく、既存の `DATABASE_URL` を編集してください。

KeyとValueの考え方は以下です。

```text
Key: DATABASE_URL
Value: postgresql://xxxxx
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

`MASTER_DATA_AUTH_TOKEN` などのValueに `DATABASE_URL` の中身を入れるわけではありません。

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
4. 不要コードを安易に削除しない
5. DB・API・画面・CSVの整合性を保つ
6. Vercel環境とローカル環境の違いを理解する
7. worker構成を理解する
8. 速度よりも安全性と正確性を優先する