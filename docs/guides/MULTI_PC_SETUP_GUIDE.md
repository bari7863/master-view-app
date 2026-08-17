# 別のPCで作業する方法

このMac以外のPCでも同じプロジェクトを開発・確認したい場合の手順。

## 1. コードを取得

```bash
git clone https://github.com/bari7863/master-view-app.git
cd master-view-app
```

## 2. 依存パッケージをインストール

```bash
npm install
```
## 2-1. npmインストール許可

```bash
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
```

`git clone`だけではコードしか取得できない。忘れずに実行する。

## 3. `.env.local` を用意

このMacの`.env.local`の中身を、そのPCの`master-view-app/.env.local`にコピーする。中身は秘密情報(DB接続文字列・認証トークン等)のため、安全な方法で転送すること(USBメモリでの受け渡し、パスワード管理ツール経由での共有等)。

これだけで、Neon・Supabaseへの接続はそのまま使える(`DATABASE_URL_NEON`・`DATABASE_URL_SUPABASE`はこのMac固有の値ではなく、どのPCからでも同じ接続先を指すため)。

## 4. ローカルPostgreSQLも使いたい場合(任意)

`DATABASE_URL_LOCAL`は「このMac自身の`localhost`」を指す値になっているため、別のPCではそのままでは使えない。そのPCでも以下が必要。

1. そのPCにPostgreSQLをインストール
2. 開発用データベースを作成
3. `.env.local`の`DATABASE_URL_LOCAL`を、そのPCのPostgreSQL接続情報に書き換える

## 5. Vercelでのプレビュー確認・本番デプロイをしたい場合(任意)

Vercel CLIの認証はPCごとに個別に必要。

```bash
npm install -g vercel
vercel login
vercel link
```

`vercel link`では対象プロジェクトとして`master-view-app`を選ぶ。以降は[VERCEL_DEPLOY_GUIDE.md](./VERCEL_DEPLOY_GUIDE.md)と同じ手順が使える。

## まとめ: やりたいことごとに必要なもの

- **コードを見る・編集するだけ** → `git clone` + `npm install`のみ
- **Neon/Supabaseで動作確認する** → 上記に加えて`.env.local`のコピー
- **ローカルPostgreSQLで動作確認する** → 上記に加えて、そのPCへのPostgreSQLインストールと`DATABASE_URL_LOCAL`の書き換え
- **Vercelプレビュー・本番デプロイをする** → 上記に加えて、そのPCでの`vercel login`
