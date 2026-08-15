# ローカル動作確認の起動方法

以下をターミナルに貼り付けて実行する。

```bash
cd "/Users/sfalq7863/アプリ/master-view-app/.claude/worktrees/integrated-system-roadmap-f1dab5"
npm run dev -- -p 3001
```

起動後、ブラウザで以下を開く。

```
http://localhost:3001/
```

ログイン画面のDB選択で「PostgreSQL」を選ぶと、このPCのローカルデータベースに接続される。

---

# DB同期コマンド(Neon ⇔ Supabase / ローカル)

## Neonの内容でSupabase・ローカルPostgreSQLを揃える(通常のバックアップ)

Neonでリストの追加・削除・精査・クローリングを行った後、これを実行してバックアップを最新化する。

```bash
npm run sync:backup
```

## Supabase(バックアップ)の内容でNeonを復旧する(Neonに問題があった場合のみ)

Neon本番への書き込みを伴うため、実行すると確認を求められる。`yes`と入力した場合のみ実行される。

```bash
npm run sync:restore
```

---

# Vercelプレビュー環境でのテスト運用

このブランチの内容を、本番(master-view-app-ruby.vercel.app)に一切影響を与えずに、Vercel上の専用URLで確認する方法。

## 初回のみ

```bash
cd "/Users/sfalq7863/アプリ/master-view-app/.claude/worktrees/integrated-system-roadmap-f1dab5"
vercel login
vercel link
```

`vercel login`はブラウザでの認証が必要。`vercel link`では対象プロジェクトとして`master-view-app`を選ぶ。

## プレビューデプロイ(何度でも実行可能、本番には反映されない)

```bash
cd "/Users/sfalq7863/アプリ/master-view-app/.claude/worktrees/integrated-system-roadmap-f1dab5"
vercel
```

実行すると、以下のような専用プレビューURLが発行される(実行のたびに新しいURLになる)。

```
https://master-view-xxxxxxxx-bari7863s-projects.vercel.app
```

このURLをブラウザで開いてログインし、Vercel環境としての動作(DB選択欄の非表示、スーパー管理者限定のデータベースメニューなど)を確認する。

## 本番へ反映する場合(慎重に)

```bash
vercel --prod
```

これは本番URL(`master-view-app-ruby.vercel.app`)に反映される。プレビューで問題ないことを確認してから実行する。

