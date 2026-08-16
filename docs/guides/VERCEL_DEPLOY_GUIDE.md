# Vercelデプロイガイド

このブランチの内容をVercelへデプロイする方法。GitHubへのpush/PR/マージによる自動デプロイは無効化済み(`vercel.json`)のため、本番反映は必ずこの手順で手動で行う。

## 初回のみ: Vercel CLIのセットアップ

```bash
cd "/Users/sfalq7863/アプリ/master-view-app/.claude/worktrees/integrated-system-roadmap-f1dab5"
vercel login
vercel link
```

- `vercel login`はブラウザでの認証が必要
- `vercel link`では対象プロジェクトとして`master-view-app`を選ぶ

## プレビューデプロイ(本番には反映されない、何度でも実行可能)

**使うタイミング**: 本番に反映する前に、Vercel環境としての動作を確認したいとき。

```bash
cd "/Users/sfalq7863/アプリ/master-view-app/.claude/worktrees/integrated-system-roadmap-f1dab5"
vercel
```

実行すると、以下のような専用プレビューURLが発行される(実行のたびに新しいURLになる)。

```
https://master-view-xxxxxxxx-bari7863s-projects.vercel.app
```

このURLをブラウザで開いてログインし、Vercel環境としての動作(DB選択欄の非表示、スーパー管理者限定のデータベースメニューなど)を確認する。本番URL(`master-view-app-ruby.vercel.app`)には一切影響しない。

## 本番へ反映する(慎重に)

**使うタイミング**: プレビューで問題ないことを確認できたとき。

```bash
vercel --prod
```

本番URL(`master-view-app-ruby.vercel.app`)に反映される。
