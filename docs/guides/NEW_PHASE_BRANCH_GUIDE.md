# 新しいフェーズに進む時の手順(ブランチの切り直し)

Phase 0が終わり、Phase 1(またはそれ以降)に進む際、今のworktreeディレクトリ(環境構築済み)をそのまま使い、Gitのブランチだけ新しく切り直す手順。

## なぜブランチを切り直すか

- `npm install`・ローカルPostgreSQLの接続設定など、環境構築をやり直さずに済む
- Gitの履歴・PRをフェーズごとに分離できる(1ブランチ=1機能、という一般的な運用に合わせる)

## 手順

### 1. 未コミットの変更がないか確認

```bash
cd "/Users/sfalq7863/アプリ/master-view-app/.claude/worktrees/integrated-system-roadmap-f1dab5"
git status
```

何か変更が残っていたら、先にコミットするか、消してよいか確認してから進める。

### 2. masterの最新を取り込む

```bash
git checkout master
git pull
```

### 3. 新しいフェーズ用のブランチを作成

```bash
git checkout -b claude/phase1-crm-foundation
```

ブランチ名はフェーズの内容に合わせて変える(例: `claude/phase2-sfa` 等)。

### 4. 作業開始

この状態で、Claude Codeに「続きを進めて」と伝えれば、新しいブランチ上で作業できる。`.env.local`・`node_modules`・ローカルPostgreSQLの接続設定はそのまま使える。

## 注意点

- 「新しいチャット」を開始した場合、Claude Codeが自動的に別の新しいworktreeを作成することがある。この手順は、**同じチャットの続きとしてPhase1に進む場合**、または新しいチャットでも明示的に「このworktreeを使って」と伝えた場合に有効
- 古いブランチ(`claude/integrated-system-roadmap-f1dab5`)は、masterにマージ済みであれば削除して問題ない

## 不要になった旧ブランチの削除(任意)

```bash
git branch -d claude/integrated-system-roadmap-f1dab5
git push origin --delete claude/integrated-system-roadmap-f1dab5
```
