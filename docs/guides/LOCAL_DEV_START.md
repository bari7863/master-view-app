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

関連する他の運用メモ:

- DBの同期・復旧コマンド → [DB_SYNC_GUIDE.md](./DB_SYNC_GUIDE.md)
- Vercelへのデプロイ手順 → [VERCEL_DEPLOY_GUIDE.md](./VERCEL_DEPLOY_GUIDE.md)
- 次のフェーズに進む時(ブランチの切り直し方) → [NEW_PHASE_BRANCH_GUIDE.md](./NEW_PHASE_BRANCH_GUIDE.md)
- 別のPCで作業する方法 → [MULTI_PC_SETUP_GUIDE.md](./MULTI_PC_SETUP_GUIDE.md)
