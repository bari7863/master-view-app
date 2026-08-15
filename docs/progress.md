# 統合管理システム構築 進捗ダッシュボード

[docs/roadmap.md](./roadmap.md) で定めたロードマップ(Phase 0〜6)の、現時点の進捗状況をまとめる。見た目重視の同内容ページは [docs/progress.html](./progress.html) を参照。

**新しい会話でこのプロジェクトの続きを行う場合は、まずこのファイルとroadmap.mdを読んで、どこまで完了しているか把握してから着手してください。**

最終更新: 2026-08-16

---

## サマリー

- 全7フェーズ中、**Phase 0が完了**、残り6フェーズは未着手
- 優先順位はロードマップのPhase番号そのまま(0が最優先、6が最後)
- **本番(masterマージ・`vercel --prod`)へはまだ反映していない**。コミット前の状態(このworktree内)で作業が止まっている

---

## フェーズ別ステータス

| 優先順位 | フェーズ | ステータス |
|---|---|---|
| 0 | データ基盤の拡張整備 | 完了(本番反映待ち) |
| 1 | CRM基礎(顧客管理) | 未着手 |
| 2 | SFA(案件管理・行動管理・予実管理) | 未着手 |
| 3 | データ分析・ダッシュボード | 未着手 |
| 4 | MA基礎(メール/フォーム自動配信) | 未着手 |
| 5 | 行動履歴分析・スコアリング | 未着手 |
| 6 | カスタマーサポート | 未着手 |

---

## Phase 0: データ基盤の拡張整備(完了・本番反映待ち)

### 完了した内容

**新テーブル・DB選択肢の追加**

- 企業マスタに紐づく新テーブルのスキーマ・作成ロジックを実装(`lib/master-data-schema.ts`)
  - `master_data_contacts`(担当者)
  - `master_data_activities`(活動履歴)
  - `master_data_deals`(案件)
- ログイン成功時に上記テーブルを自動作成する仕組みを追加(`app/api/master_data/login/route.ts`)。Neon / Supabase / ローカルPostgreSQL、いずれの接続先でも同じ仕組みで自動作成される。失敗時はキャッシュをクリアして再試行できるようにし、失敗してもログイン自体は継続する設計(Phase1未着手の機能でログインを壊さないため)
- DB選択肢に「PostgreSQL」(このPCのローカルデータベース)を追加。ローカル実行時のみ画面に表示され、Vercel本番環境では表示されない
  - `lib/master-data-auth.ts`: `MasterDataDbMode`型に`"local"`を追加
  - `lib/db.ts`: `"local"`選択時は`DATABASE_URL_LOCAL`環境変数を参照するよう分岐追加
  - `app/page.tsx`: DB選択UI(ログイン画面・DB切替メニュー)に条件付きで追加

**Vercel本番環境での制限**

- スーパー管理者以外は、Vercel上では常にNeonのみ使える(ログイン画面のDB選択欄が非表示、ログイン後の「データベース」メニューも非表示)
- 画面側だけでなくAPI側(`lib/master-data-auth.ts`の`isVercelRuntime()`・`resolveMasterDataDbModeForLogin()`)でも強制している
- Vercelプレビュー環境に実際にデプロイし、`process.env.VERCEL`が`"1"`になることを実機確認済み

**3DB運用(Neon=本番／Supabase=バックアップ／ローカルPostgreSQL=開発)**

- Neon本番の`master_data`(186,069件)を、スキーマ+データ丸ごとSupabase・ローカルPostgreSQLへ複製し、3DBを同じ内容に揃えた
- `npm run sync:backup`(Neon→Supabase/ローカル複製)、`npm run sync:restore`(Supabase→Neon復旧、確認プロンプトあり)のコマンドを追加。使い方は`LOCAL_DEV_START.md`参照
- **重要な設計上の注意**: `sync:backup`は「常に最新Neonで上書きするミラー」であり世代管理はない。誤ってNeon側を壊した直後に実行すると、唯一のバックアップだったSupabaseの正常データも同時に失われる

**本番反映前レビューで発見・修正した既存バグ**

ユーザーから「問題を漏れなくピックアップしてほしい」と依頼され、3つの異なる観点(バグ・機能性/CLAUDE.md準拠/運用・デプロイ)でcode-reviewerを並列レビューした結果、以下を発見・修正した。

- `app/api/master_data/export/route.ts`(CSV抽出)が固定Neon importのままで、DB切替を無視して常にNeonのデータを出力していた → `getCurrentMasterDataUser(req)?.dbMode`ベースに修正
- `app/api/master_data/export/route.ts`のスコープバイパス条件が他API(`route.ts`/`crawl/route.ts`/`item_inspection/route.ts`)と違い「管理者」になっていた → 「スーパー管理者」に統一
- `app/api/master_data/[id]/route.ts`が認証チェック一切なし+DB固定のまま放置されていた → フロントから未参照であることを確認の上で削除
- `app/page.tsx`の`handleSwitchDatabase`が、サーバーが解決したdbModeでなくクライアント側の要求値をそのまま使っていた → サーバー応答(`data.loginUser.dbMode`)を信頼するよう修正

**運用面のドキュメント整備**

- Vercelの自動デプロイを`vercel.json`(`git.deploymentEnabled: false`)で無効化(無料プランのデプロイ制限を避けるため)。本番反映は手動で`vercel --prod`を実行する運用に変更。README.md/docs/handover.mdに明記済み
- `LOCAL_DEV_START.md`に、ローカル起動・DB同期・Vercelプレビューデプロイのコマンドをまとめた
- `directory-tree.txt`・README.md・docs/handover.md・CLAUDE.mdを、今回の変更内容に合わせて更新済み

### 残タスク(Phase 1着手時に対応)

- 担当者/活動履歴/案件それぞれのCRUD API・画面は未実装(Phase0はテーブルと作成ロジックのみが対象範囲)
- 対応する権限キー(`contacts.view`等)は、`lib/master-data-permissions.ts`の`MASTER_DATA_PERMISSION_KEYS`配列に追加するだけで自動対応するが、`app/page.tsx`内にローカルの権限キー型定義が重複しているため、そちらも手動同期が必要(既存の技術的負債)

### 既知の問題(未対応・実害は確認されていない)

- ログイン画面表示時、ブラウザコンソールに`Hydration failed`(React error #418)エラーが出る。`git stash`で今回のセッションの変更を全て退避した状態でも再現することを確認済みのため、**今回のセッションより前から存在する既存の問題**。画面表示・ログイン・CSV抽出などの主要機能への実害は確認されていない。原因は未調査

### 関連ファイル

```text
lib/master-data-schema.ts
lib/master-data-auth.ts
lib/db.ts
app/api/master_data/login/route.ts
app/api/master_data/export/route.ts
app/page.tsx
vercel.json
scripts/sync-master-data-to-backups.mjs
scripts/restore-neon-from-supabase.mjs
LOCAL_DEV_START.md
```

### 次にやること

1. 本番反映(コミット→PR更新→masterマージ→`vercel --prod`)。ユーザーの最終承認待ちで保留中
2. Phase 1(CRM基礎)着手: 担当者/活動履歴のCRUD API・画面実装、権限キー追加

---

## Phase 1〜6(未着手)

内容・優先理由は [docs/roadmap.md](./roadmap.md) を参照。着手時にこのファイルのステータスを更新する。
