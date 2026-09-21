# 舊平行草稿轉回單代理

本文件只供曾啟用平行工作、仍有 `pipeline/` 的小說交接。日常採 **單一 Session、Luna Max**；同一 Session 完成初譯、查證、修訂、整節評分與交付。新書不建立佇列、不開子代理，也不讀本文件。舊 `parallel` 命令僅為保全進行中稿件及恢復中斷保存而保留；`init`、`claim`、`assist-request` 已停用。

## 在保存點交接一次

1. 原主 Session 停止新派工，通知既有子代理保存已產生的完整或部分稿件及疑義後停止，確認它們不再寫入。同書保留單一寫入者；技能更新本身不會停止其他任務。舊派工尚未交稿時仍可用原 `submit` 保存完整稿件，`claim_next:true` 只保存、不再領下一段。未達完整來源覆蓋的部分稿件另存既有明確工作檔，保留位置，不虛填通過。
2. 讀回 `status`，按 `job_id/generation/chunk_id` 確認目前有效槽位；已接收後殘留的槽位檔不代表新待辦。已存稿、候選詞、疑義、查證、原依據與 `interrupted_attempts` 都保留，不能清空 `pipeline/`。不為交接重建來源、重設計畫或建立新 Goal；沿用原任務授權與有效 Goal。
3. 若有 `publishing`，先依下節處理。其餘在確認保存點後執行 `serial`，即使還有待接收草稿或查證也可停用。命令只停用佇列，不會自動停止子代理、接收初稿、採納查證或填寫驗收。

```text
python -X utf8 <skill>/scripts/novel_pipeline.py parallel --work <工作目錄> --actor <原主代理標籤> --action status --input -
```

stdin 為 `{}`；原主代理標籤取自 `pipeline/state.json` 的 `actors.main`。完成以上保存點後，同一命令改用 `--action serial`，stdin：

```json
{"checkpoint": true}
```

`serial` 原樣保留槽位及查證，只將舊狀態的 `active` 設為 false，回傳有效草稿位置；可重複呼叫。交接後拒收遲到的子代理寫入，不再使用十個 `c` 的容量限制。原 `close` 仍只適用已無草稿與查證的空佇列。

## 接續翻譯與沿用草稿

執行原本的 `next --prepare`，以實際有效進度的最早缺口續譯，不從舊佇列游標、檔名或最後初譯位置開始。一般保存與驗收命令不再需要 `--actor`。

- 對下一個 `c`，先核對 `state.json` 中是否有相符的有效派工，再讀該槽位原稿與 `draft`、候選詞、疑義、查證及中斷修訂；部分稿件也必須整合。查證若在 `state.assist` 或 `assist.json`，核對識別與版本後由主 Session 自行判斷，不視為已採納。
- 使用新準備的完整原文、正式詞條及前文摘要逐段修訂舊稿；資料包或主詞依據變動仍回查差異，不照抄舊聲明。不因切回單代理而重譯有效完成段，也不能跳過沒有舊稿的段落。
- 正式 `submit-chunk` 仍由模型提供五項實際核對及問題處置；新輸入保存實際沿用的 `job_id`、`generation`、`draft_sha256` 與草稿位置（可放在 `legacy_draft` 欄位），原始證據留在 `pipeline/`。有中斷修訂或既有正式檔時先讀回整合，再按原規則使用 `--replace`，不得覆寫未整合的人工修改。
- 小段保存、整節齊備後合併並完整閱讀，分節十項評分及全書驗收照舊。待接收稿不算已完成；所有候選與疑義都須有實際處置。`pipeline/` 不再重用或清理，只作舊稿證據，正式進度以既有驗收為準。

後續只讀 [日常操作](resume-and-delivery.md#自動保存合併與共用閱讀)。約 2 MiB 來源包、精簡資料包、局部失效判定、人工版批次搜尋、串接保存與複核等既有優化都保留。

## 有中斷的正式保存時

`serial` 遇到 `publishing` 會拒絕停用，保留所有檔案。原主 Session 在同一命令介面用 `recover`、輸入 `{}`，僅在原閱讀依據、前序證據及產物仍有效時完成原保存。

依據已改而無法恢復時，以 `retry-review` 提供 `job_id/generation/source_ids/note`，記錄中斷原因並保留修訂；部分產物標為 `awaiting_recheck`。可按下節實際重查再接收後 `serial`，或在沒有 `publishing` 後 `serial`，讀回 `interrupted_attempts` 整合修訂，再用日常流程重新核對保存。遇到不符合原指紋的人工改稿，保留並整合，不能直接改旗標或指紋通過。

仍在相容接收流程且舊查證待處理時，主 Session 可用 `assist-resolve` 提供 `assist_id/status:"resolved"/source_ids/note` 採納有效結果；失效或改為自行查證時，用 `assist-cancel` 提供 `assist_id/source_ids/note` 保存撤回原因。兩者都不自行解決草稿疑義，正式接收仍需具體判斷。不得再派新查證任務。

## 中斷保存需重新複核時的相容輸入

1. 對最早待接收的 `job_id/generation` 執行 `review`。完整讀回原文、草稿、現有產物及 `changes`，逐段比對、核對指代、閱讀中文與潤色後回查。`term_ids` 可明列當前真正使用的隱含依賴，修正舊 ID 時仍會列出差異。
2. 採納專名、更新正式 `continuity.jsonl`、問題清單或文體設定後，重新取得 `review`。同一小說只有主 Session 寫這些資料；在共用 `.writer.lock` 內原子保存，不與其他主 Session 同時手改。跨節正式接收仍需要已核實的前節摘要，暫定摘要不能直接冒充正式紀錄。
3. 以 `accept` 提交版本、修訂差異及實際五項檢查。未更動的翻譯單位由工具沿用，模型不用重新輸出整份譯文。

`accept` 必填 `job_id`、`generation`、`draft_sha256`、`review_state`，取自剛實際閱讀的結果；另填：

| 欄位 | 內容 |
|---|---|
| `edits` | `[{"unit":0,"replacement":[完整語意單位]}]`，索引從 0 起；工具按原草稿索引套用，可一次替換為多個單位。跨單位合併時同時替換相關索引並保證完整有序覆蓋。未修訂填 `[]`。 |
| `reviews` | 既有 `fidelity/referents/fluency/locale/post_polish` 的實際 `status/note`；工具不填成功或評分。 |
| `state_after` | 主 Session 核實的小段結束狀態，使用 `viewpoint/time_place/speakers/known_facts/open_threads` 五欄。摘要有修正時另填 `summary_after`。 |
| `term_ids` | 實際適用的隱含專名 ID，與本次 review 一致；來源新命中詞仍自動納入檢查。 |
| `rechecks` | 每個 `changes.id` 一列 `{"change_id":"實際ID","source_ids":["實際位置"],"note":"實際回查與修正結論"}`；沒有差異才填 `[]`。 |
| `decisions` | 按候選／問題 ID 填 `{"status":"resolved","source_ids":["實際位置"],"note":"採納、修正或不採納的具體依據"}`；沒有時填 `{}`。 |
| `merge/title` | 本節齊備時可 `merge:true`，配合已核對的中文標題直接回傳整節待讀中文。合併失敗仍保留已接收小段。 |
| `replace` | 只有主 Session 已閱讀並整合既有輸入或工具產物時才明列 true；外部改過且不符合既有指紋的產物仍拒絕覆寫。 |

主詞、名稱或前文事實改變時，依位置實際回查受影響草稿；`rechecks` 是模型提供的證據，工具只能檢查其完整與版本，不能代替語意判斷。禁止只更新 `review_state` 或指紋就沿用舊聲明。原稿／人工版母檔或批次計畫變動依既有規則停止，不能自動改綁。

成功接收建立既有 `chunks/*.input.json/.txt/.map.json`，保存草稿來源版本、修訂差異、問題處置與查證紀錄。中斷稿處理後執行 `serial` 回到日常流程。切換前若需整節複核，正式寫入命令加 `--actor <原主代理標籤>`；整節完整閱讀、十項評分與全書連讀仍須實際完成。
