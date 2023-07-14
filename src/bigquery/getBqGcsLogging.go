/*
======================
GCSからのChainedTagsLoggingのデータを
BigQueryへLoadする
========================
*/
package bigquery

import (
	"context"
	"strings"
	"time"

	TABLE "bwing.app/src/bigquery/table"
	CONFIG "bwing.app/src/config"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/bigquery"
)

// Inerface
type GetGcsLogging struct{}

///////////////////////////////////////////////////
/* ===========================================
GCS Loggingデータセットのt_extract_dateを取得
※Workflowの実行時に更新される
* =========================================== */
func (g GetGcsLogging) GetExtractDate() (string, string, error) {

	var err error
	var it *bigquery.RowIterator

	//BigQueryのclientを生成
	projectId := TABLE.GetProjectId()
	bqCtx := context.Background()
	bqClient, err := bigquery.NewClient(bqCtx, projectId)
	if err != nil {
		return "", "", err
	}
	defer bqClient.Close()

	//クエリを生成
	sql := "SELECT * FROM `gcs_logging.t_extract_date` ORDER BY timestamp DESC LIMIT 1"
	q := bqClient.Query(sql)
	q.QueryConfig.UseStandardSQL = true

	//実行のためのqueryをサービスに送信してIteratorを通じて結果を返す
	it, err = q.Read(bqCtx)
	if err != nil {
		return "", "", err
	}

	//BigQueryのレコードを格納する箱を準備(カラムごとに配列される)
	var extract_start_date string
	var extract_end_date string
	for {
		var values []bigquery.Value

		//これ以上結果が存在しない場合には、iterator.Doneを返して、ループ離脱
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		//エラーハンドル
		if err != nil {
			return "", "", err
		}

		//抽出開始日
		extract_start_date = values[1].(string)

		//抽出終了日
		extract_end_date = values[2].(string)
		extract_end_date = strings.ReplaceAll(extract_end_date, "-", "/")

		//指定されたextract_start_dateに対して1日減算を行う
		//条件；bucket_time_utcがtrue以外=GCSはUTCで格納＆感ロイ
		utc := CONFIG.GetConfigArgs(CONFIG.ARGS_BUCKET_TIME_UTC) //bucket_time_utcを取得
		if utc == "" || utc == "false" {
			//end_dateをtime.Time型に変換
			t := extract_start_date + "T00:00:00+00:00"
			parsedTime, _ := time.Parse("2006-01-02T15:04:05Z07:00", t)
			//1日減算する
			time1dBefore := parsedTime.AddDate(0, 0, -1)
			//フォーマット
			extract_start_date = time1dBefore.Format("2006/01/02")
			extract_start_date = strings.ReplaceAll(extract_start_date, "-", "/")
		}
	}

	return extract_start_date, extract_end_date, nil
}
