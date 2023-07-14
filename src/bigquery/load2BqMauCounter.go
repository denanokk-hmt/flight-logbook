/*
======================
GCSからのGyroscopeのデータを
BigQueryへLoadする
========================
*/
package bigquery

import (
	"context"
	"fmt"

	TABLE "bwing.app/src/bigquery/table"
	COMMON "bwing.app/src/common"
	CONFIG "bwing.app/src/config"
	BUCKET "bwing.app/src/gcs/bucket"

	"cloud.google.com/go/bigquery"
)

// Inerface
type LoadBqMauCounter struct{}

///////////////////////////////////////////////////
/* ===========================================
//取得したGCS BuketデータをBigQueryへロードする
* =========================================== */
func (l LoadBqMauCounter) Load2BqMauCounter(_pRecord interface{}) (int, error) {

	var err error

	//BigQueryのclientを生成
	projectId := TABLE.GetProjectId()
	bqCtx := context.Background()
	bqClient, err := bigquery.NewClient(bqCtx, projectId)
	if err != nil {
		return 0, err
	}
	defer bqClient.Close()

	//BqにBulk Insertを行う
	rc, err := load2BqMauCounter(_pRecord, bqClient, &bqCtx)
	if err != nil {
		return 0, err
	}

	return rc, err
}

///////////////////////////////////////////////////
/* ===========================================
//BigQueryに、LogsをLoadする
=========================================== */
func load2BqMauCounter(_records interface{}, bqClient *bigquery.Client, bqCtx *context.Context) (int, error) {

	//引数interfaceをCast
	records := _records.([]TABLE.MauCounterImport)

	//検証用(BQへインサートをブロック)
	if CONFIG.GetConfigArgs(CONFIG.ARGS_BQ_NO_INSERT) == CONFIG.DEFAULT_STRING_VALUE_TRUE {
		fmt.Println(TABLE.MSG_BQ_NO_INSERT)
		return 0, nil
	}

	//BiqQueryのTableID情報
	dataset := CONFIG.GetConfigArgs(CONFIG.ARGS_BQ_DATESET_NAME)
	table := CONFIG.GetConfigArgs(CONFIG.ARGS_BQ_TABLE_NAME)

	//チャンクを計算
	var rss [][]TABLE.MauCounterImport
	chunks := COMMON.ChunkCalculator2(len(records), 10000)
	for _, c := range chunks.Positions {
		rs := records[c.Start:c.End]
		rss = append(rss, rs)
	}

	//Bqへバルクインサート
	for _, rs := range rss {
		u := bqClient.Dataset(dataset).Table(table).Uploader()
		err := u.Put(*bqCtx, rs)
		fmt.Printf("Gcs2Bq bucket:%s table:%s qty:%d\n", BUCKET.GCS_BUCKET_NAME_GYROSCOPE, table, len(rs))
		if err != nil {
			return 0, err
		}
	}

	return len(records), nil
}
