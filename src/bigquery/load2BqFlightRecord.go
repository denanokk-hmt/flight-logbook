/*
======================
GCSからのFlightRecordのデータを
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
type LoadBqFlightRecord struct{}

///////////////////////////////////////////////////
/* ===========================================
//取得したGCS BuketデータをBigQueryへロードする
* =========================================== */
func (l LoadBqFlightRecord) Load2BqFlightRecord(_pRecord interface{}) (int, error) {

	var err error

	//引数interfaceをCast
	//load_results := _load_results.([]GCS.ExtractBucketResults)

	//BigQueryのclientを生成
	projectId := TABLE.GetProjectId()
	bqCtx := context.Background()
	bqClient, err := bigquery.NewClient(bqCtx, projectId)
	if err != nil {
		return 0, err
	}
	defer bqClient.Close()

	//BqにBulk Insertを行う
	rc, err := load2BqFR(_pRecord, bqClient, &bqCtx)
	if err != nil {
		return 0, err
	}

	return rc, err
}

///////////////////////////////////////////////////
/* ===========================================
//BigQueryに、LogsをLoadする
=========================================== */
func load2BqFR(_records interface{}, bqClient *bigquery.Client, bqCtx *context.Context) (int, error) {

	//引数interfaceをCast
	records := _records.([]TABLE.FlightRecordImport)

	//検証用(BQへインサートをブロック)
	if CONFIG.GetConfigArgs(CONFIG.ARGS_BQ_NO_INSERT) == CONFIG.DEFAULT_STRING_VALUE_TRUE {
		fmt.Println(TABLE.MSG_BQ_NO_INSERT)
		return 0, nil
	}

	//BiqQueryのTableID情報
	dataset := CONFIG.GetConfigArgs(CONFIG.ARGS_BQ_DATESET_NAME)
	table := CONFIG.GetConfigArgs(CONFIG.ARGS_BQ_TABLE_NAME)

	//チャンクを計算
	var rss [][]TABLE.FlightRecordImport
	chunks := COMMON.ChunkCalculator2(len(records), 1000)
	for _, c := range chunks.Positions {
		rs := records[c.Start:c.End]
		rss = append(rss, rs)
	}

	//Bqへバルクインサート
	for _, rs := range rss {
		u := bqClient.Dataset(dataset).Table(table).Uploader()
		err := u.Put(*bqCtx, rs)
		fmt.Printf("Gcs2Bq bucket:%s table:%s qty:%d\n", BUCKET.GCS_BUCKET_NAME_FLIGHT_RECORD, table, len(rs))
		if err != nil {
			return 0, err
		}
	}

	return len(records), nil
}
