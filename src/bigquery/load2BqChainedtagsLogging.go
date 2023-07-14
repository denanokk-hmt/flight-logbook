/*
======================
GCSからのChainedTagsLoggingのデータを
BigQueryへLoadする
========================
*/
package bigquery

import (
	"context"
	"fmt"
	"strconv"
	"time"

	TABLE "bwing.app/src/bigquery/table"
	COMMON "bwing.app/src/common"
	CONFIG "bwing.app/src/config"
	BUCKET "bwing.app/src/gcs/bucket"

	"cloud.google.com/go/bigquery"
)

// Inerface
type LoadBqChainedTagsLogging struct{}

///////////////////////////////////////////////////
/* ===========================================
//取得したGCS BuketデータをBigQueryへロードする
* =========================================== */
func (l LoadBqChainedTagsLogging) Load2BqChainedTagsLogging(_adGroupMap interface{}) (int, error) {

	var err error

	//引数interfaceをCast
	adGroupMap := _adGroupMap.(map[string][]BUCKET.ChainedTagsLoggingImport)

	//ATID-DDID別マッピングしたログの中で、一番最後の要素のみに選別
	//*ChainedTagsのロギング方法として、TagsWordを配列に配列を入れて引き継ぐ
	//*よってATID-DDID別の中で、最後のログがすべてのTagsWordを選択された順番で
	//*配列of配列の形でもっているため
	var lRecords []BUCKET.ChainedTagsLoggingImport
	for _, m := range adGroupMap {
		lRecords = append(lRecords, m[len(m)-1])
	}

	//選別後のInsert用の箱にたたみ直し
	var iRecords []*TABLE.ChainedTagsLoggingImportInsert
	for i, lr := range lRecords {
		groupId := time.Now().Unix() ////group id用のunixtimeを発行
		gqty := len(lr.RelatedWordsLog)
		for ii, rw := range lr.RelatedWordsLog {
			//レコードを作成
			var iRecord TABLE.ChainedTagsLoggingImportInsert
			iRecord.Pdt = lr.Pdt
			iRecord.Cdt = lr.Cdt
			iRecord.ClientId = lr.ClientId
			iRecord.CustomerUuid = lr.CustomerUuid
			iRecord.WhatYaId = lr.WhatYaId
			iRecord.ATID = lr.ATID
			iRecord.DDID = lr.DDID
			iRecord.GID = strconv.Itoa(int(groupId)) + "_" + strconv.Itoa(i) + "_" + strconv.Itoa(ii)
			iRecord.GQty = gqty
			iRecord.GSort = ii
			iRecord.RelatedUnixtime = lr.RelatedUnixtime
			iRecord.PublishedAt = lr.PublishedAt
			iRecord.RelatedWordsLog = rw
			iRecord.SelectedItemId = lr.SelectedItemId
			iRecord.SelectedItemName = lr.SelectedItemName
			//レコードを箱詰め
			iRecords = append(iRecords, &iRecord)
		}
	}

	/*------------------------------------------------
	BigQueryへログをインサート
	------------------------------------------------*/

	//BigQueryのclientを生成
	projectId := TABLE.GetProjectId()
	bqCtx := context.Background()
	bqClient, err := bigquery.NewClient(bqCtx, projectId)
	if err != nil {
		return 0, err
	}
	defer bqClient.Close()

	//BqにBulk Insertを行う
	rc, err := load2BqCTL(iRecords, bqClient, &bqCtx)
	if err != nil {
		return 0, err
	}

	return rc, nil
}

///////////////////////////////////////////////////
/* ===========================================
//BigQueryに、LogsをLoadする
=========================================== */
func load2BqCTL(_records interface{}, bqClient *bigquery.Client, bqCtx *context.Context) (int, error) {

	//引数interfaceをCast
	records := _records.([]*TABLE.ChainedTagsLoggingImportInsert)

	//検証用(BQへインサートをブロック)
	if CONFIG.GetConfigArgs(CONFIG.ARGS_BQ_NO_INSERT) == CONFIG.DEFAULT_STRING_VALUE_TRUE {
		fmt.Println(TABLE.MSG_BQ_NO_INSERT)
		return 0, nil
	}

	//BiqQueryのTableID情報
	dataset := CONFIG.GetConfigArgs(CONFIG.ARGS_BQ_DATESET_NAME)
	table := CONFIG.GetConfigArgs(CONFIG.ARGS_BQ_TABLE_NAME)

	//チャンクを計算
	var rss [][]*TABLE.ChainedTagsLoggingImportInsert
	chunks := COMMON.ChunkCalculator2(len(records), 10000)
	for _, c := range chunks.Positions {
		rs := records[c.Start:c.End]
		rss = append(rss, rs)
	}

	//Bqへバルクインサート
	for _, rs := range rss {
		u := bqClient.Dataset(dataset).Table(table).Uploader()
		err := u.Put(*bqCtx, rs)
		fmt.Printf("Gcs2Bq bucket:%s table:%s qty:%d\n", BUCKET.GCS_BUCKET_NAME_CHAINED_TAGS_LOGGING, table, len(rs))
		if err != nil {
			return 0, err
		}
	}

	return len(records), nil
}
