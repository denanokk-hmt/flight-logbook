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
	"sync"

	TABLE "bwing.app/src/bigquery/table"
	COMMON "bwing.app/src/common"
	CONFIG "bwing.app/src/config"
	BUCKET "bwing.app/src/gcs/bucket"

	"cloud.google.com/go/bigquery"
)

// Inerface
type LoadBqGyroscope struct {
	migrationIds []string
	insertCntTTL int
}

///////////////////////////////////////////////////
/* ===========================================
//取得したGCS BuketデータをBigQueryへロードする
* =========================================== */
func (l LoadBqGyroscope) Load2BqGyroscope(_pRecord interface{}) (int, error) {

	var err error

	//検証用(BQへインサートをブロック)
	if CONFIG.GetConfigArgs(CONFIG.ARGS_BQ_NO_INSERT) == CONFIG.DEFAULT_STRING_VALUE_TRUE {
		fmt.Println(TABLE.MSG_BQ_NO_INSERT)
		return 0, nil
	}

	//引数interfaceをCast
	records := _pRecord.([]TABLE.GyroscopeImport)
	ch := make(chan *CONFIG.ChBqInsert, len(records))
	defer close(ch)

	//チャンクを計算
	var rss [][]TABLE.GyroscopeImport
	chunks := COMMON.ChunkCalculator2(len(records), CONFIG.DEFAULT_INT_VALUE_CHUNK_SIZE_BQ)
	for _, c := range chunks.Positions {
		rs := records[c.Start:c.End]
		rss = append(rss, rs)
	}

	var wg sync.WaitGroup
	var index = 0

	//Bqへバルクインサート
	wg.Add(len(rss))
	for _, rs := range rss {

		//初期化
		index++
		migrationId := COMMON.RandomString(20) //マイグレーションIDを発行
		//cdtBqInsert := time.Now()

		//BigQueryにレコードをインサート
		go func(index int, migrationId string, _rs interface{}) {
			defer wg.Done()
			fmt.Printf("Bq Bulk Insert gyroscope [START]. bucket:[%s] migration_id:[%s] qty:[%d] index[%d/%d]\n", BUCKET.GCS_BUCKET_NAME_GYROSCOPE, migrationId, len(rs), index, len(rss))
			load2BqGy(migrationId, _rs, ch)
			select {
			case v := <-ch:
				if v.Err != nil {
					fmt.Println(v)
					err = v.Err
					return
				}
				l.migrationIds = append(l.migrationIds, v.MigrationId)
				l.insertCntTTL = l.insertCntTTL + v.InsertedRec
			default:
				fmt.Println("no value")
			}
			rs := _rs.([]TABLE.GyroscopeImport)
			fmt.Printf("Bq Bulk Insert gyroscope [DONE]. bucket:[%s] migration_id:[%s] qty:[%d] index[%d/%d]\n", BUCKET.GCS_BUCKET_NAME_GYROSCOPE, migrationId, len(rs), index, len(rss))
		}(index, migrationId, rs)
		if err != nil {
			return 0, err
		}
	}
	wg.Wait()

	return l.insertCntTTL, nil
}

///////////////////////////////////////////////////
/* ===========================================
//BigQueryに、LogsをLoadする
=========================================== */
func load2BqGy(migrationId string, _records interface{}, ch chan *CONFIG.ChBqInsert) {

	//BigQueryのclientを生成
	projectId := TABLE.GetProjectId()
	bqCtx := context.Background()
	bqClient, err := bigquery.NewClient(bqCtx, projectId)
	if err != nil {
		ch <- &CONFIG.ChBqInsert{
			MigrationId: migrationId,
			Err:         err,
		}
		return
	}
	defer bqClient.Close()

	//引数interfaceをCast
	rs := _records.([]TABLE.GyroscopeImport)

	//MigrationIDを仕込む
	for i, _ := range rs {
		rs[i].MigrationId = migrationId
	}

	//BiqQueryのTableID情報
	dataset := CONFIG.GetConfigArgs(CONFIG.ARGS_BQ_DATESET_NAME)
	table := CONFIG.GetConfigArgs(CONFIG.ARGS_BQ_TABLE_NAME)

	//Bqへバルクインサート
	var reSizeCnt = 0
	u := bqClient.Dataset(dataset).Table(table).Uploader()
	err = u.Put(bqCtx, rs)
	if err != nil {
		if !putRetryChunkSizeDown(&bqCtx, u, rs, &reSizeCnt) {
			ch <- &CONFIG.ChBqInsert{
				MigrationId: migrationId,
				Err:         err,
			}
			return
		}
	}

	ch <- &CONFIG.ChBqInsert{
		MigrationId: migrationId,
		InsertedRec: len(rs),
	}
}

///////////////////////////////////////////////////
/* ===========================================
//Chunkサイズを半分にしてリトライ
	Error Code 413対応　(現時点でエラーコード取得が？)
=========================================== */
func putRetryChunkSizeDown(bqCtx *context.Context, u *bigquery.Inserter, rs []TABLE.GyroscopeImport, reSizeCnt *int) bool {

	//リサイズをカウント
	*reSizeCnt++

	for i := 0; i < 2; i++ {
		var rsD []TABLE.GyroscopeImport
		if i == 0 {
			rsD = rs[:len(rs)/2]
		} else {
			rsD = rs[len(rs)/2:]
		}
		err := u.Put(*bqCtx, rsD)
		if err != nil {
			if *reSizeCnt < CONFIG.DEFAULT_INT_VALUE_RESIZE_LIMIT {
				return putRetryChunkSizeDown(bqCtx, u, rsD, reSizeCnt)
			} else {
				return false
			}
		}
		fmt.Printf("Success Chunk Size Down & Bq Bulk Insert!! chunk size:[%d]\n", len(rsD))
	}
	return true
}
