/*
======================
GCS
bucket::gyroscope
バケットファイルを取得
========================
*/
package extract

import (
	"fmt"
	"time"

	TABLE "bwing.app/src/bigquery/table"
	CONFIG "bwing.app/src/config"
	BUCKET "bwing.app/src/gcs/bucket"
)

// Inerface
type ExtractTugcarApiRequest struct {
	ex ExtractBucket
}

///////////////////////////////////////////////////
/* ===========================================
GCS BucketからTugcarApiRequestのログを取得
=========================================== */
func (e *ExtractTugcarApiRequest) ExtractTugcarApiRequest() (interface{}, interface{}, error) {

	var err error

	/*------------------------------------------------
	GCSからバケットを取得
	------------------------------------------------*/

	//GCSバケットを取得
	extracts, err := e.ex.FetchBucket()
	if err != nil {
		return nil, nil, err
	}
	logss := extracts.(BUCKET.LogssStruct) //結果をCastして使い物に

	/*------------------------------------------------
	ログをパース
	------------------------------------------------*/

	//取得レコード数を確保
	//ttl := logss.Len

	cdt := time.Now()

	//結果箱の準備
	var load_results []ExtractBucketResults

	//パースしたすべてのログを平滑に入れておく箱
	var pRecords []TABLE.TugcarApiRequestImport

	//ログをパース
	for i, logs := range logss.Objs {
		for ii, log := range logs {

			//ChainedTagsLoggingログをパース(log=1時間分のものがやってくる)
			var hRecords []TABLE.TugcarApiRequestImport

			//GCSログをパースして、BQへインサート出来るように整形
			fmt.Println("Parse bucket logging now:", logss.Paths[i][ii])
			l := &BUCKET.LogTugcarApiRequest{Log: log, Path: logss.Paths[i][ii]}
			hRecords, parseErrs, err := l.TugcarApiRequestLogParser()
			if err != nil {
				return nil, nil, err
			}
			pRecords = append(pRecords, hRecords...)

			//パースできなかったinsertIdをロギング
			errIds := parseErrs.([]string) //Cast
			if len(errIds) != 0 {
				e.ex.ExtractParseErrorLogging(BUCKET.GCS_BUCKET_NAME_TUGCAR_API_REQUEST_LOGGING, logss.Paths[i][ii], errIds) //WARNロギング
			}

			//GCS 取得結果を格納
			var result ExtractBucketResults = ExtractBucketResults{
				Result:          CONFIG.RESULT_SUCCESS,
				Cdt:             cdt,
				LogNo:           i,
				LogPath:         logss.Paths[i][ii],
				LogDate:         logss.LogDate[i],
				ParseCount:      len(pRecords),
				ParseErrorCount: len(errIds),
			}
			fmt.Println(result)
			load_results = append(load_results, result)
		}
	}

	return pRecords, load_results, nil
}
