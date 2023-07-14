/*
======================
GCS
bucket::chained_tags_logging
バケットファイルを取得
========================
*/
package extract

import (
	"fmt"
	"strconv"
	"time"

	CONFIG "bwing.app/src/config"
	BUCKET "bwing.app/src/gcs/bucket"
)

// Inerface
type ExtractChainedTagsLogging struct {
	ex ExtractBucket
}

///////////////////////////////////////////////////
/* ===========================================
GCS BucketからChainedTagsのログを取得
=========================================== */
func (e *ExtractChainedTagsLogging) ExtractChainedTagsLogging() (interface{}, interface{}, error) {

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
	var pRecords []BUCKET.ChainedTagsLoggingImport

	//ログをパース
	for i, logs := range logss.Objs {
		for ii, log := range logs {

			//ChainedTagsLoggingログをパース(log=1時間分のものがやってくる)
			var hRecords []BUCKET.ChainedTagsLoggingImport

			//GCSログをパースして、BQへインサート出来るように整形
			fmt.Println("Parse bucket logging now:", logss.Paths[i][ii])
			l := &BUCKET.LogChainedTags{Log: log, Path: logss.Paths[i][ii]}
			hRecords, parseErrs, err := l.ChainedTagsLogParser()
			if err != nil {
				return nil, nil, err
			}
			pRecords = append(pRecords, hRecords...)

			//パースできなかったinsertIdをロギング
			errIds := parseErrs.([]string) //Cast
			if len(errIds) != 0 {
				e.ex.ExtractParseErrorLogging(BUCKET.GCS_BUCKET_NAME_CHAINED_TAGS_LOGGING, logss.Paths[i][ii], errIds) //WARNロギング
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

	//取得したログをATID-DDID別にマッピングする
	var adGroupMap = make(map[string][]BUCKET.ChainedTagsLoggingImport)
	for _, pr := range pRecords {
		adGroupMap[pr.ATID+"ddid-"+strconv.Itoa(pr.DDID)] = append(adGroupMap[pr.ATID+"ddid-"+strconv.Itoa(pr.DDID)], pr)
	}

	return adGroupMap, load_results, nil
}
