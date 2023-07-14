/*
======================
ETL Jobの処理のまとめやく
========================
*/
package jobs

import (
	"fmt"
	"strings"

	BQ "bwing.app/src/bigquery"
	CONFIG "bwing.app/src/config"
	GCS "bwing.app/src/gcs/extract"
	ARGS "bwing.app/src/jobs/args"
	LOGGING "bwing.app/src/log"
	"github.com/pkg/errors"
)

///////////////////////////////////////////////////
/* ===========================================
//Contentで指定されたログのETLを行う
=========================================== */
func ExecuteJobs(content, batchId string) (interface{}, int, error) {

	//コマンドライン引数で指定がなかった項目に、contentに応じたDefault値を格納
	err := ARGS.AddDefault2Args(content)
	if err != nil {
		return nil, 0, err
	}

	//コマンドライン引数のバリデーション
	err = ARGS.ValidationReqArgs(content)
	if err != nil {
		return nil, 0, err
	}

	//コマンドライン引数の値を出力
	args := CONFIG.GetConfigArgsAllString()
	fmt.Println(LOGGING.SetLogEntry(LOGGING.INFO, "Args", fmt.Sprintf("%+v", args)))

	//GCSログを取得しBigQueryへロードするJobを実行
	/////【【【コンテント追加時に必須追加】】】/////
	switch 0 {

	/*=================================================================================
	【Flight Record】
	Boardingなどのバックエンドサーバーがflight-recorderを使って生成したログ
	*/
	case strings.Index(content, CONFIG.CONTENT_NAME_FLIGHT_RECORD):

		//GCSからログデータを抽出する
		var f GCS.ExtractFlightRecord
		pr, lr, err := f.ExtractFlightRecord()
		if err != nil {
			return nil, 0, err
		}

		//BigQueryへログをロードする
		var bq BQ.LoadBqFlightRecord
		rc, err := bq.Load2BqFlightRecord(pr)
		if err != nil {
			return nil, 0, err
		}

		return lr, rc, nil

	/*=================================================================================
	【Gyroscope】
	Cockpitなどのクライアントがgyroscopeを使って生成したログ
	*/
	case strings.Index(content, CONFIG.CONTENT_NAME_GYROSCOPE):

		//GCSからログデータを抽出する
		var iRecords int
		var lResults interface{} //parse結果

		if CONFIG.GetConfigArgs(CONFIG.ARGS_FREQUENCY) == CONFIG.DEFAULT_STRING_VALUE_DAILY {
			//■■Daily
			var term string
			if content != CONFIG.CONTENT_NAME_GYROSCOPE {
				term = strings.Split(content, "T")[1] //termを取得
			}
			fd := &GCS.ExtractGyroscopeDaily{BatchId: batchId}
			lResults, iRecords, err = fd.ExtractGyroscope(term) //GyroscopeからのGCSファイル＆日次利用

		} else if CONFIG.GetConfigArgs(CONFIG.ARGS_FREQUENCY) == CONFIG.DEFAULT_STRING_VALUE_MONTHLY {
			//■■Montly
			fm := &GCS.ExtractGyroscopeMonthly{BatchId: batchId}
			lResults, iRecords, err = fm.ExtractGyroscope("") //GyroscopeからのGCSファイル＆月次利用

		} else if CONFIG.GetConfigArgs(CONFIG.ARGS_LOG_ROUTER) == CONFIG.DEFAULT_STRING_VALUE_TRUE {
			//■■logRouterバージョン
			fl := &GCS.ExtractGyroscopeLogRouter{BatchId: batchId}
			lResults, iRecords, err = fl.ExtractGyroscope() //ログルーターからのGCSファイル利用

		} else {
			err = errors.New("ARGS異常")
		}

		if err != nil {
			return nil, 0, err
		}

		return lResults, iRecords, nil

	/*=================================================================================
	【Chained tags logging】
	AttachmentのSearch APIで生産されたChained Tagsのログ
	*/
	case strings.Index(content, CONFIG.CONTENT_NAME_CHAINED_TAGS_LOGGING):

		//GCSからログデータを週出する
		var f GCS.ExtractChainedTagsLogging
		ad, lr, err := f.ExtractChainedTagsLogging()
		if err != nil {
			return nil, 0, err
		}

		//BigQueryへログをロードする
		var bq BQ.LoadBqChainedTagsLogging
		rc, err := bq.Load2BqChainedTagsLogging(ad)
		if err != nil {
			return nil, 0, err
		}

		return lr, rc, nil

	/*=================================================================================
	【Mau counter】
	Schedulerで実行されたmau-counterのログ
	*/
	case strings.Index(content, CONFIG.CONTENT_NAME_MAU_COUNTER):

		//GCSからログデータを週出する
		var f GCS.ExtractMauCounter
		ad, lr, err := f.ExtractMauCounter()
		if err != nil {
			return nil, 0, err
		}

		//BigQueryへログをロードする
		var bq BQ.LoadBqMauCounter
		rc, err := bq.Load2BqMauCounter(ad)
		if err != nil {
			return nil, 0, err
		}

		return lr, rc, nil

	/*=================================================================================
	【Tugcar api request logging】
	TugcarへのAPIリクエストのログ
	*/
	case strings.Index(content, CONFIG.CONTENT_NAME_TUGCAR_API_REQUEST_LOGGING):

		//GCSからログデータを週出する
		var f GCS.ExtractTugcarApiRequest
		ad, lr, err := f.ExtractTugcarApiRequest()
		if err != nil {
			return nil, 0, err
		}

		//BigQueryへログをロードする
		var bq BQ.LoadBqTugcarApiRequest
		rc, err := bq.Load2BqTugcarApiRequest(ad)
		if err != nil {
			return nil, 0, err
		}

		return lr, rc, nil

	/*=================================================================================
	例外処理
	*/
	default:
		err := errors.New("【Error】content is nothing.")
		if err != nil {
			return nil, 0, err
		}
	}

	//形骸リターン処理
	return nil, 0, nil
}
