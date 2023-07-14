/*
======================
Cloud-Run Jobsからの
設定値を取得
設定値についての処理
========================
*/
package args

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	BQ "bwing.app/src/bigquery"
	VALID "bwing.app/src/common"
	CONFIG "bwing.app/src/config"
)

type DVM struct {
	DefaultValueMap map[string]string
}

///////////////////////////////////////////////////
/* ===========================================
コマンドライン引数で指定がなかった項目に、contentに応じたDefault値を格納
=========================================== */
func AddDefault2Args(content string) error {

	/*-----------------------------------
	Content別にデフォルト値を準備
	-----------------------------------*/

	//デフォルト値の箱を準備(Mapper)
	var dvm DVM
	dvm.DefaultValueMap = make(map[string]string)

	/////////////////////////
	//デフォルト値を設定(content別)
	/////【【【コンテント追加時に必須追加】】】/////

	switch 0 {

	//flight_recordの場合
	case strings.Index(content, CONFIG.CONTENT_NAME_FLIGHT_RECORD):
		var args ArgsFlightReord
		args.AddDefault2Args(&dvm)

	//gyroscopeの場合
	case strings.Index(content, CONFIG.CONTENT_NAME_GYROSCOPE):

		var args ArgsGyroscope
		args.AddDefault2Args(&dvm)

	//chained_tags_loggingの場合
	case strings.Index(content, CONFIG.CONTENT_NAME_CHAINED_TAGS_LOGGING):
		var args ArgsChainedTagsLogging
		args.AddDefault2Args(&dvm)

	//mau_counterの場合
	case strings.Index(content, CONFIG.CONTENT_NAME_MAU_COUNTER):
		var args ArgsMauCounter
		args.AddDefault2Args(&dvm)

	//tugcar_api_requestの場合
	case strings.Index(content, CONFIG.CONTENT_NAME_TUGCAR_API_REQUEST_LOGGING):
		var args ArgsTugcarApiRequest
		args.AddDefault2Args(&dvm)

	//エラー
	default:
		return fmt.Errorf("【Error】[func:%s][Args:%s][msg:%s]", "/src/jobs/argsData.go/AddDefault2Args()", content, "コマンドライン引数のキーが一致しません。")
	}

	/////////////////////////
	//デフォルト値を設定(共通項目)
	dvm.DefaultValueMap[CONFIG.ARGS_CONTENT] = content
	dvm.DefaultValueMap[CONFIG.ARGS_FREQUENCY] = CONFIG.DEFAULT_STRING_VALUE_DAILY
	dvm.DefaultValueMap[CONFIG.ARGS_BUCKET_PREFIX] = ""
	dvm.DefaultValueMap[CONFIG.ARGS_BUCKET_SUFFIX] = ""
	dvm.DefaultValueMap[CONFIG.ARGS_BUCKET_PATHS] = CONFIG.DEFAULT_STRING_VALUE_FALSE
	dvm.DefaultValueMap[CONFIG.ARGS_BUCKET_TIME_UTC] = CONFIG.DEFAULT_STRING_VALUE_FALSE
	dvm.DefaultValueMap[CONFIG.ARGS_BQ_NO_INSERT] = CONFIG.DEFAULT_STRING_VALUE_FALSE
	dvm.DefaultValueMap[CONFIG.ARGS_LOG_ROUTER] = CONFIG.DEFAULT_STRING_VALUE_FALSE

	/*-----------------------------------
	抽出日のデフォルト値を設定する
	-----------------------------------*/

	//extract_start_date::デフォルト=Dailyバッチ処理のため、開始日は、実行日の前日を置く
	timeNow1dBefore := time.Now().AddDate(0, 0, -1)
	day1before := timeNow1dBefore.Format("2006/01/02")

	//extract_end_date::bucket_time_utcがtrue以外の場合、JSTで取得したいので、終了日を実行日にしておく
	timeNow := time.Now()
	dayNow := timeNow.Format("2006/01/02")

	//Workflow上から更新されたextract日付を取得
	//	Workflow上でも通常のDailyバッチ時は、実行日の前日が自動設定されるが、
	//	マイグレーション時などに、異なる日付が登録されるので、それらを採用する
	var b BQ.GetGcsLogging
	bqSd, bqEd, err := b.GetExtractDate()
	if err != nil {
		return err
	}

	//extract_start_date
	if bqSd == day1before {
		dvm.DefaultValueMap[CONFIG.ARGS_EXTRACT_START_DATE] = day1before
	} else {
		dvm.DefaultValueMap[CONFIG.ARGS_EXTRACT_START_DATE] = bqSd
	}

	//extract_end_date::
	if bqEd == dayNow {
		dvm.DefaultValueMap[CONFIG.ARGS_EXTRACT_END_DATE] = dayNow
	} else {
		dvm.DefaultValueMap[CONFIG.ARGS_EXTRACT_END_DATE] = bqEd
	}

	/*-----------------------------------
	未登録のリクエストデータにデフォルト値または、引数指定値をあてる
	-----------------------------------*/

	var args []string //CONFIG上書き用の箱
	for k, v := range dvm.DefaultValueMap {
		switch k {

		//extract_end_date特別処理
		case CONFIG.ARGS_EXTRACT_START_DATE:
			startD := CONFIG.GetConfigArgs(k)                        //Args:extract_start_dateを取得
			freq := CONFIG.GetConfigArgs(CONFIG.ARGS_FREQUENCY)      //Args:frequencyを取得
			utc := CONFIG.GetConfigArgs(CONFIG.ARGS_BUCKET_TIME_UTC) //Args:bucket_time_utcを取得

			//extract_start_dateのコマンドライン引数の指定があった場合(日付指定を想定)、
			//指定されたextract_start_dateに対して1日減算を行う(条件；bucket_time_utcがtrue以外)
			if startD != "" {
				if utc != CONFIG.DEFAULT_STRING_VALUE_TRUE && freq == CONFIG.DEFAULT_STRING_VALUE_DAILY {
					//指定引数をtime.Time型に変換
					t := strings.ReplaceAll(startD, "/", "-") + "T00:00:00+00:00"
					parsedTime, _ := time.Parse("2006-01-02T15:04:05Z07:00", t)
					//1日減算する
					time1dAfter := parsedTime.AddDate(0, 0, -1)
					//フォーマット
					day1after := time1dAfter.Format("2006/01/02")
					//CONFIG上書き用に確保
					args = append(args, k+":"+day1after)
				}
			} else {
				args = append(args, k+":"+v) //引数指定がなければ、デフォルトをあてる
			}
		default:
			//引数指定がなければ、デフォルトをあてる
			if CONFIG.GetConfigArgs(k) == "" {
				args = append(args, k+":"+v)
			}
		}
	}

	/*-----------------------------------
	ConfigのArgsMapperを上書き
	-----------------------------------*/

	//Again Set CMD Args values
	if len(args) != 0 {
		err := CONFIG.SetConfigMapArgs(args)
		if err != nil {
			return err
		}
	}

	return nil
}

///////////////////////////////////////////////////
/* ===========================================
contentに応じたValidation
	frequency::値の有無
	contents::検査しない→switchのdefaultでエラー
	force_index::検査なし
	bucket_name::値の有無
	bucket_middle_path::値の有無
	bucket_prefix::検査なし
	bucket_suffix::検査なし
	bucket_time_utc::検査なし
	bq_dataset_name::値の有無
	bq_table_name::値の有無
	bq_no_insert::検査なし
	extract_start_date::日付の確認、※1
	extract_end_date::日付の確認、※1:extract_start_date <= extract_end_dateの関係性であること
=========================================== */
func ValidationReqArgs(content string) error {

	//抽出日検証用マップの準備
	extractDate := make(map[string]int)

	//引数コマンドラインの値を取得
	configs := CONFIG.GetConfigArgsAll()

	//引数コマンドラインごとに検証を実施
	for key, val := range configs {
		switch key {
		case
			CONFIG.ARGS_FREQUENCY,
			CONFIG.ARGS_BUCKET_NAME,
			CONFIG.ARGS_BUCKET_MIDDLE_PATH,
			CONFIG.ARGS_BQ_DATESET_NAME,
			CONFIG.ARGS_BQ_TABLE_NAME:
			//値の有無を確認
			if val == "" {
				return fmt.Errorf("【Valid Error】[Args:%s][msg:%s]", key, "値が設定されていません。")
			}
		case
			CONFIG.ARGS_EXTRACT_START_DATE,
			CONFIG.ARGS_EXTRACT_END_DATE:
			//日付確認
			if !VALID.DateFormatChecker(val) {
				return fmt.Errorf("【Valid Error】[Args:%s][val:%s][msg:%s]", key, val, "日付が設定されていません。")
			}
			//日付期間確認
			num, err := strconv.Atoi(strings.ReplaceAll(val, "/", "")) //数値に変換
			if err != nil {
				return fmt.Errorf("【Error】[func:%s][Args:%s][msg:%s]", "/src/jobs/argsData.go/ValidationReqArgs()", key, err)
			}
			extractDate[key] = num
			start := extractDate[CONFIG.ARGS_EXTRACT_START_DATE]
			end := extractDate[CONFIG.ARGS_EXTRACT_END_DATE]
			if len(extractDate) == 2 {
				if start > end {
					return fmt.Errorf("【Valid Error】[Args:%s][startDate:%d][startEnd:%d][msg:%s]", key, start, end, "開始日と終了日の関係性が異常です。")
				}
			}
		}
	}

	//個別バリデーション
	/////【【【コンテント追加時に必須追加】】】/////
	switch 0 {
	case
		strings.Index(content, CONFIG.CONTENT_NAME_FLIGHT_RECORD):
	case
		strings.Index(content, CONFIG.CONTENT_NAME_GYROSCOPE):
	case
		strings.Index(content, CONFIG.CONTENT_NAME_CHAINED_TAGS_LOGGING):
	case
		strings.Index(content, CONFIG.CONTENT_NAME_MAU_COUNTER):
	case
		strings.Index(content, CONFIG.CONTENT_NAME_TUGCAR_API_REQUEST_LOGGING):
	default:
		return fmt.Errorf("【Error】[func:%s][Args:%s][msg:%s]", "/src/jobs/argsData.go/ValidationReqArgs()", content, "コマンドライン引数のキーが一致しません。")
	}
	return nil
}
