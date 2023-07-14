/*
	=================================

サーバーのConfigを設定する
* =================================
*/
package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	LOGGING "bwing.app/src/log"
)

var (
	//Component version
	SYSTEM_COMPONENT_VERSION = "1.2.0_fl"

	//defaults
	DEFAULT_STRING_VALUE_DAILY       = "daily"
	DEFAULT_STRING_VALUE_MONTHLY     = "monthly"
	DEFAULT_STRING_VALUE_TRUE        = "true"
	DEFAULT_STRING_VALUE_FALSE       = "false"
	DEFAULT_INT_VALUE_CHUNK_SIZE_GCS = 5
	DEFAULT_INT_VALUE_CHUNK_SIZE_BQ  = 10000
	DEFAULT_INT_VALUE_RESIZE_LIMIT   = 100

	//result
	RESULT_SUCCESS = "Success"
	RESULT_FAILURE = "Failure"

	//Env Key
	ENV_PORT            = "Port"
	ENV_GCP_PROJECT_ID  = "GcpProjectId"
	ENV_SERVER_CODE     = "ServerCode"
	ENV_APPLI_NAME      = "AppliName"
	ENV_ENV             = "Env"
	ENV_SERVICE_OR_JOBS = "ServiceOrJobs"

	//これより以下は、Jobsのコマンドライン引数に利用
	//コマンドラインの引数指定方法は、key:value 形式で行う
	ARGS_FORCE_INDEX         = "force_index"          //強制的に実行する位置を指定
	ARGS_FORCE_IGNORE_CLIENT = "force_ignore_clients" //強制的にログの抽出から除外する顧客ID
	ARGS_FREQUENCY           = "frequency"            //daily or monthly
	ARGS_CONTENTS            = "contents"             //実行したいcontent名::複数を指定場合、contents:content1,content2,,,のようにvalueをカンマつなぎで指定する
	ARGS_CONTENT             = "content"              //実行されたIndexから指定されたContent名が自動的に設定される
	ARGS_BUCKET_NAME         = "bucket_name"          //抽出したいログのGCSバケット名を指定
	ARGS_BUCKET_MIDDLE_PATH  = "bucket_middle_path"   //抽出したいログのGCSバケットのPATHを指定
	ARGS_BUCKET_PREFIX       = "bucket_prefix"        //バケット名にプレフィックスをつけたい場合に指定
	ARGS_BUCKET_SUFFIX       = "bucket_suffix"        //バケット名にサフィックスをつけたい場合に指定
	ARGS_BUCKET_PATHS        = "bucket_paths"         //抽出するバケットPathをcmd/bucket_paths/[content]_bucket_paths.jsonファイルから取得する
	ARGS_BUCKET_TIME_UTC     = "bucket_time_utc"      //バケットを取得する際にUST基準で取得した場合にtrue
	ARGS_BQ_DATESET_NAME     = "bq_dataset_name"      //ログの格納先になるBigQueryのデータセット名を指定
	ARGS_BQ_TABLE_NAME       = "bq_table_name"        //ログの格納先になるBigQueryのテーブル名を指定
	ARGS_BQ_NO_INSERT        = "bq_no_insert"         //ログの格納先になるBigQueryのテーブル名を指定
	ARGS_EXTRACT_START_DATE  = "extract_start_date"   //抽出したいログの開始日を指定　※月ベースの場合、初日を指定
	ARGS_EXTRACT_END_DATE    = "extract_end_date"     //抽出したいログの終了日を指定　※月ベースの場合、末日を指定
	ARGS_EXTRACT_PERIOD      = "extract_period"       //抽出したいログの開始日と終了日の期間が自動設定
	ARGS_LOG_ROUTER          = "log_router"           //GCPロギングのログルーターを使ってGCSへアップされたファイルを利用する場合、true

	/////【【【コンテント追加時に必須追加】】】/////
	CONTENT_NAME_FLIGHT_RECORD = "flight_record"
	CONTENT_NAME_GYROSCOPE     = "gyroscope"
	CONTENT_NAME_GYROSCOPET00_ = "gyroscopeT00_" //値として処理で利用しないが、指定方法サンプルとしてここに残す
	CONTENT_NAME_GYROSCOPET15_ = "gyroscopeT15_" //値として処理で利用しないが、指定方法サンプルとしてここに残す
	CONTENT_NAME_GYROSCOPET30_ = "gyroscopeT30_" //値として処理で利用しないが、指定方法サンプルとしてここに残す
	CONTENT_NAME_GYROSCOPET45_ = "gyroscopeT45_" //値として処理で利用しないが、指定方法サンプルとしてここに残す

	CONTENT_NAME_CHAINED_TAGS_LOGGING       = "chained_tags_logging"
	CONTENT_NAME_MAU_COUNTER                = "mau_counter"
	CONTENT_NAME_TUGCAR_API_REQUEST_LOGGING = "tugcar_api_request_logging"
)

var configMapEnv map[string]string  //環境変数の箱
var configMapArgs map[string]string //CMDライン引数の箱
var uuv4Tokens []string             //サーバー認証のためのTokenの箱

// /////////////////////////////////////////////////
// 起動時にGCP ProjectID、NS, Kindを登録する
func init() {

	//Set environ values
	NewConfigEnv()

	//Set CMD Args values
	NewConfigArgs()

	//output message finish config settings.
	initString := fmt.Sprintf("[Project:%s][ServerCode:%s][Appli:%s][Env:%s]",
		configMapEnv[ENV_GCP_PROJECT_ID],
		configMapEnv[ENV_SERVER_CODE],
		configMapEnv[ENV_APPLI_NAME],
		configMapEnv[ENV_ENV])
	fmt.Println(LOGGING.SetLogEntry(LOGGING.INFO, "Flight-LogBook INIT", initString))

	//Verion print
	initString = fmt.Sprintf("Flight-LogBook component version :%s", SYSTEM_COMPONENT_VERSION)
	fmt.Println(LOGGING.SetLogEntry(LOGGING.INFO, "Flight-LogBook STARTING", initString))

}

///////////////////////////////////////////////////
/* =================================
	環境変数の格納
		$PORT
		$GCP_PROJECT_ID
		$SERVER_CODE
		$APPLI_NAME
		$ENV
		$SERVICE_OR_JOBS
* ================================= */
func NewConfigEnv() {

	//環境変数をMapping
	configMapEnv = make(map[string]string)
	configMapEnv[ENV_PORT] = os.Getenv("PORT")
	configMapEnv[ENV_GCP_PROJECT_ID] = os.Getenv("GCP_PROJECT_ID")
	configMapEnv[ENV_SERVER_CODE] = os.Getenv("SERVER_CODE")
	configMapEnv[ENV_APPLI_NAME] = os.Getenv("APPLI_NAME")
	configMapEnv[ENV_ENV] = os.Getenv("ENV")
}

///////////////////////////////////////////////////
/* =================================
	環境変数の返却
* ================================= */
func GetConfigEnv(name string) string {
	return configMapEnv[name]
}
func GetConfigEnvAll() map[string]string {
	return configMapEnv
}

///////////////////////////////////////////////////
/* =================================
	コマンドライン引数の格納
		force_index:強制的実行
			"0"or "1" or ...
		frequency:jobの周期
			"daily" or "monthly" などのFrequencyを設定する
		contents:jobの種類("content1,content2,,,")
			contentをカンマつなぎで指定
			これを配列に分割し、JOBS実行には、CLOUD_RUN_TASK_INDEXと一致させて並列実行を実現行わせる
		その他の項目について
			強制的に、0の要素に対する引数と利用させたい場合にのみ指定する
			用途：検証、バッチ処理(Daily, Monthly)以外のマイグレーションなど
				(*=指定がなければDefaultが指定される)
				bucket_middle_path:*
				bucket_name:*
				bucket_middle_path:*
				bucket_prefix:*
				bucket_suffix:*
				bucket_paths:*
				bucket_time_utc:*
				bq_dataset_name:*
				bq_table_name:*
				extract_start_date:*
				extract_end_date:*
				extract_period:自動算出し設定される
* ================================= */
func NewConfigArgs() {

	//起動時の引数から取得
	flag.Parse()
	args := flag.Args()

	//コマンドライン引数をMapping
	err := SetConfigMapArgs(args)
	if err != nil {
		log.Fatal(err)
	}
}

///////////////////////////////////////////////////
/* =================================
コマンドライン引数を"key:value"に分割しMapping、格納する
* ================================= */
func SetConfigMapArgs(args []string) error {

	if len(configMapArgs) == 0 {
		configMapArgs = make(map[string]string)
	}

	for _, a := range args {
		if a == "" {
			continue
		}

		//コロンでスプリットをしkey-valueに変換
		key := strings.Split(a, ":")[0]
		value := strings.Split(a, ":")[1]

		//Mapping
		switch key {
		case ARGS_FORCE_INDEX:
			configMapArgs[ARGS_FORCE_INDEX] = value
		case ARGS_FORCE_IGNORE_CLIENT:
			configMapArgs[ARGS_FORCE_IGNORE_CLIENT] = strings.ReplaceAll(value, " ", "")
		case ARGS_FREQUENCY:
			configMapArgs[ARGS_FREQUENCY] = value
		case ARGS_CONTENTS:
			configMapArgs[ARGS_CONTENTS] = strings.ReplaceAll(value, " ", "")
		case ARGS_CONTENT:
			configMapArgs[ARGS_CONTENT] = value
		case ARGS_BUCKET_NAME:
			configMapArgs[ARGS_BUCKET_NAME] = value
		case ARGS_BUCKET_MIDDLE_PATH:
			configMapArgs[ARGS_BUCKET_MIDDLE_PATH] = value
		case ARGS_BUCKET_PREFIX:
			if value != "" {
				value = value + "_"
			}
			configMapArgs[ARGS_BUCKET_PREFIX] = value
		case ARGS_BUCKET_SUFFIX:
			if value != "" {
				value = "_" + value
			}
			configMapArgs[ARGS_BUCKET_SUFFIX] = value
		case ARGS_BUCKET_PATHS:
			configMapArgs[ARGS_BUCKET_PATHS] = value
		case ARGS_BUCKET_TIME_UTC:
			configMapArgs[ARGS_BUCKET_TIME_UTC] = value
		case ARGS_BQ_DATESET_NAME:
			configMapArgs[ARGS_BQ_DATESET_NAME] = value
		case ARGS_BQ_TABLE_NAME:
			configMapArgs[ARGS_BQ_TABLE_NAME] = value
		case ARGS_BQ_NO_INSERT:
			configMapArgs[ARGS_BQ_NO_INSERT] = value
		case ARGS_EXTRACT_START_DATE:
			configMapArgs[ARGS_EXTRACT_START_DATE] = value
		case ARGS_EXTRACT_END_DATE:
			configMapArgs[ARGS_EXTRACT_END_DATE] = value
		case ARGS_LOG_ROUTER:
			configMapArgs[ARGS_LOG_ROUTER] = value
		default:
			return fmt.Errorf("【Error】[func:%s][Args:%s]", "src/config/config.go/setConfigMapArgs()", "コマンドライン引数のキーが一致しません。")
		}
	}
	return nil
}

///////////////////////////////////////////////////
/* =================================
	コマンドライン引数の返却
* ================================= */
func GetConfigArgs(name string) string {
	return configMapArgs[name]
}
func GetConfigArgsAll() map[string]string {
	return configMapArgs
}
func GetConfigArgsAllString() string {
	var s string
	for k, v := range configMapArgs {
		s += k + ":" + v + ", "
	}
	return s
}
func GetConfigArgsAllKeyValue() (key []string, val []string) {
	var retK []string
	var retV []string
	for k, v := range configMapArgs {
		retK = append(retK, k)
		retV = append(retV, v)
	}
	return retK, retV
}

///////////////////////////////////////////////////
/* =================================
	サーバー間認証に用いるUUV4トークンをJSONファイルから取得しておく
* ================================= */
func NewUuv4Tokens() {

	//箱を準備
	type Uuv4TokenJson struct {
		Uuv4tokens []string `json:"uuv4tokens"`
	}

	//Rootディレクトリを取得して、tokensのJSONファイルの絶対パスを指定
	var (
		_, b, _, _ = runtime.Caller(0)
		root       = filepath.Join(filepath.Dir(b), "../../")
	)
	path := root + "/cmd/authorization/uuv4tokens.json"

	// JSONファイル読み込み
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	// JSONデコード
	var tokens Uuv4TokenJson
	if err := json.Unmarshal(bytes, &tokens); err != nil {
		log.Fatal(err)
	}
	// デコードしたデータを表示
	for _, t := range tokens.Uuv4tokens {
		uuv4Tokens = append(uuv4Tokens, t)
	}
}
func GetUuv4Tokens() []string {
	return uuv4Tokens
}

type Counter struct {
	PCnt     int
	PErrsCnt int
	PErrsMsg []string
}

type ChBqInsert struct {
	MigrationId string
	InsertedRec int
	Err         error
}
