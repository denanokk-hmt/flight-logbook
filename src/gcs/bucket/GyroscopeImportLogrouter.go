/*
	=================================

GCS
bucket::flight_record
バケットの構造をここで指定する
バケットファイルをパースする
* =================================
*/
package bucket

import (
	"encoding/json"
	"strings"
	"time"

	TABLE "bwing.app/src/bigquery/table"
)

///////////////////////////////////////////////////
/* ===========================================
Table::
* =========================================== */

var (
	GCS_BUCKET_NAME_GYROSCOPE_LOGROUTER = "gyroscope"
	GCS_MIDDLE_PATH_GYROSCOPE_LOGROUTER = "run.googleapis.com/" //default path 変更したい場合、コマンドライン引数に指定
)

// ログ格納
type LogGyroscopeLogRouter struct {
	Log  string
	Path string
}

// データロード結果
type LoadGyroscopeLogRouterImportResults struct {
	Result  int
	Client  string
	Cdt     time.Time
	LogNo   int
	LogPath string
	LogDate string
	TTL     int
}

// データロード情報
type LoadGyroscopeLogRouterImport struct {
	TimeStamp time.Time
}

// /////////////////////////////////////////
// GCSからバケットを取得し、JSON文字列をデコードするための箱
type JsonBucketGyLr struct {
	InsertId    string          `json:"insertId"`
	JsonPayload JsonPayloadGyLr `json:"jsonPayload"`
	Timestamp   string          `json:"timestamp"`
}
type JsonPayloadGyLr struct {
	TextPaylaod string `json:"textPayload"`
	Timestamp   string `json:"timestamp"`
}
type JsonTextPayloadGyLr struct {
	Params ParamsGyLr `json:"Params"`
}
type ParamsGyLr struct {
	Host         string
	Url          string
	Path         string
	Ip           string
	UserAgent    string
	Referrer     string
	ClientId     string
	CustomerUuid string
	CustomerId   string
	WhatyaId     string
	CategoryId   string
	Type         string
	Id           string
	Label        string
	Value        string
	WyEvent      string
}

///////////////////////////////////////////////////
/* ===========================================
// Gyroscopeのログをパース
=========================================== */
func (l *LogGyroscopeLogRouter) GyroscopeLogRouterLogParser() ([]TABLE.GyroscopeImport, interface{}, error) {

	var err error
	var gss []TABLE.GyroscopeImport
	var cdt = time.Now()

	//1時間毎のログから、個別のログを分割(抽出)
	lds := SplitGcsLoggings2Array(l.Log)

	//パースエラーのinsertIdを格納する箱
	var parseErrs []string

	//ログから必要な文字列を抜き出す→JSON文字列をオブジェクトへUnmarshal
	for _, ld := range lds {

		//jsonPayloadに紐づく文字列をUnmarshal
		var jb JsonBucketGyLr
		err = json.Unmarshal([]byte(ld), &jb)
		if err != nil {
			if jb.InsertId == "" {
				jb.InsertId = GcsLoggingExtractParseErrorNoInsertId(ld[0:50])
			}
			parseErrs = append(parseErrs, jb.InsertId)
			continue
		}

		//jsonPayload.textPayloadの中のParamsを切り出す
		params := strings.Split(jb.JsonPayload.TextPaylaod, "Params:")[1]
		pArr := strings.Split(params, ",")

		//切り出したParamsをKey&Valueにマッピング
		paramsMapper := make(map[string]string)
		for _, p := range pArr {
			kv := strings.Split(p, "=")
			if len(kv) == 2 {
				paramsMapper[kv[0]] = kv[1]
			}
		}

		//Timesampを型変換
		ts, _ := time.Parse("2006-01-02T15:04:05Z07:00", jb.Timestamp)

		//Bq向けのレコードを作成
		var gs TABLE.GyroscopeImport = TABLE.GyroscopeImport{
			Cdt:          cdt,
			Host:         paramsMapper["host"],
			Url:          paramsMapper["url"],
			Path:         paramsMapper["path"],
			Ip:           paramsMapper["ip"],
			UserAgent:    paramsMapper["user-agent"],
			Referrer:     paramsMapper["referrer"],
			ClientId:     paramsMapper["client_id"],
			CustomerUuid: paramsMapper["customer_uuid"],
			CustomerId:   paramsMapper["customer_id"],
			WhatyaId:     paramsMapper["hmt_id"],
			CategoryId:   paramsMapper["category_id"],
			Type:         paramsMapper["type"],
			Id:           paramsMapper["id"],
			Label:        paramsMapper["label"],
			Value:        paramsMapper["value"],
			WyEvent:      paramsMapper["wy_event"],
			Timestamp:    ts,
		}

		//返却箱に格納
		gss = append(gss, gs)
	}

	//Loop内で起きたエラーをキャッチ
	if err != nil {
		return nil, nil, err
	}

	return gss, parseErrs, nil
}
