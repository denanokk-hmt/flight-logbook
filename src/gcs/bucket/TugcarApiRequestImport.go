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
	GCS_BUCKET_NAME_TUGCAR_API_REQUEST_LOGGING = "tugcar_api_request_logging"
	GCS_MIDDLE_PATH_TUGCAR_API_REQUEST_LOGGING = "run.googleapis.com/stdout"
)

// ログ格納
type LogTugcarApiRequest struct {
	Log  string
	Path string
}

// データロード結果
type LoadTugcarApiRequestImportResults struct {
	Result  int
	Client  string
	Cdt     time.Time
	LogNo   int
	LogPath string
	LogDate string
	TTL     int
}

// データロード情報
type LoadTugcarApiRequestImport struct {
	TimeStamp time.Time
}

// /////////////////////////////////////////
// GCSからバケットを取得し、JSON文字列をデコードするための箱
type JsonBucketTugcarApiRequest struct {
	InsertId    string         `json:"insertId"`
	JsonPayload JsonPayloadTAR `json:"jsonPayload"`
	Timestamp   string         `json:"timestamp"`
}
type JsonPayloadTAR struct {
	TextPaylaod string `json:"textPayload"`
	Timestamp   string `json:"timestamp"`
}
type JsonTextPayloadTAR struct {
	Params ParamsTAR `json:"Params"`
}
type ParamsTAR struct {
	Type          string
	ClientId      string
	CurrentParams string
	CurrentUrl    string
	Query_Tags    string
	Query_ItemId  string
	UserAgent     string
	Referrer      string
	CustomerUuid  string
	WhatyaId      string
	SearchFrom    string
}

///////////////////////////////////////////////////
/* ===========================================
// TugcarApiRequestのログをパース
=========================================== */
func (l *LogTugcarApiRequest) TugcarApiRequestLogParser() ([]TABLE.TugcarApiRequestImport, interface{}, error) {

	var err error
	var tars []TABLE.TugcarApiRequestImport
	var cdt = time.Now() //実行時刻

	//1時間毎のログから、個別のログを分割(抽出)
	lds := SplitGcsLoggings2Array(l.Log)

	//パースエラーのinsertIdを格納する箱
	var parseErrs []string

	//ログから必要な文字列を抜き出す→JSON文字列をオブジェクトへUnmarshal
	for _, ld := range lds {

		//jsonPayloadに紐づく文字列をUnmarshal
		var jb JsonBucketTugcarApiRequest
		err = json.Unmarshal([]byte(ld), &jb)
		if err != nil {
			if jb.InsertId == "" {
				jb.InsertId = GcsLoggingExtractParseErrorNoInsertId(ld[0:50])
			}
			parseErrs = append(parseErrs, jb.InsertId)
			continue
		}

		//jsonPayload.textPayloadの中の" Params:"前後で切り出し
		tp := strings.Split(jb.JsonPayload.TextPaylaod, " Params:")

		//Urlpathを取り出す
		var urlPath string
		rArr := strings.Split(tp[0], " ")
		for _, r := range rArr {
			kv := strings.Split(r, ":")
			if len(kv) == 2 {
				if kv[0] == "Urlpath" {
					urlPath = kv[1]
					break
				}
			}
		}
		if urlPath == "" {
			continue
		}

		//jsonPayload.textPayloadの中のParams後ろの切り出し-->Paramsを格納へ
		pArr := strings.Split(tp[1], ",")

		//切り出したParamsをKey&Valueにマッピング
		paramsMapper := make(map[string]string)
		for _, p := range pArr {
			kv := strings.Split(p, ":")
			if len(kv) == 2 {
				paramsMapper[kv[0]] = kv[1]
			} else if len(kv) == 3 {
				paramsMapper[kv[0]] = kv[1] + ":" + kv[2]
			}
		}

		//Timesampを型変換
		ts, _ := time.Parse("2006-01-02T15:04:05Z07:00", jb.Timestamp)

		//Bq向けのレコードを作成
		var tar TABLE.TugcarApiRequestImport = TABLE.TugcarApiRequestImport{
			Cdt:           cdt,
			UrlPath:       urlPath,
			Type:          paramsMapper["Type"],
			ClientId:      paramsMapper["ClientId"],
			CurrentParams: paramsMapper["CurrentParams"],
			CurrentUrl:    paramsMapper["CurrentUrl"],
			QueryTags:     paramsMapper["Query_Tags"],
			QueryItemId:   paramsMapper["Query_ItemId"],
			CustomerUuid:  paramsMapper["CustomerUuid"],
			WhatYaId:      paramsMapper["WhatYaId"],
			SearchFrom:    paramsMapper["SearchFrom"],
			Timestamp:     ts,
		}

		//返却箱に格納
		tars = append(tars, tar)
	}

	//Loop内で起きたエラーをキャッチ
	if err != nil {
		return nil, nil, err
	}

	return tars, parseErrs, nil
}
