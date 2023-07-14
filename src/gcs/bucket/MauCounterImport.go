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
	GCS_BUCKET_NAME_MAU_COUNTER = "log_mau_counter"
	GCS_MIDDLE_PATH_MAU_COUNTER = "run.googleapis.com/stdout"
)

// ログ格納
type LogMauCounter struct {
	Log  string
	Path string
}

// データロード結果
type LoadMauCounterImportResults struct {
	Result  int
	Client  string
	Cdt     time.Time
	LogNo   int
	LogPath string
	LogDate string
	TTL     int
}

// データロード情報
type LoadMauCounterImport struct {
	TimeStamp time.Time
}

// /////////////////////////////////////////
// GCSからバケットを取得し、JSON文字列をデコードするための箱
type JsonBucketMau struct {
	InsertId    string `json:"insertId"`
	TextPaylaod string `json:"textPayload"`
	Timestamp   string `json:"timestamp"` //ログが生成された時刻(UTC)
}
type JsonMauCounts struct {
	Client string  `json:"client"`
	YyyyMm string  `json:"yyyymm"`
	Mau    JsonMau `json:"mau"`
	//Cdt string `json:"cdt"`
}
type JsonMau struct {
	//Date  string `json:"date"`
	Count int64 `json:"count"`
}

///////////////////////////////////////////////////
/* ===========================================
// MauCounterのログをパース
=========================================== */
func (l *LogMauCounter) MauCounterLogParser() ([]TABLE.MauCounterImport, interface{}, error) {

	var err error
	var mcs []TABLE.MauCounterImport
	var cdt = time.Now() //実行時刻

	//1時間毎のログから、個別のログを分割(抽出)
	lds := SplitGcsLoggings2Array(l.Log)

	//パースエラーのinsertIdを格納する箱
	var parseErrs []string

	//ログから必要な文字列を抜き出す→JSON文字列をオブジェクトへUnmarshal
	for _, ld := range lds {

		//jsonPayloadに紐づく文字列をUnmarshal
		var jb JsonBucketMau
		err = json.Unmarshal([]byte(ld), &jb)
		if err != nil {
			if jb.InsertId == "" {
				jb.InsertId = GcsLoggingExtractParseErrorNoInsertId(ld[0:50])
			}
			parseErrs = append(parseErrs, jb.InsertId)
			continue
		}

		//textPayloadの中のMAU_COUNTSを切り出す
		tp := strings.Split(jb.TextPaylaod, "MAU_COUNTS ")[1]

		//jsonPayloadに紐づく文字列をUnmarshal
		var jmc JsonMauCounts
		err = json.Unmarshal([]byte(tp), &jmc)
		if err != nil {
			parseErrs = append(parseErrs, jb.InsertId)
			continue
		}

		//Timesampを型変換
		ts, _ := time.Parse("2006-01-02T15:04:05Z07:00", jb.Timestamp)

		//Bq向けのレコードを作成
		var mc TABLE.MauCounterImport = TABLE.MauCounterImport{
			Client:    jmc.Client,
			Count:     jmc.Mau.Count,
			Cdt:       cdt,
			Month:     jmc.YyyyMm,
			Type:      "mau",
			Timestamp: ts,
		}

		//返却箱に格納
		mcs = append(mcs, mc)
	}

	//Loop内で起きたエラーをキャッチ
	if err != nil {
		return nil, nil, err
	}

	return mcs, parseErrs, nil
}
