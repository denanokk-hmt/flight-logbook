/*
	=================================

GCS
bucket::chained_tags_logging
バケットの構造をここで指定する
バケットファイルをパースする
* =================================
*/
package bucket

import (
	"encoding/json"
	"strings"
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
Table::
* =========================================== */

var (
	GCS_BUCKET_NAME_CHAINED_TAGS_LOGGING = "chained_tags_logging"
	GCS_MIDDLE_PATH_CHAINED_TAGS_LOGGING = "run.googleapis.com/stdout"
)

// ログ格納
type LogChainedTagsOld struct {
	Log string
}

// ログ格納
type LogChainedTags struct {
	Log  string
	Path string
}

// データロード情報
type LoadChainedTagsLoggingImport struct {
	ATID string
	DDID int
}

// /////////////////////////////////////////
// GCSからバケットを取得し、JSONデコードするための共通箱
type JsonBucketCTL struct {
	InsertId    string      `json:"insertId"`
	JsonPayload TextPayload `json:"jsonPayload"`
	Timestamp   string      `json:"timestamp"`
}
type TextPayload struct {
	InsertId    string `json:"insertId"`
	TextPayload string `json:"TextPayload"`
}
type JsonChainedTags struct {
	ClientId        string `json:"ClientId"`
	CustomerUuid    string `json:"CustomerUuid"`
	WhatYaId        string `json:"WhatYaId"`
	ATID            string `json:"ATID"`
	DDID            int    `json:"DDID"`
	RelatedUnixtime int    `json:"RelatedUnixtime"`
	PublishedAt     string `json:"PublishedAt"`
}
type JsonRelatedWordsLogsValue [][]string
type JsonRelatedWordsLogs struct {
	RelatedWordsLogsValues JsonRelatedWordsLogsValue `json:"RelatedWordsLog"`
}
type JsonSelectedItem struct {
	SelectedItemId   string `json:"SelectedItemId"`
	SelectedItemName string `json:"SelectedItemName"`
}

// GCSバケットから書き出す先のテーブル定義
type ChainedTagsLoggingImport struct {
	Pdt              time.Time
	Cdt              time.Time
	ClientId         string
	CustomerUuid     string
	WhatYaId         string
	ATID             string
	DDID             int
	RelatedUnixtime  int
	PublishedAt      string
	RelatedWordsLog  [][]string //slice of slice
	SelectedItemId   string
	SelectedItemName string
}

///////////////////////////////////////////////////
/* ===========================================
// Chained Tags loggingログをパース
=========================================== */
func (l *LogChainedTags) ChainedTagsLogParser() ([]ChainedTagsLoggingImport, interface{}, error) {

	var err error
	var cs []ChainedTagsLoggingImport
	var cdt = time.Now()

	//1時間毎のログから、個別のログを分割(抽出)
	lds := SplitGcsLoggings2Array(l.Log)

	//パースエラーのinsertIdを格納する箱
	var parseErrs []string

	//ログから必要な文字列を抜き出す→JSON文字列をオブジェクトへUnmarshal
	for _, ld := range lds {

		//jsonPayloadに紐づく文字列をUnmarshal
		var jb JsonBucketCTL
		err = json.Unmarshal([]byte(ld), &jb)
		if err != nil {
			if jb.InsertId == "" {
				jb.InsertId = GcsLoggingExtractParseErrorNoInsertId(ld[0:50])
			}
			parseErrs = append(parseErrs, jb.InsertId)
			continue
		}

		//textPayloadに紐づく文字列をUnmarshal
		var tp TextPayload = jb.JsonPayload

		//ChainedTagsログをUnmarsal(RelatedWordsLog以外→配列なのでいっしょに出来ない)
		var ct JsonChainedTags
		err = json.Unmarshal([]byte(tp.TextPayload), &ct)
		if err != nil {
			break
		}

		//RelatedWordsLogをUnmarsal
		var rwls JsonRelatedWordsLogs
		err = json.Unmarshal([]byte(tp.TextPayload), &rwls)
		if err != nil {
			break
		}

		//SelectedItemをUnmarsal
		var si JsonSelectedItem
		err = json.Unmarshal([]byte(tp.TextPayload), &si)
		if err != nil {
			break
		}

		//Loggingされた時間(PublishedAt→Pdt)をTimesampに変換
		pa := strings.Split(ct.PublishedAt, ".")[0]
		pa = strings.Replace(pa, "-", "/", -1)
		pbt, _ := time.Parse("2006/01/02 15:04:05", pa)

		//Bq向けのレコードを作成
		var c ChainedTagsLoggingImport
		c.Pdt = pbt
		c.Cdt = cdt
		c.ClientId = ct.ClientId
		c.CustomerUuid = ct.CustomerUuid
		c.WhatYaId = ct.WhatYaId
		c.ATID = ct.ATID
		c.DDID = ct.DDID
		c.RelatedUnixtime = ct.RelatedUnixtime
		c.PublishedAt = ct.PublishedAt
		c.RelatedWordsLog = append(c.RelatedWordsLog, rwls.RelatedWordsLogsValues...)
		c.SelectedItemId = si.SelectedItemId
		c.SelectedItemName = si.SelectedItemName

		//返却箱に格納
		cs = append(cs, c)
	}

	//Loop内で起きたエラーをキャッチ
	if err != nil {
		return cs, nil, err
	}

	return cs, parseErrs, nil
}
