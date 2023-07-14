/*
	=================================

BigQueryのTableの共通構造
その他、Tableを扱う共通項目や処理
* =================================
*/
package table

import (
	//共通処理
	CONFIG "bwing.app/src/config"
)

var (
	MSG_BQ_NO_INSERT = "【【【 bq_no_insert of args is true 】】】"
)

// Datastore Load result
type LoadDataResults struct {
	Results interface{}
}

/*
	=================================

//Get GCP ProjectID
* =================================
*/
func GetProjectId() string {
	return CONFIG.GetConfigEnv(CONFIG.ENV_GCP_PROJECT_ID)
}

/*
	=================================

//結果箱の空の中身
* =================================
*/
func NewLoadDataResults(p interface{}) LoadDataResults {
	var r LoadDataResults
	r.Results = p
	return r
}
