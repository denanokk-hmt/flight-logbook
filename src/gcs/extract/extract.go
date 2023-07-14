/*
	=================================

GCSのバケット抽出共通処理
* =================================
*/
package extract

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	COMMON "bwing.app/src/common"
	CONFIG "bwing.app/src/config"
	BUCKET "bwing.app/src/gcs/bucket"
	LOGGING "bwing.app/src/log"
)

// バケット抽出結果
type ExtractBucketResults struct {
	Result          string
	Cdt             time.Time
	LogNo           int
	LogPath         string
	LogDate         string
	ParseCount      int
	ParseErrorCount int
}

// バケット抽出エラー結果
type ExtractBucketParseErrors struct {
	Bucket    string
	Path      string
	InsertIds string //カンマつなぎ
}

// Inerface
type ExtractBucket struct {
	frequency        string
	bucket           string
	bucketMiddlePath string
	bucketTimeUTC    bool
	extractStartDate string
	extractEndDate   string
}

///////////////////////////////////////////////////
/* ===========================================
GCS Bucketの抽出時にパースエラーを起こしたファイルのinsertIdをWARNロギング
=========================================== */
func (eb *ExtractBucket) ExtractParseErrorLogging(bucket, path string, insertIds []string) {

	//ログをJSONエンコード
	var parseErrs = ExtractBucketParseErrors{
		Bucket:    bucket,
		Path:      path,
		InsertIds: strings.Join(insertIds, ","),
	}
	//JSON文字列化
	pes, err := json.Marshal(parseErrs)
	if err != nil {
		panic(err)
	}
	//WARNロギング
	fmt.Println(LOGGING.SetLogEntry(LOGGING.WARN, "GcsParseErrors", fmt.Sprintf("%s\n", string(pes))))
}

///////////////////////////////////////////////////
/* ===========================================
GCS Bucketからファイルを取得
=========================================== */
func (eb *ExtractBucket) FetchBucket() (interface{}, error) {

	var err error

	//Argsを取得
	sArr := eb.setBucketArgsAndGetFldPaths()

	/*------------------------------------------------
	Bucketからログを取得
	------------------------------------------------*/

	//GCS clientを生成
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var errs []error
	var logss BUCKET.LogssStruct
	for i, s := range sArr {

		//読み取りたいログのパスを取得
		prefix := eb.bucketMiddlePath + "/" + s
		paths, ctx, err := objectPathGetter(client, eb.bucket, prefix)
		if err != nil {
			errs = append(errs, ctx.Err())
			fmt.Println(errs)
			continue
			//return nil, err
		}

		//JST基準で詰め直し(bucket_time_utcがfalse、且つfrequencyがdailyの場合)
		if !eb.bucketTimeUTC && eb.frequency == CONFIG.DEFAULT_STRING_VALUE_DAILY {
			paths = pickUpBucketObjByJST(paths, eb.extractStartDate, eb.extractEndDate)
		}

		//Bucketのログデータ取得
		fmt.Println("Getting buckets now:", eb.bucket, s, len(paths))
		logs, ctx, err := objectReaderPaths(client, eb.bucket, paths)
		if err != nil {
			fmt.Println(ctx.Err())
			return nil, err
		}
		if len(logs) != 0 {
			logss.Len += len(logs)
			logss.Objs = append(logss.Objs, logs)
			logss.Paths = append(logss.Paths, paths)
			logss.LogDate = append(logss.LogDate, sArr[i])
		}
	}

	//バケットデータが存在しなかった場合、準正常として離脱させる=エラー判定しない
	/*
		if logss.Len == 0 {
			err = errors.New(BUCKET.ERR_MSG_BUCKE_DATA_DID_NOT_EXIST)
			fmt.Println(errs)
			return nil, err
		}*/

	return logss, err
}

///////////////////////////////////////////////////
/* ===========================================
GCS Bucketからファイルパスを取得
=========================================== */
func (eb *ExtractBucket) GetGyroscopeBucktPaths(client *storage.Client, term string) ([]string, error) {

	/*------------------------------------------------
	GCSからバケットファイルパスを取得
	------------------------------------------------*/

	//オブジェクトパスの箱
	var pathss []string
	var errs []error

	//TermとSuffixを分離
	var suffixRange []string
	if term != "" {
		term_suffix := strings.Split(term, "_")
		term = term_suffix[0] //term部分
		if len(term_suffix) > 1 {
			//termの後ろにサフィックスがある場合(format:[term:_0-6)
			suffix := strings.Split(term_suffix[1], "-")
			suffixRange = append(suffixRange, suffix...)
		}
	}

	//Argsを設定、フォルダパス(yyyy/mm/dd)を配列で取得
	sArr := eb.setBucketArgsAndGetFldPaths()

	//指定期間(日付=フォルダパス)のすべてのオブジェクトファイルパスを取得
	for _, s := range sArr {

		//読み取りたいログのパスを取得
		prefix := eb.bucketMiddlePath + "/" + s
		paths, ctx, err := objectPathGetter(client, eb.bucket, prefix)
		if err != nil {
			errs = append(errs, ctx.Err())
			fmt.Println(errs)
			continue
		}

		//JST基準で詰め直し(bucket_time_utcがfalse、且つfrequencyがdailyの場合)
		if !eb.bucketTimeUTC && eb.frequency == CONFIG.DEFAULT_STRING_VALUE_DAILY {
			paths = pickUpBucketObjByJST(paths, eb.extractStartDate, eb.extractEndDate)
		}

		//引数termが指定された場合、termに一致するファイルのみに絞る(term毎に処理を回して分散させる為)
		if term != "" {
			paths = pickUpBucketObjByTerm(paths, term)
		}

		//Suffix指定がある場合、suffixに一致するファイルのみに絞る(term毎に処理を回して分散させる為)
		if len(suffixRange) != 0 {
			paths = pickUpBucketObjBySuffix(paths, suffixRange)
		}

		//オブジェクトパスを箱詰め
		pathss = append(pathss, paths...)
	}

	return pathss, nil
}

///////////////////////////////////////////////////
/* =================================
	バケットパスを指定
* ================================= */
func (eb *ExtractBucket) GetBucketPathsFromBucketPathsJson(fileNm string) ([]string, error) {

	//Argsを設定、フォルダパス(yyyy/mm/dd)を配列で取得
	eb.setBucketArgsAndGetFldPaths()

	//箱を準備
	type bucketPahtsJson struct {
		Bucket_paths []string `json:"bucket_paths"`
	}

	//Rootディレクトリを取得して、tokensのJSONファイルの絶対パスを指定
	var (
		_, b, _, _ = runtime.Caller(0)
		root       = filepath.Join(filepath.Dir(b), "../../../")
	)
	path := root + "/cmd/bucket_paths/" + fileNm

	// JSONファイル読み込み
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// JSONデコード
	var paths bucketPahtsJson
	if err := json.Unmarshal(bytes, &paths); err != nil {
		return nil, err
	}
	// デコードしたデータを表示
	var pathss []string
	pathss = append(pathss, paths.Bucket_paths...)

	return pathss, err
}

///////////////////////////////////////////////////
/* ===========================================
	GCSバケットを取得するために必要なArgs値を設定し、
	バケットのパス(middle pathから後ろ :例 gs://gyroscope/events/[path] path=2023/03/03)
=========================================== */
func (eb *ExtractBucket) setBucketArgsAndGetFldPaths() []string {

	//Get the parameters needed for for the bucket from config
	configMaps := CONFIG.GetConfigArgsAll()
	eb.bucket = configMaps[CONFIG.ARGS_BUCKET_NAME]

	//Set bucket name
	eb.bucket = CONFIG.GetConfigArgs(CONFIG.ARGS_BUCKET_PREFIX) + CONFIG.GetConfigArgs(CONFIG.ARGS_BUCKET_NAME) + CONFIG.GetConfigArgs(CONFIG.ARGS_BUCKET_SUFFIX) //complete bucket name

	//Set bucket file path
	eb.bucketMiddlePath = configMaps[CONFIG.ARGS_BUCKET_MIDDLE_PATH]

	//Set frequency
	eb.frequency = configMaps[CONFIG.ARGS_FREQUENCY]

	//Set extract date from & to
	eb.extractStartDate = configMaps[CONFIG.ARGS_EXTRACT_START_DATE]
	eb.extractEndDate = configMaps[CONFIG.ARGS_EXTRACT_END_DATE]

	//バケットオブジェクトのパス(yyyy/mm/dd)を配列にして生成
	sArr := createObjPathByExtarctDate(eb.frequency, eb.extractStartDate, eb.extractEndDate)

	return sArr
}

///////////////////////////////////////////////////
/* ===========================================
	Frequencyに応じてExtractStartDate、ExtractEndDateからPathを計算
=========================================== */
func createObjPathByExtarctDate(frequency, extractStartDate, extractEndDate string) []string {

	var sArr []string
	if frequency == CONFIG.DEFAULT_STRING_VALUE_DAILY {
		//Dailyの場合、yyyy/mm/ddの形式

		//取得するログの期間の日数
		dLen := COMMON.DateDiffCalculator(extractStartDate, extractEndDate, "/")

		//日数から取得するすべての日付を配列で取得
		sArr = COMMON.DateAddCalculator(extractStartDate, "/", dLen)
	} else if frequency == CONFIG.DEFAULT_STRING_VALUE_MONTHLY {
		//Monthlyの場合、yyyy/mmの形式

		//取得するログの期間の日数
		mLen := COMMON.MonthDiffCalculator(extractStartDate, extractEndDate, "/")

		//月数から取得するすべての月を配列で取得
		sArr = COMMON.MonthAddCalculator(extractStartDate, "/", mLen)
	}

	return sArr
}

///////////////////////////////////////////////////
/* ===========================================
	GCS Bucketのオブジェクトを取得する
=========================================== */
func objectPathGetter(client *storage.Client, bucket string, prefix string) ([]string, context.Context, error) {

	var err error

	//Contextを
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	//GCS clientを生成
	if client == nil {
		client, err = storage.NewClient(ctx)
		if err != nil {
			return nil, ctx, err
		}
		defer client.Close()
	}

	//Queryを指定
	query := &storage.Query{
		Prefix: prefix,
		//Prefix: "run.googleapis.com/stdout/2022/07",
		//StartOffset: "2022/07", // Only list objects lexicographically >= "bar/"
		//EndOffset:   "07/",     // Only list objects lexicographically < "foo/"
	}

	//指定したフォルダにあるオブジェクトパス一覧の取得
	var paths []string
	it := client.Bucket(bucket).Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, ctx, err
		}
		if attrs.Name == prefix+"/" {
			continue //フォルダ名だけの場合、スキップ
		}
		paths = append(paths, attrs.Name)
	}

	return paths, ctx, nil
}

///////////////////////////////////////////////////
/* ===========================================
	GCS Bucketの単数オブジェクトを読み込む
=========================================== */
func objectReaderPath(client *storage.Client, bucket, path string) (string, context.Context, error) {

	var err error

	//GCS clientを生成
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()
	if client == nil {
		client, err = storage.NewClient(ctx)
		if err != nil {
			return "", ctx, err
		}
		defer client.Close()
	}

	//Path指定されたオブジェクトを取得(取得出来なかったものは、空文字)
	var log string

	//Bucket Readerで読み込み
	reader, err := client.Bucket(bucket).Object(path).NewReader(ctx)
	if err != nil {
		return "", ctx, err
	}
	defer reader.Close()

	//バッファリング
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(reader); err != nil {
		return "", ctx, err
	}

	//文字列に変換して格納（軽さより扱いやすさで）
	log = string(buf.String())

	return log, ctx, nil
}

///////////////////////////////////////////////////
/* ===========================================
	GCS Bucketの複数オブジェクトを読み込む
=========================================== */
func objectReaderPaths(client *storage.Client, bucket string, paths []string) ([]string, context.Context, error) {

	var err error

	//GCS clientを生成
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()
	if client == nil {
		client, err = storage.NewClient(ctx)
		if err != nil {
			return nil, ctx, err
		}
		defer client.Close()
	}

	//Path指定されたオブジェクトを取得(取得出来なかったものは、空文字)
	var logs []string
	for _, p := range paths {
		var log string

		//Bucket Readerで読み込み
		reader, err := client.Bucket(bucket).Object(p).NewReader(ctx)
		if err != nil {
			return nil, ctx, err
		}
		defer reader.Close()

		//バッファリング
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(reader); err != nil {
			return nil, ctx, err
		}

		//文字列に変換して格納（軽さより扱いやすさで）
		log = string(buf.String())
		logs = append(logs, log)
	}

	return logs, ctx, nil
}

///////////////////////////////////////////////////
/* ===========================================
	BucketのオブジェクトをJST時間で選別する
=========================================== */
func pickUpBucketObjByJST(paths []string, startDate, endDate string) []string {

	var pathsNew []string

	//content
	content := CONFIG.GetConfigArgs(CONFIG.ARGS_CONTENT)

	//middle path
	midPath := CONFIG.GetConfigArgs(CONFIG.ARGS_BUCKET_MIDDLE_PATH) + "/"

	//UTCから見たJSTの24時間に置き換える
	startUtcDateTime, _ := strconv.Atoi(strings.ReplaceAll(startDate, "/", "") + "150000") //UTC+9
	endUtcDateTime, _ := strconv.Atoi(strings.ReplaceAll(endDate, "/", "") + "150000")     //UTC+9

	//不要な文字列を削除する正規表現
	regD := regexp.MustCompile(`[-|/|:| |　|T]`)

	//コンテント名を文字列完全一致正規表現
	regM := regexp.MustCompile(content + "$")

	//pathのフォーマット例::"events/2023/03/12/2023-03-12T00:00:00_00:14:59.txt"
	//パスから日付&前半時間を取り出して数値化して比較
	for _, p := range paths {

		//JST<->UTC比較を行うための日時をパスから取得する
		splitPath := strings.Split(strings.Replace(p, midPath, "", 1), "/")
		var checkBucketPathTime int
		switch {
		case
			//logRouterを使って出力されたGCSバケットファイルフォーマットの場合(コンテント名完全一致)
			regM.MatchString(CONFIG.CONTENT_NAME_GYROSCOPE),
			regM.MatchString(CONFIG.CONTENT_NAME_FLIGHT_RECORD),
			regM.MatchString(CONFIG.CONTENT_NAME_GYROSCOPE),
			regM.MatchString(CONFIG.CONTENT_NAME_CHAINED_TAGS_LOGGING),
			regM.MatchString(CONFIG.CONTENT_NAME_MAU_COUNTER),
			regM.MatchString(CONFIG.CONTENT_NAME_TUGCAR_API_REQUEST_LOGGING):
			d := splitPath[0] + splitPath[1] + splitPath[2]
			t := splitPath[3]
			t = strings.Split(t, "_")[0]
			bt := regD.ReplaceAllString(d+t, "")
			checkBucketPathTime, _ = strconv.Atoi(bt)
		case
			//独自GCSバケットファイルフォーマット(gyroscope:Daily, Term)の場合(コンテント名前方一致)
			strings.Index(content, CONFIG.CONTENT_NAME_GYROSCOPET00_) == 0,
			strings.Index(content, CONFIG.CONTENT_NAME_GYROSCOPET15_) == 0,
			strings.Index(content, CONFIG.CONTENT_NAME_GYROSCOPET30_) == 0,
			strings.Index(content, CONFIG.CONTENT_NAME_GYROSCOPET45_) == 0:
			d := splitPath[0] + splitPath[1] + splitPath[2]
			t := strings.Split(splitPath[3], "T")[1]
			t = strings.Split(t, "_")[0]
			bt := regD.ReplaceAllString(d+t, "")
			checkBucketPathTime, _ = strconv.Atoi(bt)
		}

		//日時比較を行い、JST入力からUTC換算したファイルパスに絞り込む
		if startUtcDateTime <= checkBucketPathTime && checkBucketPathTime < endUtcDateTime {
			fmt.Println(p)
			pathsNew = append(pathsNew, p)
		}
	}

	return pathsNew
}

///////////////////////////////////////////////////
/* ===========================================
	BucketのオブジェクトをJST時間で選別する
=========================================== */
func pickUpBucketObjByTerm(paths []string, term string) []string {

	var pathsNew []string

	//pathのフォーマット例::"events/2023/03/12/2023-03-12T00:00:00_00:14:59.txt"
	//パスから日付&前半時間を取り出してTerm時間(分)が一致しているものを検索
	for _, p := range paths {
		convPath := strings.Split(p, "/")[4]
		convPath = strings.Split(convPath, "_")[0]
		pt := strings.Split(convPath, "T")[1]
		pt = strings.Split(pt, ":")[1]
		if pt == term {
			pathsNew = append(pathsNew, p)
		}
	}

	return pathsNew
}

///////////////////////////////////////////////////
/* ===========================================
	BucketのオブジェクトをSuffixで選別する
=========================================== */
func pickUpBucketObjBySuffix(paths []string, suffixRange []string) []string {

	var pathsNew []string

	//pathのサフィックスが一致するものだけに選別する
	//format:events/2023/03/19/2023-03-19T05:45:00_05:59:59_5.txt
	start, _ := strconv.Atoi(suffixRange[0])
	end, _ := strconv.Atoi(suffixRange[1])
	for i := start; i <= end; i++ {
		for _, p := range paths {
			sfx := strings.Split(p, "T")[1]
			sfx = strings.Split(sfx, "_")[2]
			sfx = strings.Replace(sfx, ".txt", "", 1)
			if strconv.Itoa(i) == sfx {
				pathsNew = append(pathsNew, p)
			}
		}
	}

	return pathsNew
}
